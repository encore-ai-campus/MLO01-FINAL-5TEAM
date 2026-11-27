"""Kafka operations shared by the local dashboard and verification tools."""
from contextlib import closing, contextmanager
from time import monotonic, sleep

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient


def open_consumer(bootstrap):
    return Consumer({
        'bootstrap.servers': bootstrap,
        'group.id': 'data06-tool-read-only',
        'enable.auto.commit': False,
        'enable.auto.offset.store': False,
        'allow.auto.create.topics': False,
        'auto.offset.reset': 'error',
        'enable.partition.eof': True,
    })


def topic_metadata(client, topic, *, timeout=5):
    metadata = client.list_topics(timeout=timeout).topics.get(topic)
    if metadata is None:
        raise LookupError(f'Kafka topic does not exist: {topic}')
    if metadata.error is not None:
        raise KafkaException(metadata.error)
    return metadata


def topic_bounds(client, topic, *, timeout=5):
    deadline = monotonic() + timeout
    while True:
        try:
            metadata = topic_metadata(client, topic, timeout=max(0.01, deadline - monotonic()))
            return {
                str(partition): list(client.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=max(0.01, deadline - monotonic())))
                for partition in sorted(metadata.partitions)
            }
        except KafkaException as exc:
            # Topic creation and leader moves can briefly leave stale partition metadata.
            if exc.args[0].code() not in (
                KafkaError.LEADER_NOT_AVAILABLE, KafkaError.NOT_LEADER_FOR_PARTITION,
            ) or monotonic() >= deadline:
                raise
            sleep(min(0.1, max(0, deadline - monotonic())))


def snapshot_records(client, topic, bounds, *, timeout, tail=None):
    """Yield records between captured low and high offsets, or raise on timeout."""
    ends = {int(partition): end for partition, (start, end) in bounds.items() if end > start}
    if not ends:
        return
    starts = []
    for partition, end in ends.items():
        start = bounds[str(partition)][0]
        offset = start if tail is None else max(start, end - tail)
        starts.append(TopicPartition(topic, partition, offset))
    client.assign(starts)
    deadline = monotonic() + timeout
    while ends:
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError(f'Kafka snapshot read timed out for {topic}: {ends}')
        for record in client.consume(num_messages=500, timeout=min(remaining, 0.2)):
            partition = record.partition()
            if partition not in ends:
                continue
            end = ends[partition]
            error = record.error()
            if error is not None:
                if error.code() != KafkaError._PARTITION_EOF:
                    raise KafkaException(error)
                if record.offset() >= end:
                    del ends[partition]
                continue
            if record.offset() < end:
                yield record
            if record.offset() + 1 >= end:
                del ends[partition]


def read_snapshot(bootstrap, topic, *, timeout):
    with closing(open_consumer(bootstrap)) as client:
        yield from snapshot_records(client, topic, topic_bounds(client, topic), timeout=timeout)


def create_topics(bootstrap, topics):
    admin = AdminClient({'bootstrap.servers': bootstrap})
    pending = admin.create_topics(topics, operation_timeout=15, request_timeout=20)
    for result in pending.values():
        result.result(timeout=22)


def open_producer(bootstrap):
    return Producer({
        'bootstrap.servers': bootstrap,
        'acks': 'all',
        'enable.idempotence': True,
        'allow.auto.create.topics': False,
        'partitioner': 'murmur2_random',
        'message.timeout.ms': 30000,
    })


@contextmanager
def acknowledged_producer(bootstrap):
    producer = open_producer(bootstrap)
    try:
        yield producer
    finally:
        producer.purge()


def send_record(producer, topic, *, key, value, timeout, partition=-1, timestamp_ms=None):
    delivery = []

    def on_delivery(error, message):
        delivery.append((error, message))

    options = {} if timestamp_ms is None else {'timestamp': timestamp_ms}
    producer.produce(topic, value=value, key=key, partition=partition,
                     on_delivery=on_delivery, **options)
    deadline = monotonic() + timeout
    while not delivery:
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError(f'Kafka did not acknowledge a record for {topic}')
        producer.poll(min(remaining, 0.2))
    error, message = delivery[0]
    if error is not None:
        raise KafkaException(error)
    return message
