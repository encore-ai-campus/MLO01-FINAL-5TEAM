"""Replay events from a sorted Parquet file using a controllable event-time clock."""
import argparse
import json
import math
import sys
import threading
from collections import deque
from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from data06 import event_schema as fields


class ReplayClock:
    def __init__(self, start, speed, now=monotonic):
        self.now = now
        self.changed = threading.Condition()
        self.event_anchor = start
        self.wall_anchor = now()
        self.speed = self._valid_speed(speed)
        self.paused = False
        self.stopped = False

    @staticmethod
    def _valid_speed(speed):
        if not math.isfinite(speed) or speed <= 0:
            raise ValueError('Speed must be finite and positive')
        return speed

    def _position(self, now):
        return self.event_anchor if self.paused else self.event_anchor + (now - self.wall_anchor) * self.speed

    def _reanchor(self):
        now = self.now()
        self.event_anchor = self._position(now)
        self.wall_anchor = now

    def set_speed(self, speed):
        speed = self._valid_speed(speed)
        with self.changed:
            self._reanchor()
            self.speed = speed
            self.changed.notify_all()

    def set_paused(self, paused):
        with self.changed:
            if paused != self.paused:
                self._reanchor()
                self.paused = paused
                self.changed.notify_all()

    def stop(self):
        with self.changed:
            self.stopped = True
            self.changed.notify_all()

    def status(self):
        with self.changed:
            return {'replay_time': datetime.fromtimestamp(self._position(self.now()), timezone.utc).isoformat(),
                    'speed': self.speed, 'paused': self.paused, 'stopped': self.stopped}

    def emit_at(self, event_time, emit):
        with self.changed:
            while not self.stopped:
                if self.paused:
                    self.changed.wait()
                    continue
                remaining = (event_time - self._position(self.now())) / self.speed
                if remaining <= 0:
                    emit()
                    return True
                self.changed.wait(timeout=min(remaining, 60))
            return False


def events_in_order(source):
    import pyarrow.parquet as pq

    previous = None
    for batch in pq.ParquetFile(source).iter_batches(batch_size=8192):
        for event in batch.to_pylist():
            event_time = event[fields.EVENT_TIME]
            sequence = event[fields.SOURCE_ROW]
            if event_time is None or sequence is None or not event[fields.SOURCE_FILE]:
                raise ValueError(f"Missing replay order fields at {event[fields.EVENT_ID]}")
            key = (event_time, event[fields.SOURCE_FILE], sequence)
            if previous is not None and key <= previous:
                raise ValueError(f"Events are out of order or repeated at {event[fields.EVENT_ID]}")
            previous = key
            yield event


def log(message):
    print(json.dumps(message, ensure_ascii=False), file=sys.stderr, flush=True)


def control_from_stdin(clock, commands):
    for line in commands:
        parts = line.split()
        if not parts:
            continue
        try:
            command = parts[0].lower()
            if command == 'speed' and len(parts) == 2:
                clock.set_speed(float(parts[1]))
            elif command == 'pause' and len(parts) == 1:
                clock.set_paused(True)
            elif command == 'resume' and len(parts) == 1:
                clock.set_paused(False)
            elif command == 'quit' and len(parts) == 1:
                clock.stop()
            elif command != 'status' or len(parts) != 1:
                raise ValueError('Commands: speed <number>, pause, resume, status, quit')
            log({'command': command, **clock.status()})
            if command == 'quit':
                return
        except ValueError as error:
            log({'error': str(error)})
    clock.stop()


def replay(source, output, speed, max_events=None, start=None, end=None, interactive=False, *, sink=None):
    speed = ReplayClock._valid_speed(speed)
    if max_events is not None and max_events < 1:
        raise ValueError('Max events must be positive')
    if start is not None and end is not None and start >= end:
        raise ValueError('Start must be earlier than end')
    if (output is None) == (sink is None):
        raise ValueError('Choose exactly one output or event sink')
    clock = None
    count = 0
    output_context = Path(output).open('x', encoding='utf-8', newline='\n') if output is not None else nullcontext()
    with output_context as stream:
        for event in events_in_order(source):
            event_time = event[fields.EVENT_TIME]
            if start is not None and event_time < start:
                continue
            if end is not None and event_time >= end:
                break
            if clock is None:
                clock = ReplayClock(event_time.timestamp(), speed)
                log({'stage': 'started', **clock.status()})
                if interactive:
                    threading.Thread(target=control_from_stdin, args=(clock, sys.stdin), daemon=True).start()
            row = {**event, fields.EVENT_TIME: event_time.isoformat()}

            def emit():
                if sink is None:
                    stream.write(json.dumps(row, ensure_ascii=False) + '\n')
                    stream.flush()
                else:
                    sink(row)

            if not clock.emit_at(event_time.timestamp(), emit):
                break
            count += 1
            if max_events is not None and count >= max_events:
                break
    log({'stage': 'finished', 'emitted': count, **(clock.status() if clock else {})})
    return count


def utc_time(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError('Use a timezone-aware ISO timestamp')
    return parsed.astimezone(timezone.utc)


def replay_to_kafka(args):
    from confluent_kafka import KafkaException, Producer

    if args.kafka_in_flight < 1:
        raise ValueError('Kafka in-flight queue must be positive')
    producer = Producer({'bootstrap.servers': args.kafka_bootstrap, 'acks': 'all', 'enable.idempotence': True,
                         'partitioner': 'murmur2_random',
                         'linger.ms': 5 if args.kafka_in_flight > 1 else 0, 'delivery.timeout.ms': 30000})
    pending = deque()

    def await_delivery():
        result = pending.popleft()
        while not result:
            producer.poll(.2)
        if result[0] is not None:
            raise KafkaException(result[0])

    def send(event):
        # Match Java/Spark's keyed partitioner so changing clients does not move previously routed users.
        user_key = getattr(args, 'kafka_user_key', False)
        key = str(event[fields.USER_ID]) if user_key else event[fields.EVENT_ID]
        result = []
        producer.produce(args.kafka_topic, key=key.encode('utf-8'), partition=-1 if user_key else 0,
                         value=json.dumps(event, ensure_ascii=False).encode('utf-8'),
                         on_delivery=lambda error, _: result.append(error))
        pending.append(result)
        if len(pending) >= args.kafka_in_flight:
            await_delivery()

    try:
        count = replay(args.source, None, args.speed, args.max_events, args.start, args.end,
                       args.interactive, sink=send)
        while pending:
            await_delivery()
        log({'stage': 'kafka_acknowledged', 'emitted': count})
        return count
    finally:
        producer.flush(30)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    destination = parser.add_mutually_exclusive_group(required=True)
    destination.add_argument('--output', type=Path)
    destination.add_argument('--kafka-bootstrap')
    parser.add_argument('--kafka-topic')
    parser.add_argument('--kafka-in-flight', type=int, default=1)
    parser.add_argument('--kafka-user-key', action='store_true', help='Partition action events by user ID')
    parser.add_argument('--speed', type=float, default=1.0)
    parser.add_argument('--max-events', type=int)
    parser.add_argument('--start', type=utc_time)
    parser.add_argument('--end', type=utc_time)
    parser.add_argument('--interactive', action='store_true')
    args = parser.parse_args()
    if args.kafka_bootstrap:
        if not args.kafka_topic:
            parser.error('--kafka-bootstrap requires --kafka-topic')
        replay_to_kafka(args)
    else:
        replay(args.source, args.output, args.speed, args.max_events, args.start, args.end, args.interactive)
