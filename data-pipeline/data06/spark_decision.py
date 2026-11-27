"""Spark-managed state for the action classification query."""
import hashlib
import json
from time import perf_counter_ns, time_ns

import event_schema as fields
from pyspark.sql import Row, functions as F
from pyspark.sql.streaming import StatefulProcessor
from pyspark.sql.types import LongType, StringType, StructField, StructType

from refined import INVALID, decision_group, decision_order, rejection_reason
from recent_actions import session_id_after


class ActionDecisions(StatefulProcessor):
    def __init__(self, allowed_lateness_ms):
        self.allowed_lateness_ms = allowed_lateness_ms

    def init(self, handle):
        self.seen = handle.getMapState('seen_event_ids', 'event_id string', 'digest string')
        self.last_order = handle.getValueState(
            'last_order', 'event_ms long, source string, seq long, partition int, offset long, session_id string')

    def classify(self, event, event_ms, prior_max, partition, offset):
        event_id = (event[fields.EVENT_ID],)
        digest = hashlib.sha256(json.dumps(event, sort_keys=True).encode()).hexdigest()
        stored = self.seen.getValue(event_id)
        if stored is not None:
            return ('duplicate', None, None) if stored[0] == digest else (INVALID, 'conflicting_event_payload', None)
        self.seen.updateValue(event_id, (digest,))
        order = decision_order(event, event_ms, partition, offset)
        previous = self.last_order.get()
        if ((prior_max > 0 and event_ms < prior_max - self.allowed_lateness_ms)
                or (previous is not None and order < previous[:-1])):
            return 'late', None, None
        session_id = session_id_after(previous[-1] if previous else None, previous[0] if previous else None,
                                      event[fields.EVENT_ID], event_ms)
        self.last_order.update((*order, session_id))
        return 'accepted', None, session_id

    def handleInputRows(self, key, rows, timerValues):
        prior_max = timerValues.getCurrentWatermarkInMs()
        for row in sorted(rows, key=lambda item: (item.partition, item.offset)):
            started = perf_counter_ns()
            event = dict(row.event or {})
            event.pop('_corrupt_record', None)
            reason = row.input_rejection
            status = INVALID
            session_id = None
            if reason is None:
                status, reason, session_id = self.classify(event, row.event_ms, prior_max, row.partition, row.offset)
            elapsed_us = (perf_counter_ns() - started) // 1000
            yield Row(partition=row.partition, offset=row.offset, broker_ms=row.broker_ms, key=row.key,
                      status=status, rejection_reason=reason, event_json=json.dumps(event), session_id=session_id,
                      classified_ms=time_ns() // 1_000_000, classify_us=elapsed_us)

    def close(self):
        pass


def decision_metadata(event, event_ms, key, run_id, partition, offset, wire_fields, live):
    payload = event.asDict() if event is not None else {}
    reason = rejection_reason(payload, event_ms, key, run_id,
                              wire_fields=set(wire_fields or ()), live=live)
    group = ('invalid', partition, offset) if reason else decision_group(payload, key, run_id)
    return json.dumps(group, separators=(',', ':')), event_ms if reason is None else None, reason


def decisions(frame, allowed_lateness_ms, run_id, live=False):
    # Only valid events advance the clock. ProcessingTime keeps late inputs visible.
    metadata = F.udf(decision_metadata, StructType([
        StructField('group', StringType()), StructField('valid_time_ms', LongType()),
        StructField('rejection_reason', StringType()),
    ]))
    timed = frame.withColumn('metadata', metadata('event', 'event_ms', 'key', F.lit(run_id), 'partition', 'offset',
                                                    F.json_object_keys('value'), F.lit(live)))
    timed = (timed.withColumn('clock_time', F.timestamp_millis('metadata.valid_time_ms'))
             .withColumn('decision_group', F.col('metadata.group'))
             .withColumn('input_rejection', F.col('metadata.rejection_reason')).drop('metadata'))
    return timed.withWatermark('clock_time', '0 seconds').groupBy('decision_group').transformWithState(
        ActionDecisions(allowed_lateness_ms),
        'partition int, offset long, broker_ms long, key string, status string, rejection_reason string, '
        'event_json string, session_id string, classified_ms long, classify_us long',
        'append', 'ProcessingTime')
