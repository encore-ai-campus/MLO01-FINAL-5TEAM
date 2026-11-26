"""Spark-managed state for the action classification query."""
import hashlib
import json

import event_schema as fields
from pyspark.sql import Row, functions as F
from pyspark.sql.streaming import StatefulProcessor

from refined import INVALID, rejection_reason


class ActionDecisions(StatefulProcessor):
    def __init__(self, allowed_lateness_ms):
        self.allowed_lateness_ms = allowed_lateness_ms

    def init(self, handle):
        self.seen = handle.getMapState('seen_event_ids', 'event_id string', 'digest string')
        self.users = handle.getMapState('user_order', 'user_id long', 'event_ms long, source string, seq long')
        self.clocks = handle.getMapState('partition_clock', 'partition int', 'event_ms long')

    def classify(self, event, event_ms, prior_max):
        event_id = (event[fields.EVENT_ID],)
        digest = hashlib.sha256(json.dumps(event, sort_keys=True).encode()).hexdigest()
        stored = self.seen.getValue(event_id)
        if stored is not None:
            return ('duplicate', None) if stored[0] == digest else (INVALID, 'conflicting_event_payload')
        self.seen.updateValue(event_id, (digest,))
        user = (event[fields.USER_ID],)
        order = (event_ms, event[fields.SOURCE_FILE], event[fields.SOURCE_ROW])
        previous = self.users.getValue(user)
        if event_ms < prior_max - self.allowed_lateness_ms or (previous is not None and order < previous):
            return 'late', None
        self.users.updateValue(user, order)
        return 'accepted', None

    def handleInputRows(self, key, rows, timerValues):
        partition_times = {}
        for row in sorted(rows, key=lambda item: (item.partition, item.offset)):
            event = dict(row.event or {})
            reason = rejection_reason(event, row.event_ms, row.key)
            status = INVALID
            if reason is None:
                partition = row.partition
                if partition not in partition_times:
                    stored = self.clocks.getValue((partition,))
                    partition_times[partition] = stored[0] if stored is not None else -(2**63)
                prior = partition_times[partition]
                status, reason = self.classify(event, row.event_ms, prior)
                if status != INVALID:
                    partition_times[partition] = max(prior, row.event_ms)
            yield Row(partition=row.partition, offset=row.offset, broker_ms=row.broker_ms, key=row.key,
                      status=status, rejection_reason=reason, event_json=json.dumps(event))
        for partition, timestamp in partition_times.items():
            self.clocks.updateValue((partition,), (timestamp,))

    def close(self):
        pass


def decisions(frame, run_id, allowed_lateness_ms):
    # A single grouping key preserves duplicate-ID conflict checks across Kafka partitions.
    # The individual event, user and clock records remain point lookups in RocksDB.
    return frame.withColumn('_replay_run', F.lit(run_id)).groupBy('_replay_run').transformWithState(
        ActionDecisions(allowed_lateness_ms),
        'partition int, offset long, broker_ms long, key string, status string, rejection_reason string, event_json string',
        'append', 'None')
