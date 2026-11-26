"""Join a team sample and the two authoritative label tables to purchase observations."""

import json
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation

from ml.conversion_model.data_prep.labels import pending_visit_reason
from ml.conversion_model.data_prep.visits import INACTIVITY
from ml.conversion_model.models.lightgbm.preprocessor import NUMERIC, REPEAT_VIEW, validate
from ml.conversion_model.models.logistic.features import sequence_inputs, utc_datetime
from ml.conversion_model.storage import read_json
from scripts.team_sample_manifest import Sample, load_samples


def comparison_inputs(values):
    validate(values, NUMERIC + (REPEAT_VIEW,))
    normalized = values.copy()
    for key in ('view_count', 'cart_count', 'distinct_product_count', REPEAT_VIEW):
        count = Decimal(str(values[key]))
        if count != count.to_integral_value():
            raise ValueError(f'Non-integer {key}')
        normalized[key] = int(count)
    return normalized


def read_jsonl(path):
    with path.open(encoding='utf-8-sig') as stream:
        for number, line in enumerate(stream, 1):
            row = json.loads(line)
            if not isinstance(row, dict):
                raise ValueError(f'Expected an object at {path.name}:{number}')
            yield row


def indexed(path, key):
    rows = {}
    for row in read_jsonl(path):
        normalize_snapshot_ids(row, path.name)
        value = row.get(key)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f'Expected a nonempty string {key} in {path.name}')
        if key == 'visit_id' and 'session_id' in row:
            value = (*record_scope(row), value)
        if value in rows:
            raise ValueError(f'Empty or duplicate {key} in {path.name}')
        rows[value] = row
    return rows


def normalize_snapshot_ids(row, filename):
    """Read the team's session identifier while preserving the internal row schema."""
    if 'session_id' not in row:
        if not isinstance(row.get('visit_id'), str) or not row['visit_id'].strip():
            raise ValueError(f'Expected session_id or legacy visit_id in {filename}')
        return
    session_id = row['session_id']
    source, run_id = record_scope(row)
    if source not in ('web', 'replay', 'loadtest'):
        raise ValueError(f'Expected source web, replay or loadtest in {filename}')
    if source == 'replay' and run_id is None:
        raise ValueError(f'Expected replay_run_id in {filename}')
    if not isinstance(session_id, str) or not session_id.strip():
        raise ValueError(f'Expected a nonempty string session_id in {filename}')
    if 'visit_id' in row and row['visit_id'] != session_id:
        raise ValueError(f'session_id and visit_id disagree in {filename}')
    for key in ('raw_session_id', 'user_id', 'replay_run_id', 'event_id'):
        value = row.get(key)
        if (key in row and (value is not None or key == 'event_id')
                and (not isinstance(value, str) or not value.strip())):
            raise ValueError(f'Expected a string {key} in {filename}')
    row['visit_id'] = session_id


def record_scope(row):
    return row.get('source'), row.get('replay_run_id')


def visit_group_id(visit):
    """Stable string for per-session metrics, including across separately loaded snapshots."""
    if 'session_id' in visit:
        return json.dumps([*record_scope(visit), visit['session_id']], ensure_ascii=False, separators=(',', ':'))
    return visit['visit_id']


def find_visit(visits, row):
    session = row['visit_id']
    return visits.get((*record_scope(row), session)) or visits.get(session)


def truth_user_matches(truth, observed):
    return truth is None or 'user_id' not in truth or truth['user_id'] == observed['user_id']


@dataclass
class TeamSnapshot:
    complete_before: datetime
    observation_start: datetime
    visits: dict
    truth: dict
    observations: dict
    late_visits: set
    input_version: str
    label_version: str
    scoped_late_visits: set = field(default_factory=set)
    offline_late_truth: set = field(default_factory=set)
    reconstructed_from_parquet: bool = False

    @classmethod
    def load(cls, folder):
        metadata = read_json(folder / 'snapshot.json')
        visits = indexed(folder / 'visits.jsonl', 'visit_id')
        truth = indexed(folder / 'truth.jsonl', 'prediction_id')
        observations = indexed(folder / 'observations.jsonl', 'prediction_id')
        late_visits = set()
        scoped_late_visits = set()
        offline_late_truth = set()
        for visit in visits.values():
            visit_id = visit['visit_id']
            late = visit.get('late_event', False)
            if type(late) is not bool:
                raise ValueError(f'Expected a boolean late_event for {visit_id}')
            if late:
                scope = record_scope(visit)
                if scope == (None, None):
                    late_visits.add(visit_id)
                else:
                    scoped_late_visits.add((*scope, visit_id))
        session_ids = {visit['visit_id'] for visit in visits.values()}
        for visit in visits.values():
            if ('session_id' not in visit
                    and (*record_scope(visit), visit['visit_id']) in visits):
                raise ValueError('Duplicate visit across legacy and team records in the same scope')
        for prediction_id, row in truth.items():
            visit_id = row['visit_id']
            if visit_id not in session_ids:
                raise ValueError(f'Unknown truth visit for {prediction_id}')
            if row.get('reason') == 'late_event':
                observed = observations.get(prediction_id)
                if observed is not None and observed['visit_id'] != visit_id:
                    raise ValueError(f'Visit identity mismatch for {prediction_id}')
                visit = find_visit(visits, row)
                if (observed is not None and visit is not None and truth_user_matches(row, observed)
                        and visit['user_id'] == observed['user_id']
                        and record_scope(row) == record_scope(observed) == record_scope(visit)):
                    scoped_late_visits.add((*record_scope(row), visit_id))
                    offline_late_truth.add((*record_scope(row), visit_id))
        complete_before = utc_datetime(metadata['complete_before_utc'])
        observation_start = utc_datetime(metadata['observation_start_utc'])
        if not observation_start < complete_before:
            raise ValueError('Invalid observation coverage')
        if any(not isinstance(metadata[key], str) or not metadata[key].strip()
               for key in ('input_version', 'label_version')):
            raise ValueError('Expected nonempty input and label rule versions')
        # Historical local Parquet reconstructions use source=test on all three tables.
        reconstructed = (metadata.get('origin') == 'local reconstruction from ordered raw events'
                         and isinstance(metadata.get('source_parquet_sha256'), str)
                         and re.fullmatch(r'[a-fA-F0-9]{64}', metadata['source_parquet_sha256']) is not None)
        return cls(complete_before, observation_start, visits, truth, observations, late_visits,
                   metadata['input_version'], metadata['label_version'], scoped_late_visits,
                   offline_late_truth, reconstructed)

    def is_late_visit(self, visit_id, scope):
        scoped_key = (*scope, visit_id)
        return (scoped_key in self.scoped_late_visits
                or (scoped_key not in self.visits and visit_id in self.late_visits))

    def rows(self, sample, start, end, *, scope=None, monitoring=False):
        """Rebuild counts and usable examples from the snapshot, including any new late exclusions."""
        if not self.observation_start <= start < end:
            raise ValueError('Invalid date window for the supplied observations')
        if monitoring and scope is None:
            raise ValueError('Monitoring requires an explicit source and run scope')
        cutoff = min(end, self.complete_before)
        rows, statuses, issues, excluded_visits = [], Counter(), Counter(), set()
        pending_visits, boundary_visits = set(), set()
        for prediction_id, observed in self.observations.items():
            observed_scope = record_scope(observed)
            if scope is not None and observed_scope != scope:
                continue
            user_id, visit_id = observed['user_id'], observed['visit_id']
            if user_id not in sample.users:
                continue
            event_time = utc_datetime(observed['event_time_utc'])
            if not start <= event_time < cutoff:
                continue
            if not monitoring:
                source, run_id = observed_scope
                local_replay = (self.reconstructed_from_parquet
                                and observed_scope == ('test', 'local-reconstructed-common-oct'))
                if source != 'replay' and not local_replay:
                    statuses['offline_source_excluded'] += 1
                    continue
                if not isinstance(run_id, str) or not run_id.strip():
                    statuses['missing_replay_run_id'] += 1
                    continue
            visit = find_visit(self.visits, observed)
            truth = self.truth.get(prediction_id)
            if visit is None or record_scope(visit) != observed_scope or (truth is not None and record_scope(truth) != observed_scope):
                statuses['source_or_run_mismatch'] += 1
                continue
            if visit['user_id'] != user_id or (truth is not None and truth['visit_id'] != visit_id):
                raise ValueError(f'Visit identity mismatch for {prediction_id}')
            if not truth_user_matches(truth, observed):
                statuses['truth_user_mismatch'] += 1
                continue
            group_id = visit_group_id(visit)
            label, status, reason = self.label(visit_id, visit, event_time, truth, start, cutoff,
                                               scope=observed_scope, offline=not monitoring)
            statuses[reason or status] += 1
            if status != 'confirmed':
                if reason == 'late_event':
                    excluded_visits.add(group_id)
                if status == 'pending':
                    pending_visits.add(group_id)
                if reason == 'crosses_window_boundary':
                    boundary_visits.add(group_id)
                continue
            try:
                actions = observed['actions']
                inputs = sequence_inputs(actions, event_time=observed['event_time_utc'],
                                         sequence=observed['sequence'], legacy_ids='session_id' not in observed)
            except (KeyError, ValueError, TypeError, OverflowError):
                inputs = None
                issues['logistic:invalid_actions'] += 1
            try:
                tabular = comparison_inputs(observed.get('comparison_inputs', {}))
            except (KeyError, ValueError, TypeError, OverflowError, InvalidOperation):
                tabular = None
                issues['lightgbm:invalid_comparison_inputs'] += 1
            rows.append({**(tabular or {}), **(inputs or {}), 'event_id': prediction_id, 'visit_id': group_id,
                         **({'session_id': visit_id} if 'session_id' in visit else {}),
                         'user_id': user_id, 'event_time_utc': event_time.isoformat(timespec='microseconds'),
                         'source_record': observed['sequence'] if inputs is not None else 0, 'label': label,
                         'label_status': 'confirmed', 'logistic_valid': inputs is not None,
                         'lightgbm_valid': tabular is not None, 'label_available_at_utc': truth['confirmed_at_utc']})
        return rows, {
            'status_counts': dict(statuses), 'model_input_issues': dict(issues),
            'late_excluded_visits': len(excluded_visits), 'pending_visits': len(pending_visits),
            'boundary_pending_visits': len(boundary_visits),
            # Older reports use this key for boundary visits omitted from the metrics.
            'boundary_excluded_visits': len(boundary_visits),
            'eligible_users': len({row['user_id'] for row in rows}),
        }

    def label(self, visit_id, visit, event_time, truth, start, cutoff, *, scope=(None, None), offline=False):
        if offline:
            late = visit.get('late_event', False) or (*scope, visit_id) in self.offline_late_truth
        else:
            late = self.is_late_visit(visit_id, scope)
        if late:
            return None, 'excluded', 'late_event'
        started = utc_datetime(visit['started_at_utc'])
        ended = utc_datetime(visit['ended_at_utc']) if visit.get('ended_at_utc') is not None else None
        purchase = utc_datetime(visit['first_purchase_at_utc']) if visit.get('first_purchase_at_utc') else None
        if started > event_time or (ended is not None and (ended <= event_time or ended <= started)):
            raise ValueError(f'Invalid times for visit {visit_id}')
        if purchase is not None and (purchase < started or (ended is not None and purchase >= ended)):
            raise ValueError(f'First purchase is outside visit {visit_id}')
        if purchase is not None and purchase <= event_time:
            return None, 'excluded', 'same_time_as_first_purchase' if purchase == event_time else 'after_first_purchase'
        if started <= self.observation_start + INACTIVITY:
            return None, 'excluded', 'insufficient_history'
        reason = pending_visit_reason(started, ended, start, cutoff, self.complete_before)
        if reason:
            return None, 'pending', reason
        if (truth is not None and truth['status'] in ('pending', 'excluded')
                and truth.get('reason') == 'crosses_window_boundary' and truth.get('label') is None):
            return None, 'pending', 'crosses_window_boundary'
        if truth is None or truth['status'] == 'pending':
            return None, 'pending', 'awaiting_truth'
        if truth['status'] == 'excluded' and truth.get('reason') and truth.get('label') is None:
            return None, 'excluded', truth['reason']
        label = int(purchase is not None)
        if truth['status'] != 'confirmed' or type(truth.get('label')) is not int or truth['label'] != label:
            raise ValueError(f'Prediction truth disagrees with visit result for {visit_id}')
        confirmed_at = truth.get('confirmed_at_utc')
        if confirmed_at is None:
            return None, 'pending', 'awaiting_truth'
        available = utc_datetime(confirmed_at)
        if available <= ended:
            raise ValueError(f'Truth confirmation must be after visit close for {visit_id}')
        if available >= cutoff:
            return None, 'pending', 'awaiting_truth'
        return label, 'confirmed', ''

    def pending_quality(self, sample, previous_cutoff, *, scope=None):
        if self.complete_before <= previous_cutoff:
            return {'status': 'watermark_not_advanced', 'overdue_visits': {}, 'unknown_activity_visits': 0}
        overdue, unknown = {}, set()
        for prediction_id, observed in self.observations.items():
            observed_scope = record_scope(observed)
            if scope is not None and observed_scope != scope:
                continue
            visit_id = observed['visit_id']
            if observed['user_id'] not in sample.users or self.is_late_visit(visit_id, observed_scope):
                continue
            truth = self.truth.get(prediction_id)
            visit = find_visit(self.visits, observed)
            if visit is None or (scope is not None and record_scope(visit) != observed_scope):
                continue
            if scope is not None and truth is not None and record_scope(truth) != observed_scope:
                truth = None
            if not truth_user_matches(truth, observed):
                truth = None
            if truth is not None:
                if (truth['status'] == 'excluded'
                        or (truth['status'] == 'pending' and truth.get('reason') == 'crosses_window_boundary')):
                    continue
                confirmed_at = truth.get('confirmed_at_utc')
                if (truth['status'] == 'confirmed' and confirmed_at is not None
                        and utc_datetime(confirmed_at) < self.complete_before):
                    continue
            event = utc_datetime(observed['event_time_utc'])
            if not self.observation_start <= event < self.complete_before:
                continue
            if visit['user_id'] != observed['user_id'] or (truth is not None and truth['visit_id'] != visit_id):
                raise ValueError(f'Visit identity mismatch for {prediction_id}')
            purchase = visit.get('first_purchase_at_utc')
            if purchase and utc_datetime(purchase) <= event:
                continue
            ended = visit.get('ended_at_utc')
            if ended is None:
                last = visit.get('last_event_at_utc')
                if not last:
                    unknown.add(visit_group_id(visit))
                    continue
                last_event = utc_datetime(last)
                if not event <= last_event < self.complete_before:
                    raise ValueError(f'Invalid last event time for open visit {visit_id}')
                expected_close = last_event + INACTIVITY
                reason = 'awaiting_visit_close'
            else:
                expected_close = utc_datetime(ended)
                reason = 'awaiting_truth'
            if self.complete_before - expected_close > timedelta(hours=24):
                overdue[visit_group_id(visit)] = reason
        status = 'alert' if overdue else 'activity_time_missing' if unknown else 'ok'
        return {'status': status, 'overdue_visits': overdue,
                'unknown_activity_visits': len(unknown)}
