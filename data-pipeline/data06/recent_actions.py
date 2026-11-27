"""The 19-2 action list and its broker-confirmed online write."""
import hashlib
import json
from datetime import datetime, timezone
from importlib import import_module

import event_schema as fields
from redis_contract import parse_time


ACTION_TYPES = ('view', 'cart')
SESSION_GAP_MS = 30 * 60 * 1000


def session_id_after(session_id, last_ms, event_id, event_ms):
    if last_ms is None or event_ms - last_ms > SESSION_GAP_MS:
        return event_id
    return session_id


def format_time(value):
    return parse_time(value).astimezone(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')


def session_keys(session_id):
    key = f'feat:session:{session_id}'
    return key, key + ':actions'


def advance_actions(actions, *, event_id, product_id, event_type, sequence, event_time):
    if event_type not in ACTION_TYPES:
        return actions
    action = {'seq': actions[-1]['seq'] + 1 if actions else 1, 'sequence': sequence,
              'event_id': event_id, 'product_id': str(product_id), 'event_type': event_type,
              'event_time_utc': format_time(event_time)}
    return [*actions, action][-5:]


def apply_list_event(actions, event):
    sequence = getattr(event, 'sequence', None)
    if sequence is None:
        sequence = getattr(event, fields.SOURCE_ROW)
    return advance_actions(actions, event_id=getattr(event, fields.EVENT_ID),
                           product_id=getattr(event, fields.PRODUCT_ID),
                           event_type=getattr(event, fields.EVENT_TYPE), sequence=sequence,
                           event_time=getattr(event, fields.EVENT_TIME))


def replay_sample(session_id, permille):
    digest = hashlib.sha256(session_id.encode('utf-8')).digest()
    return int.from_bytes(digest[:8], 'big') % 1000 < permille


def restore_session_lists(client, run_id, users, ttl):
    snapshots = []
    for state in users.values():
        session_id = state['features']['session_start_event_id']
        key, list_key = session_keys(session_id)
        if client.hget(key, 'run_id') not in (None, run_id):
            raise ValueError('Use a separate Redis database for each replay')
        actions = []
        purchased = False
        for row in state['history']:
            if row['session_start_event_id'] == session_id:
                actions = advance_actions(actions, event_id=row['event_id'], product_id=row['product_id'],
                                          event_type=row['event_type'], sequence=row['event_seq'],
                                          event_time=row['event_time'])
                purchased |= row['event_type'] == 'purchase'
        state['session_purchased'] = purchased
        state['recent_action_seq'] = actions[-1]['seq'] if actions else 0
        partition, offset = state['origin_partition'], state['origin_offset']
        revision = f'{run_id}:{partition}:{offset}' if offset is not None else ''
        snapshots.append((key, list_key, state, actions, revision))

    # The recovery caller pauses the live writers and replaces the user namespace separately.
    previous = [key for key in client.scan_iter(match='feat:session:*', _type='hash')
                if client.hget(key, 'run_id') == run_id]
    seconds, microseconds = client.time()
    expires_ms = seconds * 1000 + microseconds // 1000 + ttl * 1000
    with client.pipeline(transaction=True) as pipe:
        for key in previous:
            pipe.delete(key, key + ':actions')
        for key, list_key, state, actions, revision in snapshots:
            pipe.hset(key, mapping={'run_id': run_id, 'features': json.dumps(state['features']),
                                    'feature_revision': revision, 'origin_partition': str(state['origin_partition']),
                                    'origin_offset': str(state['origin_offset']),
                                    'purchased': int(state['session_purchased'])})
            pipe.pexpireat(key, expires_ms)
            pipe.delete(list_key)
            if actions:
                pipe.rpush(list_key, *(json.dumps(action) for action in actions))
                pipe.pexpireat(list_key, expires_ms)
        pipe.execute()


class ConversionRequests:
    def __init__(self, args):
        from confluent_kafka import Producer
        from tool_kafka import send_record

        module, function = args.prediction_input_calculator.rsplit(':', 1)
        self.calculate = getattr(import_module(module), function)
        self.producer = Producer({'bootstrap.servers': args.kafka, 'acks': 'all',
                                  'enable.idempotence': True, 'allow.auto.create.topics': False})
        self.send = send_record
        self.topic = args.prediction_topic
        self.version = args.prediction_input_version
        self.permille = args.replay_prediction_permille
        self.ttl = args.session_safety_ttl_seconds

    def publish(self, run_id, event, session_id, actions):
        source = getattr(event, 'origin_type', None) or 'replay'
        if source == 'replay' and not replay_sample(session_id, self.permille):
            return
        current = actions[-1]
        inputs = self.calculate(actions)
        if not isinstance(inputs, dict) or len(inputs) != 6:
            raise ValueError('Prediction input calculator must return six named inputs')
        user = getattr(event, fields.USER_ID)
        raw_session = getattr(event, 'raw_session_id', None) or getattr(event, fields.USER_SESSION, None)
        message = {
            'schema_version': '1', 'source': source, 'replay_run_id': run_id,
            'event_id': current['event_id'], 'event_type': current['event_type'], 'sequence': current['sequence'],
            'event_time_utc': current['event_time_utc'],
            'replay_time_utc': format_time(getattr(event, 'replay_time_utc', None) or current['event_time_utc']),
            'user_id': str(user) if user is not None else None, 'raw_session_id': raw_session,
            'session_id': session_id, 'input_event_id': current['event_id'],
            'input_time_utc': current['event_time_utc'], 'input_version': self.version, 'inputs': inputs,
            'feature_revision': f'{run_id}:{event.origin_partition}:{event.origin_offset}',
            'produced_at_utc': datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
        }
        key = str(user) if source == 'replay' else raw_session
        self.send(self.producer, self.topic, key=key.encode(), value=json.dumps(message, allow_nan=False).encode(),
                  timeout=30)
