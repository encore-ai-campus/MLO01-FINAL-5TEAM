import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from data06.event_schema import ACTION_FIELDS
from reco.convert_parquet import convert
from reco.replay_parquet import replay


class EventSchemaTests(unittest.TestCase):
    def test_conversion_and_replay_preserve_timestamp_and_field_types(self):
        with tempfile.TemporaryDirectory() as folder:
            source = Path(folder) / 'fixture.csv'
            output = Path(folder) / 'fixture.parquet'
            source.write_text(
                'event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session\n'
                '2019-11-01 00:00:00 UTC,view,1001,7,,brand,12.5,42,session\n',
                encoding='utf-8',
            )
            convert(source, output)
            schema = pq.read_schema(output)
            self.assertEqual(schema.names, [name for name, _ in ACTION_FIELDS])
            self.assertEqual(schema.field('event_time').type, pa.timestamp('ms', tz='UTC'))
            self.assertEqual(schema.field('product_id').type, pa.int64())
            self.assertEqual(schema.field('price').type, pa.float64())
            events = []
            self.assertEqual(replay(output, None, 1, sink=events.append), 1)
            self.assertEqual(events[0]['event_id'], 'fixture.csv:0')
            self.assertEqual(events[0]['event_time'], '2019-11-01T00:00:00+00:00')
            self.assertIsNone(events[0]['category_code'])
