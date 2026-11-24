import unittest

from reco.replay_parquet import ReplayClock


class ReplayClockTests(unittest.TestCase):
    def test_pause_resume_and_speed_preserve_event_clock(self):
        wall = [10.0]
        clock = ReplayClock(0, 2, now=lambda: wall[0])
        wall[0] = 12.0
        clock.set_paused(True)
        paused_time = clock.status()['replay_time']
        wall[0] = 20.0
        clock.set_speed(4)
        self.assertEqual(clock.status()['replay_time'], paused_time)
        clock.set_paused(False)
        self.assertEqual(clock.status()['replay_time'], paused_time)
        wall[0] = 21.0
        self.assertEqual(clock.status()['replay_time'], '1970-01-01T00:00:08+00:00')
        clock.set_speed(0.5)
        wall[0] = 23.0
        self.assertEqual(clock.status()['replay_time'], '1970-01-01T00:00:09+00:00')

    def test_stop_and_invalid_speed(self):
        clock = ReplayClock(0, 1)
        with self.assertRaises(ValueError):
            clock.set_speed(float('nan'))
        clock.stop()
        self.assertFalse(clock.emit_at(0, lambda: self.fail('Stopped replay emitted an event')))
