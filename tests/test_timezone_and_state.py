import unittest

from cninfo_models import publish_date_from_ms
from crawl_state import advance_backfill_if_success


class TimezoneTest(unittest.TestCase):
    def test_publish_date_uses_beijing_time_independent_of_runner_timezone(self):
        self.assertEqual(publish_date_from_ms(1782230400000), "2026-06-24")
        self.assertEqual(publish_date_from_ms(1782303488000), "2026-06-24")


class BackfillStateTest(unittest.TestCase):
    def test_backfill_advances_past_target_start_after_final_batch(self):
        state = {
            "backfill": {
                "target_start_date": "2026-06-24",
                "next_end_date": "2026-06-24",
                "completed_ranges": [],
                "failed_dates": [],
            },
            "date_status": {
                "2026-06-24": {"status": "completed"},
            },
        }

        advance_backfill_if_success(state, ["2026-06-24"])

        self.assertEqual(state["backfill"]["next_end_date"], "2026-06-23")
        self.assertEqual(state["backfill"]["completed_ranges"], [{"start": "2026-06-24", "end": "2026-06-24"}])

    def test_backfill_only_marks_attempted_failed_dates(self):
        state = {
            "backfill": {
                "target_start_date": "2026-06-23",
                "next_end_date": "2026-06-24",
                "completed_ranges": [],
                "failed_dates": [],
            },
            "date_status": {
                "2026-06-24": {"status": "completed"},
            },
        }

        advance_backfill_if_success(state, ["2026-06-24"])

        self.assertEqual(state["backfill"]["failed_dates"], [])
        self.assertEqual(state["backfill"]["next_end_date"], "2026-06-23")


if __name__ == "__main__":
    unittest.main()
