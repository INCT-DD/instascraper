from __future__ import annotations

from datetime import date, time, timezone
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SRC))

from instagram_collector.pipeline import daily_window_for_timezone, explicit_window
from instagram_collector.jobs import JobProcessor
from instagram_collector.sessions import SessionPool
from instagram_scraper import AuthError


class DateWindowTests(unittest.TestCase):
    def test_explicit_window_uses_utc_day_boundaries(self) -> None:
        start, end = explicit_window(date(2026, 8, 6), date(2026, 8, 6))

        self.assertEqual(start.date(), date(2026, 8, 6))
        self.assertEqual(start.time(), time.min)
        self.assertEqual(start.tzinfo, timezone.utc)
        self.assertEqual(end.date(), date(2026, 8, 6))
        self.assertEqual(end.time().replace(microsecond=0), time(23, 59, 59))
        self.assertEqual(end.tzinfo, timezone.utc)

    def test_daily_window_converts_local_day_to_utc(self) -> None:
        start, end = daily_window_for_timezone(date(2026, 8, 6), 0, "America/Bahia")

        self.assertEqual(start.isoformat(), "2026-08-06T03:00:00+00:00")
        self.assertEqual(end.isoformat(), "2026-08-07T02:59:59+00:00")


class SessionPoolTests(unittest.TestCase):
    def test_rotation_disabled_always_uses_first_session(self) -> None:
        pool = SessionPool(
            [
                {"name": "first", "instagram_cookie_json": "first.json"},
                {"name": "second", "instagram_cookie_json": "second.json"},
            ],
            rotation_enabled=False,
        )

        self.assertEqual(pool.next().instagram_cookie_json, "first.json")
        self.assertEqual(pool.next().instagram_cookie_json, "first.json")
        self.assertEqual(pool.alternatives(), [])

    def test_rotation_enabled_cycles_and_lists_alternatives(self) -> None:
        pool = SessionPool(
            [
                {"name": "first", "instagram_cookie_json": "first.json"},
                {"name": "second", "instagram_cookie_json": "second.json"},
            ],
            rotation_enabled=True,
        )

        first = pool.next()
        second = pool.next()

        self.assertEqual(first.instagram_cookie_json, "first.json")
        self.assertEqual(second.instagram_cookie_json, "second.json")
        self.assertEqual([item.instagram_cookie_json for item in pool.alternatives(first)], ["second.json"])


class JobProcessorRotationTests(unittest.IsolatedAsyncioTestCase):
    async def test_job_processing_tries_alternative_session_after_auth_error(self) -> None:
        pool = SessionPool(
            [
                {"name": "bad", "instagram_cookie_json": "bad.json"},
                {"name": "good", "instagram_cookie_json": "good.json"},
            ],
            rotation_enabled=True,
        )
        settings = SimpleNamespace(
            rps=1.0,
            account_rotation_enabled=True,
            job_limit_per_run=100,
            comment_queue_time_limit_seconds=None,
        )
        processor = _FallbackJobProcessor(object(), settings, session_pool=pool)

        counts = await processor._process_job_with_sessions({"id": 1, "job_type": "comments"})

        self.assertEqual(counts, {"comments_inserted": 1})
        self.assertEqual(processor.seen_cookie_paths, ["bad.json", "good.json"])


class _FallbackJobProcessor(JobProcessor):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.seen_cookie_paths = []

    async def _process_job_with_session(self, session, job):
        self.seen_cookie_paths.append(session.instagram_cookie_json)
        if session.instagram_cookie_json == "bad.json":
            raise AuthError("expired cookie")
        return {"comments_inserted": 1}


if __name__ == "__main__":
    unittest.main()
