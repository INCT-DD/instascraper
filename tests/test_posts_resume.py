from datetime import datetime, timezone
from pathlib import Path
import sys
import unittest
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from instagram_collector import pipeline
from instagram_collector.cli import build_parser
from instagram_collector.storage import Database
from test_gallerydl_posts import settings


START = datetime(2026, 9, 1, tzinfo=timezone.utc)
END = datetime(2026, 9, 8, 23, 59, 59, tzinfo=timezone.utc)


class PostsResumeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.db = Database("sqlite:///:memory:")
        self.addCleanup(self.db.close)
        self.db.init_schema()
        self.db.seed_profiles([{"username": name, "name": name} for name in ("alpha", "beta", "gamma")], False, False)

    def attempt(self, name, status, start=START):
        profile = self.db.get_profile_by_username(name)
        run = self.db.start_run(profile["id"], "profile_posts", start, END)
        if status != "running":
            self.db.finish_run(run, status, posts_found=0)
        return run

    def names(self):
        return [p["username"] for p in self.db.list_incomplete_profiles_for_posts(START, END)]

    def test_skips_success_even_empty_and_starts_with_last_failure(self):
        self.attempt("alpha", "success")
        self.attempt("beta", "failed")
        self.assertEqual(self.names(), ["beta", "gamma"])

    def test_latest_attempt_wins_and_interrupted_profile_is_retried(self):
        self.attempt("alpha", "success")
        self.attempt("alpha", "failed")
        self.attempt("beta", "running")
        self.assertEqual(self.names(), ["beta", "alpha", "gamma"])
        self.attempt("alpha", "success")
        self.assertEqual(self.names(), ["beta", "gamma"])

    def test_other_period_success_does_not_skip_current_period(self):
        self.attempt("alpha", "success", datetime(2026, 9, 2, tzinfo=timezone.utc))
        self.assertEqual(self.names(), ["alpha", "beta", "gamma"])

    def test_failed_only_uses_latest_status_and_excludes_running_and_unattempted(self):
        self.attempt("alpha", "failed")
        self.attempt("alpha", "success")
        self.attempt("beta", "failed")
        self.assertEqual([p["username"] for p in self.db.list_incomplete_profiles_for_posts(START, END, failed_only=True)], ["beta"])
        self.attempt("beta", "running")
        self.assertEqual(self.db.list_incomplete_profiles_for_posts(START, END, failed_only=True), [])

    async def run_period(self, resume, username=None):
        calls = []

        async def collect(db, configured, name, *args, **kwargs):
            calls.append(name)
            self.attempt(name, "success")
            return pipeline.PostCollectionAttempt(pipeline.ProfileCollectionStats(), "fixture", [])

        with patch.object(pipeline, "seed_profiles"), \
             patch.object(pipeline, "load_sessions", return_value=[{"name": "fixture"}]), \
             patch.object(pipeline, "_collect_profile_posts_with_sessions", new=AsyncMock(side_effect=collect)):
            await pipeline.collect_posts_period(self.db, settings(), START, END, username=username, resume=resume)
        return calls

    async def test_resume_retries_failure_then_unattempted_and_can_finish(self):
        self.attempt("alpha", "success")
        self.attempt("beta", "failed")
        self.assertEqual(await self.run_period(True), ["beta", "gamma"])
        self.assertEqual(await self.run_period(True), [])

    async def test_default_still_refreshes_completed_profiles(self):
        self.attempt("alpha", "success")
        self.assertEqual(await self.run_period(False), ["alpha", "beta", "gamma"])

    async def test_resume_respects_username(self):
        self.attempt("beta", "failed")
        self.assertEqual(await self.run_period(True, "@beta"), ["beta"])

    def test_cli_resume_is_opt_in(self):
        args = ["collect-posts", "--start-date", "2026-09-01", "--end-date", "2026-09-08"]
        self.assertFalse(build_parser().parse_args(args).resume)
        self.assertTrue(build_parser().parse_args(args + ["--resume"]).resume)


if __name__ == "__main__":
    unittest.main()
