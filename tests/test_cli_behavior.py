from __future__ import annotations

from contextlib import ExitStack, redirect_stderr, redirect_stdout
from dataclasses import replace
from datetime import datetime, timezone
import io
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import instagram_scraper
from instagram_collector import cli, config
from instagram_collector.files import story_media_directories
from instagram_collector.jobs import JobStats
from instagram_collector.media_jobs import MediaJobProcessor, MediaJobStats
from instagram_collector.sessions import SessionPool
from instagram_collector.storage import Database


class CliBehaviorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        with patch.dict(os.environ, {}, clear=True):
            self.settings = config.load_settings(env_file=None)
        self.db = Mock()
        self.settings_loader = self.stack.enter_context(patch.object(cli, "load_settings", return_value=self.settings))
        self.db_factory = self.stack.enter_context(patch.object(cli, "Database", return_value=self.db))
        self.stack.enter_context(patch.object(cli, "configure_logging"))
        self.seed = self.stack.enter_context(patch.object(cli, "seed_profiles"))
        self.notifier = self.stack.enter_context(patch.object(cli, "send_report_notification", return_value=True))
        self.output = io.StringIO()
        self.stack.enter_context(redirect_stdout(self.output))

    def test_post_summary_and_exit_distinguish_partial_failures(self) -> None:
        results = [{"status": status} for status in ("success", "partial", "failed")]
        with patch.object(cli, "collect_posts_period", new=AsyncMock(return_value=results)) as collect:
            with self.assertRaises(SystemExit) as caught:
                cli.main(["collect-posts", "--start-date", "2026-08-29", "--end-date", "2026-08-31", "--no-comments", "--rps", "0.03"])
        self.assertEqual(caught.exception.code, 1)
        self.assertIn("1 succeeded, 1 partial, 1 failed", self.output.getvalue())
        self.assertFalse(collect.call_args.kwargs["enqueue_comments"])
        self.assertEqual(collect.call_args.kwargs["rps"], 0.03)
        self.db.close.assert_called_once()

    def test_successful_posts_exit_normally(self) -> None:
        with patch.object(cli, "collect_posts_period", new=AsyncMock(return_value=[{"status": "success"}])):
            self.assertIsNone(cli.main(["collect-posts", "--start-date", "2026-08-29", "--end-date", "2026-08-31"]))

    def test_daily_and_scheduled_report_failures_exit_nonzero(self) -> None:
        report = {"posts_found": 0, "stories_found": 0, "profiles_error": 2}
        for command in ("run-daily", "run-scheduled"):
            with self.subTest(command=command), patch.object(cli, "run_daily_collection", new=AsyncMock(return_value=report)):
                with self.assertRaises(SystemExit) as caught:
                    cli.main([command, "--date", "2026-09-06", "--skip-jobs"])
                self.assertEqual(caught.exception.code, 1)
        self.notifier.assert_called_once_with(report, force=False)

    def test_startup_failures_are_notified_and_original_error_survives(self) -> None:
        for stage in (self.settings_loader, self.db_factory, self.db.init_schema):
            with self.subTest(stage=stage):
                error = RuntimeError("startup failure")
                stage.side_effect = error
                self.notifier.reset_mock()
                try:
                    with self.assertRaises(RuntimeError) as caught:
                        cli.main(["run-scheduled", "--date", "2026-09-06", "--notify"])
                    self.assertIs(caught.exception, error)
                    self.notifier.assert_called_once()
                    self.assertEqual(self.notifier.call_args.args[0]["status"], "failed")
                finally:
                    stage.side_effect = None

    def test_no_notify_is_respected_on_database_failure(self) -> None:
        self.db_factory.side_effect = ConnectionError("offline")
        with self.assertRaises(ConnectionError):
            cli.main(["run-scheduled", "--no-notify"])
        self.notifier.assert_not_called()

    def test_notification_failure_does_not_mask_database_failure(self) -> None:
        error = ConnectionError("offline")
        self.db_factory.side_effect = error
        self.notifier.side_effect = RuntimeError("SMTP unavailable")
        with self.assertRaises(ConnectionError) as caught:
            cli.main(["run-scheduled", "--notify"])
        self.assertIs(caught.exception, error)
        self.assertIn("Notification failed", self.output.getvalue())

    def test_comment_runs_reflect_job_results(self) -> None:
        self.settings_loader.return_value = replace(self.settings, collect_comments_default=True)
        for stats, status in ((JobStats(failed=2), "failed"), (JobStats(processed=1, failed=1), "partial"), (JobStats(processed=2), "success")):
            with self.subTest(status=status):
                processor = Mock(process_pending_jobs=AsyncMock(return_value=stats))
                with patch.object(cli, "JobProcessor", return_value=processor):
                    if stats.failed:
                        with self.assertRaises(SystemExit) as caught:
                            cli.main(["process-comments-queue"])
                        self.assertEqual(caught.exception.code, 1)
                    else:
                        cli.main(["process-comments-queue"])
                self.assertEqual(self.db.finish_run.call_args.args[1], status)

    def test_media_failures_exit_nonzero_in_one_shot_mode(self) -> None:
        processor = Mock(process_pending_jobs=AsyncMock(return_value=MediaJobStats(failed=1)))
        with patch.object(cli, "MediaJobProcessor", return_value=processor):
            with self.assertRaises(SystemExit) as caught:
                cli.main(["process-media-queue"])
        self.assertEqual(caught.exception.code, 1)

    def test_refresh_failed_media_uses_inclusive_cli_date_range(self) -> None:
        stats = SimpleNamespace(posts_found=2, posts_refreshed=1, assets_refreshed=3, posts_failed=0)
        with patch.object(cli, "refresh_failed_post_media", new=AsyncMock(return_value=stats)) as refresh:
            cli.main(["refresh-failed-media", "--start-date", "2026-09-10", "--end-date", "2026-09-11"])
        refresh.assert_awaited_once_with(self.db, self.settings, "2026-09-10", "2026-09-12", None)
        self.assertIn("3 media URLs", self.output.getvalue())

    def test_profile_alias_propagates_post_and_comment_failures(self) -> None:
        self.settings_loader.return_value = replace(self.settings, collect_comments_default=True)
        processor = Mock(process_pending_jobs=AsyncMock(return_value=JobStats(failed=1)))
        with patch.object(cli, "collect_posts_period", new=AsyncMock(return_value=[{"status": "success"}])), patch.object(cli, "JobProcessor", return_value=processor):
            with self.assertRaises(SystemExit):
                cli.main(["collect-profile", "--username", "test", "--from", "2026-08-29", "--to", "2026-08-31", "--comments"])
        processor.process_pending_jobs.assert_awaited_once()

    def test_stories_are_enqueued_without_loading_cookies_when_queue_enabled(self) -> None:
        self.db.list_active_profiles.return_value = [{"id": 1, "username": "test", "priority": 4}]
        with patch.object(cli, "GalleryDlStoryCollector") as gallery, patch.object(cli, "load_sessions") as sessions:
            cli.main(["collect-stories", "--date", "2026-09-06"])
        self.db.enqueue_job.assert_called_once_with("stories", profile_id=1, cursor="2026-09-06", priority=104, max_attempts=3)
        gallery.assert_not_called()
        sessions.assert_not_called()

    def test_disabled_stories_do_not_enqueue(self) -> None:
        self.settings_loader.return_value = replace(self.settings, gallery_dl_enabled=False)
        cli.main(["collect-stories"])
        self.db.enqueue_job.assert_not_called()

    def test_direct_stories_persist_and_continue_after_errors(self) -> None:
        self.settings_loader.return_value = replace(self.settings, media_queue_enabled=False)
        self.db.list_active_profiles.return_value = [
            {"id": 1, "username": "first"}, {"id": 2, "username": "second"}, {"id": 3, "username": "third"}
        ]
        collector = Mock()
        collector.collect_profile_stories.side_effect = [
            SimpleNamespace(status="failed", error_message="expired cookies", output_dir="", files_found=0, session_alias="test"),
            RuntimeError("download failed"),
            SimpleNamespace(status="success", output_dir="fake", files_found=1, session_alias="test"),
        ]
        collector.load_story_metadata.return_value = [{"id": "story"}]
        self.db.upsert_story.return_value = (10, True)
        with patch.object(cli, "GalleryDlStoryCollector", return_value=collector), patch.object(cli, "load_sessions", return_value=[{"name": "test"}]):
            with self.assertRaises(SystemExit):
                cli.main(["collect-stories"])
        self.assertEqual(collector.collect_profile_stories.call_count, 3)
        self.db.upsert_story.assert_called_once_with(3, {"id": "story"})
        self.assertIn("expired cookies", self.output.getvalue())

    def test_default_day_uses_configured_timezone_but_explicit_day_is_preserved(self) -> None:
        instant = datetime(2026, 9, 7, 1, tzinfo=timezone.utc)
        self.db.list_active_profiles.return_value = [{"id": 1, "username": "test"}]
        with patch.object(config, "datetime") as clock:
            clock.now.side_effect = lambda tz: instant.astimezone(tz)
            cli.main(["collect-stories"])
            self.assertEqual(self.db.enqueue_job.call_args.kwargs["cursor"], "2026-09-06")
            cli.main(["collect-stories", "--date", "2026-09-07"])
            self.assertEqual(self.db.enqueue_job.call_args.kwargs["cursor"], "2026-09-07")

    def test_invalid_rps_is_rejected_before_database_access(self) -> None:
        for value in ("0", "-1", "nan", "inf"):
            with self.subTest(value=value), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as caught:
                    cli.main(["run-daily", "--rps", value])
                self.assertEqual(caught.exception.code, 2)
        self.db_factory.assert_not_called()

    def test_migrate_does_not_require_profiles_or_sessions(self) -> None:
        cli.main(["migrate"])
        self.db.migrate.assert_called_once()
        self.seed.assert_not_called()

    def test_cleanup_preserves_post_media_and_json_and_supports_dry_run(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.settings_loader.return_value = replace(
                self.settings, data_dir=str(root / "data"), story_media_dir=str(root / "exports" / "instagram")
            )
            story_files = [
                root / "data/raw/stories/old.jpg",
                root / "exports/instagram/Candidate/stories/old.jpg",
                root / "exports/instagram_06-09-26/Candidate/stories/today.jpg",
                root / "exports/instagram_29-08-26-to-31-08-26/Candidate/stories/past.jpg",
            ]
            keep_files = [
                root / "exports/instagram_06-09-26/Candidate/media/post.jpg",
                root / "exports/instagram_06-09-26/Candidate/06092026.json",
                root / "exports/instagram_backup/Candidate/stories/keep.jpg",
                root / "exports/other_06-09-26/Candidate/stories/keep.jpg",
            ]
            for path in [*story_files, *keep_files]:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"fixture")
            cli.main(["cleanup-secondary-data", "--story-media-files"])
            self.assertTrue(all(path.exists() for path in [*story_files, *keep_files]))
            cli.main(["cleanup-secondary-data", "--story-media-files", "--confirm"])
            self.assertTrue(all(not path.exists() for path in story_files))
            self.assertTrue(all(path.exists() for path in keep_files))
            self.db.cleanup_stories_data.assert_not_called()
            self.db.cleanup_comments_data.assert_not_called()


class StoryDirectorySafetyTests(unittest.TestCase):
    def test_cleanup_does_not_follow_link_to_external_directory(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            external = root / "external"
            external.mkdir()
            (external / "keep.jpg").write_bytes(b"fixture")
            candidate = root / "exports/instagram_06-09-26/Candidate"
            candidate.mkdir(parents=True)
            try:
                (candidate / "stories").symlink_to(external, target_is_directory=True)
            except OSError as exc:
                self.skipTest(f"Symlinks unavailable: {exc}")
            self.assertEqual(story_media_directories(str(root / "data"), str(root / "exports/instagram")), [])
            self.assertTrue((external / "keep.jpg").exists())


class RateLimiterTests(unittest.IsolatedAsyncioTestCase):
    async def test_low_rate_waits_requested_interval(self) -> None:
        limiter = instagram_scraper.RateLimiter(0.03)
        limiter._last = 100.0
        with patch.object(instagram_scraper.time, "perf_counter", return_value=100.0), patch.object(instagram_scraper.asyncio, "sleep", new=AsyncMock()) as sleep:
            await limiter.wait()
        self.assertAlmostEqual(sleep.call_args.args[0], 1 / 0.03)

    def test_invalid_rates_are_rejected_by_engine(self) -> None:
        for value in (0, -1, float("nan"), float("inf")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                instagram_scraper.RateLimiter(value)


class MediaPriorityTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.db = Database("sqlite:///:memory:")
        self.db.init_schema()
        self.addCleanup(self.db.close)
        pool = SessionPool([{"name": "test"}], rotation_enabled=False)
        settings = SimpleNamespace(job_limit_per_run=10, media_rate_limit_retry_seconds=300)
        self.processor = MediaJobProcessor(self.db, settings, session_pool=pool)

    def test_stories_precede_posts_regardless_of_numeric_priority(self) -> None:
        post_id = self.db.enqueue_job("post_media", None, cursor="post", priority=10000)
        story_id = self.db.enqueue_job("stories", None, cursor="story", priority=-10000)
        self.db.enqueue_job("comments", None, cursor="comment", priority=20000)
        jobs = self.db.fetch_pending_jobs(10, job_types=("stories", "post_media"))
        self.assertEqual([job["id"] for job in jobs], [story_id, post_id])
        remaining = self.db.fetch_pending_jobs(10, job_types=("stories", "post_media"), exclude_job_ids=(story_id,))
        self.assertEqual([job["id"] for job in remaining], [post_id])

    async def test_story_arriving_mid_cycle_precedes_remaining_posts(self) -> None:
        self.db.enqueue_job("post_media", None, cursor="first", priority=10)
        self.db.enqueue_job("post_media", None, cursor="second", priority=5)
        seen = []

        async def process(job):
            seen.append(job["cursor"])
            if job["cursor"] == "first":
                self.db.enqueue_job("stories", None, cursor="new-story", priority=-10)
            return {}

        with patch.object(self.processor, "_process_job", new=AsyncMock(side_effect=process)), patch("builtins.print"):
            stats = await self.processor.process_pending_jobs(3)
        self.assertEqual(seen, ["first", "new-story", "second"])
        self.assertEqual(stats.processed, 3)

    async def test_failed_story_is_not_retried_repeatedly_in_same_cycle(self) -> None:
        self.db.enqueue_job("stories", None, cursor="story", max_attempts=3)
        self.db.enqueue_job("post_media", None, cursor="post")
        seen = []

        async def process(job):
            seen.append(job["job_type"])
            if job["job_type"] == "stories":
                raise RuntimeError("temporary failure")
            return {}

        with patch.object(self.processor, "_process_job", new=AsyncMock(side_effect=process)), patch("builtins.print"):
            stats = await self.processor.process_pending_jobs(10)
        self.assertEqual(seen, ["stories", "post_media"])
        self.assertEqual((stats.failed, stats.processed), (1, 1))
        self.assertEqual(self.db.count_jobs_by_status(), {"retry": 1, "done": 1})

    async def test_rate_limited_story_is_scheduled_for_later(self) -> None:
        story_id = self.db.enqueue_job("stories", None, cursor="story", max_attempts=3)
        with patch.object(
            self.processor,
            "_process_job",
            new=AsyncMock(side_effect=RuntimeError("429 Too Many Requests")),
        ), patch("builtins.print"):
            await self.processor.process_pending_jobs(1)

        job = self.db._fetchone("SELECT status, scheduled_at FROM collection_jobs WHERE id = ?", (story_id,))
        self.assertEqual(job["status"], "retry")
        self.assertEqual(self.db.fetch_pending_jobs(1), [])
        self.assertGreater(datetime.fromisoformat(job["scheduled_at"]), datetime.now(timezone.utc))


if __name__ == "__main__":
    unittest.main()
