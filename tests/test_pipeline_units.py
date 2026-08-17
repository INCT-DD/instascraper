from __future__ import annotations

from datetime import date, time, timezone
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SRC))

import instagram_scraper
import instagram_collector.pipeline as pipeline_module
from instagram_collector.config import load_profiles
from instagram_collector.files import archive_filename
from instagram_collector.pipeline import (
    PostCollectionAttempt,
    ProfileCollectionStats,
    collect_posts_period,
    daily_window_for_timezone,
    explicit_window,
)
from instagram_collector.jobs import JobProcessor
from instagram_collector.media_jobs import JOB_TYPE_POST_MEDIA, JOB_TYPE_STORIES
from instagram_collector.sessions import SessionPool
from instagram_collector.storage import Database
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


class ProfileConfigTests(unittest.TestCase):
    def test_load_profiles_accepts_csv_with_instagram_url(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "candidatos.csv"
            path.write_bytes(
                (
                    "Nome;Estado;Partido;Sigla;Links das Redes Sociais do Candidato;;;\n"
                    ";;;;X;Facebook;Instagram\n"
                    "Candidata Teste;Acre;Partido dos Trabalhadores;PT;;;"
                    "https://www.instagram.com/candidata.teste/\n"
                ).encode("cp1252")
            )

            profiles = load_profiles(str(path))

        self.assertEqual(profiles[0]["name"], "Candidata Teste")
        self.assertEqual(profiles[0]["username"], "candidata.teste")
        self.assertNotIn("collect_comments", profiles[0])


class ArchiveFileTests(unittest.TestCase):
    def test_archive_filename_uses_day_month_year(self) -> None:
        self.assertEqual(archive_filename(date(2026, 8, 16), date(2026, 8, 16)), "16082026.json")
        self.assertEqual(archive_filename(date(2026, 8, 14), date(2026, 8, 16)), "14082026_16082026.json")


class PostMediaParsingTests(unittest.TestCase):
    def test_normalize_v1_item_extracts_carousel_media_assets(self) -> None:
        item = {
            "code": "ABC",
            "pk": "1",
            "taken_at": 1,
            "media_type": 8,
            "carousel_media": [
                {
                    "pk": "1_1",
                    "media_type": 1,
                    "image_versions2": {"candidates": [{"url": "https://cdn/img-small.jpg", "width": 10, "height": 10}]},
                },
                {
                    "pk": "1_2",
                    "media_type": 2,
                    "video_versions": [{"url": "https://cdn/video.mp4", "width": 1920, "height": 1080}],
                },
            ],
        }

        normalized = instagram_scraper._normalize_v1_item(item)

        self.assertEqual([asset["media_type"] for asset in normalized["media_assets"]], ["image", "video"])
        self.assertEqual(normalized["media_assets"][1]["url"], "https://cdn/video.mp4")


class CommentDisableTests(unittest.IsolatedAsyncioTestCase):
    async def test_collect_posts_period_respects_global_comment_disable(self) -> None:
        seen = {}

        async def fake_collect(*args, **kwargs):
            seen["enqueue_comments"] = kwargs["enqueue_comments"]
            return PostCollectionAttempt(ProfileCollectionStats(posts_found=1), "default", [])

        db = SimpleNamespace(
            get_profile_by_username=lambda username: {"id": 1, "username": username, "name": username},
            list_active_profiles=lambda: [{"id": 1, "username": "teste", "name": "Teste"}],
        )
        settings = SimpleNamespace(
            collect_comments_default=False,
            account_rotation_enabled=False,
        )

        with (
            patch.object(pipeline_module, "seed_profiles", lambda *_: None),
            patch.object(pipeline_module, "load_sessions", lambda *_: [{"name": "default"}]),
            patch.object(pipeline_module, "_collect_profile_posts_with_sessions", fake_collect),
        ):
            await collect_posts_period(
                db,
                settings,
                *explicit_window(date(2026, 8, 16), date(2026, 8, 16)),
                enqueue_comments=True,
            )

        self.assertFalse(seen["enqueue_comments"])


class CollectionJobQueueTests(unittest.TestCase):
    def test_fetch_pending_jobs_can_filter_by_type(self) -> None:
        with TemporaryDirectory() as tmp:
            db = Database(f"sqlite:///{Path(tmp) / 'collector.db'}")
            db.init_schema()
            db.seed_profiles(
                [{"name": "Teste", "username": "teste"}],
                collect_comments_default=False,
                collect_replies_default=False,
            )
            profile = db.get_profile_by_username("teste")
            db.enqueue_job("comments", profile["id"], shortcode="ABC")
            db.enqueue_job(JOB_TYPE_STORIES, profile["id"], cursor="2026-08-16", priority=100)

            jobs = db.fetch_pending_jobs(10, job_types=(JOB_TYPE_STORIES, JOB_TYPE_POST_MEDIA))
            db.close()

        self.assertEqual([job["job_type"] for job in jobs], [JOB_TYPE_STORIES])

    def test_story_jobs_are_unique_per_profile_and_date(self) -> None:
        with TemporaryDirectory() as tmp:
            db = Database(f"sqlite:///{Path(tmp) / 'collector.db'}")
            db.init_schema()
            db.seed_profiles(
                [{"name": "Teste", "username": "teste"}],
                collect_comments_default=False,
                collect_replies_default=False,
            )
            profile = db.get_profile_by_username("teste")
            first = db.enqueue_job(JOB_TYPE_STORIES, profile["id"], cursor="2026-08-16")
            second = db.enqueue_job(JOB_TYPE_STORIES, profile["id"], cursor="2026-08-16")
            third = db.enqueue_job(JOB_TYPE_STORIES, profile["id"], cursor="2026-08-17")
            db.close()

        self.assertIsNotNone(first)
        self.assertIsNone(second)
        self.assertIsNotNone(third)

    def test_daily_retry_lists_only_profiles_without_successful_status(self) -> None:
        with TemporaryDirectory() as tmp:
            db = Database(f"sqlite:///{Path(tmp) / 'collector.db'}")
            db.init_schema()
            db.seed_profiles(
                [
                    {"name": "Ok", "username": "ok"},
                    {"name": "Fail", "username": "fail"},
                    {"name": "Missing", "username": "missing"},
                ],
                collect_comments_default=False,
                collect_replies_default=False,
            )
            date_from, date_to = explicit_window(date(2026, 8, 16), date(2026, 8, 16))
            run_id = db.start_run(None, "daily", date_from, date_to)
            ok = db.get_profile_by_username("ok")
            fail = db.get_profile_by_username("fail")
            db.record_profile_status(run_id, ok["id"], "ok", "daily", "", "success")
            db.record_profile_status(run_id, fail["id"], "fail", "daily", "", "partial", error_message="failed")

            profiles = db.list_incomplete_profiles_for_daily(date_from, date_to)
            db.close()

        self.assertEqual([profile["username"] for profile in profiles], ["fail", "missing"])


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
