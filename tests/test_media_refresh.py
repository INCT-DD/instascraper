from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from pathlib import Path
import os
import sys
import unittest
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from instagram_collector import config
from instagram_collector.gallerydl_posts import normalize_gallery_post
from instagram_collector.media_refresh import refresh_failed_post_media
from instagram_collector.storage import Database


def raw_post() -> dict:
    return {
        "pk": "123",
        "code": "EXAMPLE",
        "taken_at": 1789084800,
        "media_type": 8,
        "caption": {"text": "fixture"},
        "user": {"pk": "42", "username": "example"},
        "carousel_media": [
            {
                "pk": "124",
                "code": "FIRST",
                "taken_at": 1789084800,
                "media_type": 1,
                "image_versions2": {"candidates": [{"url": "https://old.test/first.jpg", "width": 1080, "height": 1080}]},
            },
            {
                "pk": "125",
                "code": "SECOND",
                "taken_at": 1789084800,
                "media_type": 1,
                "image_versions2": {"candidates": [{"url": "https://old.test/second.jpg", "width": 1080, "height": 1080}]},
            },
        ],
    }


class MediaRefreshTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.db = Database("sqlite:///:memory:")
        self.db.init_schema()
        self.addCleanup(self.db.close)
        self.db.seed_profiles([{"name": "Example", "username": "example"}], False, False)
        self.profile = self.db.get_profile_by_username("example")
        self.post = normalize_gallery_post(raw_post(), "fixture")
        self.post_id, _, _ = self.db.upsert_post(self.profile["id"], self.post)
        for media in self.post["media_assets"]:
            media["download_status"] = "pending"
            self.db.upsert_post_media(self.post_id, media)
        media = self.db.list_post_media_for_post(self.post_id)
        self.db.update_post_media_download(media[0]["id"], "success", "/archive/first.jpg")
        self.db.update_post_media_download(media[1]["id"], "failed", error_message="403 Forbidden")
        self.job_id = self.db.enqueue_job("post_media", self.profile["id"], post_id=self.post_id, max_attempts=3)
        self.db.mark_job_started(self.job_id)
        self.db.mark_job_failed(self.job_id, "1 post media files failed")
        with patch.dict(os.environ, {}, clear=True):
            self.settings = replace(config.load_settings(env_file=None), account_rotation_enabled=True)

    async def test_refresh_updates_only_failed_asset_and_reopens_job(self) -> None:
        refreshed_post = deepcopy(self.post)
        refreshed_post["media_assets"][0]["url"] = "https://new.test/first.jpg"
        refreshed_post["media_assets"][1]["url"] = "https://new.test/second.jpg"
        sessions = [
            {"name": "one", "instagram_cookie_json": "one.json", "gallery_dl_cookies": "one.txt"},
            {"name": "two", "instagram_cookie_json": "two.json", "gallery_dl_cookies": "two.txt"},
        ]
        fetch = AsyncMock(side_effect=[RuntimeError("first account blocked"), refreshed_post])
        with patch("instagram_collector.media_refresh.load_sessions", return_value=sessions), patch(
            "instagram_collector.media_refresh.fetch_gallery_post", new=fetch
        ):
            stats = await refresh_failed_post_media(self.db, self.settings, "2026-09-11", "2026-09-12")

        self.assertEqual((stats.posts_found, stats.posts_refreshed, stats.assets_refreshed, stats.posts_failed), (1, 1, 1, 0))
        self.assertEqual(fetch.await_count, 2)
        rows = self.db.list_post_media_for_post(self.post_id)
        self.assertEqual(rows[0]["source_url"], "https://old.test/first.jpg")
        self.assertEqual(rows[0]["download_status"], "success")
        self.assertEqual(rows[1]["source_url"], "https://new.test/second.jpg")
        self.assertEqual(rows[1]["download_status"], "pending")
        job = self.db._fetchone("SELECT * FROM collection_jobs WHERE id = ?", (self.job_id,))
        self.assertEqual((job["status"], job["attempts"], job["error_message"]), ("pending", 0, None))

    async def test_refresh_keeps_failed_job_when_all_sessions_fail(self) -> None:
        sessions = [{"name": "one", "instagram_cookie_json": "one.json", "gallery_dl_cookies": "one.txt"}]
        with patch("instagram_collector.media_refresh.load_sessions", return_value=sessions), patch(
            "instagram_collector.media_refresh.fetch_gallery_post",
            new=AsyncMock(side_effect=RuntimeError("blocked")),
        ):
            stats = await refresh_failed_post_media(self.db, self.settings, "2026-09-11", "2026-09-12")
        self.assertEqual(stats.posts_failed, 1)
        job = self.db._fetchone("SELECT status, attempts FROM collection_jobs WHERE id = ?", (self.job_id,))
        self.assertEqual((job["status"], job["attempts"]), ("retry", 1))

    async def test_date_window_excludes_other_days(self) -> None:
        with patch("instagram_collector.media_refresh.load_sessions") as sessions:
            stats = await refresh_failed_post_media(self.db, self.settings, "2026-09-12", "2026-09-13")
        self.assertEqual(stats.posts_found, 0)
        sessions.assert_not_called()

    async def test_job_claim_prevents_race_with_media_worker(self) -> None:
        self.db.mark_job_started(self.job_id)
        with patch("instagram_collector.media_refresh.load_sessions") as sessions:
            stats = await refresh_failed_post_media(self.db, self.settings, "2026-09-11", "2026-09-12")
        self.assertEqual((stats.posts_refreshed, stats.posts_failed), (0, 0))
        sessions.assert_not_called()

    def test_image_success_does_not_hide_failed_video_at_same_index(self) -> None:
        video = {
            "index": 1,
            "media_type": "video",
            "url": "https://old.test/video.mp4",
            "download_status": "failed",
        }
        self.db.upsert_post_media(self.post_id, video)
        refreshed = {**video, "url": "https://new.test/video.mp4"}
        self.assertTrue(self.db.refresh_post_media_asset(self.post_id, refreshed))
        updated = {(row["media_index"], row["media_type"]): row for row in self.db.list_post_media_for_post(self.post_id)}
        self.assertEqual(updated[(1, "image")]["download_status"], "success")
        self.assertEqual(updated[(1, "video")]["download_status"], "pending")
        self.assertEqual(updated[(1, "video")]["source_url"], "https://new.test/video.mp4")
