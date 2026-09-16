from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import os
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from instagram_collector import config, post_metrics
from instagram_collector.gallerydl_posts import normalize_gallery_post
from instagram_collector.post_metrics import InstagramPostMetricsCollector, refresh_existing_post_metrics
from instagram_collector.sessions import CollectorSession
from instagram_collector.storage import Database
from test_media_refresh import raw_post


COOKIES = {"sessionid": "fixture", "csrftoken": "csrf", "mid": "mid", "ds_user_id": "42"}


class PostMetricsCollectorTests(unittest.IsolatedAsyncioTestCase):
    async def test_media_info_exposes_extended_metrics_and_preserves_zero(self) -> None:
        raw = {
            "pk": "123",
            "like_count": 0,
            "comment_count": 4,
            "play_count": 0,
            "ig_play_count": 999,
            "media_repost_count": 7,
        }
        limiter = SimpleNamespace(wait=AsyncMock())
        with patch.object(post_metrics, "load_cookies", return_value=COOKIES):
            collector = InstagramPostMetricsCollector(
                CollectorSession("fixture", "cookies.json", "cookies.txt"), limiter,
            )
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"items": [raw]})),
        ) as client:
            collector.client = client
            result = await collector.fetch("123", "EXAMPLE")

        self.assertEqual(result, raw)
        self.assertEqual(post_metrics.extract_post_metrics(result), {
            "likes": 0, "comments_count": 4, "views": 0, "reposts": 7,
        })
        limiter.wait.assert_awaited_once()


class PostMetricRefreshTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.db = Database("sqlite:///:memory:")
        self.db.init_schema()
        self.addCleanup(self.db.close)
        self.db.seed_profiles([{"name": "Example", "username": "example"}], False, False)
        profile = self.db.get_profile_by_username("example")
        post = normalize_gallery_post(raw_post(), "fixture")
        self.post_id, _, _ = self.db.upsert_post(profile["id"], post)
        with patch.dict(os.environ, {}, clear=True):
            self.settings = replace(config.load_settings(env_file=None), account_rotation_enabled=True)

    async def test_refresh_rotates_session_updates_only_metrics_and_audits_payload(self) -> None:
        sessions = [
            {"name": "one", "instagram_cookie_json": "one.json"},
            {"name": "two", "instagram_cookie_json": "two.json"},
        ]

        class FakeCollector:
            def __init__(self, session, limiter):
                self.session = session

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_):
                return None

            async def fetch(self, *_):
                if self.session.name == "one":
                    raise RuntimeError("blocked")
                return {
                    "pk": "123", "like_count": 20, "comment_count": 8,
                    "ig_play_count": 300, "media_repost_count": 6,
                }

        with patch.object(post_metrics, "load_sessions", return_value=sessions), patch.object(
            post_metrics, "InstagramPostMetricsCollector", FakeCollector,
        ):
            stats = await refresh_existing_post_metrics(
                self.db, self.settings, "2026-01-01", "2027-01-01", rps=10,
            )

        stored = self.db.get_post(self.post_id)
        self.assertEqual((stats.posts_found, stats.posts_updated, stats.posts_failed), (1, 1, 0))
        self.assertEqual((stored["likes"], stored["comments_count"], stored["views"], stored["reposts"]), (20, 8, 300, 6))
        self.assertEqual(self.db.count_rows("raw_payloads"), 1)

    def test_missing_metrics_preserve_existing_values_but_zero_updates_them(self) -> None:
        before = self.db.get_post(self.post_id)
        self.db.update_post_metrics(self.post_id, {
            "likes": None, "comments_count": None, "views": 0, "reposts": 0,
        })
        after = self.db.get_post(self.post_id)

        self.assertEqual(after["likes"], before["likes"])
        self.assertEqual(after["comments_count"], before["comments_count"])
        self.assertEqual((after["views"], after["reposts"]), (0, 0))
