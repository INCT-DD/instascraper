from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import AsyncMock, Mock, patch
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from instagram_scraper import AuthError, ScrapeError
from instagram_collector import config, gallerydl_posts as adapter, pipeline
from instagram_collector.sessions import CollectorSession
from instagram_collector.storage import Database


def raw_post(media_type=1):
    return {
        "pk": "123", "code": "EXAMPLE", "taken_at": 1788220800,
        "media_type": media_type, "caption": {"text": "Research fixture"},
        "like_count": 10, "comment_count": 12, "media_repost_count": 5,
        "accessibility_caption": "A photo", "user": {"pk": "42", "username": "example"},
        "original_width": 1080, "original_height": 720,
        "image_versions2": {"candidates": [{"url": "https://example.test/image.jpg", "width": 1080, "height": 720}]},
        "additional_payload_field": {"preserved": True},
    }


def settings():
    with patch.dict(os.environ, {}, clear=True):
        return config.load_settings(env_file=None)


class GalleryNormalizationTests(unittest.TestCase):
    def test_installed_extractor_preserves_raw_payload_across_pages(self):
        import requests
        from gallery_dl import config as gallery_config

        paths = []
        pages = 0

        def send(request, **kwargs):
            nonlocal pages
            path = urlsplit(request.url).path
            paths.append(path)
            if path == "/web/search/topsearch/":
                payload = {"users": [{"user": {"id": "42", "pk": "42", "username": "example"}}]}
            else:
                self.assertEqual(path, "/api/v1/feed/user/42/")
                pages += 1
                payload = {"items": [raw_post()] if pages == 1 else [], "more_available": pages == 1, "next_max_id": "next"}
            response = requests.Response()
            response.status_code = 200
            response.url = request.url
            response._content = json.dumps(payload).encode()
            return response

        with patch.dict(gallery_config._config, {}, clear=True), patch.object(requests.Session, "send", side_effect=send):
            result = adapter._extract_raw_posts({
                "username": "example", "start": 1788220800, "end": 1788220800,
                "rps": 1000, "sleep_request": 0, "cookies": {"sessionid": "fixture"},
            })
        self.assertEqual(pages, 2)
        self.assertEqual(result["posts"], [raw_post()])
        self.assertEqual(paths[0], "/web/search/topsearch/")

    def test_retains_database_fields_and_full_payload(self):
        raw = raw_post()
        post = adapter.normalize_gallery_post(raw, "fixture")
        self.assertEqual([post[k] for k in ("post_id", "shortcode", "caption", "likes", "comments_count", "reposts")],
                         ["123", "EXAMPLE", "Research fixture", 10, 12, 5])
        self.assertEqual(post["accessibility_caption"], "A photo")
        self.assertEqual(post["media_assets"][0]["width"], 1080)
        self.assertEqual(post["raw_json"]["additional_payload_field"], raw["additional_payload_field"])
        self.assertEqual(post["raw_json"]["_collector"]["backend"], "gallery-dl")
        self.assertNotIn("_collector", raw)

    def test_missing_metrics_are_unknown_and_zero_is_preserved(self):
        raw = raw_post()
        for key in ("like_count", "comment_count", "media_repost_count"):
            raw.pop(key)
        post = adapter.normalize_gallery_post(raw, "fixture")
        self.assertTrue(all(post[k] is None for k in ("likes", "comments_count", "reposts", "views")))
        raw.update(like_count=0, comment_count=0, media_repost_count=0, play_count=0)
        post = adapter.normalize_gallery_post(raw, "fixture")
        self.assertEqual([post[k] for k in ("likes", "comments_count", "reposts", "views")], [0, 0, 0, 0])

    def test_carousel_keeps_all_media_and_video(self):
        raw = raw_post(8)
        first, second = raw_post(), raw_post(2)
        second.update(pk="124", video_versions=[{"url": "https://example.test/video.mp4", "width": 720, "height": 1280}])
        raw["carousel_media"] = [first, second]
        post = adapter.normalize_gallery_post(raw, "fixture")
        self.assertEqual(post["media_type"], "carousel")
        self.assertEqual({m["index"] for m in post["media_assets"]}, {1, 2})
        self.assertTrue(any(m["media_type"] == "video" for m in post["media_assets"]))
        self.assertEqual(post["raw_json"]["carousel_media"], raw["carousel_media"])

    def test_bad_identity_is_not_silently_accepted(self):
        raw = raw_post()
        raw.pop("code")
        with self.assertRaises(ScrapeError):
            adapter.normalize_gallery_post(raw, "fixture")

    def test_period_handles_pinned_posts_and_deduplicates(self):
        pinned = {**raw_post(), "pk": "pin", "taken_at": 1, "timeline_pinned_user_ids": [42]}
        in_range = raw_post()
        future = {**raw_post(), "pk": "future", "taken_at": 1900000000}
        items = list(adapter.posts_in_window([pinned, future, in_range, in_range], 1788220800, 1788220800))
        self.assertEqual(items, [in_range])

    def test_period_stops_after_older_page_and_rejects_missing_dates(self):
        def stream():
            for _ in range(30):
                yield {**raw_post(), "taken_at": 1}
            raise AssertionError("unbounded pagination")
        self.assertEqual(list(adapter.posts_in_window(stream(), 2, 10)), [])
        with self.assertRaises(ScrapeError):
            list(adapter.posts_in_window([{"pk": "123"}], 2, 10))

    def test_storage_preserves_old_metrics_when_omitted_but_accepts_zero(self):
        db = Database("sqlite:///:memory:")
        self.addCleanup(db.close)
        db.init_schema()
        db.seed_profiles([{"name": "Fixture", "username": "example"}], False, False)
        profile = db.get_profile_by_username("example")
        post = adapter.normalize_gallery_post(raw_post(), "fixture")
        post_id, _, _ = db.upsert_post(profile["id"], post)
        for key in ("likes", "comments_count", "reposts", "accessibility_caption"):
            post[key] = None
        _, inserted, changed = db.upsert_post(profile["id"], post)
        self.assertFalse(inserted or changed)
        stored = db.get_post(post_id)
        self.assertEqual([stored[k] for k in ("likes", "comments_count", "reposts", "accessibility_caption")], [10, 12, 5, "A photo"])
        post.update(likes=0, comments_count=0, reposts=0)
        _, _, changed = db.upsert_post(profile["id"], post)
        self.assertTrue(changed)
        self.assertEqual(db.get_post(post_id)["reposts"], 0)


class GalleryBackendTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.settings = settings()
        # These cases exercise the existing REST/gallery fallback after GraphQL fails.
        graphql = AsyncMock()
        graphql.__aenter__.return_value.fetch_profile_posts.side_effect = ScrapeError("GraphQL unavailable")
        graphql_patch = patch.object(adapter, "InstagramGraphqlPostCollector", return_value=graphql)
        graphql_patch.start()
        self.addCleanup(graphql_patch.stop)
        self.session = CollectorSession("fixture", "fixture.json", "fixture.txt")
        self.start = datetime(2026, 9, 1, tzinfo=timezone.utc)
        self.end = datetime(2026, 9, 6, tzinfo=timezone.utc)

    async def fetch(self):
        return await adapter.fetch_posts_with_backend(self.settings, self.session, "example", self.start, self.end, 0.3)

    async def test_empty_success_does_not_trigger_fallback(self):
        context = AsyncMock()
        context.__aenter__.return_value.fetch_profile_posts.return_value = []
        with patch.object(adapter, "InstagramCollector", return_value=context), patch.object(adapter, "fetch_gallery_posts", new=AsyncMock()) as gallery:
            self.assertEqual(await self.fetch(), [])
        gallery.assert_not_awaited()

    async def test_auto_falls_back_on_auth_error_and_keeps_metadata(self):
        context = AsyncMock()
        context.__aenter__.return_value.fetch_profile_posts.side_effect = AuthError("redirect")
        expected = [adapter.normalize_gallery_post(raw_post(), "fixture")]
        with patch.object(adapter, "InstagramCollector", return_value=context), patch.object(adapter, "fetch_gallery_posts", new=AsyncMock(return_value=expected)) as gallery:
            self.assertEqual(await self.fetch(), expected)
        gallery.assert_awaited_once()

    async def test_scraper_only_does_not_fallback(self):
        self.settings = replace(self.settings, posts_backend="scraper")
        context = AsyncMock()
        context.__aenter__.return_value.fetch_profile_posts.side_effect = AuthError("redirect")
        with patch.object(adapter, "InstagramCollector", return_value=context), patch.object(adapter, "fetch_gallery_posts", new=AsyncMock()) as gallery:
            with self.assertRaises(AuthError):
                await self.fetch()
        gallery.assert_not_awaited()

    async def test_gallery_only_does_not_use_primary(self):
        self.settings = replace(self.settings, posts_backend="gallery-dl")
        with patch.object(adapter, "InstagramCollector") as primary, patch.object(adapter, "fetch_gallery_posts", new=AsyncMock(return_value=[])):
            self.assertEqual(await self.fetch(), [])
        primary.assert_not_called()

    async def test_configuration_failure_is_not_hidden_by_fallback(self):
        with patch.object(adapter, "InstagramCollector", side_effect=FileNotFoundError("fixture")), patch.object(adapter, "fetch_gallery_posts", new=AsyncMock()) as gallery:
            with self.assertRaises(FileNotFoundError):
                await self.fetch()
        gallery.assert_not_awaited()

    async def test_two_failures_are_reported_together(self):
        context = AsyncMock()
        context.__aenter__.return_value.fetch_profile_posts.side_effect = AuthError("primary redirect")
        with patch.object(adapter, "InstagramCollector", return_value=context), patch.object(adapter, "fetch_gallery_posts", new=AsyncMock(side_effect=ScrapeError("gallery redirect"))):
            with self.assertRaisesRegex(ScrapeError, "primary redirect.*gallery redirect"):
                await self.fetch()

    async def test_subprocess_preserves_raw_data_and_uses_stdin_for_cookies(self):
        process = Mock(returncode=0, communicate=AsyncMock(return_value=(json.dumps({"posts": [raw_post()], "version": "fixture"}).encode(), b"")))
        with patch.object(adapter.asyncio, "create_subprocess_exec", new=AsyncMock(return_value=process)) as spawn, patch.object(adapter.GalleryDlStoryCollector, "_gallery_cookies", return_value={"sessionid": "fixture-secret"}):
            posts = await adapter.fetch_gallery_posts(self.settings, self.session, "example", self.start, self.end, 0.3)
        self.assertEqual(posts[0]["reposts"], 5)
        self.assertNotIn("fixture-secret", str(spawn.call_args))
        request = json.loads(process.communicate.call_args.args[0])
        self.assertEqual(request["cookies"], {"sessionid": "fixture-secret"})

    def test_story_config_throttles_extraction_and_downloads(self):
        collector = adapter.GalleryDlStoryCollector(self.settings)
        with TemporaryDirectory() as tmp, patch.object(collector, "_gallery_cookies", return_value={}):
            config_data = collector._config(Path(tmp), self.session)
        extractor = config_data["extractor"]
        self.assertEqual(extractor["sleep"], self.settings.gallery_dl_sleep_download)
        self.assertEqual(extractor["sleep-request"], self.settings.gallery_dl_sleep_request)

    async def test_timeout_stops_child_and_reports_error(self):
        process = Mock(returncode=None, communicate=AsyncMock(side_effect=[asyncio.TimeoutError(), (b"", b"")]))
        with patch.object(adapter.asyncio, "create_subprocess_exec", new=AsyncMock(return_value=process)), patch.object(adapter.GalleryDlStoryCollector, "_gallery_cookies", return_value={}):
            with self.assertRaisesRegex(ScrapeError, "timed out"):
                await adapter.fetch_gallery_posts(self.settings, self.session, "example", self.start, self.end, 0.3)
        process.kill.assert_called_once()
        self.assertEqual(process.communicate.await_count, 2)

    async def test_real_subprocess_imports_adapter_without_installing_project(self):
        with patch.object(adapter.GalleryDlStoryCollector, "_gallery_cookies", return_value={}):
            with self.assertRaisesRegex(ScrapeError, "Invalid Instagram username"):
                await adapter.fetch_gallery_posts(self.settings, self.session, "invalid/name", self.start, self.end, 0.3)

    async def test_pipeline_persists_payload_and_enqueues_carousel_media(self):
        db = Database("sqlite:///:memory:")
        self.addCleanup(db.close)
        db.init_schema()
        db.seed_profiles([{"name": "Fixture", "username": "example", "collect_comments": False}], False, False)
        raw = raw_post(8)
        raw["carousel_media"] = [deepcopy(raw_post()), {**raw_post(), "pk": "124"}]
        posts = [adapter.normalize_gallery_post(raw, "fixture")]
        with TemporaryDirectory() as tmp:
            s = replace(self.settings, collect_post_media=True, media_queue_enabled=True, data_dir=tmp, candidate_archive_dir=str(Path(tmp) / "exports/instagram"), post_media_dir=str(Path(tmp) / "exports/instagram"))
            with patch.object(pipeline, "fetch_posts_with_backend", new=AsyncMock(return_value=posts)):
                result = await pipeline.collect_profile(db, s, "example", self.start, self.end, enqueue_comments=False, session=self.session)
        self.assertEqual((result.posts_inserted, result.media_jobs_enqueued, result.jobs_enqueued), (1, 1, 0))
        jobs = db.fetch_pending_jobs(10)
        self.assertEqual([job["job_type"] for job in jobs], ["post_media"])
        stored = db.get_post(jobs[0]["post_id"])
        self.assertEqual(stored["reposts"], 5)
        self.assertEqual(stored["raw_json"]["additional_payload_field"], {"preserved": True})
        self.assertEqual(len(db.list_post_media_for_post(stored["id"])), 2)


if __name__ == "__main__":
    unittest.main()
