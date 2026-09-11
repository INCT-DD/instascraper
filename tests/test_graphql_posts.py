from __future__ import annotations

from contextlib import ExitStack
from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch
from urllib.parse import parse_qs

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from instagram_scraper import CollectionBlockedError, ProfileAccessError, ScrapeError, build_headers
from instagram_collector import config, gallerydl_posts as backend, graphql_posts as graphql, pipeline
from instagram_collector.media_jobs import MediaJobProcessor
from instagram_collector.sessions import CollectorSession, SessionPool
from instagram_collector.storage import Database
from test_gallerydl_posts import raw_post, settings


COOKIES = {"sessionid": "fixture-session", "csrftoken": "fixture-csrf", "mid": "fixture", "ds_user_id": "42"}
START = datetime(2026, 9, 1, tzinfo=timezone.utc)
END = datetime(2026, 9, 6, 23, 59, 59, tzinfo=timezone.utc)


def page(nodes, more=False, cursor=None):
    return {"status": "ok", "data": {graphql.TIMELINE_CONNECTION: {
        "edges": [{"node": node} for node in nodes],
        "page_info": {"has_next_page": more, "end_cursor": cursor},
    }}}


class GraphqlTimelineTests(unittest.IsolatedAsyncioTestCase):
    async def fetch(self, handler, start=START, end=END):
        with patch.object(graphql, "load_cookies", return_value=COOKIES):
            limiter = SimpleNamespace(wait=AsyncMock())
            collector = graphql.InstagramGraphqlPostCollector("fixture", 0.3, "12345", limiter)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            collector.client = client
            result = await collector.fetch_profile_posts("@example", start, end)
        return result, limiter

    async def test_real_transport_paginates_with_csrf_and_preserves_dates_and_payload(self):
        first = raw_post()
        second = {**raw_post(), "pk": "124", "code": "SECOND", "taken_at": int(END.timestamp())}
        pinned = {**raw_post(), "pk": "pin", "taken_at": 1, "timeline_pinned_user_ids": [42]}
        future = {**raw_post(), "pk": "future", "taken_at": int(END.timestamp()) + 1}
        requests = []
        responses = [page([pinned, first, future], True, "next"), page([first, second])]

        def handle(request):
            requests.append(request)
            self.assertEqual((request.method, request.url.path), ("POST", "/graphql/query"))
            self.assertEqual(request.headers["X-CSRFToken"], COOKIES["csrftoken"])
            form = parse_qs(request.content.decode())
            variables = json.loads(form["variables"][0])
            self.assertEqual(form["doc_id"], ["12345"])
            self.assertEqual(variables["username"], "example")
            self.assertIs(variables[graphql.TIMELINE_RELAY_VARIABLE], False)
            if len(requests) == 2:
                self.assertEqual(variables["after"], "next")
                self.assertEqual(variables["first"], 12)
            return httpx.Response(200, json=responses.pop(0))

        posts, limiter = await self.fetch(handle)
        self.assertEqual([p["post_id"] for p in posts], ["123", "124"])
        self.assertEqual(posts[0]["raw_json"]["additional_payload_field"], {"preserved": True})
        self.assertEqual(posts[0]["raw_json"]["_collector"]["doc_id"], "12345")
        self.assertEqual(limiter.wait.await_count, 2)
        self.assertEqual(posts[-1]["taken_at"], int(END.timestamp()))

    async def test_old_page_stops_without_requesting_more_history(self):
        handler = Mock(return_value=httpx.Response(200, json=page([{**raw_post(), "taken_at": 1}], True, "next")))
        posts, _ = await self.fetch(handler)
        self.assertEqual(posts, [])
        self.assertEqual(handler.call_count, 1)

    async def test_pinned_only_page_does_not_stop_history(self):
        responses = [
            page([{**raw_post(), "taken_at": 1, "timeline_pinned_user_ids": [42]}], True, "next"),
            page([raw_post()]),
        ]
        posts, _ = await self.fetch(lambda _: httpx.Response(200, json=responses.pop(0)))
        self.assertEqual(len(posts), 1)
        self.assertEqual(responses, [])

    async def test_empty_connection_is_valid(self):
        posts, _ = await self.fetch(lambda _: httpx.Response(200, json=page([])))
        self.assertEqual(posts, [])

    async def test_invalid_responses_are_not_mistaken_for_zero_posts(self):
        invalid = [
            {"status": "ok"},
            {"status": "ok", "data": None},
            {**page([raw_post()]), "errors": [{"message": "execution error"}]},
            page([raw_post()], True, None),
            page([], True, "next"),
            page([None]),
            page([{**raw_post(), "pk": None}]),
            page([{**raw_post(), "taken_at": True}]),
        ]
        no_flag = page([])
        no_flag["data"][graphql.TIMELINE_CONNECTION]["page_info"] = {}
        invalid.append(no_flag)
        for payload in invalid:
            with self.subTest(payload=payload):
                with self.assertRaises(ScrapeError):
                    await self.fetch(lambda _: httpx.Response(200, json=payload))

    async def test_blocking_responses_stop_even_when_body_is_html(self):
        responses = [
            httpx.Response(429, text="<html>limited</html>", headers={"Retry-After": "120"}),
            httpx.Response(401, text="Unauthorized"),
            httpx.Response(403, text="Forbidden"),
            httpx.Response(302, headers={"location": "https://www.instagram.com/accounts/login/"}),
            httpx.Response(400, json={"status": "fail", "message": "feedback_required"}),
            httpx.Response(200, json={"errors": [{"message": "challenge_required"}]}),
        ]
        for response in responses:
            with self.subTest(status=response.status_code):
                handler = Mock(return_value=response)
                expected = ProfileAccessError if response.status_code in {401, 403} else CollectionBlockedError
                with self.assertRaises(expected):
                    await self.fetch(handler)
                self.assertEqual(handler.call_count, 1)

    async def test_second_page_failure_does_not_return_partial_success(self):
        responses = [httpx.Response(200, json=page([raw_post()], True, "next")), httpx.Response(500)]
        with self.assertRaises(ScrapeError):
            await self.fetch(lambda _: responses.pop(0))

    async def test_repeated_cursor_is_an_error(self):
        with self.assertRaisesRegex(ScrapeError, "repeated"):
            await self.fetch(lambda _: httpx.Response(200, json=page([raw_post()], True, "same")))

    def test_metrics_media_and_provenance_preserve_unknown_and_zero(self):
        raw = raw_post(8)
        video = {**raw_post(2), "pk": "124", "video_versions": [{"url": "https://example.test/video.mp4"}]}
        raw["carousel_media"] = [raw_post(), video]
        raw.pop("media_repost_count")
        raw.update(like_count=0, comment_count=0, play_count=0, view_count=100)
        original = deepcopy(raw)
        post = graphql.normalize_graphql_post(raw, "12345")
        self.assertEqual((post["likes"], post["comments_count"], post["views"]), (0, 0, 0))
        self.assertIsNone(post["reposts"])
        self.assertEqual(len(post["media_assets"]), 3)
        self.assertEqual(post["media_type"], "carousel")
        self.assertEqual(raw, original)
        self.assertEqual(post["raw_json"]["carousel_media"], raw["carousel_media"])

    def test_rest_csrf_uses_cookie_parser(self):
        headers = build_headers("https://www.instagram.com/", 'sessionid=fixture; csrftoken="token=123"; mid=fixture')
        self.assertEqual(headers["X-CSRFToken"], "token=123")

    def test_graphql_backend_and_doc_id_are_configurable(self):
        with patch.dict(os.environ, {"POSTS_BACKEND": "graphql", "INSTAGRAM_TIMELINE_DOC_ID": "12345"}, clear=True):
            configured = config.load_settings(env_file=None)
        self.assertEqual((configured.posts_backend, configured.instagram_timeline_doc_id), ("graphql", "12345"))


class GraphqlBackendTests(unittest.IsolatedAsyncioTestCase):
    async def test_auto_and_explicit_graphql_use_timeline_first_without_rest_lookup(self):
        for mode in ("auto", "graphql"):
            for expected in ([], [graphql.normalize_graphql_post(raw_post(), "12345")]):
                with self.subTest(mode=mode, empty=not expected):
                    context = AsyncMock()
                    context.__aenter__.return_value.fetch_profile_posts.return_value = expected
                    with patch.object(backend, "InstagramGraphqlPostCollector", return_value=context), \
                         patch.object(backend, "InstagramCollector") as rest, \
                         patch.object(backend, "fetch_gallery_posts", new=AsyncMock()) as gallery:
                        actual = await backend.fetch_posts_with_backend(
                            replace(settings(), posts_backend=mode), CollectorSession("fixture", "f.json", ""),
                            "example", START, END, 0.3,
                        )
                    self.assertEqual(actual, expected)
                    rest.assert_not_called()
                    gallery.assert_not_awaited()

    async def test_explicit_block_never_triggers_fallback(self):
        context = AsyncMock()
        context.__aenter__.return_value.fetch_profile_posts.side_effect = CollectionBlockedError("HTTP 429")
        with patch.object(backend, "InstagramGraphqlPostCollector", return_value=context), \
             patch.object(backend, "InstagramCollector") as rest, \
             patch.object(backend, "fetch_gallery_posts", new=AsyncMock()) as gallery:
            with self.assertRaises(CollectionBlockedError):
                await backend.fetch_posts_with_backend(settings(), CollectorSession("fixture", "f.json", ""), "example", START, END, 0.3)
        rest.assert_not_called()
        gallery.assert_not_awaited()


class GraphqlPipelineIntegrationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        tmp = self.stack.enter_context(TemporaryDirectory())
        self.settings = replace(
            settings(), collect_post_media=True, media_queue_enabled=True,
            profile_block_wait_seconds=300,
            data_dir=str(Path(tmp) / "data"), logs_dir=str(Path(tmp) / "logs"),
            reports_dir=str(Path(tmp) / "reports"), exports_dir=str(Path(tmp) / "exports"),
            candidate_archive_dir=str(Path(tmp) / "exports/instagram"), post_media_dir=str(Path(tmp) / "exports/instagram"),
        )
        self.db = Database("sqlite:///:memory:")
        self.addCleanup(self.db.close)
        self.db.init_schema()
        self.db.seed_profiles([{"username": "example", "name": "Fixture"}, {"username": "second", "name": "Second"}], False, False)
        self.session = CollectorSession("fixture", "fixture.json", "")
        self.stack.enter_context(patch.object(graphql, "load_cookies", return_value=COOKIES))
        self.stack.enter_context(patch.object(pipeline.asyncio, "sleep", new=AsyncMock()))

    def use_transport(self, handler):
        original = httpx.AsyncClient
        return self.stack.enter_context(patch.object(httpx, "AsyncClient", side_effect=lambda **kw: original(
            transport=httpx.MockTransport(handler), **kw,
        )))

    async def test_graphql_to_database_export_and_real_media_worker(self):
        raw = raw_post(8)
        raw.pop("media_repost_count")
        raw["carousel_media"] = [raw_post(), {**raw_post(2), "pk": "124", "video_versions": [{"url": "https://example.test/video.mp4"}]}]
        api_requests = []

        def handle(request):
            if request.url.host == "www.instagram.com":
                api_requests.append(request)
                return httpx.Response(200, json=page([raw]))
            self.assertEqual(request.url.host, "example.test")
            return httpx.Response(200, content=b"fixture-media", headers={"Content-Type": "application/octet-stream"})

        self.use_transport(handle)
        result = await pipeline.collect_profile(self.db, self.settings, "example", START, END, enqueue_comments=False, session=self.session)
        self.assertEqual((result.posts_inserted, result.media_jobs_enqueued, result.jobs_enqueued), (1, 1, 0))
        job = self.db.fetch_pending_jobs(1)[0]
        stored = self.db.get_post(job["post_id"])
        self.assertIsNone(stored["reposts"])
        self.assertEqual(stored["raw_json"]["_collector"]["backend"], "instagram-graphql")
        self.assertEqual(stored["raw_json"]["additional_payload_field"], raw["additional_payload_field"])
        exports = list(Path(self.settings.exports_dir).rglob("01092026.json"))
        self.assertEqual(len(exports), 1)

        processor = MediaJobProcessor(self.db, self.settings, session_pool=SessionPool([{"name": "fixture"}], False))
        downloaded = await processor.process_pending_jobs(1)
        self.assertEqual((downloaded.processed, downloaded.failed, downloaded.post_media_downloaded), (1, 0, 3))
        media = self.db.list_post_media_for_post(stored["id"])
        self.assertTrue(all(m["download_status"] == "success" and Path(m["local_path"]).read_bytes() == b"fixture-media" for m in media))
        self.assertEqual(len(api_requests), 1)
        again = await pipeline.collect_profile(self.db, self.settings, "example", START, END, enqueue_comments=False, session=self.session)
        self.assertEqual((again.posts_inserted, again.posts_updated, again.media_jobs_enqueued), (0, 1, 0))

    async def test_period_rate_limit_exhausts_retries_and_continues_all_profiles(self):
        handler = Mock(return_value=httpx.Response(429, text="limited"))
        self.use_transport(handler)
        with patch.object(pipeline, "seed_profiles"), patch.object(pipeline, "load_sessions", return_value=[
            {"name": "first"}, {"name": "second"},
        ]):
            results = await pipeline.collect_posts_period(self.db, replace(self.settings, account_rotation_enabled=True), START, END)
        self.assertEqual([r["status"] for r in results], ["failed", "failed"])
        self.assertEqual(handler.call_count, 4)
        runs = self.db._fetchall("SELECT status, finished_at FROM collection_runs", ())
        self.assertEqual(len(runs), 4)
        self.assertTrue(all(r["status"] == "failed" and r["finished_at"] for r in runs))
        self.assertEqual(self.db._fetchone("SELECT COUNT(*) AS total FROM posts", ())["total"], 0)

    async def test_period_rate_limit_recovers_and_finishes_next_profile(self):
        requests = []
        responses = [httpx.Response(429), httpx.Response(200, json=page([])), httpx.Response(200, json=page([]))]

        def handle(request):
            requests.append(json.loads(parse_qs(request.content.decode())["variables"][0])["username"])
            return responses.pop(0)

        self.use_transport(handle)
        with patch.object(pipeline, "seed_profiles"), patch.object(pipeline, "load_sessions", return_value=[{"name": "fixture"}]):
            results = await pipeline.collect_posts_period(self.db, self.settings, START, END)
        self.assertEqual(requests, ["example", "example", "second"])
        self.assertEqual([r["status"] for r in results], ["success", "success"])
        self.assertEqual(self.db.list_incomplete_profiles_for_posts(START, END, failed_only=True), [])
        runs = self.db._fetchall("SELECT status FROM collection_runs ORDER BY id", ())
        self.assertEqual([r["status"] for r in runs], ["failed", "success", "success"])

    async def test_period_access_error_continues_and_retry_selects_only_failed_profile(self):
        requests = []
        refuse = True

        def handle(request):
            variables = json.loads(parse_qs(request.content.decode())["variables"][0])
            username = variables["username"]
            requests.append(username)
            if refuse and username == "example":
                return httpx.Response(401, text="Unauthorized")
            return httpx.Response(200, json=page([]))

        self.use_transport(handle)
        with patch.object(pipeline, "seed_profiles"), patch.object(pipeline, "load_sessions", return_value=[
            {"name": "first"}, {"name": "second"},
        ]):
            configured = replace(self.settings, account_rotation_enabled=True)
            results = await pipeline.collect_posts_period(self.db, configured, START, END)
            self.assertEqual(requests, ["example", "second"])
            self.assertEqual([r["status"] for r in results], ["failed", "success"])
            requests.clear()
            refuse = False
            results = await pipeline.collect_posts_period(self.db, configured, START, END, retry_failed=True)
            self.assertEqual(requests, ["example"])
            self.assertEqual([r["status"] for r in results], ["success"])
        runs = self.db._fetchall("SELECT status FROM collection_runs ORDER BY id", ())
        self.assertEqual([r["status"] for r in runs], ["failed", "success", "success"])

    async def test_daily_access_error_does_not_stop_next_profile(self):
        responses = [httpx.Response(403, text="Forbidden"), httpx.Response(200, json=page([]))]
        self.use_transport(lambda _: responses.pop(0))
        with patch.object(pipeline, "seed_profiles"), \
             patch.object(pipeline, "load_sessions", return_value=[{"name": "fixture"}]):
            report = await pipeline.run_daily_collection(
                self.db, self.settings, date(2026, 9, 1), collect_stories_enabled=False, process_jobs=False,
            )
        self.assertEqual(responses, [])
        self.assertEqual(report["profiles_error"], 1)
        self.assertEqual(report["profiles_success"], 1)

    async def test_daily_block_continues_all_profiles_and_finishes_partial_report(self):
        handler = Mock(return_value=httpx.Response(400, json={"message": "feedback_required"}))
        self.use_transport(handler)
        with patch.object(pipeline, "seed_profiles"), \
             patch.object(pipeline, "load_sessions", return_value=[{"name": "fixture"}]), \
             patch.object(pipeline, "_process_daily_jobs", new=AsyncMock()) as jobs:
            report = await pipeline.run_daily_collection(
                self.db, self.settings, date(2026, 9, 1), collect_stories_enabled=False,
            )
        self.assertEqual(handler.call_count, 2)
        self.assertEqual(report["profiles_error"], 2)
        self.assertEqual(report["errors"][0]["stage"], "posts")
        self.assertEqual(len(report["profile_results"]), 2)
        jobs.assert_not_awaited()
        runs = self.db._fetchall("SELECT status FROM collection_runs ORDER BY id", ())
        self.assertEqual([r["status"] for r in runs], ["partial", "failed", "failed"])


if __name__ == "__main__":
    unittest.main()
