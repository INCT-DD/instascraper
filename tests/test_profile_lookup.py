from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import instagram_scraper as engine
from instagram_collector import pipeline
from instagram_collector.scraper import InstagramCollector


class ProfileLookupTests(unittest.IsolatedAsyncioTestCase):
    async def test_rate_limit_retries_same_endpoint_before_fallback(self) -> None:
        requests = []
        responses = [httpx.Response(429, headers={"retry-after": "12"}), httpx.Response(200, json={"data": {"user": {"id": "42"}}})]

        def handle(request):
            requests.append(request)
            return responses.pop(0)

        limiter = SimpleNamespace(wait=AsyncMock())
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            with patch.object(engine.asyncio, "sleep", new=AsyncMock()) as sleep, patch.object(engine.tqdm, "write"):
                result = await engine.fetch_user_id(client, "example", "fixture", limiter)
        self.assertEqual(result, "42")
        self.assertEqual([r.url.path for r in requests], ["/api/v1/users/web_profile_info/"] * 2)
        sleep.assert_awaited_once_with(12.0)
        self.assertEqual(limiter.wait.await_count, 2)

    async def test_persistent_rate_limit_never_calls_search(self) -> None:
        paths = []

        def handle(request):
            paths.append(request.url.path)
            return httpx.Response(429, headers={"retry-after": "invalid"})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            with patch.object(engine.asyncio, "sleep", new=AsyncMock()) as sleep, patch.object(engine.tqdm, "write"):
                with self.assertRaisesRegex(engine.ScrapeError, "HTTP 429"):
                    await engine.fetch_user_id(client, "example", "fixture")
        self.assertEqual(paths, ["/api/v1/users/web_profile_info/"] * 3)
        self.assertEqual([call.args[0] for call in sleep.await_args_list], [60.0] * 2)

    async def test_search_fallback_preserves_matching_and_respects_rate_limit(self) -> None:
        paths = []
        responses = [
            httpx.Response(400),
            httpx.Response(429, headers={"retry-after": "3"}),
            httpx.Response(200, json={"users": [{"user": {"username": "EXAMPLE", "pk": 42}}]}),
        ]

        def handle(request):
            paths.append(request.url.path)
            return responses.pop(0)

        limiter = SimpleNamespace(wait=AsyncMock())
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            with patch.object(engine.asyncio, "sleep", new=AsyncMock()) as sleep, patch.object(engine.tqdm, "write"):
                result = await engine.fetch_user_id(client, "example", "fixture", limiter)
        self.assertEqual(result, "42")
        self.assertEqual(paths, ["/api/v1/users/web_profile_info/", "/web/search/topsearch/", "/web/search/topsearch/"])
        self.assertEqual(limiter.wait.await_count, 3)
        sleep.assert_awaited_once_with(3.0)

    async def test_retry_after_http_date_is_respected(self) -> None:
        responses = [httpx.Response(429, headers={"retry-after": "Mon, 07 Sep 2026 02:01:00 GMT"}), httpx.Response(200)]
        clock = Mock()
        clock.now.return_value = datetime(2026, 9, 7, 2, tzinfo=timezone.utc)
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: responses.pop(0))) as client:
            with patch.dict(engine._profile_lookup_get.__globals__, {"datetime": clock}), patch.object(engine.asyncio, "sleep", new=AsyncMock()) as sleep, patch.object(engine.tqdm, "write"):
                await engine._profile_lookup_get(client, "https://www.instagram.com/", {}, None)
        sleep.assert_awaited_once_with(60.0)

    async def test_redirect_still_raises_auth_error_without_search(self) -> None:
        handler = Mock(return_value=httpx.Response(302, headers={"location": "https://www.instagram.com/"}))
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with self.assertRaises(engine.AuthError):
                await engine.fetch_user_id(client, "example", "fixture")
        self.assertEqual(handler.call_count, 1)

    async def test_collector_waits_between_lookup_fallback_and_feed(self) -> None:
        limiter = engine.RateLimiter(0.3)
        moments = []
        now = [100.0]

        async def sleep(seconds):
            now[0] += seconds

        def handle(request):
            moments.append((request.url.path, now[0]))
            if "web_profile_info" in request.url.path:
                return httpx.Response(400)
            if "topsearch" in request.url.path:
                return httpx.Response(200, json={"users": [{"user": {"username": "example", "pk": 42}}]})
            return httpx.Response(200, json={"items": [], "more_available": False})

        with patch("instagram_collector.scraper.load_cookies", return_value={n: "fixture" for n in ("sessionid", "csrftoken", "mid", "ds_user_id")}):
            collector = InstagramCollector("fixture", 0.3, limiter=limiter)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            collector.client = client
            with patch.object(engine.time, "perf_counter", side_effect=lambda: now[0]), patch.object(engine.asyncio, "sleep", new=AsyncMock(side_effect=sleep)):
                await collector.fetch_profile_posts("example", datetime(2026, 9, 1), datetime(2026, 9, 6))
        self.assertEqual(len(moments), 3)
        self.assertAlmostEqual(moments[1][1] - moments[0][1], 1 / 0.3)
        self.assertAlmostEqual(moments[2][1] - moments[1][1], 1 / 0.3)

    async def test_incremental_collector_ignores_known_pinned_post_as_boundary(self) -> None:
        limiter = SimpleNamespace(wait=AsyncMock())
        pinned = {
            "id": "known-pinned", "shortcode": "PINNED", "taken_at": 1788220800,
            "timeline_pinned_user_ids": [42],
        }
        new_post = {"id": "new", "shortcode": "NEW", "taken_at": 1788220800}
        known_regular = {"id": "known-regular", "shortcode": "KNOWN", "taken_at": 1788134400}
        pages = [
            ([pinned, new_post], True, "next"),
            ([known_regular], True, "later"),
        ]
        cookies = {name: "fixture" for name in ("sessionid", "csrftoken", "mid", "ds_user_id")}
        with patch("instagram_collector.scraper.load_cookies", return_value=cookies), patch(
            "instagram_collector.scraper.fetch_user_id", new=AsyncMock(return_value="42"),
        ), patch(
            "instagram_collector.scraper.fetch_posts_page", new=AsyncMock(side_effect=pages),
        ) as fetch_page:
            collector = InstagramCollector("fixture", 10, limiter=limiter)
            collector.client = AsyncMock()
            result = await collector.fetch_profile_posts(
                "example",
                datetime(2026, 9, 1),
                datetime(2026, 9, 30),
                stop_post_ids={"known-pinned", "known-regular"},
            )

        self.assertEqual([post["post_id"] for post in result], ["new"])
        self.assertEqual(fetch_page.await_count, 2)

    async def test_period_preserves_limiter_across_failed_profiles(self) -> None:
        db = Mock()
        db.list_active_profiles.return_value = [{"username": "first"}, {"username": "second"}]
        settings = SimpleNamespace(collect_comments_default=False, account_rotation_enabled=False, rps=0.3)
        collect = AsyncMock(return_value=pipeline.PostCollectionAttempt(None, None, ["HTTP 429"]))
        with patch.object(pipeline, "seed_profiles"), patch.object(pipeline, "load_sessions", return_value=[{"name": "fixture"}]), patch.object(pipeline, "_collect_profile_posts_with_sessions", new=collect):
            await pipeline.collect_posts_period(db, settings, *pipeline.explicit_window(date(2026, 9, 1), date(2026, 9, 6)))
        self.assertEqual(collect.await_count, 2)
        first, second = [call.kwargs["request_limiter"] for call in collect.await_args_list]
        self.assertIs(first, second)
        self.assertAlmostEqual(first.interval, 1 / 0.3)


if __name__ == "__main__":
    unittest.main()
