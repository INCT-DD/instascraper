import asyncio
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
import os
import sys
import unittest
from unittest.mock import AsyncMock, Mock, patch

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from instagram_scraper import CollectionBlockedError, RateLimitError, fetch_posts_page, retry_after_seconds
from instagram_collector import config, pipeline
from test_gallerydl_posts import settings as default_settings


def settings():
    return replace(default_settings(), profile_block_wait_seconds=300)


class CollectionResilienceTests(unittest.IsolatedAsyncioTestCase):
    async def collect(self, outcomes, configured=None):
        fn = AsyncMock(side_effect=outcomes)
        delay = AsyncMock()
        with patch.object(pipeline, "collect_profile", new=fn), patch.object(pipeline.asyncio, "sleep", new=delay):
            result = await pipeline._collect_profile_with_rate_limit_retry(
                Mock(), configured or settings(), "example", datetime.now(timezone.utc), datetime.now(timezone.utc), session="fixture",
            )
        return result, fn, delay

    async def test_default_does_not_wait_or_retry_failed_profile(self):
        for failure in (RateLimitError("429", 900), CollectionBlockedError("challenge_required")):
            fn = AsyncMock(side_effect=failure)
            with patch.object(pipeline, "collect_profile", new=fn), patch.object(pipeline.asyncio, "sleep", new=AsyncMock()) as delay:
                with self.assertRaises(CollectionBlockedError):
                    await pipeline._collect_profile_with_rate_limit_retry(Mock(), default_settings(), "example", None, None)
            fn.assert_awaited_once()
            delay.assert_not_awaited()

    async def test_rate_limit_recovers_same_profile_and_respects_retry_after(self):
        expected = pipeline.ProfileCollectionStats()
        result, fn, delay = await self.collect([RateLimitError("429", 900), expected])
        self.assertIs(result, expected)
        self.assertEqual(fn.await_count, 2)
        self.assertEqual(fn.await_args_list[0], fn.await_args_list[1])
        delay.assert_awaited_once_with(900)

    async def test_retry_budget_and_exponential_wait_are_bounded(self):
        failure = RateLimitError("429", 30)
        fn = AsyncMock(side_effect=failure)
        with patch.object(pipeline, "collect_profile", new=fn), patch.object(pipeline.asyncio, "sleep", new=AsyncMock()) as delay:
            with self.assertRaises(RateLimitError):
                await pipeline._collect_profile_with_rate_limit_retry(Mock(), settings(), "example", None, None)
        self.assertEqual(fn.await_count, 2)
        self.assertEqual([c.args[0] for c in delay.await_args_list], [300, 600])

    async def test_challenge_waits_but_does_not_retry(self):
        fn = AsyncMock(side_effect=CollectionBlockedError("challenge_required"))
        with patch.object(pipeline, "collect_profile", new=fn), patch.object(pipeline.asyncio, "sleep", new=AsyncMock()) as delay:
            with self.assertRaises(CollectionBlockedError):
                await pipeline._collect_profile_with_rate_limit_retry(Mock(), settings(), "example", None, None)
        fn.assert_awaited_once()
        delay.assert_awaited_once_with(300)

    async def test_zero_retry_budget_still_waits_before_next_profile(self):
        fn = AsyncMock(side_effect=RateLimitError("429"))
        with patch.object(pipeline, "collect_profile", new=fn), patch.object(pipeline.asyncio, "sleep", new=AsyncMock()) as delay:
            with self.assertRaises(RateLimitError):
                await pipeline._collect_profile_with_rate_limit_retry(Mock(), replace(settings(), profile_rate_limit_retries=0), "example", None, None)
        fn.assert_awaited_once()
        delay.assert_awaited_once_with(300)

    async def test_user_cancellation_is_not_swallowed(self):
        fn = AsyncMock(side_effect=RateLimitError("429"))
        with patch.object(pipeline, "collect_profile", new=fn), \
             patch.object(pipeline.asyncio, "sleep", new=AsyncMock(side_effect=asyncio.CancelledError)):
            with self.assertRaises(asyncio.CancelledError):
                await pipeline._collect_profile_with_rate_limit_retry(Mock(), settings(), "example", None, None)
        fn.assert_awaited_once()

    async def test_rest_feed_429_is_typed_and_header_is_validated(self):
        handler = Mock(return_value=httpx.Response(429, headers={"retry-after": "invalid"}))
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with patch.object(pipeline.asyncio, "sleep", new=AsyncMock()) as delay:
                with self.assertRaises(RateLimitError) as caught:
                    await fetch_posts_page(client, "123", "fixture", "example")
        self.assertEqual(handler.call_count, 3)
        self.assertEqual(caught.exception.retry_after, 60)
        self.assertEqual([c.args[0] for c in delay.await_args_list], [60, 60])

    def test_retry_after_invalid_values_use_default(self):
        for value in (None, "invalid", "NaN", "inf", "-10"):
            with self.subTest(value=value):
                self.assertEqual(retry_after_seconds(value), 60)
        self.assertEqual(retry_after_seconds("0"), 0)
        self.assertEqual(retry_after_seconds("120"), 120)

    def test_retry_configuration_is_validated(self):
        for key, value in (("PROFILE_RATE_LIMIT_RETRIES", "-1"), ("PROFILE_RATE_LIMIT_RETRIES", "6"), ("PROFILE_BLOCK_WAIT_SECONDS", "-1")):
            with patch.dict(os.environ, {key: value}, clear=True), self.assertRaises(ValueError):
                config.load_settings(env_file=None)


if __name__ == "__main__":
    unittest.main()
