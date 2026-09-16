from __future__ import annotations

from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Any, Dict, Optional

import httpx

from instagram_scraper import (
    ProfileAccessError,
    RateLimitError,
    RateLimiter,
    ScrapeError,
    _raise_for_instagram_redirect,
    build_cookie_string,
    build_headers,
    extract_post_metrics,
    load_cookies,
    retry_after_seconds,
)

from .config import Settings, load_sessions
from .sessions import CollectorSession, SessionPool
from .storage import Database


@dataclass
class PostMetricRefreshStats:
    posts_found: int = 0
    posts_updated: int = 0
    posts_unchanged: int = 0
    posts_failed: int = 0
    views_available: int = 0
    reposts_available: int = 0


class InstagramPostMetricsCollector:
    def __init__(self, session: CollectorSession, limiter: RateLimiter) -> None:
        self.session = session
        self.limiter = limiter
        self.cookie_string = build_cookie_string(load_cookies(session.instagram_cookie_json))
        self.client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "InstagramPostMetricsCollector":
        self.client = httpx.AsyncClient(
            http2=True,
            timeout=httpx.Timeout(25.0, connect=10.0),
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
        )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self.client:
            await self.client.aclose()

    async def fetch(self, platform_post_id: str, shortcode: str) -> Dict[str, Any]:
        if not self.client:
            raise RuntimeError("InstagramPostMetricsCollector must be used as an async context manager.")
        await self.limiter.wait()
        response = await self.client.get(
            f"https://www.instagram.com/api/v1/media/{platform_post_id}/info/",
            headers=build_headers(f"https://www.instagram.com/p/{shortcode}/", self.cookie_string),
            follow_redirects=False,
        )
        _raise_for_instagram_redirect(response, "media info")
        if response.status_code in {401, 403}:
            raise ProfileAccessError(f"Instagram media info refused the session (HTTP {response.status_code}).")
        if response.status_code == 429:
            raise RateLimitError(
                "Instagram media info rate limited the session (HTTP 429).",
                retry_after_seconds(response.headers.get("retry-after")),
            )
        if response.status_code != 200:
            raise ScrapeError(f"Instagram media info failed with HTTP {response.status_code}.")
        try:
            payload = response.json()
        except ValueError as exc:
            raise ScrapeError("Instagram media info returned a non-JSON response.") from exc
        items = payload.get("items") if isinstance(payload, dict) else None
        if not isinstance(items, list) or len(items) != 1 or not isinstance(items[0], dict):
            raise ScrapeError("Instagram media info returned an invalid item list.")
        item = items[0]
        identity = str(item.get("pk") or item.get("id") or "")
        if identity and identity != str(platform_post_id):
            raise ScrapeError("Instagram media info returned a different post identity.")
        return item


async def refresh_existing_post_metrics(
    db: Database,
    settings: Settings,
    date_from: str,
    date_to_exclusive: str,
    username: Optional[str] = None,
    limit: Optional[int] = None,
    rps: Optional[float] = None,
) -> PostMetricRefreshStats:
    posts = db.list_posts_for_metric_refresh(date_from, date_to_exclusive, username, limit)
    stats = PostMetricRefreshStats(posts_found=len(posts))
    if not posts:
        return stats

    pool = SessionPool(load_sessions(settings), settings.account_rotation_enabled)
    limiter = RateLimiter(rps if rps is not None else settings.rps)
    collectors: Dict[str, InstagramPostMetricsCollector] = {}
    async with AsyncExitStack() as stack:
        for post in posts:
            first_session = pool.next()
            raw = None
            last_error: Optional[Exception] = None
            for session in [first_session, *pool.alternatives(first_session)]:
                try:
                    collector = collectors.get(session.alias)
                    if collector is None:
                        collector = await stack.enter_async_context(InstagramPostMetricsCollector(session, limiter))
                        collectors[session.alias] = collector
                    raw = await collector.fetch(str(post["platform_post_id"]), str(post["shortcode"]))
                    break
                except Exception as exc:
                    last_error = exc
            if raw is None:
                stats.posts_failed += 1
                print(f"@{post['username']} {post['shortcode']}: metric refresh failed: {last_error}")
                continue

            metrics = extract_post_metrics(raw)
            if metrics["views"] is not None:
                stats.views_available += 1
            if metrics["reposts"] is not None:
                stats.reposts_available += 1
            changed = db.update_post_metrics(int(post["id"]), metrics)
            db.store_raw_payload(
                "post_metrics",
                int(post["id"]),
                int(post["profile_id"]),
                {**raw, "_collector": {"backend": "instagram-rest", "api": "media-info"}},
            )
            if changed:
                stats.posts_updated += 1
            else:
                stats.posts_unchanged += 1

    return stats
