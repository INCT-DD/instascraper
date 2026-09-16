from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import httpx

from instagram_scraper import (
    AuthError,
    RateLimiter,
    ScrapeError,
    build_cookie_string,
    fetch_comments_for_post,
    fetch_posts_page,
    fetch_replies_for_comment,
    fetch_user_id,
    load_cookies,
    parse_post_metadata,
)


class InstagramCollector:
    """Non-interactive wrapper around the existing scraper engine."""

    def __init__(self, cookie_json_path: str, rps: float, limiter: Optional[RateLimiter] = None) -> None:
        self.cookie_json_path = cookie_json_path
        self.rps = rps
        self.limiter = limiter if limiter is not None else RateLimiter(rps)
        self.cookie_str = build_cookie_string(load_cookies(cookie_json_path))
        self.client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "InstagramCollector":
        limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
        self.client = httpx.AsyncClient(
            http2=True,
            timeout=httpx.Timeout(25.0, connect=10.0),
            limits=limits,
        )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self.client:
            await self.client.aclose()

    async def fetch_profile_posts(
        self,
        username: str,
        date_from: datetime,
        date_to: datetime,
        stop_post_ids: Optional[Set[str]] = None,
    ) -> List[Dict[str, Any]]:
        if not self.client:
            raise RuntimeError("InstagramCollector must be used as an async context manager.")

        user_id = await fetch_user_id(self.client, username, self.cookie_str, limiter=self.limiter)
        collected_posts: List[Dict[str, Any]] = []
        after = None
        ts_from = int(date_from.timestamp())
        ts_to = int(date_to.timestamp())
        known_ids = stop_post_ids or set()

        while True:
            await self.limiter.wait()
            posts_raw, has_next, after = await fetch_posts_page(
                self.client,
                user_id,
                self.cookie_str,
                username,
                after,
            )

            regular_timestamps = []
            reached_known_post = False

            for node in posts_raw:
                identity = str(node.get("id") or node.get("pk") or "")
                pinned = bool(
                    node.get("timeline_pinned_user_ids") or node.get("clips_tab_pinned_user_ids")
                )
                taken_at = node.get("taken_at") or node.get("taken_at_timestamp", 0)
                if not pinned:
                    regular_timestamps.append(taken_at)
                if identity in known_ids and not pinned:
                    reached_known_post = True
                    continue
                if identity in known_ids:
                    continue
                if taken_at > ts_to:
                    continue
                if taken_at < ts_from:
                    continue
                collected_posts.append(parse_post_metadata(node))

            if reached_known_post or (regular_timestamps and max(regular_timestamps) < ts_from):
                break
            if not has_next or not after:
                break

        return collected_posts

    async def fetch_post_comments(
        self,
        shortcode: str,
        fetch_replies: bool = False,
        max_comments: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        if not self.client:
            raise RuntimeError("InstagramCollector must be used as an async context manager.")
        return await fetch_comments_for_post(
            self.client,
            shortcode,
            self.cookie_str,
            self.limiter,
            fetch_replies=fetch_replies,
            max_comments=max_comments,
        )

    async def fetch_comment_replies(
        self,
        comment_id: str,
        cursor: Optional[str],
        shortcode: str,
    ) -> List[Dict[str, Any]]:
        if not self.client:
            raise RuntimeError("InstagramCollector must be used as an async context manager.")
        if not cursor:
            return []
        return await fetch_replies_for_comment(
            self.client,
            comment_id,
            cursor,
            self.cookie_str,
            f"https://www.instagram.com/p/{shortcode}/",
            self.limiter,
        )


__all__ = ["AuthError", "InstagramCollector", "ScrapeError"]
