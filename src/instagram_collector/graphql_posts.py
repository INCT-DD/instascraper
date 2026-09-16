from __future__ import annotations

from datetime import datetime
import json
import math
import re
from typing import Any, Dict, List, Optional, Set, Tuple

import httpx

from instagram_scraper import (
    AuthError,
    CollectionBlockedError,
    ProfileAccessError,
    RateLimitError,
    RateLimiter,
    ScrapeError,
    _normalize_v1_item,
    _raise_for_instagram_redirect,
    extract_post_metrics,
    load_cookies,
    parse_post_metadata,
    retry_after_seconds,
)


TIMELINE_CONNECTION = "xdt_api__v1__feed__user_timeline_graphql_connection"
TIMELINE_RELAY_VARIABLE = "__relay_internal__pv__PolarisFeedShareMenurelayprovider"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
)


def normalize_graphql_post(raw: Dict[str, Any], doc_id: str) -> Dict[str, Any]:
    if not (raw.get("pk") or raw.get("id")) or not raw.get("code") or not raw.get("taken_at"):
        raise ScrapeError("Instagram GraphQL returned a post without identity or publication date.")
    post = parse_post_metadata(_normalize_v1_item(raw))
    post.update(extract_post_metrics(raw))
    post["collection_source"] = "instagram-graphql"
    post["raw_json"] = {
        **raw,
        "_collector": {"backend": "instagram-graphql", "api": "graphql", "doc_id": doc_id},
    }
    return post


class InstagramGraphqlPostCollector:
    def __init__(
        self,
        cookie_json_path: str,
        rps: float,
        doc_id: str,
        limiter: Optional[RateLimiter] = None,
    ) -> None:
        if not doc_id.strip().isascii() or not doc_id.strip().isdigit():
            raise ValueError("INSTAGRAM_TIMELINE_DOC_ID must contain only digits.")
        self.cookies = load_cookies(cookie_json_path)
        self.doc_id = doc_id.strip()
        self.limiter = limiter if limiter is not None else RateLimiter(rps)
        self.client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "InstagramGraphqlPostCollector":
        limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
        self.client = httpx.AsyncClient(
            cookies=self.cookies,
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
            raise RuntimeError("InstagramGraphqlPostCollector must be used as an async context manager.")
        username = username.lstrip("@")
        if not re.fullmatch(r"[A-Za-z0-9._]+", username):
            raise ValueError("Invalid Instagram username.")
        if date_to < date_from:
            raise ValueError("End date must not precede start date.")

        start = int(date_from.timestamp())
        end = int(date_to.timestamp())
        collected: List[Dict[str, Any]] = []
        seen_posts = set()
        seen_cursors = set()
        known_ids = stop_post_ids or set()
        after: Optional[str] = None

        while True:
            await self.limiter.wait()
            nodes, page_info = await self._fetch_page(username, after)
            regular_timestamps = []
            reached_known_post = False
            for raw in nodes:
                taken_at = raw.get("taken_at")
                if isinstance(taken_at, bool) or not isinstance(taken_at, (int, float)) or not math.isfinite(taken_at) or taken_at <= 0:
                    raise ScrapeError("Instagram GraphQL returned a post without a valid publication timestamp.")
                identity = str(raw.get("pk") or raw.get("id") or "")
                if not identity or not raw.get("code"):
                    raise ScrapeError("Instagram GraphQL returned a post without identity.")
                pinned = _is_pinned(raw)
                if not pinned:
                    regular_timestamps.append(taken_at)
                    reached_known_post = reached_known_post or identity in known_ids
                if taken_at < start or taken_at > end:
                    continue
                if identity in known_ids:
                    continue
                if identity not in seen_posts:
                    seen_posts.add(identity)
                    collected.append(normalize_graphql_post(raw, self.doc_id))

            if reached_known_post or (regular_timestamps and max(regular_timestamps) < start):
                break
            next_cursor = page_info.get("end_cursor")
            if not page_info["has_next_page"]:
                break
            if next_cursor in seen_cursors:
                raise ScrapeError("Instagram GraphQL repeated a pagination cursor.")
            seen_cursors.add(next_cursor)
            after = str(next_cursor)

        return collected

    async def _fetch_page(
        self,
        username: str,
        after: Optional[str],
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        assert self.client is not None
        variables: Dict[str, Any] = {
            "data": {
                "count": 12,
                "include_relationship_info": True,
                "latest_besties_reel_media": True,
                "latest_reel_media": True,
            },
            "username": username,
            TIMELINE_RELAY_VARIABLE: False,
        }
        if after:
            variables.update({"after": after, "before": None, "first": 12, "last": None})
        response = await self.client.post(
            "https://www.instagram.com/graphql/query",
            data={
                "variables": json.dumps(variables, separators=(",", ":")),
                "doc_id": self.doc_id,
                "server_timestamps": "true",
            },
            headers=_headers(self.cookies["csrftoken"], username),
            follow_redirects=False,
        )
        try:
            _raise_for_instagram_redirect(response, "timeline GraphQL")
        except AuthError as exc:
            raise CollectionBlockedError(str(exc)) from exc
        if response.status_code in {401, 403}:
            raise ProfileAccessError(f"Instagram timeline GraphQL refused this profile request (HTTP {response.status_code}).")
        if response.status_code == 429:
            retry_after = response.headers.get("retry-after", "not supplied")
            raise RateLimitError(
                f"Instagram timeline GraphQL rate limited the session (HTTP 429; Retry-After: {retry_after}).",
                retry_after_seconds(response.headers.get("retry-after")),
            )
        payload = _response_payload(response)
        messages = [payload.get("message")]
        if isinstance(payload.get("errors"), list):
            messages.extend(error.get("message") for error in payload["errors"] if isinstance(error, dict))
        for message in messages:
            if message in ("feedback_required", "challenge_required", "checkpoint_required", "login_required"):
                raise CollectionBlockedError(f"Instagram timeline GraphQL returned {message}.")
        if response.status_code != 200:
            raise ScrapeError(f"Instagram timeline GraphQL failed with HTTP {response.status_code}.")
        if payload.get("status") not in {None, "ok"}:
            raise ScrapeError(f"Instagram timeline GraphQL returned status {payload.get('status')}.")
        if payload.get("errors"):
            raise ScrapeError("Instagram timeline GraphQL returned errors; partial data was not accepted.")

        data = payload.get("data")
        connection = data.get(TIMELINE_CONNECTION) if isinstance(data, dict) else None
        if not isinstance(connection, dict):
            raise ScrapeError("Instagram timeline GraphQL response did not include the posts connection.")
        edges = connection.get("edges")
        page_info = connection.get("page_info")
        if not isinstance(edges, list) or not isinstance(page_info, dict):
            raise ScrapeError("Instagram timeline GraphQL returned invalid pagination data.")
        if not isinstance(page_info.get("has_next_page"), bool):
            raise ScrapeError("Instagram timeline GraphQL omitted the pagination completion flag.")
        if page_info["has_next_page"] and (
            not edges or not isinstance(page_info.get("end_cursor"), str) or not page_info["end_cursor"]
        ):
            raise ScrapeError("Instagram timeline GraphQL indicated another page without posts or cursor.")
        nodes = [edge.get("node") for edge in edges if isinstance(edge, dict)]
        if any(not isinstance(node, dict) for node in nodes) or len(nodes) != len(edges):
            raise ScrapeError("Instagram timeline GraphQL returned an invalid post node.")
        return nodes, page_info


def _headers(csrf_token: str, username: str) -> Dict[str, str]:
    return {
        "User-Agent": USER_AGENT,
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.8",
        "Origin": "https://www.instagram.com",
        "Referer": f"https://www.instagram.com/{username}/",
        "X-Instagram-AJAX": "1",
        "X-Requested-With": "XMLHttpRequest",
        "X-IG-App-ID": "936619743392459",
        "X-CSRFToken": csrf_token,
    }


def _response_payload(response: httpx.Response) -> Dict[str, Any]:
    try:
        payload = response.json()
    except ValueError as exc:
        raise ScrapeError("Instagram timeline GraphQL returned a non-JSON response.") from exc
    if not isinstance(payload, dict):
        raise ScrapeError("Instagram timeline GraphQL returned an invalid JSON response.")
    return payload


def _is_pinned(raw: Dict[str, Any]) -> bool:
    return bool(raw.get("timeline_pinned_user_ids") or raw.get("clips_tab_pinned_user_ids"))
