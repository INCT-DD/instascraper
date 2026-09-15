from __future__ import annotations

import asyncio
from datetime import datetime
import json
import logging
import math
import os
from pathlib import Path
import re
import sys
from typing import Any, Dict, Iterable, List

import httpx

from instagram_scraper import CollectionBlockedError, ProfileAccessError, RateLimitError, RateLimiter, ScrapeError, _first_int, _normalize_v1_item, parse_post_metadata

from .config import Settings
from .gallerydl import GalleryDlStoryCollector
from .graphql_posts import InstagramGraphqlPostCollector
from .scraper import InstagramCollector
from .sessions import CollectorSession


def normalize_gallery_post(raw: Dict[str, Any], version: str) -> Dict[str, Any]:
    if not (raw.get("pk") or raw.get("id")) or not raw.get("code") or not raw.get("taken_at"):
        raise ScrapeError("gallery-dl returned a post without identity or publication date.")
    post = parse_post_metadata(_normalize_v1_item(raw))
    post["likes"] = _first_int(raw.get("like_count"))
    post["comments_count"] = _first_int(raw.get("comment_count"))
    post["views"] = _first_int(raw.get("play_count"), raw.get("view_count"))
    post["collection_source"] = "gallery-dl"
    post["raw_json"] = {**raw, "_collector": {"backend": "gallery-dl", "version": version, "api": "rest"}}
    return post


def posts_in_window(posts: Iterable[Dict[str, Any]], start: int, end: int) -> Iterable[Dict[str, Any]]:
    seen = set()
    older_count = 0
    # A full feed page of older, unpinned posts ends the chronological scan.
    for raw in posts:
        taken_at = raw.get("taken_at")
        if not isinstance(taken_at, (int, float)) or not math.isfinite(taken_at) or taken_at <= 0:
            raise ScrapeError("gallery-dl returned a post without a valid publication timestamp.")
        if taken_at < start:
            if not raw.get("timeline_pinned_user_ids") and not raw.get("clips_tab_pinned_user_ids"):
                older_count += 1
                if older_count >= 30:
                    break
            continue
        older_count = 0
        if taken_at > end:
            continue
        identity = str(raw.get("pk") or raw.get("id") or "")
        if not identity or not raw.get("code"):
            raise ScrapeError("gallery-dl returned a post without identity.")
        if identity not in seen:
            seen.add(identity)
            yield raw


def _extract_raw_posts(request: Dict[str, Any]) -> Dict[str, Any]:
    from gallery_dl import config, extractor, version

    config.set(("extractor",), "sleep-request", request["sleep_request"])
    config.set(("extractor",), "retries", 2)
    config.set(("extractor",), "timeout", 25)
    config.set(("extractor", "instagram"), "api", "rest")
    config.set(("extractor", "instagram"), "user-cache", "memory")
    config.set(("extractor", "instagram"), "user-strategy", ["search", "web"])
    config.set(("extractor", "instagram"), "cookies", request["cookies"])
    username = request["username"].lstrip("@")
    if not re.fullmatch(r"[A-Za-z0-9._]+", username):
        raise ValueError("Invalid Instagram username.")
    ex = extractor.find(f"https://www.instagram.com/{username}/posts/")
    if ex is None or not callable(getattr(ex, "posts", None)):
        raise RuntimeError("Installed gallery-dl does not provide the Instagram posts extractor.")
    ex.request_interval_min = 1.0 / request["rps"]
    try:
        ex.initialize()
        ex.login()
        posts = list(posts_in_window(ex.posts(), request["start"], request["end"]))
        return {"posts": posts, "version": version.__version__}
    finally:
        if ex.session is not None:
            ex.session.close()


def _extract_raw_post(request: Dict[str, Any]) -> Dict[str, Any]:
    from gallery_dl import config, extractor, version

    config.set(("extractor",), "sleep-request", request["sleep_request"])
    config.set(("extractor",), "retries", 2)
    config.set(("extractor",), "timeout", 25)
    config.set(("extractor", "instagram"), "api", "rest")
    config.set(("extractor", "instagram"), "cookies", request["cookies"])
    shortcode = request["shortcode"]
    if not re.fullmatch(r"[A-Za-z0-9_-]+", shortcode):
        raise ValueError("Invalid Instagram shortcode.")
    ex = extractor.find(f"https://www.instagram.com/p/{shortcode}/")
    if ex is None or not callable(getattr(ex, "posts", None)):
        raise RuntimeError("Installed gallery-dl does not provide the Instagram post extractor.")
    try:
        ex.initialize()
        ex.login()
        posts = list(ex.posts())
        post = next((item for item in posts if str(item.get("code")) == shortcode), None)
        if post is None:
            raise ScrapeError(f"gallery-dl did not return post {shortcode}.")
        return {"post": post, "version": version.__version__}
    finally:
        if ex.session is not None:
            ex.session.close()


async def _run_gallery_request(settings: Settings, request: Dict[str, Any]) -> Dict[str, Any]:
    child_env = os.environ.copy()
    child_env["PYTHONPATH"] = os.pathsep.join(filter(None, (
        str(Path(__file__).resolve().parents[1]), child_env.get("PYTHONPATH"),
    )))
    process = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "instagram_collector.gallerydl_posts",
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        env=child_env,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(json.dumps(request).encode("utf-8")),
            timeout=settings.gallery_dl_timeout_seconds,
        )
    except asyncio.TimeoutError as exc:
        if process.returncode is None:
            process.kill()
        await process.communicate()
        raise ScrapeError(f"gallery-dl posts timed out after {settings.gallery_dl_timeout_seconds}s.") from exc
    except BaseException:
        if process.returncode is None:
            process.kill()
        await process.communicate()
        raise
    if process.returncode:
        error = stderr.decode("utf-8", errors="replace").strip()[-1500:]
        error = re.sub(r"https?://\S+", "[Instagram endpoint]", error)
        if re.search(r"Too Many Requests|\b429\b", error, re.IGNORECASE):
            raise RateLimitError(f"gallery-dl posts rate limited the session: {error}")
        if re.search(r"feedback_required|challenge_required|checkpoint_required|login_required|AuthRequired", error, re.IGNORECASE):
            raise CollectionBlockedError(f"gallery-dl posts restricted the session: {error}")
        raise ScrapeError(f"gallery-dl posts failed: {error or 'extractor process failed'}")
    try:
        return json.loads(stdout)
    except (ValueError, TypeError) as exc:
        raise ScrapeError("Invalid raw-post response from gallery-dl.") from exc


async def fetch_gallery_posts(
    settings: Settings,
    session: CollectorSession,
    username: str,
    date_from: datetime,
    date_to: datetime,
    rps: float,
) -> List[Dict[str, Any]]:
    if not math.isfinite(rps) or rps <= 0:
        raise ValueError("RPS must be finite and greater than zero.")
    if date_to < date_from:
        raise ValueError("End date must not precede start date.")
    request = {
        "username": username,
        "start": int(date_from.timestamp()),
        "end": int(date_to.timestamp()),
        "rps": rps,
        "sleep_request": settings.gallery_dl_sleep_request,
        "cookies": GalleryDlStoryCollector(settings)._gallery_cookies(session),
    }
    # Isolate gallery-dl's global config and synchronous HTTP client per session.
    response = await _run_gallery_request(settings, request)
    try:
        return [normalize_gallery_post(raw, response["version"]) for raw in response["posts"]]
    except (ValueError, KeyError, TypeError) as exc:
        raise ScrapeError("Invalid raw-post response from gallery-dl.") from exc


async def fetch_gallery_post(
    settings: Settings,
    session: CollectorSession,
    shortcode: str,
) -> Dict[str, Any]:
    request = {
        "operation": "post",
        "shortcode": shortcode,
        "sleep_request": settings.gallery_dl_sleep_request,
        "cookies": GalleryDlStoryCollector(settings)._gallery_cookies(session),
    }
    response = await _run_gallery_request(settings, request)
    try:
        return normalize_gallery_post(response["post"], response["version"])
    except (ValueError, KeyError, TypeError) as exc:
        raise ScrapeError("Invalid single-post response from gallery-dl.") from exc


async def fetch_posts_with_backend(
    settings: Settings,
    session: CollectorSession,
    username: str,
    date_from: datetime,
    date_to: datetime,
    rps: float,
    limiter: RateLimiter | None = None,
) -> List[Dict[str, Any]]:
    backend = settings.posts_backend
    if backend not in {"auto", "graphql", "scraper", "gallery-dl"}:
        raise ValueError("POSTS_BACKEND must be auto, graphql, scraper or gallery-dl.")
    graphql_error = None
    if backend in {"auto", "graphql"}:
        try:
            async with InstagramGraphqlPostCollector(
                session.instagram_cookie_json, rps, settings.instagram_timeline_doc_id, limiter=limiter,
            ) as scraper:
                return await scraper.fetch_profile_posts(username, date_from, date_to)
        except (CollectionBlockedError, ProfileAccessError):
            raise
        except (ScrapeError, httpx.HTTPError) as exc:
            if backend == "graphql":
                raise
            graphql_error = str(exc)
            print(f"@{username}: {type(exc).__name__} in GraphQL; trying the REST scraper.")
    if backend != "gallery-dl":
        try:
            async with InstagramCollector(session.instagram_cookie_json, rps, limiter=limiter) as scraper:
                return await scraper.fetch_profile_posts(username, date_from, date_to)
        except CollectionBlockedError:
            raise
        except (ScrapeError, httpx.HTTPError) as exc:
            if backend == "scraper":
                raise
            print(f"@{username}: {type(exc).__name__} in scraper; trying gallery-dl with raw metadata.")
            try:
                return await fetch_gallery_posts(settings, session, username, date_from, date_to, rps)
            except CollectionBlockedError:
                raise
            except Exception as fallback_error:
                detail = f"GraphQL failed: {graphql_error}; " if graphql_error else ""
                raise ScrapeError(f"{detail}Scraper failed: {exc}; fallback failed: {fallback_error}") from fallback_error
    return await fetch_gallery_posts(settings, session, username, date_from, date_to, rps)


def main() -> None:
    logging.basicConfig(stream=sys.stderr, level=logging.WARNING)
    try:
        request = json.load(sys.stdin)
        response = _extract_raw_post(request) if request.get("operation") == "post" else _extract_raw_posts(request)
        json.dump(response, sys.stdout)
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
