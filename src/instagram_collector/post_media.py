from __future__ import annotations

import mimetypes
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlparse

import httpx

from .files import safe_name


@dataclass
class PostMediaDownloadStats:
    attempted: int = 0
    downloaded: int = 0
    failed: int = 0


async def download_post_media(
    posts: List[Dict[str, Any]],
    base_dir: str,
    run_date: date,
    candidate_name: str,
    timeout_seconds: int,
) -> PostMediaDownloadStats:
    stats = PostMediaDownloadStats()
    timeout = httpx.Timeout(float(timeout_seconds), connect=15.0)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Referer": "https://www.instagram.com/",
    }
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=headers) as client:
        for post in posts:
            assets = post.get("media_assets") or []
            if not isinstance(assets, list):
                continue
            for asset in assets:
                if not isinstance(asset, dict) or not asset.get("url"):
                    continue
                stats.attempted += 1
                try:
                    media_path = await _download_asset(client, asset, base_dir, run_date, candidate_name, post)
                    asset["local_path"] = str(media_path)
                    asset["download_status"] = "success"
                    stats.downloaded += 1
                except Exception as exc:
                    asset["download_status"] = "failed"
                    asset["download_error"] = str(exc)[:500]
                    stats.failed += 1
    return stats


async def download_post_media_asset(
    asset: Dict[str, Any],
    post: Dict[str, Any],
    base_dir: str,
    run_date: date,
    candidate_name: str,
    timeout_seconds: int,
) -> Path:
    timeout = httpx.Timeout(float(timeout_seconds), connect=15.0)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Referer": "https://www.instagram.com/",
    }
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=headers) as client:
        return await _download_asset(client, asset, base_dir, run_date, candidate_name, post)


async def _download_asset(
    client: httpx.AsyncClient,
    asset: Dict[str, Any],
    base_dir: str,
    run_date: date,
    candidate_name: str,
    post: Dict[str, Any],
) -> Path:
    shortcode = safe_name(str(post.get("shortcode") or post.get("post_id") or "post"))
    index = int(asset.get("index") or 1)
    media_type = str(asset.get("media_type") or "media")
    response = await client.get(str(asset["url"]))
    response.raise_for_status()
    extension = _extension(asset, response.headers.get("content-type", ""))
    target_dir = Path(base_dir) / safe_name(candidate_name) / "media" / run_date.isoformat() / shortcode
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / f"{index:02d}_{media_type}{extension}"
    target.write_bytes(response.content)
    return target


def _extension(asset: Dict[str, Any], content_type: str) -> str:
    parsed = urlparse(str(asset.get("url") or ""))
    suffix = Path(parsed.path).suffix.lower()
    if suffix and len(suffix) <= 8:
        return suffix
    guessed = mimetypes.guess_extension(content_type.split(";", 1)[0].strip())
    if guessed:
        return guessed
    if str(asset.get("media_type")) == "video":
        return ".mp4"
    return ".jpg"
