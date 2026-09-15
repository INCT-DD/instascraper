from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .config import Settings, load_sessions
from .gallerydl_posts import fetch_gallery_post
from .sessions import SessionPool
from .storage import Database


@dataclass
class MediaRefreshStats:
    posts_found: int = 0
    posts_refreshed: int = 0
    assets_refreshed: int = 0
    posts_failed: int = 0


async def refresh_failed_post_media(
    db: Database,
    settings: Settings,
    date_from: str,
    date_to_exclusive: str,
    limit: Optional[int] = None,
) -> MediaRefreshStats:
    candidates = db.list_post_media_refresh_candidates(date_from, date_to_exclusive, limit)
    stats = MediaRefreshStats(posts_found=len(candidates))
    if not candidates:
        return stats

    pool = SessionPool(load_sessions(settings), settings.account_rotation_enabled)
    for post in candidates:
        if not db.claim_post_media_refresh(post["media_job_id"]):
            continue
        first_session = pool.next()
        sessions = [first_session, *pool.alternatives(first_session)]
        refreshed_post = None
        last_error: Optional[Exception] = None
        for session in sessions:
            try:
                refreshed_post = await fetch_gallery_post(settings, session, str(post["shortcode"]))
                break
            except Exception as exc:
                last_error = exc

        if refreshed_post is None:
            db.mark_job_failed(post["media_job_id"], str(last_error or "URL refresh failed"))
            stats.posts_failed += 1
            print(f"@{post['username']} {post['shortcode']}: URL refresh failed: {last_error}")
            continue

        refreshed = 0
        for media in refreshed_post.get("media_assets") or []:
            if isinstance(media, dict) and db.refresh_post_media_asset(post["id"], media):
                refreshed += 1
        if refreshed:
            db.reopen_post_media_job(post["media_job_id"])
            stats.posts_refreshed += 1
            stats.assets_refreshed += refreshed
            print(f"@{post['username']} {post['shortcode']}: {refreshed} media URLs refreshed.")
        else:
            remaining = db.list_post_media_for_post(post["id"], only_pending=True)
            if remaining:
                db.mark_job_failed(post["media_job_id"], "No matching failed media URLs were refreshed.")
                stats.posts_failed += 1
            else:
                db.mark_job_success(post["media_job_id"])
            print(f"@{post['username']} {post['shortcode']}: no failed media required refreshing.")

    return stats
