from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Optional

from .config import Settings, load_sessions
from .gallerydl import GalleryDlStoryCollector
from .post_media import download_post_media_asset
from .sessions import SessionPool
from .storage import Database


JOB_TYPE_POST_MEDIA = "post_media"
JOB_TYPE_STORIES = "stories"


@dataclass
class MediaJobStats:
    processed: int = 0
    failed: int = 0
    post_media_downloaded: int = 0
    post_media_failed: int = 0
    stories_saved: int = 0


class MediaJobProcessor:
    def __init__(
        self,
        db: Database,
        settings: Settings,
        session_pool: Optional[SessionPool] = None,
    ) -> None:
        self.db = db
        self.settings = settings
        self.session_pool = session_pool or SessionPool(
            load_sessions(settings),
            settings.account_rotation_enabled,
        )
        self.story_collector = GalleryDlStoryCollector(settings)

    async def process_pending_jobs(self, limit: Optional[int] = None, quiet: bool = False) -> MediaJobStats:
        job_limit = limit if limit is not None else self.settings.job_limit_per_run
        jobs = self.db.fetch_pending_jobs(job_limit, job_types=(JOB_TYPE_STORIES, JOB_TYPE_POST_MEDIA))
        stats = MediaJobStats()
        if not jobs:
            if not quiet:
                print("No pending media jobs.")
            return stats

        for job in jobs:
            self.db.mark_job_started(job["id"])
            try:
                counts = await self._process_job(job)
                self.db.mark_job_success(job["id"])
                stats.processed += 1
                stats.post_media_downloaded += counts.get("post_media_downloaded", 0)
                stats.post_media_failed += counts.get("post_media_failed", 0)
                stats.stories_saved += counts.get("stories_saved", 0)
                print(f"Media job {job['id']} done: {job['job_type']}")
            except Exception as exc:
                self.db.mark_job_failed(job["id"], str(exc))
                stats.failed += 1
                print(f"Media job {job['id']} failed: {exc}")

        return stats

    async def watch(self, limit: Optional[int], sleep_seconds: Optional[int] = None) -> None:
        delay = sleep_seconds if sleep_seconds is not None else self.settings.media_worker_sleep_seconds
        while True:
            await self.process_pending_jobs(limit, quiet=True)
            await asyncio.sleep(max(1, delay))

    async def _process_job(self, job: Dict[str, Any]) -> Dict[str, int]:
        if job["job_type"] == JOB_TYPE_POST_MEDIA:
            return await self._process_post_media_job(job)
        if job["job_type"] == JOB_TYPE_STORIES:
            return await self._process_story_job(job)
        raise ValueError(f"Unknown media job_type: {job['job_type']}")

    async def _process_post_media_job(self, job: Dict[str, Any]) -> Dict[str, int]:
        post = self.db.get_post(job["post_id"])
        if not post:
            raise ValueError(f"Post not found for media job {job['id']}")
        profile = self.db.get_profile(post["profile_id"])
        candidate_name = str((profile or {}).get("name") or (profile or {}).get("username") or "unknown")
        run_date = self._post_run_date(post)

        downloaded = 0
        failed = 0
        media_rows = self.db.list_post_media_for_post(post["id"], only_pending=True)
        if not media_rows:
            return {"post_media_downloaded": 0, "post_media_failed": 0}

        for media in media_rows:
            asset = dict(media.get("raw_json") or {})
            asset.setdefault("url", media.get("source_url"))
            asset.setdefault("source_url", media.get("source_url"))
            asset.setdefault("index", media.get("media_index"))
            asset.setdefault("media_type", media.get("media_type") or "media")
            try:
                local_path = await download_post_media_asset(
                    asset,
                    post,
                    self.settings.post_media_dir,
                    run_date,
                    candidate_name,
                    self.settings.post_media_timeout_seconds,
                )
                asset["local_path"] = str(local_path)
                asset["download_status"] = "success"
                self.db.update_post_media_download(media["id"], "success", str(local_path), raw_json=asset)
                downloaded += 1
            except Exception as exc:
                failed += 1
                asset["download_status"] = "failed"
                asset["download_error"] = str(exc)[:500]
                self.db.update_post_media_download(media["id"], "failed", error_message=str(exc), raw_json=asset)

        if failed:
            raise RuntimeError(f"{failed} post media files failed; {downloaded} downloaded.")
        return {"post_media_downloaded": downloaded, "post_media_failed": failed}

    async def _process_story_job(self, job: Dict[str, Any]) -> Dict[str, int]:
        profile = self.db.get_profile(job["profile_id"])
        if not profile:
            raise ValueError(f"Profile not found for story job {job['id']}")
        run_date = date.fromisoformat(str(job.get("cursor") or date.today().isoformat()))
        session = self.session_pool.next()
        result = self.story_collector.collect_profile_stories(
            profile["username"],
            run_date,
            [session, *self.session_pool.alternatives(session)],
            candidate_name=str(profile.get("name") or profile["username"]),
        )
        if result.status == "failed":
            raise RuntimeError(result.error_message or "gallery-dl story collection failed.")

        stories_saved = 0
        if result.output_dir:
            for story in self.story_collector.load_story_metadata(result.output_dir):
                story_id, inserted = self.db.upsert_story(profile["id"], story)
                self.db.store_raw_payload("story", story_id, profile["id"], story.get("raw_json", story))
                if inserted:
                    stories_saved += 1
        return {"stories_saved": stories_saved}

    def _post_run_date(self, post: Dict[str, Any]) -> date:
        taken_at_iso = str(post.get("taken_at_iso") or "")
        if len(taken_at_iso) >= 10:
            try:
                return date.fromisoformat(taken_at_iso[:10])
            except ValueError:
                pass
        return date.today()
