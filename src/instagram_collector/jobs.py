from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import time
from typing import Any, Dict, Optional

from instagram_scraper import AuthError, ScrapeError, parse_comment_node

from .config import Settings, load_sessions
from .files import write_profile_comments
from .scraper import InstagramCollector
from .sessions import CollectorSession, SessionPool
from .storage import Database


JOB_TYPE_COMMENTS = "comments"
JOB_TYPE_REPLIES = "replies"


@dataclass
class JobStats:
    processed: int = 0
    failed: int = 0
    comments_inserted: int = 0
    replies_inserted: int = 0


class JobProcessor:
    def __init__(
        self,
        db: Database,
        settings: Settings,
        rps: Optional[float] = None,
        session_pool: Optional[SessionPool] = None,
    ) -> None:
        self.db = db
        self.settings = settings
        self.rps = rps if rps is not None else settings.rps
        self.session_pool = session_pool or SessionPool(
            load_sessions(settings),
            settings.account_rotation_enabled,
        )

    async def process_pending_jobs(self, limit: Optional[int] = None) -> JobStats:
        job_limit = limit if limit is not None else self.settings.job_limit_per_run
        jobs = self.db.fetch_pending_jobs(job_limit, job_types=(JOB_TYPE_COMMENTS, JOB_TYPE_REPLIES))
        stats = JobStats()
        started_at = time.monotonic()
        if not jobs:
            print("No pending jobs.")
            return stats

        for job in jobs:
            if self._time_limit_reached(started_at):
                print("Comment queue time limit reached; remaining jobs stay pending.")
                break
            self.db.mark_job_started(job["id"])
            try:
                counts = await self._process_job_with_sessions(job)
                self.db.mark_job_success(job["id"])
                stats.processed += 1
                stats.comments_inserted += counts.get("comments_inserted", 0)
                stats.replies_inserted += counts.get("replies_inserted", 0)
                print(f"Job {job['id']} done: {job['job_type']}")
            except (AuthError, ScrapeError, Exception) as exc:
                self.db.mark_job_failed(job["id"], str(exc))
                stats.failed += 1
                print(f"Job {job['id']} failed: {exc}")

        return stats

    async def _process_job_with_sessions(self, job: Dict[str, Any]) -> Dict[str, int]:
        session = self.session_pool.next()
        failures = []
        for candidate in [session, *self.session_pool.alternatives(session)]:
            try:
                counts = await self._process_job_with_session(candidate, job)
                return counts
            except (AuthError, ScrapeError) as exc:
                failures.append(f"{candidate.alias}: {exc}")

        if failures:
            raise ScrapeError("All configured sessions failed for job: " + " ; ".join(failures))
        raise RuntimeError("No collector session could process the job.")

    async def _process_job_with_session(
        self,
        session: CollectorSession,
        job: Dict[str, Any],
    ) -> Dict[str, int]:
        async with InstagramCollector(session.instagram_cookie_json, self.rps) as scraper:
            if job["job_type"] == JOB_TYPE_COMMENTS:
                return await self._process_comment_job(scraper, job)
            if job["job_type"] == JOB_TYPE_REPLIES:
                return await self._process_reply_job(scraper, job)
            raise ValueError(f"Unknown job_type: {job['job_type']}")

    async def _process_comment_job(
        self,
        scraper: InstagramCollector,
        job: Dict[str, Any],
    ) -> Dict[str, int]:
        post = self.db.get_post(job["post_id"])
        if not post:
            raise ValueError(f"Post not found for job {job['id']}")
        profile = self.db.get_profile(job["profile_id"]) if job.get("profile_id") else None

        comments = await scraper.fetch_post_comments(
            job["shortcode"],
            fetch_replies=False,
            max_comments=self.settings.max_comments_per_post,
        )
        comments_inserted = 0
        profile_username = profile["username"] if profile else "unknown"
        write_profile_comments(
            self.settings.data_dir,
            date.today(),
            profile_username,
            job["shortcode"],
            comments,
        )
        for comment in comments:
            comment_id, inserted = self.db.save_comment(post["id"], comment)
            if inserted:
                comments_inserted += 1
                self.db.store_raw_payload("comment", comment_id, post["profile_id"], comment.get("raw_json", comment))

            if profile and bool(profile.get("collect_replies")):
                cursor = self._reply_cursor(comment)
                if self._has_reply_payload(comment):
                    self.db.enqueue_job(
                        JOB_TYPE_REPLIES,
                        profile_id=post["profile_id"],
                        post_id=post["id"],
                        comment_id=comment_id,
                        shortcode=post["shortcode"],
                        cursor=cursor,
                        priority=int(profile.get("priority", 0)),
                        max_attempts=self.settings.max_job_attempts,
                    )

        return {"comments_inserted": comments_inserted}

    async def _process_reply_job(
        self,
        scraper: InstagramCollector,
        job: Dict[str, Any],
    ) -> Dict[str, int]:
        comment = self.db.get_comment(job["comment_id"])
        if not comment:
            raise ValueError(f"Comment not found for job {job['id']}")

        inserted = 0
        raw_json = comment.get("raw_json") or {}
        thread = raw_json.get("edge_threaded_comments", {})
        for edge in thread.get("edges", []):
            reply = parse_comment_node(edge.get("node", {}))
            reply_id, was_inserted = self.db.save_reply(comment["id"], reply)
            if was_inserted:
                inserted += 1
                self.db.store_raw_payload("reply", reply_id, job.get("profile_id"), reply.get("raw_json", reply))

        cursor = job.get("cursor")
        if cursor:
            replies = await scraper.fetch_comment_replies(
                comment["platform_comment_id"],
                cursor,
                job["shortcode"],
            )
            for reply in replies:
                reply_id, was_inserted = self.db.save_reply(comment["id"], reply)
                if was_inserted:
                    inserted += 1
                    self.db.store_raw_payload("reply", reply_id, job.get("profile_id"), reply.get("raw_json", reply))

        return {"replies_inserted": inserted}

    def _reply_cursor(self, comment: Dict[str, Any]) -> Optional[str]:
        raw_json = comment.get("raw_json") or {}
        page_info = raw_json.get("edge_threaded_comments", {}).get("page_info", {})
        if page_info.get("has_next_page"):
            return page_info.get("end_cursor")
        return None

    def _has_reply_payload(self, comment: Dict[str, Any]) -> bool:
        raw_json = comment.get("raw_json") or {}
        thread = raw_json.get("edge_threaded_comments", {})
        return bool(thread.get("edges") or thread.get("page_info", {}).get("has_next_page"))

    def _time_limit_reached(self, started_at: float) -> bool:
        limit = self.settings.comment_queue_time_limit_seconds
        return limit is not None and (time.monotonic() - started_at) >= limit
