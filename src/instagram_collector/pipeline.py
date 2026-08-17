from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from instagram_scraper import AuthError, ScrapeError

from .config import Settings, load_profiles, load_sessions
from .files import ensure_runtime_dirs, write_candidate_archive, write_daily_report, write_profile_posts
from .gallerydl import GalleryDlStoryCollector
from .jobs import JobProcessor, JobStats
from .media_jobs import JOB_TYPE_POST_MEDIA, JOB_TYPE_STORIES
from .post_media import download_post_media
from .scraper import InstagramCollector
from .sessions import CollectorSession, SessionPool
from .storage import Database


@dataclass
class ProfileCollectionStats:
    posts_found: int = 0
    posts_inserted: int = 0
    posts_updated: int = 0
    jobs_enqueued: int = 0
    stories_found: int = 0
    media_attempted: int = 0
    media_downloaded: int = 0
    media_failed: int = 0
    media_jobs_enqueued: int = 0
    errors: int = 0


@dataclass
class PostCollectionAttempt:
    stats: Optional[ProfileCollectionStats]
    session_alias: Optional[str]
    errors: List[str]


def daily_window(target_date: date, margin_days: int) -> tuple[datetime, datetime]:
    start_date = target_date - timedelta(days=margin_days)
    date_from = datetime.combine(start_date, time.min, tzinfo=timezone.utc)
    date_to = datetime.combine(target_date, time(23, 59, 59), tzinfo=timezone.utc)
    return date_from, date_to


def daily_window_for_timezone(
    target_date: date,
    margin_days: int,
    timezone_name: str,
) -> tuple[datetime, datetime]:
    tz = _load_timezone(timezone_name)
    start_date = target_date - timedelta(days=margin_days)
    local_from = datetime.combine(start_date, time.min, tzinfo=tz)
    local_to = datetime.combine(target_date, time(23, 59, 59), tzinfo=tz)
    return local_from.astimezone(timezone.utc), local_to.astimezone(timezone.utc)


def explicit_window(date_from: date, date_to: date) -> tuple[datetime, datetime]:
    return (
        datetime.combine(date_from, time.min, tzinfo=timezone.utc),
        datetime.combine(date_to, time(23, 59, 59), tzinfo=timezone.utc),
    )


def _load_timezone(timezone_name: str):
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        fixed_offsets = {
            "America/Bahia": timezone(timedelta(hours=-3)),
            "America/Sao_Paulo": timezone(timedelta(hours=-3)),
            "UTC": timezone.utc,
        }
        if timezone_name in fixed_offsets:
            return fixed_offsets[timezone_name]
        raise


def seed_profiles(db: Database, settings: Settings) -> None:
    db.init_schema()
    db.seed_profiles(
        load_profiles(settings.profiles_path),
        collect_comments_default=settings.collect_comments_default,
        collect_replies_default=settings.collect_replies_default,
    )


def _new_daily_report(
    target_date: date,
    started_at: datetime,
    date_from: datetime,
    date_to: datetime,
    timezone_name: str,
    profiles_total: int,
) -> Dict[str, Any]:
    return {
        "run_date": target_date.isoformat(),
        "started_at": started_at.isoformat(),
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "timezone": timezone_name,
        "profiles_total": profiles_total,
        "profiles_success": 0,
        "profiles_error": 0,
        "posts_found": 0,
        "posts_inserted": 0,
        "posts_updated": 0,
        "stories_found": 0,
        "post_media_attempted": 0,
        "post_media_downloaded": 0,
        "post_media_failed": 0,
        "media_jobs_enqueued": 0,
        "story_jobs_enqueued": 0,
        "comment_jobs_enqueued": 0,
        "comments_inserted": 0,
        "replies_inserted": 0,
        "jobs_pending": 0,
        "profile_results": [],
        "files_generated": [],
        "errors": [],
    }


def _new_profile_result(username: str, session_alias: Optional[str]) -> Dict[str, Any]:
    return {
        "username": username,
        "status": "success",
        "posts_found": 0,
        "posts_inserted": 0,
        "posts_updated": 0,
        "stories_found": 0,
        "post_media_attempted": 0,
        "post_media_downloaded": 0,
        "post_media_failed": 0,
        "media_jobs_enqueued": 0,
        "story_jobs_enqueued": 0,
        "comment_jobs_enqueued": 0,
        "session_alias": session_alias,
        "story_session_alias": None,
        "errors": [],
    }


def _apply_post_stats(
    report: Dict[str, Any],
    profile_result: Dict[str, Any],
    stats: ProfileCollectionStats,
) -> None:
    profile_result["posts_found"] = stats.posts_found
    profile_result["posts_inserted"] = stats.posts_inserted
    profile_result["posts_updated"] = stats.posts_updated
    profile_result["comment_jobs_enqueued"] = stats.jobs_enqueued
    profile_result["post_media_attempted"] = stats.media_attempted
    profile_result["post_media_downloaded"] = stats.media_downloaded
    profile_result["post_media_failed"] = stats.media_failed
    profile_result["media_jobs_enqueued"] = stats.media_jobs_enqueued
    report["posts_found"] += stats.posts_found
    report["posts_inserted"] += stats.posts_inserted
    report["posts_updated"] += stats.posts_updated
    report["comment_jobs_enqueued"] += stats.jobs_enqueued
    report["post_media_attempted"] += stats.media_attempted
    report["post_media_downloaded"] += stats.media_downloaded
    report["post_media_failed"] += stats.media_failed
    report["media_jobs_enqueued"] += stats.media_jobs_enqueued


def _apply_profile_status(
    report: Dict[str, Any],
    profile_result: Dict[str, Any],
    collect_posts_enabled: bool,
    collect_stories_enabled: bool,
) -> None:
    if profile_result["errors"]:
        profile_result["status"] = "partial"
        report["profiles_error"] += 1
        return

    if (
        profile_result["posts_found"] == 0
        and profile_result["stories_found"] == 0
        and int(profile_result.get("media_jobs_enqueued", 0)) == 0
        and int(profile_result.get("story_jobs_enqueued", 0)) == 0
        and (collect_posts_enabled or collect_stories_enabled)
    ):
        profile_result["status"] = "empty_for_day"

    report["profiles_success"] += 1


def _record_profile_result(
    db: Database,
    daily_run_id: int,
    profile: Dict[str, Any],
    profile_result: Dict[str, Any],
) -> None:
    db.record_profile_status(
        run_id=daily_run_id,
        profile_id=profile.get("id"),
        handle=profile["username"],
        source="daily",
        session_alias=str(profile_result.get("session_alias") or profile_result.get("story_session_alias") or ""),
        status=profile_result["status"],
        posts_found=int(profile_result.get("posts_found", 0)),
        stories_found=int(profile_result.get("stories_found", 0)),
        posts_saved=int(profile_result.get("posts_inserted", 0)) + int(profile_result.get("posts_updated", 0)),
        stories_saved=int(profile_result.get("stories_saved", 0)),
        comments_enqueued=int(profile_result.get("comment_jobs_enqueued", 0)),
        error_type="profile_error" if profile_result.get("errors") else None,
        error_message=" ; ".join([str(error) for error in profile_result.get("errors", [])]) or None,
    )


async def _collect_profile_posts_with_sessions(
    db: Database,
    settings: Settings,
    username: str,
    date_from: datetime,
    date_to: datetime,
    enqueue_comments: bool,
    rps: Optional[float],
    session: CollectorSession,
    sessions: SessionPool,
    output_date: date,
    archive_date_from: date,
    archive_date_to: date,
) -> PostCollectionAttempt:
    last_error: Optional[Exception] = None
    errors = []
    for candidate in [session, *sessions.alternatives(session)]:
        try:
            stats = await collect_profile(
                db,
                settings,
                username,
                date_from,
                date_to,
                enqueue_comments=enqueue_comments,
                rps=rps,
                session=candidate,
                output_date=output_date,
                archive_date_from=archive_date_from,
                archive_date_to=archive_date_to,
            )
            return PostCollectionAttempt(stats=stats, session_alias=candidate.alias, errors=errors)
        except Exception as exc:
            last_error = exc
            errors.append(f"{candidate.alias}: {exc}")

    message = str(last_error or RuntimeError("No session could collect posts."))
    errors.append(f"posts: {message}")
    return PostCollectionAttempt(stats=None, session_alias=None, errors=errors)


def _collect_profile_stories(
    db: Database,
    story_collector: GalleryDlStoryCollector,
    profile: Dict[str, Any],
    target_date: date,
    session: CollectorSession,
    sessions: SessionPool,
    report: Dict[str, Any],
    profile_result: Dict[str, Any],
) -> None:
    story_sessions = [session, *sessions.alternatives(session)]
    story_result = story_collector.collect_profile_stories(
        profile["username"],
        target_date,
        story_sessions,
        candidate_name=str(profile.get("name") or profile["username"]),
    )
    profile_result["stories_found"] = story_result.files_found
    profile_result["story_session_alias"] = story_result.session_alias
    report["stories_found"] += story_result.files_found
    if story_result.output_dir:
        report["files_generated"].append(story_result.output_dir)
        for story in story_collector.load_story_metadata(story_result.output_dir):
            _, inserted = db.upsert_story(profile["id"], story)
            if inserted:
                profile_result["stories_saved"] = int(profile_result.get("stories_saved", 0)) + 1
    if story_result.status == "failed":
        profile_result["errors"].append(story_result.error_message)


def _enqueue_story_collection_job(
    db: Database,
    settings: Settings,
    profile: Dict[str, Any],
    target_date: date,
    report: Dict[str, Any],
    profile_result: Dict[str, Any],
) -> None:
    job_id = db.enqueue_job(
        JOB_TYPE_STORIES,
        profile_id=profile["id"],
        cursor=target_date.isoformat(),
        priority=int(profile.get("priority", 0)) + 100,
        max_attempts=settings.max_job_attempts,
    )
    if job_id:
        profile_result["story_jobs_enqueued"] = int(profile_result.get("story_jobs_enqueued", 0)) + 1
        report["story_jobs_enqueued"] += 1
    profile_result["story_session_alias"] = "media-worker"


def _pending_jobs_count(db: Database) -> int:
    counts = db.count_jobs_by_status()
    return counts.get("pending", 0) + counts.get("retry", 0)


def _finish_daily_run(
    db: Database,
    settings: Settings,
    daily_run_id: int,
    target_date: date,
    started_at: datetime,
    report: Dict[str, Any],
) -> Dict[str, Any]:
    report["jobs_pending"] = _pending_jobs_count(db)
    report["finished_at"] = datetime.now(tz=timezone.utc).isoformat()
    report["elapsed_seconds"] = (datetime.fromisoformat(report["finished_at"]) - started_at).total_seconds()
    report_path = write_daily_report(settings.reports_dir, target_date, report)
    report["files_generated"].append(str(report_path))
    status = "success"
    if report["errors"] or int(report.get("profiles_error") or 0) > 0 or int(report.get("jobs_failed") or 0) > 0:
        status = "partial"
    db.finish_run(
        daily_run_id,
        status,
        posts_found=report["posts_found"],
        posts_inserted=report["posts_inserted"],
        posts_updated=report["posts_updated"],
        comments_inserted=report["comments_inserted"],
        replies_inserted=report["replies_inserted"],
    )
    return report


async def _process_daily_jobs(
    db: Database,
    settings: Settings,
    rps: Optional[float],
    report: Dict[str, Any],
) -> None:
    print("Processing queued comment/reply jobs...")
    processor = JobProcessor(db, settings, rps=rps)
    run_id = db.start_run(None, "jobs", None, None)
    try:
        stats: JobStats = await processor.process_pending_jobs(settings.job_limit_per_run)
        db.finish_run(
            run_id,
            "success",
            comments_inserted=stats.comments_inserted,
            replies_inserted=stats.replies_inserted,
        )
        report["comments_inserted"] = stats.comments_inserted
        report["replies_inserted"] = stats.replies_inserted
        report["jobs_failed"] = stats.failed
        report["jobs_processed"] = stats.processed
    except Exception as exc:
        db.finish_run(run_id, "failed", error_message=str(exc))
        report["errors"].append({"stage": "jobs", "error": str(exc)})


async def collect_profile(
    db: Database,
    settings: Settings,
    username: str,
    date_from: datetime,
    date_to: datetime,
    enqueue_comments: bool = True,
    rps: Optional[float] = None,
    session: Optional[CollectorSession] = None,
    output_date: Optional[date] = None,
    archive_date_from: Optional[date] = None,
    archive_date_to: Optional[date] = None,
) -> ProfileCollectionStats:
    profile = db.get_profile_by_username(username)
    if not profile:
        raise ValueError(f"Profile @{username} is not registered. Run seed-profiles first.")

    stats = ProfileCollectionStats()
    run_id = db.start_run(profile["id"], "profile_posts", date_from, date_to)
    cookie_path = session.instagram_cookie_json if session else settings.cookie_json_path
    session_alias = session.alias if session else "default"
    print(f"Collecting @{username} from {date_from.date()} to {date_to.date()} using {session_alias}...")

    try:
        async with InstagramCollector(cookie_path, rps or settings.rps) as scraper:
            posts = await scraper.fetch_profile_posts(username, date_from, date_to)

        if settings.collect_post_media and posts and not settings.media_queue_enabled:
            media_stats = await download_post_media(
                posts,
                settings.post_media_dir,
                output_date or date_to.date(),
                str(profile.get("name") or username),
                settings.post_media_timeout_seconds,
            )
            stats.media_attempted = media_stats.attempted
            stats.media_downloaded = media_stats.downloaded
            stats.media_failed = media_stats.failed

        write_profile_posts(settings.data_dir, output_date or date_to.date(), username, posts)
        write_candidate_archive(
            settings.candidate_archive_dir,
            str(profile.get("name") or username),
            username,
            archive_date_from or date_from.date(),
            archive_date_to or date_to.date(),
            posts,
        )
        stats.posts_found = len(posts)
        last_seen_post_at = None
        for post in posts:
            post_id, inserted, comments_changed = db.upsert_post(profile["id"], post)
            db.store_raw_payload("post", post_id, profile["id"], post.get("raw_json", post))
            media_count = 0
            for media in post.get("media_assets") or []:
                if isinstance(media, dict):
                    media_count += 1
                    if settings.collect_post_media and settings.media_queue_enabled:
                        media.setdefault("download_status", "pending")
                    db.upsert_post_media(post_id, media)
            if settings.collect_post_media and settings.media_queue_enabled and media_count:
                pending_media = db.list_post_media_for_post(post_id, only_pending=True)
                stats.media_attempted += len(pending_media)
                job_id = db.enqueue_job(
                    JOB_TYPE_POST_MEDIA,
                    profile_id=profile["id"],
                    post_id=post_id,
                    shortcode=post["shortcode"],
                    priority=int(profile.get("priority", 0)),
                    max_attempts=settings.max_job_attempts,
                ) if pending_media else None
                if job_id:
                    stats.media_jobs_enqueued += 1
            if inserted:
                stats.posts_inserted += 1
            else:
                stats.posts_updated += 1

            if post.get("taken_at_iso"):
                last_seen_post_at = max(last_seen_post_at or post["taken_at_iso"], post["taken_at_iso"])

            should_enqueue_comments = (
                enqueue_comments
                and bool(profile.get("collect_comments"))
                and int(post.get("comments_count") or 0) > 0
                and (inserted or comments_changed)
            )
            if should_enqueue_comments:
                job_id = db.enqueue_job(
                    "comments",
                    profile_id=profile["id"],
                    post_id=post_id,
                    shortcode=post["shortcode"],
                    priority=int(profile.get("priority", 0)),
                    max_attempts=settings.max_job_attempts,
                )
                if job_id:
                    stats.jobs_enqueued += 1

        db.mark_profile_success(profile["id"], last_seen_post_at)
        db.finish_run(
            run_id,
            "success",
            posts_found=stats.posts_found,
            posts_inserted=stats.posts_inserted,
            posts_updated=stats.posts_updated,
        )
        print(
            f"@{username}: {stats.posts_found} found, "
            f"{stats.posts_inserted} inserted, {stats.posts_updated} updated, "
            f"{stats.jobs_enqueued} comment jobs, {stats.media_jobs_enqueued} media jobs."
        )
        return stats
    except (AuthError, ScrapeError, Exception) as exc:
        db.finish_run(run_id, "failed", error_message=str(exc))
        print(f"@{username} failed: {exc}")
        raise


async def run_daily_collection(
    db: Database,
    settings: Settings,
    target_date: date,
    margin_days: Optional[int] = None,
    rps: Optional[float] = None,
    process_jobs: bool = True,
    collect_posts_enabled: bool = True,
    collect_stories_enabled: bool = True,
    retry_incomplete: bool = False,
) -> Dict[str, Any]:
    started_at = datetime.now(tz=timezone.utc)
    ensure_runtime_dirs(settings.data_dir, settings.logs_dir, settings.reports_dir, settings.exports_dir)
    seed_profiles(db, settings)
    date_from, date_to = daily_window_for_timezone(
        target_date,
        margin_days if margin_days is not None else settings.margin_days,
        settings.timezone,
    )
    profiles = (
        db.list_incomplete_profiles_for_daily(date_from, date_to)
        if retry_incomplete
        else db.list_active_profiles()
    )
    daily_run_id = db.start_run(None, "daily", date_from, date_to)
    sessions = SessionPool(load_sessions(settings), settings.account_rotation_enabled)
    story_collector = GalleryDlStoryCollector(settings)
    report = _new_daily_report(target_date, started_at, date_from, date_to, settings.timezone, len(profiles))
    mode = "incomplete profiles" if retry_incomplete else "profiles"
    print(f"Starting daily collection for {len(profiles)} {mode}.")

    for profile in profiles:
        session = sessions.next()
        profile_result = _new_profile_result(profile["username"], session.alias)
        try:
            if collect_posts_enabled:
                attempt = await _collect_profile_posts_with_sessions(
                    db,
                    settings,
                    profile["username"],
                    date_from,
                    date_to,
                    enqueue_comments=settings.collect_comments_default,
                    rps=rps,
                    session=session,
                    sessions=sessions,
                    output_date=target_date,
                    archive_date_from=target_date,
                    archive_date_to=target_date,
                )
                profile_result["errors"].extend(attempt.errors)
                if attempt.session_alias:
                    profile_result["session_alias"] = attempt.session_alias
                if attempt.stats is None:
                    message = attempt.errors[-1] if attempt.errors else "No session could collect posts."
                    report["errors"].append({"username": profile["username"], "stage": "posts", "error": message})
                else:
                    _apply_post_stats(report, profile_result, attempt.stats)

            if collect_stories_enabled:
                if settings.media_queue_enabled:
                    _enqueue_story_collection_job(db, settings, profile, target_date, report, profile_result)
                else:
                    _collect_profile_stories(
                        db,
                        story_collector,
                        profile,
                        target_date,
                        session,
                        sessions,
                        report,
                        profile_result,
                    )

            _apply_profile_status(report, profile_result, collect_posts_enabled, collect_stories_enabled)
        except Exception as exc:
            profile_result["status"] = "failed"
            profile_result["errors"].append(str(exc))
            report["profiles_error"] += 1
            report["errors"].append({"username": profile["username"], "error": str(exc)})
        _record_profile_result(db, daily_run_id, profile, profile_result)
        report["profile_results"].append(profile_result)

    if not process_jobs or not settings.collect_comments_default:
        return _finish_daily_run(db, settings, daily_run_id, target_date, started_at, report)

    await _process_daily_jobs(db, settings, rps, report)
    return _finish_daily_run(db, settings, daily_run_id, target_date, started_at, report)


async def collect_posts_period(
    db: Database,
    settings: Settings,
    date_from: datetime,
    date_to: datetime,
    username: Optional[str] = None,
    enqueue_comments: bool = True,
    rps: Optional[float] = None,
) -> List[Dict[str, Any]]:
    seed_profiles(db, settings)
    sessions = SessionPool(load_sessions(settings), settings.account_rotation_enabled)
    profiles = [db.get_profile_by_username(username.lstrip("@"))] if username else db.list_active_profiles()
    results = []
    enqueue_comments = enqueue_comments and settings.collect_comments_default
    for profile in [item for item in profiles if item]:
        session = sessions.next()
        attempt = await _collect_profile_posts_with_sessions(
            db,
            settings,
            profile["username"],
            date_from,
            date_to,
            enqueue_comments=enqueue_comments,
            rps=rps,
            session=session,
            sessions=sessions,
            output_date=date_to.date(),
            archive_date_from=date_from.date(),
            archive_date_to=date_to.date(),
        )
        if attempt.stats is None:
            results.append(
                {
                    "username": profile["username"],
                    "status": "failed",
                    "error": " ; ".join(attempt.errors),
                }
            )
        else:
            results.append(
                {
                    "username": profile["username"],
                    "status": "success" if not attempt.errors else "partial",
                    "session_alias": attempt.session_alias,
                    "errors": attempt.errors,
                    **attempt.stats.__dict__,
                }
            )
    return results


def export_collected_day(db: Database, settings: Settings, run_date: date) -> Path:
    from .files import export_day, write_csv

    posts = db.list_posts_for_date(run_date.isoformat())
    stories = db.list_stories_for_date(run_date.isoformat())
    write_csv(Path(settings.data_dir) / "processed" / f"posts_{run_date.isoformat()}.csv", posts)
    write_csv(Path(settings.data_dir) / "processed" / f"stories_{run_date.isoformat()}.csv", stories)
    return export_day(settings.data_dir, settings.reports_dir, settings.exports_dir, run_date)
