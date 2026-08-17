from __future__ import annotations

import argparse
import asyncio
from datetime import date
from pathlib import Path
import shutil
from typing import Optional

from .config import load_sessions, load_settings, parse_iso_date
from .gallerydl import GalleryDlStoryCollector
from .jobs import JobProcessor
from .logging_setup import configure_logging
from .media_jobs import MediaJobProcessor
from .notifications import build_crash_report, send_report_notification
from .pipeline import collect_posts_period, export_collected_day, explicit_window, run_daily_collection, seed_profiles
from .sessions import SessionPool
from .storage import Database


def _collection_modes(args: argparse.Namespace, settings) -> tuple[bool, bool]:
    if args.posts_only and args.stories_only:
        raise ValueError("--posts-only and --stories-only cannot be used together.")
    if args.posts_only:
        return True, False
    if args.stories_only:
        return False, True
    return settings.collect_posts_default, settings.collect_stories_default


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="instagram_collector")
    sub = parser.add_subparsers(dest="command", required=True)

    daily = sub.add_parser("run-daily", help="Run post collection for all active profiles.")
    daily.add_argument("--date", dest="target_date", help="Target date in YYYY-MM-DD. Defaults to today.")
    daily.add_argument("--margin-days", type=int, default=None)
    daily.add_argument("--rps", type=float, default=None)
    daily.add_argument("--skip-jobs", action="store_true", help="Only collect posts and enqueue jobs.")
    daily.add_argument("--posts-only", action="store_true", help="Skip story collection.")
    daily.add_argument("--stories-only", action="store_true", help="Skip post collection.")
    daily.add_argument("--retry-incomplete", action="store_true", help="Collect only profiles not completed for the target date.")

    scheduled = sub.add_parser("run-scheduled", help="Run daily collection, export optionally, and notify on failures.")
    scheduled.add_argument("--date", dest="target_date", help="Target date in YYYY-MM-DD. Defaults to today.")
    scheduled.add_argument("--margin-days", type=int, default=None)
    scheduled.add_argument("--rps", type=float, default=None)
    scheduled.add_argument("--skip-jobs", action="store_true", help="Only collect posts/stories and enqueue jobs.")
    scheduled.add_argument("--posts-only", action="store_true", help="Skip story collection.")
    scheduled.add_argument("--stories-only", action="store_true", help="Skip post collection.")
    scheduled.add_argument("--retry-incomplete", action="store_true", help="Collect only profiles not completed for the target date.")
    scheduled.add_argument("--export", action="store_true", help="Export collected day after the run.")
    scheduled.add_argument("--notify", action="store_true", help="Send notification even if NOTIFY_ENABLED=false.")
    scheduled.add_argument("--no-notify", action="store_true", help="Disable notification for this run.")

    for command_name in ("process-jobs", "process-comments-queue"):
        jobs = sub.add_parser(command_name, help="Process pending comment/reply jobs.")
        jobs.add_argument("--limit", type=int, default=None)
        jobs.add_argument("--rps", type=float, default=None)

    media_jobs = sub.add_parser("process-media-queue", help="Process pending post media and story download jobs.")
    media_jobs.add_argument("--limit", type=int, default=None)
    media_jobs.add_argument("--watch", action="store_true", help="Keep polling for pending media jobs.")
    media_jobs.add_argument("--sleep", type=int, default=None, help="Polling interval in seconds when --watch is used.")

    posts = sub.add_parser("collect-posts", help="Collect posts for a date range.")
    posts.add_argument("--start-date", required=True)
    posts.add_argument("--end-date", required=True)
    posts.add_argument("--username")
    posts.add_argument("--no-comments", action="store_true", help="Do not enqueue comment jobs.")
    posts.add_argument("--rps", type=float, default=None)

    stories = sub.add_parser("collect-stories", help="Collect currently available stories with gallery-dl.")
    stories.add_argument("--date", dest="target_date", help="Run date in YYYY-MM-DD. Defaults to today.")
    stories.add_argument("--username")

    profile = sub.add_parser("collect-profile", help="Compatibility alias for collect-posts.")
    profile.add_argument("--username", required=True)
    profile.add_argument("--from", dest="date_from", required=True)
    profile.add_argument("--to", dest="date_to", required=True)
    profile.add_argument("--comments", action="store_true", help="Enqueue and process comment jobs after posts.")
    profile.add_argument("--rps", type=float, default=None)

    export = sub.add_parser("export", help="Create a copyable export folder for a run date.")
    export.add_argument("--date", dest="target_date", required=True)

    sub.add_parser("seed-profiles", help="Create/update monitored profiles and database schema.")
    sub.add_parser("init-db", help="Create/update database schema only.")
    sub.add_parser("migrate", help="Run database migrations.")

    cleanup = sub.add_parser("cleanup-secondary-data", help="Remove optional comments/stories data after confirmation.")
    cleanup.add_argument("--comments", action="store_true", help="Remove comments, replies and comment jobs.")
    cleanup.add_argument("--stories", action="store_true", help="Remove story rows and story raw payloads.")
    cleanup.add_argument("--story-media-files", action="store_true", help="Also remove data/raw/stories files.")
    cleanup.add_argument("--confirm", action="store_true", help="Actually delete data. Without this flag only prints counts.")
    return parser


async def _run_async(args: argparse.Namespace) -> None:
    settings = load_settings()
    db = Database(settings.database_url)
    try:
        run_date = parse_iso_date(getattr(args, "target_date", None))
        configure_logging(settings.logs_dir, run_date)

        if args.command == "init-db":
            db.init_schema()
            print("Database schema is ready.")
            return

        if args.command == "migrate":
            db.migrate()
            print("Database schema is migrated.")
            return

        if args.command == "cleanup-secondary-data":
            db.init_schema()
            if not args.comments and not args.stories and not args.story_media_files:
                raise ValueError("Choose at least one cleanup target: --comments, --stories or --story-media-files.")
            if args.comments:
                print(
                    "Comments cleanup target: "
                    f"{db.count_rows('comments')} comments, {db.count_rows('replies')} replies."
                )
            if args.stories:
                print(f"Stories cleanup target: {db.count_rows('stories')} story rows.")
            story_dir = Path(settings.data_dir) / "raw" / "stories"
            if args.story_media_files:
                print(f"Story media cleanup target: {story_dir}")
            if not args.confirm:
                print("Dry run only. Re-run with --confirm to delete.")
                return
            if args.comments:
                deleted = db.cleanup_comments_data()
                print(f"Deleted comments data: {deleted}")
            if args.stories:
                deleted = db.cleanup_stories_data()
                print(f"Deleted stories data: {deleted}")
            if args.story_media_files and story_dir.exists():
                shutil.rmtree(story_dir)
                print(f"Deleted story media files: {story_dir}")
            return

        if args.command == "seed-profiles":
            seed_profiles(db, settings)
            print("Monitored profiles seeded.")
            return

        db.init_schema()

        if args.command == "run-daily":
            target_date = parse_iso_date(args.target_date)
            collect_posts_enabled, collect_stories_enabled = _collection_modes(args, settings)
            report = await run_daily_collection(
                db,
                settings,
                target_date=target_date,
                margin_days=args.margin_days,
                rps=args.rps,
                process_jobs=not args.skip_jobs,
                collect_posts_enabled=collect_posts_enabled,
                collect_stories_enabled=collect_stories_enabled,
                retry_incomplete=args.retry_incomplete,
            )
            print(
                f"Daily run done: {report['posts_found']} posts, {report['stories_found']} story files, "
                f"{report.get('jobs_processed', 0)} jobs processed, {report['profiles_error']} profile errors."
            )
            return

        if args.command == "run-scheduled":
            target_date = parse_iso_date(args.target_date)
            collect_posts_enabled, collect_stories_enabled = _collection_modes(args, settings)
            report = None
            try:
                report = await run_daily_collection(
                    db,
                    settings,
                    target_date=target_date,
                    margin_days=args.margin_days,
                    rps=args.rps,
                    process_jobs=not args.skip_jobs,
                    collect_posts_enabled=collect_posts_enabled,
                    collect_stories_enabled=collect_stories_enabled,
                    retry_incomplete=args.retry_incomplete,
                )
                if args.export:
                    export_path = export_collected_day(db, settings, target_date)
                    report["export_path"] = str(export_path)
            except Exception as exc:
                report = build_crash_report(target_date, exc)
                if not args.no_notify:
                    try:
                        notified = send_report_notification(report, force=args.notify)
                        print(f"Notification sent: {notified}")
                    except Exception as notify_exc:
                        print(f"Notification failed: {notify_exc}")
                raise

            if not args.no_notify:
                try:
                    notified = send_report_notification(report, force=args.notify)
                    print(f"Notification sent: {notified}")
                except Exception as notify_exc:
                    print(f"Notification failed: {notify_exc}")

            print(
                f"Scheduled run done: {report['posts_found']} posts, {report['stories_found']} story files, "
                f"{report.get('jobs_processed', 0)} jobs processed, {report['profiles_error']} profile errors."
            )
            return

        if args.command in {"process-jobs", "process-comments-queue"}:
            if not settings.collect_comments_default:
                print("Comment collection is disabled by COLLECT_COMMENTS_DEFAULT=false.")
                return
            processor = JobProcessor(db, settings, rps=args.rps)
            run_id = db.start_run(None, "jobs", None, None)
            try:
                stats = await processor.process_pending_jobs(args.limit)
                db.finish_run(
                    run_id,
                    "success",
                    comments_inserted=stats.comments_inserted,
                    replies_inserted=stats.replies_inserted,
                )
                print(
                    f"Jobs processed: {stats.processed}; failed: {stats.failed}; "
                    f"comments inserted: {stats.comments_inserted}; replies inserted: {stats.replies_inserted}."
                )
            except Exception as exc:
                db.finish_run(run_id, "failed", error_message=str(exc))
                raise
            return

        if args.command == "process-media-queue":
            processor = MediaJobProcessor(db, settings)
            if args.watch:
                await processor.watch(args.limit, args.sleep)
            else:
                stats = await processor.process_pending_jobs(args.limit)
                print(
                    f"Media jobs processed: {stats.processed}; failed: {stats.failed}; "
                    f"post media downloaded: {stats.post_media_downloaded}; stories saved: {stats.stories_saved}."
                )
            return

        if args.command == "collect-posts":
            date_from, date_to = explicit_window(
                date.fromisoformat(args.start_date),
                date.fromisoformat(args.end_date),
            )
            results = await collect_posts_period(
                db,
                settings,
                date_from,
                date_to,
                username=args.username,
                enqueue_comments=not args.no_comments,
                rps=args.rps,
            )
            failed = sum(1 for result in results if result.get("status") == "failed")
            succeeded = len(results) - failed
            print(f"Post collection finished: {succeeded} succeeded, {failed} failed.")
            return

        if args.command == "collect-stories":
            seed_profiles(db, settings)
            target_date = parse_iso_date(args.target_date)
            pool = SessionPool(load_sessions(settings), settings.account_rotation_enabled)
            collector = GalleryDlStoryCollector(settings)
            profiles = [db.get_profile_by_username(args.username.lstrip("@"))] if args.username else db.list_active_profiles()
            for profile in [item for item in profiles if item]:
                session = pool.next()
                result = collector.collect_profile_stories(
                    profile["username"],
                    target_date,
                    [session, *pool.alternatives(session)],
                    candidate_name=str(profile.get("name") or profile["username"]),
                )
                stories_saved = 0
                if result.output_dir:
                    for story in collector.load_story_metadata(result.output_dir):
                        story_id, inserted = db.upsert_story(profile["id"], story)
                        db.store_raw_payload("story", story_id, profile["id"], story.get("raw_json", story))
                        if inserted:
                            stories_saved += 1
                print(
                    f"@{profile['username']}: {result.status}, {result.files_found} files, "
                    f"{stories_saved} stories saved, {result.session_alias}"
                )
            return

        if args.command == "collect-profile":
            date_from, date_to = explicit_window(
                date.fromisoformat(args.date_from),
                date.fromisoformat(args.date_to),
            )
            await collect_posts_period(
                db,
                settings,
                date_from=date_from,
                date_to=date_to,
                username=args.username.lstrip("@"),
                enqueue_comments=args.comments,
                rps=args.rps,
            )
            if args.comments and settings.collect_comments_default:
                processor = JobProcessor(db, settings, rps=args.rps)
                await processor.process_pending_jobs(settings.job_limit_per_run)
            return

        if args.command == "export":
            export_path = export_collected_day(db, settings, date.fromisoformat(args.target_date))
            print(f"Export ready: {export_path}")
            return

        raise ValueError(f"Unknown command: {args.command}")
    finally:
        db.close()


def main(argv: Optional[list[str]] = None) -> None:
    args = build_parser().parse_args(argv)
    asyncio.run(_run_async(args))


if __name__ == "__main__":
    main()
