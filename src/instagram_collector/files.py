from __future__ import annotations

import csv
import json
import shutil
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


def ensure_runtime_dirs(*paths: str) -> None:
    for path in paths:
        Path(path).mkdir(parents=True, exist_ok=True)


def safe_name(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in value).strip("_")


def json_default(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def write_json(path: str | Path, payload: Any) -> Path:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    tmp = output.with_suffix(output.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=json_default), encoding="utf-8")
    tmp.replace(output)
    return output


def append_jsonl(path: str | Path, records: Iterable[Dict[str, Any]]) -> int:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with output.open("a", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False, default=json_default) + "\n")
            count += 1
    return count


def write_csv(path: str | Path, rows: List[Dict[str, Any]], fields: Optional[List[str]] = None) -> Path:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = fields or sorted({key for row in rows for key in row.keys()})
    with output.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return output


def raw_path(data_dir: str, content_type: str, run_date: date, username: str, filename: str) -> Path:
    return Path(data_dir) / "raw" / content_type / run_date.isoformat() / safe_name(username) / filename


def write_profile_posts(data_dir: str, run_date: date, username: str, posts: List[Dict[str, Any]]) -> Optional[Path]:
    if not posts:
        return None
    path = raw_path(data_dir, "posts", run_date, username, "posts.ndjson")
    append_jsonl(path, posts)
    return path


def write_profile_comments(
    data_dir: str,
    run_date: date,
    username: str,
    shortcode: str,
    comments: List[Dict[str, Any]],
) -> Optional[Path]:
    if not comments:
        return None
    path = raw_path(data_dir, "comments", run_date, username, f"{safe_name(shortcode)}.ndjson")
    append_jsonl(path, comments)
    return path


def write_daily_report(reports_dir: str, run_date: date, report: Dict[str, Any]) -> Path:
    report = dict(report)
    report.setdefault("generated_at", datetime.now(tz=timezone.utc).isoformat())
    return write_json(Path(reports_dir) / f"{run_date.isoformat()}.json", report)


def archive_filename(date_from: date, date_to: date) -> str:
    start = date_from.strftime("%d%m%Y")
    end = date_to.strftime("%d%m%Y")
    return f"{start}.json" if start == end else f"{start}_{end}.json"


def dated_export_folder_name(base_name: str, date_from: date, date_to: date) -> str:
    start = date_from.strftime("%d-%m-%y")
    end = date_to.strftime("%d-%m-%y")
    suffix = start if start == end else f"{start}-to-{end}"
    return f"{base_name}_{suffix}"


def dated_export_root(base_dir: str, date_from: date, date_to: date) -> Path:
    base = Path(base_dir)
    return base.with_name(dated_export_folder_name(base.name, date_from, date_to))


def _date_range(date_from: date, date_to: date) -> Iterable[date]:
    if date_to < date_from:
        raise ValueError("date_to must be greater than or equal to date_from")
    current = date_from
    while current <= date_to:
        yield current
        current += timedelta(days=1)


def _post_date(post: Dict[str, Any], fallback: date) -> date:
    taken_at_iso = str(post.get("taken_at_iso") or "")
    if len(taken_at_iso) >= 10:
        try:
            return date.fromisoformat(taken_at_iso[:10])
        except ValueError:
            pass

    taken_at = post.get("taken_at")
    if taken_at:
        try:
            return datetime.fromtimestamp(int(taken_at), tz=timezone.utc).date()
        except (TypeError, ValueError, OSError, OverflowError):
            pass
    return fallback


def write_candidate_archives(
    archive_dir: str,
    candidate_name: str,
    username: str,
    date_from: date,
    date_to: date,
    posts: List[Dict[str, Any]],
) -> List[Path]:
    export_dir = dated_export_root(archive_dir, date_from, date_to) / safe_name(candidate_name)
    posts_by_date = {run_date: [] for run_date in _date_range(date_from, date_to)}
    for post in posts:
        run_date = _post_date(post, date_to)
        if run_date not in posts_by_date:
            run_date = date_to
        posts_by_date[run_date].append(post)

    generated_at = datetime.now(tz=timezone.utc).isoformat()
    paths = []
    for run_date, daily_posts in posts_by_date.items():
        payload = {
            "platform": "instagram",
            "candidate_name": candidate_name,
            "username": username,
            "date_from": run_date.isoformat(),
            "date_to": run_date.isoformat(),
            "collection_date_from": date_from.isoformat(),
            "collection_date_to": date_to.isoformat(),
            "generated_at": generated_at,
            "posts_count": len(daily_posts),
            "posts": daily_posts,
        }
        paths.append(write_json(export_dir / archive_filename(run_date, run_date), payload))

    if date_from != date_to:
        legacy_archive = export_dir / archive_filename(date_from, date_to)
        if legacy_archive.exists():
            legacy_archive.unlink()
    return paths


def write_candidate_archive(
    archive_dir: str,
    candidate_name: str,
    username: str,
    date_from: date,
    date_to: date,
    posts: List[Dict[str, Any]],
) -> Path:
    """Compatibility wrapper returning the last daily archive path."""
    return write_candidate_archives(
        archive_dir,
        candidate_name,
        username,
        date_from,
        date_to,
        posts,
    )[-1]


def export_day(data_dir: str, reports_dir: str, exports_dir: str, run_date: date) -> Path:
    target = Path(exports_dir) / run_date.isoformat()
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)

    for source in [
        Path(data_dir) / "raw" / "posts" / run_date.isoformat(),
        Path(data_dir) / "raw" / "stories" / run_date.isoformat(),
        Path(data_dir) / "raw" / "comments" / run_date.isoformat(),
        Path(reports_dir) / f"{run_date.isoformat()}.json",
    ]:
        if source.exists():
            destination = target / source.name if source.is_file() else target / source.parent.name / source.name
            if source.is_dir():
                shutil.copytree(source, destination)
            else:
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, destination)
    return target
