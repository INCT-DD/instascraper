from __future__ import annotations

import csv
import json
import shutil
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, timezone
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
