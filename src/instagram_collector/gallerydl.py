from __future__ import annotations

import json
import shlex
import shutil
import subprocess
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, Iterable, List, Optional

from .config import Settings
from .files import safe_name
from .sessions import CollectorSession


@dataclass
class StoryCollectionResult:
    username: str
    session_alias: str
    status: str
    files_found: int
    output_dir: str
    error_message: Optional[str] = None


class GalleryDlStoryCollector:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def collect_profile_stories(
        self,
        username: str,
        run_date: date,
        sessions: Iterable[CollectorSession],
    ) -> StoryCollectionResult:
        if not self.settings.gallery_dl_enabled:
            return StoryCollectionResult(username, "disabled", "skipped", 0, "")

        binary_cmd = shlex.split(self.settings.gallery_dl_binary)
        if not self._command_available(binary_cmd):
            return StoryCollectionResult(
                username=username,
                session_alias="none",
                status="failed",
                files_found=0,
                output_dir="",
                error_message=(
                    f"{self.settings.gallery_dl_binary} not found. "
                    "Install gallery-dl or set GALLERY_DL_ENABLED=false."
                ),
            )

        last_error = None
        for session in sessions:
            result = self._collect_with_session(username, run_date, session)
            if result.status == "success":
                return result
            last_error = result.error_message

        return StoryCollectionResult(
            username=username,
            session_alias="exhausted",
            status="failed",
            files_found=0,
            output_dir=str(self._target_dir(run_date, username)),
            error_message=last_error or "All configured gallery-dl sessions failed.",
        )

    def _collect_with_session(
        self,
        username: str,
        run_date: date,
        session: CollectorSession,
    ) -> StoryCollectionResult:
        target_dir = self._target_dir(run_date, username)
        target_dir.mkdir(parents=True, exist_ok=True)
        stories_before = self._story_count(target_dir)

        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "gallery-dl.conf"
            config_path.write_text(
                json.dumps(self._config(target_dir, session), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            cmd = [
                *shlex.split(self.settings.gallery_dl_binary),
                "-c",
                str(config_path),
                "--write-info-json",
                "--mtime",
                "date",
                f"https://www.instagram.com/{username}/",
            ]

            try:
                completed = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=self.settings.gallery_dl_timeout_seconds,
                    check=False,
                )
            except subprocess.TimeoutExpired as exc:
                return StoryCollectionResult(
                    username,
                    session.alias,
                    "failed",
                    max(0, self._story_count(target_dir) - stories_before),
                    str(target_dir),
                    f"gallery-dl timed out after {exc.timeout}s",
                )

        files_found = max(0, self._story_count(target_dir) - stories_before)
        if completed.returncode == 0:
            return StoryCollectionResult(username, session.alias, "success", files_found, str(target_dir))

        error_message = (completed.stderr or completed.stdout or "").strip()[-2000:]
        return StoryCollectionResult(
            username,
            session.alias,
            "failed",
            files_found,
            str(target_dir),
            error_message or f"gallery-dl exited with code {completed.returncode}",
        )

    def _config(self, target_dir: Path, session: CollectorSession) -> Dict[str, object]:
        state_dir = Path(self.settings.data_dir) / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        cookies = self._gallery_cookies(session)
        return {
            "extractor": {
                "base-directory": str(target_dir),
                "archive": str(state_dir / "gallery-dl-archive.sqlite3"),
                "skip": True,
                "sleep-request": self.settings.gallery_dl_sleep_request,
                "sleep-429": "linear:60=300-900",
                "retries": 4,
                "instagram": {
                    "api": "rest",
                    "include": ["stories"],
                    "cookies": cookies,
                },
            }
        }

    def _gallery_cookies(self, session: CollectorSession) -> object:
        cookies_txt = Path(session.gallery_dl_cookies)
        if cookies_txt.exists() and self._is_netscape_cookie_file(cookies_txt):
            return str(cookies_txt)

        cookie_json = Path(session.instagram_cookie_json)
        if cookie_json.exists():
            data = json.loads(cookie_json.read_text(encoding="utf-8"))
            cookies = data.get("cookies", data)
            if isinstance(cookies, list):
                return {
                    str(item["name"]): str(item["value"])
                    for item in cookies
                    if isinstance(item, dict) and item.get("name") and item.get("value") is not None
                }
            return cookies

        return session.gallery_dl_cookies

    def _is_netscape_cookie_file(self, path: Path) -> bool:
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except UnicodeDecodeError:
            return False

        cookie_lines = [
            line
            for line in lines
            if line.strip() and not line.lstrip().startswith("#")
        ]
        if not cookie_lines:
            return False
        return all(len(line.split("\t")) == 7 for line in cookie_lines)

    def _target_dir(self, run_date: date, username: str) -> Path:
        return Path(self.settings.data_dir) / "raw" / "stories" / run_date.isoformat() / safe_name(username)

    def _file_count(self, path: Path) -> int:
        if not path.exists():
            return 0
        return sum(1 for item in path.rglob("*") if item.is_file())

    def _story_count(self, path: Path) -> int:
        metadata_files = len(self.story_metadata_files(path))
        return metadata_files or self._file_count(path)

    def _command_available(self, command: List[str]) -> bool:
        if not command:
            return False
        if command[0] in {"python", "python.exe"} and len(command) >= 3 and command[1] == "-m":
            return True
        return shutil.which(command[0]) is not None

    def story_metadata_files(self, path: Path) -> List[Path]:
        if not path.exists():
            return []
        return [
            item
            for item in path.rglob("*.json")
            if item.is_file() and not item.name.lower().startswith("info")
        ]

    def story_media_files(self, path: Path) -> List[Path]:
        if not path.exists():
            return []
        return [
            item
            for item in path.rglob("*")
            if item.is_file() and item.suffix.lower() not in {".json", ".part", ".ytdl"}
        ]

    def load_story_metadata(self, output_dir: str) -> List[Dict[str, object]]:
        records: List[Dict[str, object]] = []
        base = Path(output_dir)
        for metadata_file in self.story_metadata_files(base):
            try:
                payload = json.loads(metadata_file.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                continue
            media_path = self._find_media_for_metadata(metadata_file)
            payload["media_path"] = str(media_path) if media_path else None
            payload["raw_json"] = dict(payload)
            records.append(payload)
        if records:
            return records

        for media_file in self.story_media_files(base):
            records.append(
                {
                    "id": str(media_file),
                    "filename": str(media_file),
                    "media_path": str(media_file),
                    "extension": media_file.suffix.lower().lstrip("."),
                    "raw_json": {
                        "filename": str(media_file),
                        "metadata_source": "media_file_fallback",
                    },
                }
            )
        return records

    def _find_media_for_metadata(self, metadata_file: Path) -> Optional[Path]:
        stem = metadata_file.with_suffix("")
        for candidate in metadata_file.parent.glob(stem.name + ".*"):
            if candidate == metadata_file or candidate.suffix.lower() == ".json":
                continue
            return candidate
        return None
