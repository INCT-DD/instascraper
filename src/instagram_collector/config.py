from __future__ import annotations

import os
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional


DEFAULT_PROFILES: List[Dict[str, Any]] = [
    {"name": "Lula", "username": "lulaoficial", "priority": 10},
    {"name": "Flavio Bolsonaro", "username": "flaviobolsonaro", "priority": 10},
    {"name": "Jair Bolsonaro", "username": "jairmessiasbolsonaro", "priority": 10},
    {"name": "Ronaldo Caiado", "username": "ronaldocaiado", "priority": 8},
    {"name": "Renan Santos", "username": "renansantosmbl", "priority": 8},
    {"name": "Romeu Zema", "username": "romeuzemaoficial", "priority": 8},
    {"name": "Michelle Bolsonaro", "username": "michellebolsonaro", "priority": 8},
    {"name": "Eduardo Bolsonaro", "username": "bolsonarosp", "priority": 8},
    {"name": "Joaquim Barbosa", "username": "joaquimbarbosaoficial", "priority": 7},
    {"name": "Tarcisio de Freitas", "username": "tarcisiogdf", "priority": 8},
    {"name": "Fernando Haddad", "username": "fernandohaddadoficial", "priority": 8},
    {"name": "Aecio Neves", "username": "aecionevesoficial", "priority": 7},
    {"name": "Augusto Cury", "username": "augustocury", "priority": 6},
    {"name": "Cabo Daciolo", "username": "cabodaciolo", "priority": 6},
    {"name": "Samara Martins", "username": "samaramartinsup", "priority": 6},
    {"name": "Hertz Dias", "username": "hertzdiaspstu", "priority": 6},
    {"name": "Edmilson Costa", "username": "edpcb", "priority": 6},
]


@dataclass(frozen=True)
class Settings:
    database_url: str
    timezone: str
    cookie_json_path: str
    profiles_path: str
    sessions_path: str
    data_dir: str
    logs_dir: str
    reports_dir: str
    exports_dir: str
    rps: float
    margin_days: int
    collect_comments_default: bool
    collect_replies_default: bool
    max_job_attempts: int
    job_limit_per_run: int
    max_comments_per_post: Optional[int]
    comment_queue_time_limit_seconds: Optional[int]
    account_rotation_enabled: bool
    gallery_dl_enabled: bool
    gallery_dl_binary: str
    gallery_dl_sleep_request: str
    gallery_dl_timeout_seconds: int


def _load_env_file(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def _bool_env(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def load_settings(env_file: Optional[str] = ".env") -> Settings:
    if env_file:
        _load_env_file(env_file)

    return Settings(
        database_url=os.environ.get("DATABASE_URL") or _database_url_from_postgres_env(),
        timezone=os.environ.get("TIMEZONE", "America/Bahia"),
        cookie_json_path=os.environ.get("COOKIE_JSON_PATH", "cookies/collector-01.json"),
        profiles_path=os.environ.get("PROFILES_PATH", "profiles.json"),
        sessions_path=os.environ.get("SESSIONS_PATH", "sessions.json"),
        data_dir=os.environ.get("DATA_DIR", "data"),
        logs_dir=os.environ.get("LOGS_DIR", "logs"),
        reports_dir=os.environ.get("REPORTS_DIR", "reports"),
        exports_dir=os.environ.get("EXPORTS_DIR", "exports"),
        rps=float(os.environ.get("RPS", "1")),
        margin_days=int(os.environ.get("MARGIN_DAYS", "0")),
        collect_comments_default=_bool_env("COLLECT_COMMENTS_DEFAULT", True),
        collect_replies_default=_bool_env("COLLECT_REPLIES_DEFAULT", True),
        max_job_attempts=int(os.environ.get("MAX_JOB_ATTEMPTS", "3")),
        job_limit_per_run=int(os.environ.get("JOB_LIMIT_PER_RUN", "100")),
        max_comments_per_post=_optional_int_env("MAX_COMMENTS_PER_POST", default=500),
        comment_queue_time_limit_seconds=_optional_int_env("COMMENT_QUEUE_TIME_LIMIT_SECONDS"),
        account_rotation_enabled=_bool_env("ACCOUNT_ROTATION_ENABLED", False),
        gallery_dl_enabled=_bool_env("GALLERY_DL_ENABLED", True),
        gallery_dl_binary=os.environ.get("GALLERY_DL_BINARY", "gallery-dl"),
        gallery_dl_sleep_request=os.environ.get("GALLERY_DL_SLEEP_REQUEST", "6.0-12.0"),
        gallery_dl_timeout_seconds=int(os.environ.get("GALLERY_DL_TIMEOUT_SECONDS", "900")),
    )


def parse_iso_date(value: Optional[str]) -> date:
    if value:
        return date.fromisoformat(value)
    return date.today()


def _database_url_from_postgres_env() -> str:
    host = os.environ.get("POSTGRES_HOST")
    if not host:
        return "sqlite:///collector.db"

    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "instagram_collector")
    user = os.environ.get("POSTGRES_USER", "collector")
    password = os.environ.get("POSTGRES_PASSWORD", "collector")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def _optional_int_env(name: str, default: Optional[int] = None) -> Optional[int]:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return default
    return int(value)


def load_profiles(path: str) -> List[Dict[str, Any]]:
    profile_path = Path(path)
    if not profile_path.exists():
        return DEFAULT_PROFILES

    data = json.loads(profile_path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError(f"{path} must contain a JSON list of profiles.")

    profiles = []
    for item in data:
        if not item.get("username"):
            raise ValueError("Each profile requires a username.")
        profiles.append(
            {
                "name": item.get("name") or item["username"],
                "username": item["username"].lstrip("@"),
                "platform": item.get("platform", "instagram"),
                "active": bool(item.get("active", True)),
                "collect_comments": bool(item.get("collect_comments", True)),
                "collect_replies": bool(item.get("collect_replies", True)),
                "priority": int(item.get("priority", 0)),
                "notes": item.get("notes", ""),
            }
        )
    return profiles


def load_sessions(settings: Settings) -> List[Dict[str, Any]]:
    session_path = Path(settings.sessions_path)
    if session_path.exists():
        data = json.loads(session_path.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            raise ValueError(f"{settings.sessions_path} must contain a JSON list of sessions.")
        sessions = [session for session in data if session.get("active", True)]
    else:
        sessions = [
            {
                "name": "default",
                "active": True,
                "instagram_cookie_json": settings.cookie_json_path,
                "gallery_dl_cookies": os.environ.get("GALLERY_DL_COOKIES", "cookies/collector-01.txt"),
            }
        ]

    if not sessions:
        raise ValueError("No active sessions configured.")

    for index, session in enumerate(sessions):
        session.setdefault("name", f"session-{index + 1}")
        session.setdefault("instagram_cookie_json", settings.cookie_json_path)
        session.setdefault("gallery_dl_cookies", session.get("cookies") or "cookies/collector-01.txt")
    return sessions
