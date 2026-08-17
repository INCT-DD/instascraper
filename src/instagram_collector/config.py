from __future__ import annotations

import os
import csv
import io
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
    candidate_archive_dir: str
    collect_post_media: bool
    media_queue_enabled: bool
    media_worker_sleep_seconds: int
    post_media_dir: str
    story_media_dir: str
    post_media_timeout_seconds: int
    rps: float
    margin_days: int
    collect_posts_default: bool
    collect_stories_default: bool
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

    exports_dir = os.environ.get("EXPORTS_DIR", "exports")
    candidate_archive_dir = os.environ.get("CANDIDATE_ARCHIVE_DIR", str(Path(exports_dir) / "instagram"))
    return Settings(
        database_url=os.environ.get("DATABASE_URL") or _database_url_from_postgres_env(),
        timezone=os.environ.get("TIMEZONE", "America/Bahia"),
        cookie_json_path=os.environ.get("COOKIE_JSON_PATH", "cookies/collector-01.json"),
        profiles_path=os.environ.get("PROFILES_PATH", "profiles.json"),
        sessions_path=os.environ.get("SESSIONS_PATH", "sessions.json"),
        data_dir=os.environ.get("DATA_DIR", "data"),
        logs_dir=os.environ.get("LOGS_DIR", "logs"),
        reports_dir=os.environ.get("REPORTS_DIR", "reports"),
        exports_dir=exports_dir,
        candidate_archive_dir=candidate_archive_dir,
        collect_post_media=_bool_env("COLLECT_POST_MEDIA", False),
        media_queue_enabled=_bool_env("MEDIA_QUEUE_ENABLED", True),
        media_worker_sleep_seconds=int(os.environ.get("MEDIA_WORKER_SLEEP_SECONDS", "10")),
        post_media_dir=os.environ.get("POST_MEDIA_DIR", candidate_archive_dir),
        story_media_dir=os.environ.get("STORY_MEDIA_DIR", candidate_archive_dir),
        post_media_timeout_seconds=int(os.environ.get("POST_MEDIA_TIMEOUT_SECONDS", "60")),
        rps=float(os.environ.get("RPS", "1")),
        margin_days=int(os.environ.get("MARGIN_DAYS", "0")),
        collect_posts_default=_bool_env("COLLECT_POSTS_DEFAULT", True),
        collect_stories_default=_bool_env("COLLECT_STORIES_DEFAULT", True),
        collect_comments_default=_bool_env("COLLECT_COMMENTS_DEFAULT", False),
        collect_replies_default=_bool_env("COLLECT_REPLIES_DEFAULT", False),
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

    if profile_path.is_dir():
        profiles: List[Dict[str, Any]] = []
        for child in sorted(profile_path.glob("*")):
            if child.suffix.lower() in {".json", ".csv"}:
                profiles.extend(load_profiles(str(child)))
        return _dedupe_profiles(profiles)

    if profile_path.suffix.lower() == ".csv":
        return _load_profiles_csv(profile_path)

    data = json.loads(profile_path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError(f"{path} must contain a JSON list of profiles.")

    return _normalize_profiles(data)


def _normalize_profiles(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    profiles = []
    for raw_item in data:
        item = {str(key).strip().lower(): value for key, value in raw_item.items()}
        username = _profile_username(item)
        if not username:
            continue
        profiles.append(
            {
                "name": _profile_name(item, username),
                "username": username,
                "platform": item.get("platform", "instagram"),
                "active": _profile_active(item),
                "priority": _int_value(item.get("priority"), 0),
                "notes": _profile_notes(item),
            }
        )
        if "collect_comments" in item:
            profiles[-1]["collect_comments"] = _bool_value(item.get("collect_comments"), True)
        if "collect_replies" in item:
            profiles[-1]["collect_replies"] = _bool_value(item.get("collect_replies"), True)
    return _dedupe_profiles(profiles)


def _profile_name(item: Dict[str, Any], username: str) -> str:
    return str(
        item.get("name")
        or item.get("nome")
        or item.get("partido")
        or item.get("candidato")
        or username
    ).strip()


def _profile_active(item: Dict[str, Any]) -> bool:
    if "active" in item:
        return _bool_value(item.get("active"), True)
    for key in ("coletar", "monitorar", "incluir"):
        if key in item:
            return _bool_value(item.get(key), True)
    for key in ("excluir", "nao coletar", "não coletar", "vermelho", "red"):
        value = str(item.get(key, "")).strip().lower()
        if value in {"1", "true", "yes", "y", "on", "sim", "x"}:
            return False
    status = str(item.get("status") or item.get("situacao") or item.get("situação") or "").strip().lower()
    if any(term in status for term in ("vermelho", "excluir", "nao coletar", "não coletar", "inativo")):
        return False
    return True


def _profile_notes(item: Dict[str, Any]) -> str:
    parts = []
    for key in ("estado", "uf", "partido", "sigla", "legenda"):
        value = str(item.get(key) or "").strip()
        if value:
            parts.append(f"{key}={value}")
    return "; ".join(parts)


def _load_profiles_csv(path: Path) -> List[Dict[str, Any]]:
    text = _read_text_with_fallback(path)
    sample = text[:4096]
    delimiter = ";" if sample.count(";") >= sample.count(",") else ","
    rows = list(csv.reader(io.StringIO(text), delimiter=delimiter))
    if not rows:
        return []

    headers, data_start = _csv_headers(rows)
    records = []
    for raw_row in rows[data_start:]:
        if not any(str(cell).strip() for cell in raw_row):
            continue
        padded = [*raw_row, *([""] * max(0, len(headers) - len(raw_row)))]
        records.append({headers[index]: padded[index] for index in range(len(headers))})
    return _normalize_profiles(records)


def _read_text_with_fallback(path: Path) -> str:
    data = path.read_bytes()
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("latin-1", errors="replace")


def _csv_headers(rows: List[List[str]]) -> tuple[List[str], int]:
    first = rows[0]
    second = rows[1] if len(rows) > 1 else []
    has_social_second_header = any(_clean_header(value) in {"instagram", "facebook", "tiktok", "youtube", "x"} for value in second)
    header_rows = 2 if has_social_second_header else 1
    max_len = max(len(first), len(second))
    headers = []
    for index in range(max_len):
        top = first[index] if index < len(first) else ""
        bottom = second[index] if index < len(second) else ""
        value = bottom if bottom.strip() else top
        headers.append(_clean_header(value) or f"col_{index + 1}")
    return headers, header_rows


def _clean_header(value: str) -> str:
    return (
        value.strip()
        .lower()
        .replace("í", "i")
        .replace("ã", "a")
        .replace("á", "a")
        .replace("é", "e")
        .replace("ç", "c")
        .replace("ó", "o")
        .replace("ú", "u")
    )


def _profile_username(item: Dict[str, Any]) -> str:
    keys = (
        "username",
        "instagram_username",
        "instagram",
        "perfil do instagram",
        "perfil_instagram",
        "perfil",
        "profile",
        "handle",
        "arroba",
        "user",
    )
    for key in keys:
        value = item.get(key)
        if value:
            return _clean_username(str(value))
    for value in item.values():
        if isinstance(value, str) and "instagram.com" in value.lower():
            return _clean_username(value)
    return ""


def _clean_username(value: str) -> str:
    cleaned = value.strip().strip("/")
    if "instagram.com" in cleaned.lower():
        cleaned = cleaned.split("instagram.com/", 1)[-1]
    cleaned = cleaned.strip().strip("/")
    cleaned = cleaned.split("?", 1)[0].split("/", 1)[0]
    return cleaned.lstrip("@").strip().lower()


def _bool_value(value: Any, default: bool) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on", "sim"}


def _int_value(value: Any, default: int) -> int:
    if value is None or value == "":
        return default
    return int(value)


def _dedupe_profiles(profiles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_username: Dict[str, Dict[str, Any]] = {}
    for profile in profiles:
        username = str(profile["username"]).lower()
        by_username[username] = profile
    return list(by_username.values())


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
