from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse


POSTGRES_SCHEMA = """
CREATE TABLE IF NOT EXISTS profiles (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    username TEXT NOT NULL UNIQUE,
    platform TEXT NOT NULL DEFAULT 'instagram',
    active BOOLEAN NOT NULL DEFAULT TRUE,
    collect_comments BOOLEAN NOT NULL DEFAULT TRUE,
    collect_replies BOOLEAN NOT NULL DEFAULT TRUE,
    priority INTEGER NOT NULL DEFAULT 0,
    last_successful_collection_at TIMESTAMPTZ,
    last_seen_post_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS collection_runs (
    id BIGSERIAL PRIMARY KEY,
    profile_id BIGINT REFERENCES profiles(id),
    run_type TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    date_from TIMESTAMPTZ,
    date_to TIMESTAMPTZ,
    posts_found INTEGER NOT NULL DEFAULT 0,
    posts_inserted INTEGER NOT NULL DEFAULT 0,
    posts_updated INTEGER NOT NULL DEFAULT 0,
    comments_inserted INTEGER NOT NULL DEFAULT 0,
    replies_inserted INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS profile_collection_status (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT REFERENCES collection_runs(id),
    profile_id BIGINT REFERENCES profiles(id),
    handle TEXT NOT NULL,
    source TEXT NOT NULL,
    session_alias TEXT,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    posts_found INTEGER NOT NULL DEFAULT 0,
    stories_found INTEGER NOT NULL DEFAULT 0,
    posts_saved INTEGER NOT NULL DEFAULT 0,
    stories_saved INTEGER NOT NULL DEFAULT 0,
    comments_enqueued INTEGER NOT NULL DEFAULT 0,
    error_type TEXT,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS posts (
    id BIGSERIAL PRIMARY KEY,
    profile_id BIGINT NOT NULL REFERENCES profiles(id),
    platform_post_id TEXT NOT NULL UNIQUE,
    shortcode TEXT NOT NULL UNIQUE,
    url TEXT NOT NULL,
    taken_at BIGINT,
    taken_at_iso TEXT,
    media_type TEXT,
    caption TEXT,
    likes INTEGER,
    comments_count INTEGER,
    reposts INTEGER,
    views INTEGER,
    is_video BOOLEAN,
    accessibility_caption TEXT,
    raw_json JSONB,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS post_media (
    id BIGSERIAL PRIMARY KEY,
    post_id BIGINT NOT NULL REFERENCES posts(id),
    media_index INTEGER NOT NULL,
    media_type TEXT,
    source_url TEXT,
    local_path TEXT,
    width INTEGER,
    height INTEGER,
    download_status TEXT,
    error_message TEXT,
    raw_json JSONB,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (post_id, media_index, media_type, source_url)
);

CREATE TABLE IF NOT EXISTS stories (
    id BIGSERIAL PRIMARY KEY,
    profile_id BIGINT NOT NULL REFERENCES profiles(id),
    platform_story_id TEXT NOT NULL UNIQUE,
    shortcode TEXT,
    handle TEXT,
    url TEXT,
    media_path TEXT,
    media_type TEXT,
    published_at TEXT,
    expires_at TEXT,
    raw_json JSONB,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS comments (
    id BIGSERIAL PRIMARY KEY,
    post_id BIGINT NOT NULL REFERENCES posts(id),
    platform_comment_id TEXT NOT NULL UNIQUE,
    username TEXT,
    user_id TEXT,
    text TEXT,
    created_at BIGINT,
    created_at_iso TEXT,
    likes INTEGER,
    raw_json JSONB,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS replies (
    id BIGSERIAL PRIMARY KEY,
    comment_id BIGINT NOT NULL REFERENCES comments(id),
    platform_reply_id TEXT NOT NULL UNIQUE,
    username TEXT,
    user_id TEXT,
    text TEXT,
    created_at BIGINT,
    created_at_iso TEXT,
    likes INTEGER,
    raw_json JSONB,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS collection_jobs (
    id BIGSERIAL PRIMARY KEY,
    job_type TEXT NOT NULL,
    status TEXT NOT NULL,
    profile_id BIGINT REFERENCES profiles(id),
    post_id BIGINT REFERENCES posts(id),
    comment_id BIGINT REFERENCES comments(id),
    shortcode TEXT,
    cursor TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    priority INTEGER NOT NULL DEFAULT 0,
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw_payloads (
    id BIGSERIAL PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id BIGINT,
    profile_id BIGINT REFERENCES profiles(id),
    payload JSONB NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS profiles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    username TEXT NOT NULL UNIQUE,
    platform TEXT NOT NULL DEFAULT 'instagram',
    active INTEGER NOT NULL DEFAULT 1,
    collect_comments INTEGER NOT NULL DEFAULT 1,
    collect_replies INTEGER NOT NULL DEFAULT 1,
    priority INTEGER NOT NULL DEFAULT 0,
    last_successful_collection_at TEXT,
    last_seen_post_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS collection_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_id INTEGER REFERENCES profiles(id),
    run_type TEXT NOT NULL,
    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TEXT,
    status TEXT NOT NULL,
    date_from TEXT,
    date_to TEXT,
    posts_found INTEGER NOT NULL DEFAULT 0,
    posts_inserted INTEGER NOT NULL DEFAULT 0,
    posts_updated INTEGER NOT NULL DEFAULT 0,
    comments_inserted INTEGER NOT NULL DEFAULT 0,
    replies_inserted INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS profile_collection_status (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER REFERENCES collection_runs(id),
    profile_id INTEGER REFERENCES profiles(id),
    handle TEXT NOT NULL,
    source TEXT NOT NULL,
    session_alias TEXT,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TEXT,
    posts_found INTEGER NOT NULL DEFAULT 0,
    stories_found INTEGER NOT NULL DEFAULT 0,
    posts_saved INTEGER NOT NULL DEFAULT 0,
    stories_saved INTEGER NOT NULL DEFAULT 0,
    comments_enqueued INTEGER NOT NULL DEFAULT 0,
    error_type TEXT,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_id INTEGER NOT NULL REFERENCES profiles(id),
    platform_post_id TEXT NOT NULL UNIQUE,
    shortcode TEXT NOT NULL UNIQUE,
    url TEXT NOT NULL,
    taken_at INTEGER,
    taken_at_iso TEXT,
    media_type TEXT,
    caption TEXT,
    likes INTEGER,
    comments_count INTEGER,
    reposts INTEGER,
    views INTEGER,
    is_video INTEGER,
    accessibility_caption TEXT,
    raw_json TEXT,
    collected_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS post_media (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    post_id INTEGER NOT NULL REFERENCES posts(id),
    media_index INTEGER NOT NULL,
    media_type TEXT,
    source_url TEXT,
    local_path TEXT,
    width INTEGER,
    height INTEGER,
    download_status TEXT,
    error_message TEXT,
    raw_json TEXT,
    collected_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (post_id, media_index, media_type, source_url)
);

CREATE TABLE IF NOT EXISTS stories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_id INTEGER NOT NULL REFERENCES profiles(id),
    platform_story_id TEXT NOT NULL UNIQUE,
    shortcode TEXT,
    handle TEXT,
    url TEXT,
    media_path TEXT,
    media_type TEXT,
    published_at TEXT,
    expires_at TEXT,
    raw_json TEXT,
    collected_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS comments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    post_id INTEGER NOT NULL REFERENCES posts(id),
    platform_comment_id TEXT NOT NULL UNIQUE,
    username TEXT,
    user_id TEXT,
    text TEXT,
    created_at INTEGER,
    created_at_iso TEXT,
    likes INTEGER,
    raw_json TEXT,
    collected_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS replies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    comment_id INTEGER NOT NULL REFERENCES comments(id),
    platform_reply_id TEXT NOT NULL UNIQUE,
    username TEXT,
    user_id TEXT,
    text TEXT,
    created_at INTEGER,
    created_at_iso TEXT,
    likes INTEGER,
    raw_json TEXT,
    collected_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS collection_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_type TEXT NOT NULL,
    status TEXT NOT NULL,
    profile_id INTEGER REFERENCES profiles(id),
    post_id INTEGER REFERENCES posts(id),
    comment_id INTEGER REFERENCES comments(id),
    shortcode TEXT,
    cursor TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    priority INTEGER NOT NULL DEFAULT 0,
    scheduled_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TEXT,
    finished_at TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_payloads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id INTEGER,
    profile_id INTEGER REFERENCES profiles(id),
    payload TEXT NOT NULL,
    collected_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


class Database:
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self.is_postgres = database_url.startswith(("postgres://", "postgresql://"))
        self.conn = self._connect(database_url)
        self.placeholder = "%s" if self.is_postgres else "?"

    def _connect(self, database_url: str):
        if self.is_postgres:
            try:
                import psycopg
                from psycopg.rows import dict_row
            except ImportError as exc:
                raise RuntimeError(
                    "PostgreSQL requires psycopg. Install with: pip install 'psycopg[binary]'"
                ) from exc
            return psycopg.connect(database_url, row_factory=dict_row)

        parsed = urlparse(database_url)
        if parsed.scheme and parsed.scheme != "sqlite":
            raise ValueError(f"Unsupported DATABASE_URL scheme: {parsed.scheme}")
        db_path = parsed.path.lstrip("/") if parsed.scheme == "sqlite" else database_url
        conn = sqlite3.connect(db_path or "collector.db")
        conn.row_factory = sqlite3.Row
        return conn

    def close(self) -> None:
        self.conn.close()

    def init_schema(self) -> None:
        if self.is_postgres:
            with self.conn.cursor() as cur:
                for statement in POSTGRES_SCHEMA.split(";"):
                    if statement.strip():
                        cur.execute(statement)
            self.conn.commit()
            return

        self.conn.executescript(SQLITE_SCHEMA)
        self.conn.commit()

    def migrate(self) -> None:
        self.init_schema()
        self.ensure_column("posts", "reposts", "INTEGER")

    def ensure_column(self, table: str, column: str, definition: str) -> None:
        if self.is_postgres:
            with closing(self.conn.cursor()) as cur:
                cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {definition}")
            self.conn.commit()
            return

        with closing(self.conn.cursor()) as cur:
            cur.execute(f"PRAGMA table_info({table})")
            columns = {row["name"] for row in cur.fetchall()}
            if column not in columns:
                cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        self.conn.commit()

    def _now(self) -> str:
        return datetime.now(tz=timezone.utc).isoformat()

    def _json_param(self, value: Any) -> Any:
        if self.is_postgres:
            from psycopg.types.json import Jsonb

            return Jsonb(value)
        return json.dumps(value, ensure_ascii=False)

    def _bool_param(self, value: Any) -> Any:
        return bool(value) if self.is_postgres else int(bool(value))

    def _decode_row(self, row: Optional[Any]) -> Optional[Dict[str, Any]]:
        if row is None:
            return None
        data = dict(row)
        for key in ("raw_json", "payload"):
            if isinstance(data.get(key), str):
                try:
                    data[key] = json.loads(data[key])
                except json.JSONDecodeError:
                    pass
        return data

    def _fetchone(self, sql: str, params: Tuple[Any, ...] = ()) -> Optional[Dict[str, Any]]:
        with closing(self.conn.cursor()) as cur:
            cur.execute(sql, params)
            return self._decode_row(cur.fetchone())

    def _fetchall(self, sql: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
        with closing(self.conn.cursor()) as cur:
            cur.execute(sql, params)
            return [self._decode_row(row) for row in cur.fetchall()]

    def seed_profiles(
        self,
        profiles: Iterable[Dict[str, Any]],
        collect_comments_default: bool,
        collect_replies_default: bool,
    ) -> None:
        configured_usernames = []
        for profile in profiles:
            configured_usernames.append(profile["username"])
            existing = self.get_profile_by_username(profile["username"])
            values = (
                profile["name"],
                profile["username"],
                profile.get("platform", "instagram"),
                self._bool_param(profile.get("active", True)),
                self._bool_param(profile.get("collect_comments", collect_comments_default)),
                self._bool_param(profile.get("collect_replies", collect_replies_default)),
                int(profile.get("priority", 0)),
                self._now(),
            )
            if existing:
                sql = (
                    "UPDATE profiles SET name = ?, platform = ?, active = ?, "
                    "collect_comments = ?, collect_replies = ?, priority = ?, updated_at = ? "
                    "WHERE username = ?"
                )
                params = (values[0], values[2], values[3], values[4], values[5], values[6], values[7], values[1])
            else:
                sql = (
                    "INSERT INTO profiles "
                    "(name, username, platform, active, collect_comments, collect_replies, priority, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                )
                params = values
            if self.is_postgres:
                sql = sql.replace("?", "%s")
            with closing(self.conn.cursor()) as cur:
                cur.execute(sql, params)
        if configured_usernames:
            placeholders = ", ".join([self.placeholder] * len(configured_usernames))
            sql = f"UPDATE profiles SET active = {self.placeholder}, updated_at = {self.placeholder} WHERE username NOT IN ({placeholders})"
            params = (self._bool_param(False), self._now(), *configured_usernames)
            with closing(self.conn.cursor()) as cur:
                cur.execute(sql, params)
        self.conn.commit()

    def list_active_profiles(self) -> List[Dict[str, Any]]:
        return self._fetchall(
            "SELECT * FROM profiles WHERE active = 1 ORDER BY priority DESC, username"
            if not self.is_postgres
            else "SELECT * FROM profiles WHERE active = TRUE ORDER BY priority DESC, username"
        )

    def list_incomplete_profiles_for_daily(self, date_from: Any, date_to: Any) -> List[Dict[str, Any]]:
        active_condition = "p.active = TRUE" if self.is_postgres else "p.active = 1"
        sql = (
            "SELECT p.* FROM profiles p "
            f"WHERE {active_condition} AND NOT EXISTS ("
            "SELECT 1 FROM profile_collection_status pcs "
            "JOIN collection_runs cr ON cr.id = pcs.run_id "
            "WHERE pcs.profile_id = p.id "
            "AND cr.run_type = 'daily' "
            f"AND cr.date_from = {self.placeholder} "
            f"AND cr.date_to = {self.placeholder} "
            "AND pcs.status IN ('success', 'empty_for_day')"
            ") "
            "ORDER BY p.priority DESC, p.username"
        )
        return self._fetchall(sql, (str(date_from), str(date_to)))

    def get_profile_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        return self._fetchone(
            f"SELECT * FROM profiles WHERE username = {self.placeholder}",
            (username,),
        )

    def get_profile(self, profile_id: int) -> Optional[Dict[str, Any]]:
        return self._fetchone(
            f"SELECT * FROM profiles WHERE id = {self.placeholder}",
            (profile_id,),
        )

    def start_run(self, profile_id: Optional[int], run_type: str, date_from: Any, date_to: Any) -> int:
        sql = (
            "INSERT INTO collection_runs (profile_id, run_type, status, date_from, date_to) "
            "VALUES (?, ?, ?, ?, ?)"
        )
        if self.is_postgres:
            sql = sql.replace("?", "%s") + " RETURNING id"
        params = (profile_id, run_type, "running", str(date_from) if date_from else None, str(date_to) if date_to else None)
        with closing(self.conn.cursor()) as cur:
            cur.execute(sql, params)
            run_id = cur.fetchone()["id"] if self.is_postgres else cur.lastrowid
        self.conn.commit()
        return int(run_id)

    def finish_run(self, run_id: int, status: str, **fields: Any) -> None:
        allowed = {
            "posts_found",
            "posts_inserted",
            "posts_updated",
            "comments_inserted",
            "replies_inserted",
            "error_message",
        }
        assignments = ["status = ?", "finished_at = ?"]
        params: List[Any] = [status, self._now()]
        for key, value in fields.items():
            if key in allowed:
                assignments.append(f"{key} = ?")
                params.append(value)
        params.append(run_id)
        sql = f"UPDATE collection_runs SET {', '.join(assignments)} WHERE id = ?"
        if self.is_postgres:
            sql = sql.replace("?", "%s")
        with closing(self.conn.cursor()) as cur:
            cur.execute(sql, tuple(params))
        self.conn.commit()

    def record_profile_status(
        self,
        run_id: Optional[int],
        profile_id: Optional[int],
        handle: str,
        source: str,
        session_alias: Optional[str],
        status: str,
        posts_found: int = 0,
        stories_found: int = 0,
        posts_saved: int = 0,
        stories_saved: int = 0,
        comments_enqueued: int = 0,
        error_type: Optional[str] = None,
        error_message: Optional[str] = None,
        started_at: Optional[str] = None,
        finished_at: Optional[str] = None,
    ) -> None:
        sql = (
            "INSERT INTO profile_collection_status "
            "(run_id, profile_id, handle, source, session_alias, status, started_at, finished_at, "
            "posts_found, stories_found, posts_saved, stories_saved, comments_enqueued, error_type, error_message) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        if self.is_postgres:
            sql = sql.replace("?", "%s")
        now = self._now()
        params = (
            run_id,
            profile_id,
            handle,
            source,
            session_alias,
            status,
            started_at or now,
            finished_at or now,
            posts_found,
            stories_found,
            posts_saved,
            stories_saved,
            comments_enqueued,
            error_type,
            error_message[:2000] if error_message else None,
        )
        with closing(self.conn.cursor()) as cur:
            cur.execute(sql, params)
        self.conn.commit()

    def mark_profile_success(self, profile_id: int, last_seen_post_at: Optional[str]) -> None:
        sql = "UPDATE profiles SET last_successful_collection_at = ?, last_seen_post_at = ?, updated_at = ? WHERE id = ?"
        if self.is_postgres:
            sql = sql.replace("?", "%s")
        with closing(self.conn.cursor()) as cur:
            cur.execute(sql, (self._now(), last_seen_post_at, self._now(), profile_id))
        self.conn.commit()

    def upsert_post(self, profile_id: int, post: Dict[str, Any]) -> Tuple[int, bool, bool]:
        existing = self._fetchone(
            f"SELECT id, comments_count FROM posts WHERE platform_post_id = {self.placeholder} OR shortcode = {self.placeholder}",
            (str(post["post_id"]), post["shortcode"]),
        )
        comments_changed = bool(existing and existing.get("comments_count") != post.get("comments_count"))
        params = (
            profile_id,
            str(post["post_id"]),
            post["shortcode"],
            post["url"],
            post.get("taken_at"),
            post.get("taken_at_iso"),
            post.get("media_type"),
            post.get("caption"),
            post.get("likes"),
            post.get("comments_count"),
            post.get("reposts"),
            post.get("views"),
            self._bool_param(post.get("is_video")),
            post.get("accessibility_caption"),
            self._json_param(post.get("raw_json", post)),
            self._now(),
        )
        if existing:
            sql = (
                "UPDATE posts SET profile_id = ?, url = ?, taken_at = ?, taken_at_iso = ?, "
                "media_type = ?, caption = ?, likes = ?, comments_count = ?, reposts = ?, views = ?, "
                "is_video = ?, accessibility_caption = ?, raw_json = ?, updated_at = ? WHERE id = ?"
            )
            update_params = (
                profile_id,
                post["url"],
                post.get("taken_at"),
                post.get("taken_at_iso"),
                post.get("media_type"),
                post.get("caption"),
                post.get("likes"),
                post.get("comments_count"),
                post.get("reposts"),
                post.get("views"),
                self._bool_param(post.get("is_video")),
                post.get("accessibility_caption"),
                self._json_param(post.get("raw_json", post)),
                self._now(),
                existing["id"],
            )
            if self.is_postgres:
                sql = sql.replace("?", "%s")
            with closing(self.conn.cursor()) as cur:
                cur.execute(sql, update_params)
            self.conn.commit()
            return int(existing["id"]), False, comments_changed

        sql = (
            "INSERT INTO posts "
            "(profile_id, platform_post_id, shortcode, url, taken_at, taken_at_iso, media_type, "
            "caption, likes, comments_count, reposts, views, is_video, accessibility_caption, raw_json, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        if self.is_postgres:
            sql = sql.replace("?", "%s") + " RETURNING id"
        with closing(self.conn.cursor()) as cur:
            cur.execute(sql, params)
            post_id = cur.fetchone()["id"] if self.is_postgres else cur.lastrowid
        self.conn.commit()
        return int(post_id), True, True

    def get_post(self, post_id: int) -> Optional[Dict[str, Any]]:
        return self._fetchone(f"SELECT * FROM posts WHERE id = {self.placeholder}", (post_id,))

    def upsert_post_media(self, post_id: int, media: Dict[str, Any]) -> Tuple[int, bool]:
        source_url = str(media.get("url") or media.get("source_url") or "")
        existing = self._fetchone(
            (
                "SELECT id, local_path, download_status, error_message FROM post_media WHERE post_id = ? AND media_index = ? "
                "AND COALESCE(media_type, '') = COALESCE(?, '') "
                "AND COALESCE(source_url, '') = COALESCE(?, '')"
            ).replace("?", self.placeholder),
            (post_id, int(media.get("index") or 1), media.get("media_type"), source_url),
        )
        params = (
            post_id,
            int(media.get("index") or 1),
            media.get("media_type"),
            source_url,
            media.get("local_path"),
            media.get("width"),
            media.get("height"),
            media.get("download_status"),
            media.get("download_error"),
            self._json_param(media),
            self._now(),
        )
        if existing:
            incoming_status = params[7]
            existing_status = existing.get("download_status")
            if incoming_status == "pending" and existing_status in {"success", "downloaded"}:
                incoming_status = existing_status
                params = (
                    params[0],
                    params[1],
                    params[2],
                    params[3],
                    existing.get("local_path"),
                    params[5],
                    params[6],
                    incoming_status,
                    existing.get("error_message"),
                    params[9],
                    params[10],
                )
            sql = (
                "UPDATE post_media SET local_path = ?, width = ?, height = ?, download_status = ?, "
                "error_message = ?, raw_json = ?, updated_at = ? WHERE id = ?"
            )
            update_params = (
                params[4],
                params[5],
                params[6],
                params[7],
                params[8],
                params[9],
                params[10],
                existing["id"],
            )
            if self.is_postgres:
                sql = sql.replace("?", "%s")
            with closing(self.conn.cursor()) as cur:
                cur.execute(sql, update_params)
            self.conn.commit()
            return int(existing["id"]), False

        sql = (
            "INSERT INTO post_media "
            "(post_id, media_index, media_type, source_url, local_path, width, height, "
            "download_status, error_message, raw_json, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        if self.is_postgres:
            sql = sql.replace("?", "%s") + " RETURNING id"
        with closing(self.conn.cursor()) as cur:
            cur.execute(sql, params)
            media_id = cur.fetchone()["id"] if self.is_postgres else cur.lastrowid
        self.conn.commit()
        return int(media_id), True

    def list_post_media_for_post(self, post_id: int, only_pending: bool = False) -> List[Dict[str, Any]]:
        status_filter = ""
        if only_pending:
            status_filter = "AND COALESCE(download_status, '') NOT IN ('success', 'downloaded') "
        return self._fetchall(
            (
                "SELECT * FROM post_media "
                f"WHERE post_id = {self.placeholder} {status_filter}"
                "ORDER BY media_index, id"
            ),
            (post_id,),
        )

    def update_post_media_download(
        self,
        media_id: int,
        status: str,
        local_path: Optional[str] = None,
        error_message: Optional[str] = None,
        raw_json: Optional[Dict[str, Any]] = None,
    ) -> None:
        sql = (
            "UPDATE post_media SET download_status = ?, local_path = COALESCE(?, local_path), "
            "error_message = ?, raw_json = COALESCE(?, raw_json), updated_at = ? WHERE id = ?"
        )
        if self.is_postgres:
            sql = sql.replace("?", "%s")
        with closing(self.conn.cursor()) as cur:
            cur.execute(
                sql,
                (
                    status,
                    local_path,
                    error_message[:2000] if error_message else None,
                    self._json_param(raw_json) if raw_json is not None else None,
                    self._now(),
                    media_id,
                ),
            )
        self.conn.commit()

    def upsert_story(self, profile_id: int, story: Dict[str, Any]) -> Tuple[int, bool]:
        platform_story_id = str(
            story.get("id")
            or story.get("story_id")
            or story.get("media_id")
            or story.get("filename")
            or story.get("media_path")
        )
        existing = self._fetchone(
            f"SELECT id FROM stories WHERE platform_story_id = {self.placeholder}",
            (platform_story_id,),
        )
        params = (
            profile_id,
            platform_story_id,
            story.get("shortcode") or story.get("code"),
            story.get("username") or story.get("handle"),
            story.get("url") or story.get("webpage_url"),
            story.get("media_path") or story.get("filename"),
            story.get("media_type") or story.get("extension"),
            story.get("date") or story.get("published_at"),
            story.get("expires_at"),
            self._json_param(story.get("raw_json", story)),
            self._now(),
        )
        if existing:
            sql = (
                "UPDATE stories SET profile_id = ?, shortcode = ?, handle = ?, url = ?, media_path = ?, "
                "media_type = ?, published_at = ?, expires_at = ?, raw_json = ?, updated_at = ? WHERE id = ?"
            )
            update_params = (
                profile_id,
                params[2],
                params[3],
                params[4],
                params[5],
                params[6],
                params[7],
                params[8],
                params[9],
                params[10],
                existing["id"],
            )
            if self.is_postgres:
                sql = sql.replace("?", "%s")
            with closing(self.conn.cursor()) as cur:
                cur.execute(sql, update_params)
            self.conn.commit()
            return int(existing["id"]), False

        sql = (
            "INSERT INTO stories "
            "(profile_id, platform_story_id, shortcode, handle, url, media_path, media_type, "
            "published_at, expires_at, raw_json, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        if self.is_postgres:
            sql = sql.replace("?", "%s") + " RETURNING id"
        with closing(self.conn.cursor()) as cur:
            cur.execute(sql, params)
            story_id = cur.fetchone()["id"] if self.is_postgres else cur.lastrowid
        self.conn.commit()
        return int(story_id), True

    def list_posts_for_date(self, iso_date: str) -> List[Dict[str, Any]]:
        sql = (
            "SELECT p.*, pr.username AS profile_username, pr.name AS profile_name "
            "FROM posts p JOIN profiles pr ON pr.id = p.profile_id "
            f"WHERE p.taken_at_iso LIKE {self.placeholder} "
            "ORDER BY pr.username, p.taken_at_iso"
        )
        return self._fetchall(sql, (f"{iso_date}%",))

    def list_stories_for_date(self, iso_date: str) -> List[Dict[str, Any]]:
        collected_expr = "CAST(s.collected_at AS TEXT)" if self.is_postgres else "s.collected_at"
        sql = (
            "SELECT s.*, pr.username AS profile_username, pr.name AS profile_name "
            "FROM stories s JOIN profiles pr ON pr.id = s.profile_id "
            f"WHERE {collected_expr} LIKE {self.placeholder} OR s.published_at LIKE {self.placeholder} "
            "ORDER BY pr.username, s.published_at"
        )
        return self._fetchall(sql, (f"{iso_date}%", f"{iso_date}%"))

    def list_comments_for_post(self, post_id: int) -> List[Dict[str, Any]]:
        return self._fetchall(
            f"SELECT * FROM comments WHERE post_id = {self.placeholder} ORDER BY created_at",
            (post_id,),
        )

    def save_comment(self, post_id: int, comment: Dict[str, Any]) -> Tuple[int, bool]:
        existing = self._fetchone(
            f"SELECT id FROM comments WHERE platform_comment_id = {self.placeholder}",
            (str(comment["comment_id"]),),
        )
        if existing:
            return int(existing["id"]), False
        sql = (
            "INSERT INTO comments "
            "(post_id, platform_comment_id, username, user_id, text, created_at, created_at_iso, likes, raw_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        params = (
            post_id,
            str(comment["comment_id"]),
            comment.get("username"),
            comment.get("user_id"),
            comment.get("text"),
            comment.get("created_at"),
            comment.get("created_at_iso"),
            comment.get("likes"),
            self._json_param(comment.get("raw_json", comment)),
        )
        if self.is_postgres:
            sql = sql.replace("?", "%s") + " RETURNING id"
        with closing(self.conn.cursor()) as cur:
            cur.execute(sql, params)
            comment_id = cur.fetchone()["id"] if self.is_postgres else cur.lastrowid
        self.conn.commit()
        return int(comment_id), True

    def get_comment(self, comment_id: int) -> Optional[Dict[str, Any]]:
        return self._fetchone(f"SELECT * FROM comments WHERE id = {self.placeholder}", (comment_id,))

    def save_reply(self, comment_id: int, reply: Dict[str, Any]) -> Tuple[int, bool]:
        reply_platform_id = str(reply.get("reply_id") or reply.get("comment_id"))
        existing = self._fetchone(
            f"SELECT id FROM replies WHERE platform_reply_id = {self.placeholder}",
            (reply_platform_id,),
        )
        if existing:
            return int(existing["id"]), False
        sql = (
            "INSERT INTO replies "
            "(comment_id, platform_reply_id, username, user_id, text, created_at, created_at_iso, likes, raw_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        params = (
            comment_id,
            reply_platform_id,
            reply.get("username"),
            reply.get("user_id"),
            reply.get("text"),
            reply.get("created_at"),
            reply.get("created_at_iso"),
            reply.get("likes"),
            self._json_param(reply.get("raw_json", reply)),
        )
        if self.is_postgres:
            sql = sql.replace("?", "%s") + " RETURNING id"
        with closing(self.conn.cursor()) as cur:
            cur.execute(sql, params)
            reply_id = cur.fetchone()["id"] if self.is_postgres else cur.lastrowid
        self.conn.commit()
        return int(reply_id), True

    def enqueue_job(
        self,
        job_type: str,
        profile_id: Optional[int],
        post_id: Optional[int] = None,
        comment_id: Optional[int] = None,
        shortcode: Optional[str] = None,
        cursor: Optional[str] = None,
        priority: int = 0,
        max_attempts: int = 3,
    ) -> Optional[int]:
        existing = self._fetchone(
            (
                "SELECT id FROM collection_jobs WHERE job_type = ? AND status IN ('pending', 'retry', 'running') "
                "AND COALESCE(profile_id, -1) = COALESCE(?, -1) "
                "AND COALESCE(post_id, -1) = COALESCE(?, -1) "
                "AND COALESCE(comment_id, -1) = COALESCE(?, -1) "
                "AND COALESCE(cursor, '') = COALESCE(?, '')"
            ).replace("?", self.placeholder),
            (job_type, profile_id, post_id, comment_id, cursor),
        )
        if existing:
            return None
        sql = (
            "INSERT INTO collection_jobs "
            "(job_type, status, profile_id, post_id, comment_id, shortcode, cursor, priority, max_attempts) "
            "VALUES (?, 'pending', ?, ?, ?, ?, ?, ?, ?)"
        )
        params = (job_type, profile_id, post_id, comment_id, shortcode, cursor, priority, max_attempts)
        if self.is_postgres:
            sql = sql.replace("?", "%s") + " RETURNING id"
        with closing(self.conn.cursor()) as cur:
            cur.execute(sql, params)
            job_id = cur.fetchone()["id"] if self.is_postgres else cur.lastrowid
        self.conn.commit()
        return int(job_id)

    def fetch_pending_jobs(self, limit: int, job_types: Optional[Tuple[str, ...]] = None) -> List[Dict[str, Any]]:
        type_filter = ""
        params: Tuple[Any, ...] = ()
        if job_types:
            placeholders = ", ".join([self.placeholder] * len(job_types))
            type_filter = f"AND job_type IN ({placeholders}) "
            params = tuple(job_types)
        sql = (
            "SELECT * FROM collection_jobs "
            f"WHERE status IN ('pending', 'retry') AND attempts < max_attempts {type_filter}"
            "ORDER BY priority DESC, scheduled_at ASC, id ASC LIMIT ?"
        )
        params = (*params, limit)
        if self.is_postgres:
            sql = sql.replace("?", "%s")
        return self._fetchall(sql, params)

    def count_jobs_by_status(self) -> Dict[str, int]:
        rows = self._fetchall("SELECT status, COUNT(*) AS count FROM collection_jobs GROUP BY status")
        return {str(row["status"]): int(row["count"]) for row in rows}

    def count_rows(self, table: str) -> int:
        allowed = {"comments", "replies", "stories", "collection_jobs", "raw_payloads", "post_media"}
        if table not in allowed:
            raise ValueError(f"Unsupported table for count: {table}")
        row = self._fetchone(f"SELECT COUNT(*) AS count FROM {table}")
        return int(row["count"]) if row else 0

    def cleanup_comments_data(self) -> Dict[str, int]:
        before = {
            "replies": self.count_rows("replies"),
            "comments": self.count_rows("comments"),
            "comment_jobs": self._count_jobs_for_types(("comments", "replies")),
        }
        with closing(self.conn.cursor()) as cur:
            cur.execute(f"DELETE FROM raw_payloads WHERE entity_type IN ({self.placeholder}, {self.placeholder})", ("comment", "reply"))
            cur.execute(f"DELETE FROM collection_jobs WHERE job_type IN ({self.placeholder}, {self.placeholder})", ("comments", "replies"))
            cur.execute("DELETE FROM replies")
            cur.execute("DELETE FROM comments")
        self.conn.commit()
        return before

    def cleanup_stories_data(self) -> Dict[str, int]:
        before = {"stories": self.count_rows("stories")}
        with closing(self.conn.cursor()) as cur:
            cur.execute(f"DELETE FROM raw_payloads WHERE entity_type = {self.placeholder}", ("story",))
            cur.execute("DELETE FROM stories")
        self.conn.commit()
        return before

    def _count_jobs_for_types(self, job_types: Tuple[str, ...]) -> int:
        placeholders = ", ".join([self.placeholder] * len(job_types))
        row = self._fetchone(
            f"SELECT COUNT(*) AS count FROM collection_jobs WHERE job_type IN ({placeholders})",
            tuple(job_types),
        )
        return int(row["count"]) if row else 0

    def mark_job_started(self, job_id: int) -> None:
        sql = "UPDATE collection_jobs SET status = 'running', started_at = ?, attempts = attempts + 1, updated_at = ? WHERE id = ?"
        if self.is_postgres:
            sql = sql.replace("?", "%s")
        now = self._now()
        with closing(self.conn.cursor()) as cur:
            cur.execute(sql, (now, now, job_id))
        self.conn.commit()

    def mark_job_success(self, job_id: int) -> None:
        sql = "UPDATE collection_jobs SET status = 'done', finished_at = ?, updated_at = ?, error_message = NULL WHERE id = ?"
        if self.is_postgres:
            sql = sql.replace("?", "%s")
        now = self._now()
        with closing(self.conn.cursor()) as cur:
            cur.execute(sql, (now, now, job_id))
        self.conn.commit()

    def mark_job_failed(self, job_id: int, error_message: str) -> None:
        job = self._fetchone(f"SELECT attempts, max_attempts FROM collection_jobs WHERE id = {self.placeholder}", (job_id,))
        status = "failed"
        if job and int(job["attempts"]) < int(job["max_attempts"]):
            status = "retry"
        sql = "UPDATE collection_jobs SET status = ?, finished_at = ?, updated_at = ?, error_message = ? WHERE id = ?"
        if self.is_postgres:
            sql = sql.replace("?", "%s")
        now = self._now()
        with closing(self.conn.cursor()) as cur:
            cur.execute(sql, (status, now, now, error_message[:2000], job_id))
        self.conn.commit()

    def store_raw_payload(
        self,
        entity_type: str,
        entity_id: Optional[int],
        profile_id: Optional[int],
        payload: Any,
    ) -> None:
        sql = "INSERT INTO raw_payloads (entity_type, entity_id, profile_id, payload) VALUES (?, ?, ?, ?)"
        if self.is_postgres:
            sql = sql.replace("?", "%s")
        with closing(self.conn.cursor()) as cur:
            cur.execute(sql, (entity_type, entity_id, profile_id, self._json_param(payload)))
        self.conn.commit()
