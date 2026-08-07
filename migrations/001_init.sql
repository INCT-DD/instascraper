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
