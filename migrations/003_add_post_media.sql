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
