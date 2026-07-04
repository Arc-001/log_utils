CREATE TABLE templates (
    id            BIGSERIAL PRIMARY KEY,
    signature     TEXT NOT NULL UNIQUE,
    regex         TEXT NOT NULL,
    fields_schema JSONB NOT NULL DEFAULT '{}',
    sample_lines  TEXT[] NOT NULL DEFAULT '{}',
    model_used    TEXT,
    status        TEXT NOT NULL DEFAULT 'review', -- 'active' | 'review'
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE logs (
    id           BIGSERIAL PRIMARY KEY,
    source       TEXT NOT NULL,
    template_id  BIGINT REFERENCES templates(id),
    ts           TIMESTAMPTZ,
    raw_message  TEXT NOT NULL,
    fields       JSONB NOT NULL DEFAULT '{}',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ON logs (source, ts);
CREATE INDEX ON logs USING GIN (fields);
