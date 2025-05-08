CREATE DATABASE analytics;

\connect analytics;

CREATE TABLE IF NOT EXISTS public.dbz_signal (
    id   VARCHAR(64),
    type VARCHAR(32),
    data VARCHAR(2048)
);

CREATE TABLE IF NOT EXISTS public.dbz_heartbeat (
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.backend_events (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            event_type VARCHAR(50) NOT NULL,
            ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE public.backend_events REPLICA IDENTITY FULL;