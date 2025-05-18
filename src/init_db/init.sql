CREATE DATABASE backend;

\connect backend;

CREATE TABLE IF NOT EXISTS public.dbz_signal (
    id   VARCHAR(64),
    type VARCHAR(32),
    data VARCHAR(2048)
);

CREATE TABLE IF NOT EXISTS public.dbz_heartbeat (
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.app_installs (
    user_id INTEGER,
    os TEXT,
    ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.order_events (
            id SERIAL PRIMARY KEY,
            order_id INTEGER NOT NULL,
            status VARCHAR(50) NOT NULL,
            ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE public.order_events REPLICA IDENTITY FULL;


CREATE TABLE public.regions (
    place_pattern TEXT PRIMARY KEY,
    region TEXT,
    place_hash TEXT
);

INSERT INTO public.regions (place_pattern, region, place_hash)
VALUES
    ('south of the Fiji Islands', 'Fiji', MD5(LOWER(TRIM('south of the Fiji Islands')))),
    ('Fiji region', 'Fiji', MD5(LOWER(TRIM('Fiji region')))),
    ('West Chile Rise', 'Chile', MD5(LOWER(TRIM('West Chile Rise')))),
    ('South Georgia Island region', 'South Georgia Island', MD5(LOWER(TRIM('South Georgia Island region')))),
    ('Pacific-Antarctic Ridge', 'Southern Ocean', MD5(LOWER(TRIM('Pacific-Antarctic Ridge')))),
    ('Mid-Indian Ridge', 'Indian Ocean', MD5(LOWER(TRIM('Mid-Indian Ridge')))),
    ('western Indian-Antarctic Ridge', 'Southern Ocean', MD5(LOWER(TRIM('western Indian-Antarctic Ridge')))),
    ('Kermadec Islands region', 'New Zealand', MD5(LOWER(TRIM('Kermadec Islands region')))),
    ('southern East Pacific Rise', 'Pacific Ocean', MD5(LOWER(TRIM('southern East Pacific Rise')))),
    ('South Sandwich Islands region', 'South Sandwich Islands', MD5(LOWER(TRIM('South Sandwich Islands region'))));


CREATE DATABASE metadata;

\connect metadata;

CREATE TABLE public.s3_max_dates (
    table_name TEXT PRIMARY KEY,
    max_date DATE,
    updated_at TIMESTAMP
);