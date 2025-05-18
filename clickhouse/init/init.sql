CREATE TABLE IF NOT EXISTS default.enriched_earthquakes (
    id String,
    ts DateTime,
    place String,
    region String,
    magnitude Float32,
    felt Nullable(Int32),
    tsunami Int32,
    url String,
    longitude Float64,
    latitude Float64,
    depth Float64,
    load_date Date
) ENGINE = MergeTree
PARTITION BY toYYYYMM(load_date)
ORDER BY (load_date, region);