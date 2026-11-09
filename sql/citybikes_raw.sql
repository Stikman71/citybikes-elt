-- Задание 1. Исходный (RAW) слой citybikes
CREATE TABLE IF NOT EXISTS NAME_PREFIX_networks (
    id TEXT PRIMARY KEY,
    name TEXT,
    location JSONB,
    href TEXT,
    company JSONB,
    gbfs_href TEXT,
    system TEXT,
    source TEXT,
    ebikes BOOLEAN,
    license JSONB,
    instances JSONB,
    scooters BOOLEAN
);

CREATE TABLE IF NOT EXISTS NAME_PREFIX_stations (
    station_id TEXT PRIMARY KEY,
    network_id TEXT NOT NULL REFERENCES NAME_PREFIX_networks(id),
    name TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    "timestamp" TEXT,
    free_bikes INTEGER,
    empty_slots INTEGER,
    extra JSONB,
    source_file TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS NAME_PREFIX_stations_network_id_idx
    ON NAME_PREFIX_stations (network_id);
