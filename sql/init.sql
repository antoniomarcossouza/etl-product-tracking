CREATE TABLE IF NOT EXISTS "t_deliveries" (
    "id" varchar PRIMARY KEY,
    "created_at" timestamp,
    "updated_at" timestamp,
    "last_sync_tracker" timestamp
);

CREATE TABLE IF NOT EXISTS "t_tracking_events" (
    "operation_id" varchar,
    "tracking_code" varchar,
    "created_at" timestamp,
    "status" varchar,
    "description" varchar,
    "tracker_type" varchar,
    "origin" varchar,
    "destination" varchar
);
ALTER TABLE "t_tracking_events"
ADD FOREIGN KEY ("operation_id") REFERENCES "t_deliveries" ("id");

CREATE OR REPLACE PROCEDURE sp_upsert_deliveries_and_tracking_events(
    _operation_id TEXT,
    _operation_created_at TIMESTAMP,
    _operation_updated_at TIMESTAMP,
    _operation_last_sync_tracker TIMESTAMP,
    _tracking_event_tracking_codes TEXT [],
    _tracking_event_created_at TIMESTAMP [],
    _tracking_event_statuses TEXT [],
    _tracking_event_descriptions TEXT [],
    _tracking_event_tracker_types TEXT [],
    _tracking_event_origins TEXT [],
    _tracking_event_destinations TEXT []
) LANGUAGE plpgsql AS $$
DECLARE i INT;
BEGIN
CREATE TEMP TABLE IF NOT EXISTS st_deliveries (LIKE t_deliveries);
INSERT INTO st_deliveries (id, created_at, updated_at, last_sync_tracker)
VALUES (
        _operation_id,
        _operation_created_at,
        _operation_updated_at,
        _operation_last_sync_tracker
    );
UPDATE t_deliveries
SET updated_at = st_deliveries.updated_at,
    last_sync_tracker = st_deliveries.last_sync_tracker
FROM st_deliveries
WHERE t_deliveries.id = st_deliveries.id;
INSERT INTO t_deliveries
SELECT st_deliveries.*
FROM st_deliveries
    LEFT JOIN t_deliveries ON t_deliveries.id = st_deliveries.id
WHERE t_deliveries.id IS NULL;
DELETE FROM st_deliveries;
IF array_length(_tracking_event_tracking_codes, 1) IS NOT NULL THEN 
CREATE TEMP TABLE IF NOT EXISTS st_tracking_events (LIKE t_tracking_events);
FOR i IN 1..array_length(_tracking_event_tracking_codes, 1) LOOP
INSERT INTO st_tracking_events (
        operation_id,
        tracking_code,
        created_at,
        status,
        description,
        tracker_type,
        origin,
        destination
    )
VALUES (
        _operation_id,
        _tracking_event_tracking_codes [i],
        _tracking_event_created_at [i],
        _tracking_event_statuses [i],
        _tracking_event_descriptions [i],
        _tracking_event_tracker_types [i],
        _tracking_event_origins [i],
        _tracking_event_destinations [i]
    );
END LOOP;
UPDATE t_tracking_events
SET status = st_tracking_events.status,
    description = st_tracking_events.description,
    tracker_type = st_tracking_events.tracker_type,
    origin = st_tracking_events.origin,
    destination = st_tracking_events.destination
FROM st_tracking_events
WHERE t_tracking_events.operation_id = st_tracking_events.operation_id
    AND t_tracking_events.tracking_code = st_tracking_events.tracking_code
    AND t_tracking_events.created_at = st_tracking_events.created_at;
INSERT INTO t_tracking_events
SELECT st_tracking_events.*
FROM st_tracking_events
    LEFT JOIN t_tracking_events ON t_tracking_events.operation_id = st_tracking_events.operation_id
    AND t_tracking_events.tracking_code = st_tracking_events.tracking_code
WHERE t_tracking_events.operation_id IS NULL
    AND t_tracking_events.tracking_code IS NULL;
DELETE FROM st_tracking_events;
END IF;
END;
$$;

CREATE OR REPLACE VIEW vw_trackings_per_minute AS
SELECT
    date_trunc('minute', created_at) AS minute,
    count(*) AS total
FROM t_deliveries
GROUP BY minute
ORDER BY total DESC;

CREATE OR REPLACE VIEW vw_events_per_tracking_code AS
SELECT
    tracking_code,
    COUNT(*) AS total
FROM t_tracking_events
GROUP BY tracking_code
ORDER BY total DESC;

CREATE OR REPLACE VIEW vw_top_ten_event_descriptions AS
SELECT
    description,
    COUNT(*) AS total
FROM t_tracking_events
GROUP BY description
ORDER BY total DESC
LIMIT 10;