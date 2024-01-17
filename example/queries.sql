CREATE TABLE IF NOT EXISTS "t_operations" (
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
ADD FOREIGN KEY ("operation_id") REFERENCES "t_operations" ("id");

CREATE OR REPLACE PROCEDURE sp_upsert_operations_and_tracking_events(
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
CREATE TEMP TABLE IF NOT EXISTS st_operations (LIKE t_operations);
INSERT INTO st_operations (id, created_at, updated_at, last_sync_tracker)
VALUES (
        _operation_id,
        _operation_created_at,
        _operation_updated_at,
        _operation_last_sync_tracker
    );
UPDATE t_operations
SET updated_at = st_operations.updated_at,
    last_sync_tracker = st_operations.last_sync_tracker
FROM st_operations
WHERE t_operations.id = st_operations.id;
INSERT INTO t_operations
SELECT st_operations.*
FROM st_operations
    LEFT JOIN t_operations ON t_operations.id = st_operations.id
WHERE t_operations.id IS NULL;
DELETE FROM st_operations;
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
