CREATE TABLE IF NOT EXISTS "t_operations" (
    "id" varchar PRIMARY KEY,
    "created_at" timestamp,
    "updated_at" timestamp,
    "last_sync_tracker" timestamp
);
CREATE TABLE IF NOT EXISTS "st_operations" (LIKE "t_operations");

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
CREATE TABLE IF NOT EXISTS "st_tracking_events" (LIKE "t_tracking_events");