CREATE TABLE IF NOT EXISTS "operations" (
    "id" varchar PRIMARY KEY,
    "created_at" timestamp,
    "updated_at" timestamp,
    "last_sync_tracker" timestamp
);
CREATE TABLE IF NOT EXISTS "tracking_events" (
    "operation_id" varchar,
    "tracking_code" varchar,
    "created_at" timestamp,
    "status" varchar,
    "description" varchar,
    "tracker_type" varchar,
    "origin" varchar,
    "destination" varchar
);
ALTER TABLE "tracking_events"
ADD FOREIGN KEY ("operation_id") REFERENCES "operations" ("id");