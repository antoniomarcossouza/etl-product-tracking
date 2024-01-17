"""Módulo para construir a query de inserção no banco de dados"""

import ast
from datetime import datetime

import pandas as pd
import psycopg2


def format_date(date: int) -> datetime.datetime:
    """Formata a data para o padrão do banco de dados"""

    return datetime.fromtimestamp(date)


def upsert_operations_and_tracking_events(row: pd.Series) -> None:
    """Função que executa a procedure no banco de dados"""

    try:
        tracking_events = ast.literal_eval(row["array_trackingEvents"])

        operation_id = row["oid__id"]
        operation_created_at = format_date(row["createdAt"])
        operation_updated_at = format_date(row["updatedAt"])
        operation_last_sync_tracker = format_date(row["lastSyncTracker"])
        tracking_event_tracking_codes = [
            item["trackingCode"] for item in tracking_events
        ]
        tracking_event_created_at = [
            format_date(item["createdAt"]["$date"] / 1000) for item in tracking_events
        ]
        tracking_event_statuses = [item["status"] for item in tracking_events]
        tracking_event_descriptions = [item["description"] for item in tracking_events]
        tracking_event_tracker_types = [item["trackerType"] for item in tracking_events]
        tracking_event_origins = [item["from"] for item in tracking_events]
        tracking_event_destinations = [item["to"] for item in tracking_events]

        connection = psycopg2.connect(
            user="postgres",
            password="root",
            host="localhost",
            port="5432",
            database="product_tracking",
        )
        cur = connection.cursor()

        cur.execute(
            "CALL public.sp_upsert_operations_and_tracking_events(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                operation_id,
                operation_created_at,
                operation_updated_at,
                operation_last_sync_tracker,
                tracking_event_tracking_codes,
                tracking_event_created_at,
                tracking_event_statuses,
                tracking_event_descriptions,
                tracking_event_tracker_types,
                tracking_event_origins,
                tracking_event_destinations,
            ),
        )

        connection.commit()

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
