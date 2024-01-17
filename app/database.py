"""Módulo para construir a query de inserção no banco de dados"""

import psycopg2

from delivery import Delivery


def upsert_operations_and_tracking_events(row: Delivery) -> None:
    """Função que executa a procedure no banco de dados"""
    try:
        connection = psycopg2.connect(
            user="postgres",
            password="root",
            host="localhost",
            port="5432",
            database="product_tracking",
        )
        cur = connection.cursor()

        cur.execute(
            "CALL sp_upsert_operations_and_tracking_events(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                row.id,
                row.created_at,
                row.updated_at,
                row.last_sync_tracker,
                row.events.tracking_codes,
                row.events.created_at,
                row.events.statuses,
                row.events.descriptions,
                row.events.tracker_types,
                row.events.origins,
                row.events.destinations,
            ),
        )
        connection.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
