"""Módulo para construir a query de inserção no banco de dados"""

import psycopg2


class DatabaseError(Exception):
    """Exception raised for database errors."""


def upsert_operations_and_tracking_events(data: list) -> None:
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

        cur.executemany(
            "CALL sp_upsert_operations_and_tracking_events(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            data,
        )
        connection.commit()
        cur.close()
    except psycopg2.DatabaseError as e:
        print(e)
    finally:
        if connection:
            connection.close()
