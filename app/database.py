"""Módulo para construir a query de inserção no banco de dados"""
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()


class DatabaseError(Exception):
    """Exception raised for database errors."""


def upsert_operations_and_tracking_events(data: list) -> None:
    """Função que executa a procedure no banco de dados"""
    connection = None

    try:
        connection = psycopg2.connect(
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            database=os.getenv("POSTGRES_DB"),
        )
        cur = connection.cursor()

        cur.executemany(
            "CALL sp_upsert_deliveries_and_tracking_events(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            data,
        )
        connection.commit()
        cur.close()
    except psycopg2.DatabaseError as e:
        print(e)
    finally:
        if connection:
            connection.close()
