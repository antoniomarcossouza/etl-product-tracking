"""Módulo para construir a query de inserção no banco de dados"""
import os
from datetime import datetime

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def create_database_connection() -> psycopg2.extensions.connection:
    """Retorna uma conexão com o banco de dados"""

    return psycopg2.connect(
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        database=os.getenv("POSTGRES_DB"),
    )


def log_processed_file(
    file_name: str, started_at: datetime, concluded_at: datetime
) -> None:
    """Função que insere o nome do arquivo processado no banco de dados"""

    connection = None

    try:
        connection = create_database_connection()
        cur = connection.cursor()

        cur.execute(
            "INSERT INTO t_processed_files (created_at, started_at,concluded_at) VALUES (%s,%s,%s)",
            (datetime.strptime(file_name, "%Y%m%d-%H%M%S%f"), started_at, concluded_at),
        )
        connection.commit()
        cur.close()
    except psycopg2.DatabaseError as e:
        print(e)
    finally:
        if connection:
            connection.close()


def upsert_operations_and_tracking_events(data: list) -> None:
    """Função que executa a procedure no banco de dados"""
    connection = None

    try:
        connection = create_database_connection()
        cur = connection.cursor()

        print(isinstance(data[0], (list, tuple)))

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
