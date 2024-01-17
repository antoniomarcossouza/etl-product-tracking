"""Módulo para construir a query de inserção no banco de dados"""

import ast
from datetime import datetime

import pandas as pd


def format_date(date: str) -> str:
    """Formata a data para o padrão do banco de dados"""

    return datetime.fromtimestamp(date)


def create_values_string(values: list) -> str:
    """Cria uma string com os valores para a query"""

    return ", ".join(
        [
            f"'{v}'" if isinstance(v, str) else "NULL" if v is None else str(v)
            for v in values
        ]
    )


def create_tracking_events_values(fk: str, values: list):
    """Agrupa os valores em uma string para a query"""

    def format_item(item):
        """Formata o evento de rastreio para a query"""

        return create_values_string(
            [
                fk,
                item["trackingCode"],
                format_date(item["createdAt"]["$date"] / 1000),
                item["status"],
                item["description"],
                item["trackerType"],
                item["from"],
                item["to"],
            ]
        )

    formatted_items = [f"({format_item(item)})" for item in values]

    return ", ".join(formatted_items)


def build_query(row: pd.Series):
    """Constrói a query para inserir a linha do DataFrame"""
    if (ast.literal_eval(row['array_trackingEvents'])) == []:
        return None

    operation_values = [
        row["oid__id"],
        format_date(row["createdAt"]),
        format_date(row["updatedAt"]),
        format_date(row["lastSyncTracker"]),
    ]

    query = "BEGIN;\n"
    query += "INSERT INTO st_operations (id, created_at, updated_at, last_sync_tracker)\n"
    query += f"VALUES ({create_values_string(values=operation_values)});\n\n"

    query += "UPDATE t_operations\n"
    query += "SET updated_at = st_operations.updated_at, last_sync_tracker = st_operations.last_sync_tracker\n"
    query += "FROM st_operations\n"
    query += "WHERE t_operations.id = st_operations.id;\n\n"

    query += "INSERT INTO t_operations\n"
    query += "SELECT st_operations.*\n"
    query += "FROM st_operations\n"
    query += "LEFT JOIN t_operations ON t_operations.id = st_operations.id\n"
    query += "WHERE t_operations.id IS NULL;\n\n"

    query += "DELETE FROM st_operations;\n\n"

    query += "INSERT INTO st_tracking_events (operation_id, tracking_code, created_at, status, description, tracker_type, origin, destination)\n"
    query += f"VALUES {create_tracking_events_values(fk=row['oid__id'],values=ast.literal_eval(row['array_trackingEvents']))};\n\n"

    query += "UPDATE t_tracking_events\n"
    query += "SET status = st_tracking_events.status, description = st_tracking_events.description, tracker_type = st_tracking_events.tracker_type, origin = st_tracking_events.origin, destination = st_tracking_events.destination\n"
    query += "FROM st_tracking_events\n"
    query += "WHERE t_tracking_events.operation_id = st_tracking_events.operation_id AND t_tracking_events.tracking_code = st_tracking_events.tracking_code AND t_tracking_events.created_at = st_tracking_events.created_at;\n"

    query += "INSERT INTO t_tracking_events\n"
    query += "SELECT st_tracking_events.*\n"
    query += "FROM st_tracking_events\n"
    query += "LEFT JOIN t_tracking_events ON t_tracking_events.operation_id = st_tracking_events.operation_id AND t_tracking_events.tracking_code = st_tracking_events.tracking_code\n"
    query += "WHERE t_tracking_events.operation_id IS NULL AND t_tracking_events.tracking_code IS NULL;\n"

    query += "COMMIT;"
    return query
