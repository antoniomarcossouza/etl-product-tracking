"""Script de extração dos dados"""

import ast
import os
import glob
from datetime import datetime

import pandas as pd


def build_query(row: pd.Series):
    """Constrói a query para inserir a linha do DataFrame"""

    def create_upsert(table_name: str, columns: list, values: list):
        """Constrói o INSERT"""

        columns = ", ".join(columns)
        values = ", ".join(
            [
                f"'{v}'" if isinstance(v, str) else "NULL" if v is None else str(v)
                for v in values
            ]
        )
        set_values = [f"{c} = excluded.{c}" for c in columns.split(", ")]
        query = f"""
        INSERT INTO {table_name} ({columns})
        VALUES ({values}) ON CONFLICT ({columns}) DO 
        UPDATE SET {', '.join(set_values)};"""

        return query

    query = "BEGIN;\n"

    op_id = row["oid__id"]
    created_at = datetime.fromtimestamp(row["createdAt"]).strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )
    updated_at = datetime.fromtimestamp(row["updatedAt"]).strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )
    last_sync = datetime.fromtimestamp(row["lastSyncTracker"]).strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )

    operation_query = create_upsert(
        table_name="operations",
        columns=["id", "created_at", "updated_at", "last_sync"],
        values=[op_id, created_at, updated_at, last_sync],
    )
    query += f"{chr(9)}{operation_query}{chr(10)}"

    for item in ast.literal_eval(row["array_trackingEvents"]):
        created_at = datetime.fromtimestamp(
            (item["createdAt"]["$date"] / 1000)
        ).strftime("%Y-%m-%d %H:%M:%S.%f")
        tracking_code = item["trackingCode"]
        status = item["status"]
        description = item["description"]
        tracker_type = item["trackerType"]
        origin = item["from"]
        destination = item["to"]

        tracking_event = create_upsert(
            table_name="tracking_events",
            columns=[
                "operation_id",
                "tracking_code",
                "created_at",
                "status",
                "description",
                "tracker_type",
                "origin",
                "destination",
            ],
            values=[
                op_id,
                tracking_code,
                created_at,
                status,
                description,
                tracker_type,
                origin,
                destination,
            ],
        )

        query += f"{chr(9)}{tracking_event}{chr(10)}"

    query += "COMMIT;"
    return query


def process_file(csv_file: str):
    """Lê o arquivo CSV, processa as informações e deleta o arquivo"""

    df = pd.read_csv(csv_file)
    # _ = csv_file.split("/")[-1][:-4]

    for _, row in df.iterrows():
        query = build_query(row)
        print(query)
        break


if __name__ == "__main__":
    files = glob.glob(os.path.join("./data", "*.csv"))

    for file in files:
        process_file(file)
        break
