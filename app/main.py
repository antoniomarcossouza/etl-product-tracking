"""Script de extração dos dados"""

import ast
import os
import glob
from datetime import datetime

import pandas as pd


def build_sql_insert(table_name: str, columns: list, values: list):
    """Constrói o comando SQL de inserção"""

    columns = ", ".join(columns)
    values = ", ".join(
        [
            f"'{v}'" if isinstance(v, str) else "NULL" if v is None else str(v)
            for v in values
        ]
    )

    return f"INSERT INTO {table_name} ({columns}) VALUES ({values});"


def build_query(file_id: str, row: pd.Series):
    """Constrói a query SQL"""

    file_id = datetime.strptime(file_id, "%Y%m%d-%H%M%S%f").strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )
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

    print(
        build_sql_insert(
            table_name="operations",
            columns=["file_id", "op_id", "created_at", "updated_at", "last_sync"],
            values=[file_id, op_id, created_at, updated_at, last_sync],
        )
    )


def process_file(csv_file: str):
    """Lê o arquivo CSV, processa as informações e deleta o arquivo"""

    df = pd.read_csv(csv_file)
    file_name = csv_file.split("/")[-1][:-4]

    for _, row in df.iterrows():
        build_query(file_name, row)
        break

    # for _, value in df["array_trackingEvents"].items():
    #     tracking_events = ast.literal_eval(value)
    #     break


if __name__ == "__main__":
    files = glob.glob(os.path.join("./data", "*.csv"))

    for i in range(0, len(files), 5):
        file_list = files[i : i + 5]

        for file in file_list:
            process_file(file)
            break
        break
