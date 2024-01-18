"""Script de extração dos dados"""

import csv
import os
import glob
from datetime import datetime

from custom_logger import create_logger
from database import upsert_operations_and_tracking_events
from delivery import Delivery


def process_file(file_path: str):
    """Lê o arquivo CSV, processa as informações e deleta o arquivo"""

    with open(file_path, "r", encoding="utf-8") as csv_file:
        reader = csv.reader(csv_file, delimiter=",", quotechar='"')

        next(reader)

        data = []
        for row in reader:
            row_class = Delivery(row)
            data.append(
                (
                    row_class.id,
                    row_class.created_at,
                    row_class.updated_at,
                    row_class.last_sync_tracker,
                    row_class.events.tracking_codes,
                    row_class.events.created_at,
                    row_class.events.statuses,
                    row_class.events.descriptions,
                    row_class.events.tracker_types,
                    row_class.events.origins,
                    row_class.events.destinations,
                )
            )
        upsert_operations_and_tracking_events(data)


if __name__ == "__main__":
    files = glob.glob(os.path.join("./data", "*.csv"))

    logger = create_logger()

    for file in files:
        file_name = file.split("/")[-1]
        start = datetime.now()
        process_file(file)
        finish = datetime.now()
        logger.info("Arquivo %s processado com sucesso.", file.split("/")[-1])
        break
    logger.info("Todos os arquivos foram processados com sucesso.")
