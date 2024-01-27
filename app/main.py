"""Script de extração dos dados"""

import csv
from datetime import datetime
import glob
import os

from custom_logger import create_logger
from database import (
    check_already_processed,
    log_processed_file,
    upsert_operations_and_tracking_events,
)
from delivery import Delivery


def process_file(file_path: str) -> None:
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
        if check_already_processed(file_name[:-4]):
            logger.info("Arquivo %s já foi processado.", file_name)
            continue
        start = datetime.now()
        process_file(file)
        finish = datetime.now()
        logger.info("Arquivo %s processado com sucesso.", file_name)
        log_processed_file(file_name[:-4], start, finish)
    logger.info("Todos os arquivos foram processados com sucesso.")
