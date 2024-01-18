"""Módulo com logger customizado"""

import logging
from datetime import datetime

import pytz


class CustomFormatter(logging.Formatter):
    """Logging formatter to customize the timestamp"""

    def __init__(self, fmt=None, datefmt=None, timezone=None):
        super().__init__(fmt, datefmt)
        self.timezone = timezone

    def formatTime(self, record, datefmt=None):
        """Função que formata a timestamp para um fuso horário específico"""
        current_datetime = datetime.fromtimestamp(record.created, tz=pytz.utc)
        if self.timezone:
            current_datetime = current_datetime.astimezone(pytz.timezone(self.timezone))

        if self.datefmt:
            time = current_datetime.strftime(self.datefmt)
        else:
            time = current_datetime.isoformat()

        return time


def create_logger():
    """Função que cria um logger customizado"""

    logger = logging.getLogger()

    logger.setLevel(logging.INFO)

    formatter = CustomFormatter(
        "[%(asctime)s %(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        timezone="America/Sao_Paulo",
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)

    return logger
