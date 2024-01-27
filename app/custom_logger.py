"""Módulo com logger customizado"""

import logging
from typing import Optional


class CustomFormatter(logging.Formatter):
    """Logging formatter to customize the timestamp"""

    def __init__(
        self: "CustomFormatter",
        fmt: Optional[str] = None,
        datefmt: Optional[str] = None,
        timezone: Optional[str] = None,
    ) -> None:
        super().__init__(fmt, datefmt)
        self.timezone = timezone


def create_logger() -> logging.Logger:
    """Função que cria um logger customizado"""

    logger = logging.getLogger()

    logger.setLevel(logging.INFO)

    formatter = CustomFormatter(
        fmt="[%(asctime)s %(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        timezone="America/Sao_Paulo",
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)

    return logger
