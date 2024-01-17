"""Módulo para a classe que representa uma entrega e seus eventos de rastreio"""

import ast
from dataclasses import dataclass
from datetime import datetime


def format_date(date: int) -> datetime:
    """Formata a data para o padrão do banco de dados"""

    return datetime.fromtimestamp(date)


@dataclass
class Delivery:
    """Representa uma entrega de produto"""

    @dataclass
    class TrackingEvents:
        """Representa todos os eventos de rastreio de uma entrega"""

        def __init__(self, tracking_events: list):
            self.tracking_codes = [item["trackingCode"] for item in tracking_events]
            self.created_at = [
                format_date(item["createdAt"]["$date"] / 1000)
                for item in tracking_events
            ]
            self.statuses = [item["status"] for item in tracking_events]
            self.descriptions = [item["description"] for item in tracking_events]
            self.tracker_types = [item["trackerType"] for item in tracking_events]
            self.origins = [item["from"] for item in tracking_events]
            self.destinations = [item["to"] for item in tracking_events]

    def __init__(self, csv_row: list):
        self.id = csv_row[1]
        self.created_at = csv_row[2]
        self.updated_at = csv_row[3]
        self.last_sync_tracker = csv_row[4]

        self.events = self.TrackingEvents(ast.literal_eval(csv_row[5]))