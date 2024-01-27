"""Classe que representa uma entrega e seus eventos de rastreio"""

import ast
from dataclasses import dataclass
from datetime import datetime


def format_date(date: str) -> datetime:
    """Formata a data para o padrão do banco de dados"""

    return datetime.fromtimestamp(int(date))


@dataclass
class Delivery:
    """Representa uma entrega de produto"""

    @dataclass
    class TrackingEvents:
        """Representa todos os eventos de rastreio de uma entrega"""

        def __init__(
            self: "Delivery.TrackingEvents", tracking_events: list
        ) -> None:
            self.tracking_codes = [
                item["trackingCode"] for item in tracking_events
            ]
            self.created_at = [
                format_date(item["createdAt"]["$date"] / 1000)
                for item in tracking_events
            ]
            self.statuses = [item["status"] for item in tracking_events]
            self.descriptions = [
                item["description"] for item in tracking_events
            ]
            self.tracker_types = [
                item["trackerType"] for item in tracking_events
            ]
            self.origins = [item["from"] for item in tracking_events]
            self.destinations = [item["to"] for item in tracking_events]

    def __init__(self: "Delivery", csv_row: list) -> None:
        self.id = csv_row[1]
        self.created_at = format_date(csv_row[2])
        self.updated_at = format_date(csv_row[3])
        self.last_sync_tracker = format_date(csv_row[4])

        self.events = self.TrackingEvents(ast.literal_eval(csv_row[5]))
