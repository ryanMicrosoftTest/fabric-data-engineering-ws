from datetime import datetime
from typing import Dict, Any


class Event:
    def __init__(self, name: str, timestamp: datetime, attributes: Dict[str, Any]):
        self._name = name
        self._timestamp = timestamp
        self._attributes = attributes

    @property
    def name(self):
        return self._name

    @property
    def timestamp(self):
        return self._timestamp

    @property
    def attributes(self):
        return self._attributes
