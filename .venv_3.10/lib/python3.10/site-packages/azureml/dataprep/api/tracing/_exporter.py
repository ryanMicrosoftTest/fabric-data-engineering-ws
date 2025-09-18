import json
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Sequence

from ._span import Span
from ._event import Event

logger = None


def get_logger():
    from azureml.dataprep.api._loggerfactory import _LoggerFactory

    global logger
    if logger is not None:
        return logger

    logger = _LoggerFactory.get_logger("JsonLineExporter")
    return logger


class SpanExporter(ABC):
    @abstractmethod
    def export(self, spans: Sequence[Span]) -> None:
        pass

    @abstractmethod
    def shutdown(self) -> None:
        pass


class JsonLineExporter(SpanExporter):
    def __init__(self, session_id: str, base_directory: Optional[str] = None):
        path = os.path.join(base_directory, 'python_spans_{}.jsonl'.format(session_id))
        self._file = open(path, 'w')

    def export(self, spans: Sequence[Span]) -> None:
        json_lines = '\n'.join(map(self.__class__.to_json, spans))
        self._file.write('{}\n'.format(json_lines))
        self._file.flush()

    def shutdown(self) -> None:
        self._file.close()

    @staticmethod
    def to_json(span_data: Span) -> str:
        if not span_data:
            return ''

        def serialize_span(span: Span):
            context = span.get_context()
            return json.dumps({
                'traceId': context.trace_id.to_bytes(16, 'big').hex(),
                'spanId': context.span_id.to_bytes(8, 'big').hex(),
                'parentSpanId': span.parent.span_id.to_bytes(8, 'big').hex() if span.parent else '',
                'name': span.name,
                'kind': str(span.kind),
                'startTime': to_iso_8601(span.start_time),
                'endTime': to_iso_8601(span.end_time),
                'attributes': convert_attributes(span.attributes),
                'events': convert_events(span.events),
                'status': span.status.canonical_code.value
            })

        def convert_events(events: Sequence[Event]):
            return list(map(lambda event: {
                'name': event.name,
                'timeStamp': to_iso_8601(event.timestamp),
                'attributes': convert_attributes(event.attributes)
            }, events))

        def convert_attributes(attributes):
            return attributes

        return serialize_span(span_data)


def to_iso_8601(time):
    if isinstance(time, datetime):
        return time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    return time
