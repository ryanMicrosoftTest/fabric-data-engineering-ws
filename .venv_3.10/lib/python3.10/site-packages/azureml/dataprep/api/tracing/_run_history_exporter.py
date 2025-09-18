import atexit
from queue import Queue, Empty
from threading import Event, Thread
from time import perf_counter, sleep
from typing import Sequence, List, Dict, Any

from ._event import Event as SpanEvent
from ._exporter import SpanExporter, to_iso_8601
from ._span import Span

_logger = None


def get_logger():
    from azureml.dataprep.api._loggerfactory import _LoggerFactory

    global _logger
    if _logger is not None:
        return _logger

    _logger = _LoggerFactory.get_logger("RunHistoryExporter")
    return _logger


class RunHistoryExporter(SpanExporter):
    def __init__(self, max_buffer_size: int = 512, max_flush_interval: int = 1):
        self._queue = Queue()
        self._event = Event()
        self._task = Thread(target=RunHistoryExporter.worker,
                            args=(self._queue, self._event, max_buffer_size, max_flush_interval))
        self._task.daemon = True
        self._task.start()
        atexit.register(self.__class__.on_exit, self._event, self._task)

    def export(self, spans: Sequence[Span]) -> None:
        self._queue.put(spans)

    def shutdown(self) -> None:
        self.__class__.on_exit(self._event, self._task)

    def __del__(self):
        self.__class__.on_exit(self._event, self._task)

    @staticmethod
    def worker(queue: Queue, event: Event, max_buffer_size: int, max_flush_interval: int):
        buffer = []  # type: List[Span]
        last_flushed = 0
        workspace, run = RunHistoryExporter._retry(RunHistoryExporter.get_workspace_and_run, 'Run.get_context')

        if not workspace or not run:
            return

        while not event.is_set() or not queue.empty():
            try:
                new_spans = queue.get(block=True, timeout=1)  # type: Sequence[Span]
                buffer.extend(new_spans)
            except Empty:
                pass
            last_flushed = RunHistoryExporter.flush(workspace, run, buffer, max_buffer_size,
                                                    max_flush_interval, last_flushed)

    @staticmethod
    def flush(workspace: 'Workspace', run: 'Run', buffer: List[Span], max_buffer_size: int,
              max_flush_interval: int, last_flush: float) -> float:
        def flush_internal():
            import requests

            from azureml._base_sdk_common.service_discovery import get_service_url

            payload = RunHistoryExporter.to_json(buffer, run)
            scope = ('/subscriptions/{}/resourceGroups/{}/providers'
                     '/Microsoft.MachineLearningServices'
                     '/workspaces/{}').format(workspace.subscription_id, workspace.resource_group, workspace.name)
            host = get_service_url(workspace._auth, scope, workspace._workspace_id, workspace.discovery_url)
            path = ('history/v1.0/private/subscriptions/{}/resourceGroups/{}'
                    '/providers/Microsoft.MachineLearningServices/workspaces/{}'
                    '/runs/{}/spans').format(
                workspace.subscription_id, workspace.resource_group, workspace.name, run.id
            )
            url = '{}/{}'.format(host.rstrip('/'), path)
            headers = workspace._auth.get_authentication_header()
            headers['Content-Type'] = 'application/json; charset=utf-8'
            res = requests.post(url, json=payload, headers=headers)
            if res.status_code >= 400:
                get_logger().warn('Failed to upload spans to {} with status code {} and reason {}'.format(
                    url, res.status_code, res.reason
                ))
            buffer.clear()

        elapsed = perf_counter() - last_flush
        if elapsed <= max_flush_interval and len(buffer) <= max_buffer_size:
            return last_flush

        if len(buffer) == 0:
            return last_flush

        flush_internal()
        return perf_counter()

    @staticmethod
    def get_workspace_and_run():
        from azureml.core import Run

        try:
            run = Run.get_context()
            workspace = run.experiment.workspace
            return workspace, run
        except AttributeError:
            return None, None

    @staticmethod
    def to_json(buffer: List[Span], run: 'Run'):
        def serialize_event(event: SpanEvent):
            return {
                'Name': event.name,
                'Timestamp': to_iso_8601(event.timestamp),
                'Attributes': serialize_attributes(event.attributes)
            }

        def serialize_attributes(attributes: Dict[str, Any]):
            return [{
                'key': key,
                'value': value
            } for key, value in attributes.items()]

        payload = []
        for span in buffer:
            span_context = span.get_context()
            payload.append({
                'Context': {
                    'TraceId': span_context.trace_id.to_bytes(16, 'big').hex(),
                    'SpanId': span_context.span_id.to_bytes(8, 'big').hex(),
                    'IsRemote': span_context.is_remote,
                    'IsValid': True,
                    'Tracestate': None
                },
                'Name': span.name,
                'Status': run.get_status(),
                'ParentSpanId': span.parent.span_id.to_bytes(8, 'big').hex() if span.parent else '',
                'Attributes': serialize_attributes(span.attributes),
                'Events': list(map(serialize_event, span.events)),
                'Links': [],
                'StartTimestamp': to_iso_8601(span.start_time),
                'EndTimestamp': to_iso_8601(span.end_time)
            })

        return {"Spans": payload}

    @staticmethod
    def on_exit(event: Event, task: Thread):
        event.set()
        task.join(5)

    @staticmethod
    def _retry(func, name, max_attempt=3, wait=1):
        for i in range(1, max_attempt + 1):
            try:
                return func()
            except Exception as e:
                get_logger().debug('{} failed. Attempt: {}. Error: {}'.format(name, i + 1, e))
                if i == max_attempt:
                    raise
                sleep(wait)
