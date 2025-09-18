import atexit
import logging
import os
from abc import ABC, abstractmethod
from queue import Queue, Empty
from threading import Event, Thread
from typing import List, Callable, Any, Dict

from azureml.dataprep.api.tracing._constants import USER_FACING_NAME, EXCEPTION_MESSAGE, EXCEPTION_STACKTRACE, VERBOSITY

logger = None
_run_id = None


def get_logger():
    global logger
    if logger is not None:
        return logger

    from .._loggerfactory import _LoggerFactory

    logger = _LoggerFactory.get_logger("SpanProcessor")
    return logger


class SpanProcessor(ABC):
    def on_start(self, span: 'Span') -> None:
        pass

    @abstractmethod
    def on_end(self, span: 'Span') -> None:
        pass

    def shutdown(self) -> None:
        pass

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True


class _ChainedSpanProcessor(SpanProcessor):
    def __init__(self, span_processor: SpanProcessor):
        self._next_processor = span_processor

    def on_start(self, span: 'Span') -> None:
        self._next_processor.on_start(span)

    def on_end(self, span: 'Span') -> None:
        self._next_processor.on_end(span)

    def shutdown(self) -> None:
        self._next_processor.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return self._next_processor.force_flush(timeout_millis)


class ExporterSpanProcessor(SpanProcessor):
    def __init__(self, span_exporter: 'SpanExporter'):
        self._span_exporter = span_exporter

    def on_end(self, span: 'Span') -> None:
        try:
            self._span_exporter.export((span,))
        except Exception as e:
            get_logger().error('Exception of type {} while exporting spans.'.format(type(e).__name__))

    def shutdown(self) -> None:
        self._span_exporter.shutdown()


class AggregatedSpanProcessor(SpanProcessor):
    def __init__(self, span_processors: List[SpanProcessor]):
        self._span_processors = span_processors
        self._event = Event()
        self._start_queue = Queue()
        self._end_queue = Queue()
        self._start_task = None
        self._end_task = None
        if any(span_processors):
            on_start = lambda processor, span: processor.on_start(span)  # type: Callable[[SpanProcessor, 'Span'], None]
            on_end = lambda processor, span: processor.on_end(span)  # type: Callable[[SpanProcessor, 'Span'], None]
            self._start_task = Thread(
                target=AggregatedSpanProcessor._worker,
                args=(self._start_queue, self._event, self._span_processors, on_start)
            )
            self._end_task = Thread(
                target=AggregatedSpanProcessor._worker,
                args=(self._end_queue, self._event, self._span_processors, on_end)
            )
            self._start_task.daemon = True
            self._start_task.start()
            self._end_task.daemon = True
            self._end_task.start()
            atexit.register(self.__class__._atexit, self._event, self._end_task, self._span_processors)

    def on_end(self, span: 'Span') -> None:
        self._end_queue.put(span)

    def shutdown(self) -> None:
        self.__class__._atexit(self._event, self._end_task, self._span_processors)

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        all_successful = True
        for span_processor in self._span_processors:
            all_successful = span_processor.force_flush(timeout_millis) and all_successful
        return all_successful

    def __del__(self):
        self._event.set()
        if self._end_task:
            self._end_task.join(5)

    @staticmethod
    def _worker(queue: Queue, event: Event, span_processors: List[SpanProcessor],
                action: Callable[[SpanProcessor, 'Span'], None]):
        while not event.is_set() or not queue.empty():
            try:
                span = queue.get(block=True, timeout=1)  # type: 'Span'
                for span_processor in span_processors:
                    action(span_processor, span)
            except Empty:
                pass

    @staticmethod
    def _atexit(event: Event, task: Thread, processors: List[SpanProcessor]):
        event.set()
        task.join(5)
        for span_processor in processors:
            span_processor.shutdown()


class UserFacingSpanProcessor(_ChainedSpanProcessor):
    def __init__(self, span_processor: SpanProcessor):
        super().__init__(span_processor)

    def on_end(self, span: 'Span') -> None:
        if USER_FACING_NAME not in span.attributes:
            return

        span = _clone_span(span)

        def remove_dev_attributes(attributes):
            for key in attributes:
                if key.startswith('aml.attr.dev.'):
                    del attributes[key]

        remove_dev_attributes(span.attributes)
        for event in span.events:
            remove_dev_attributes(event.attributes)

        super().on_end(span)


class DevFacingSpanProcessor(_ChainedSpanProcessor):
    def __init__(self, span_processor: SpanProcessor):
        super().__init__(span_processor)

    def on_end(self, span: 'Span') -> None:
        span = _clone_span(span)

        def scrub_attribute(attributes: Dict[str, Any]):
            redacted = '[REDACTED]'
            predicates = [
                lambda k: k.startswith('aml.attr.user.'),
                lambda k: k == EXCEPTION_MESSAGE,
                lambda k: k == EXCEPTION_STACKTRACE
            ]  # type: List[Callable[[str], bool]]

            for key in attributes:
                for predicate in predicates:
                    if predicate(key):
                        attributes[key] = redacted

        scrub_attribute(span.attributes)
        for event in span.events:
            scrub_attribute(event.attributes)

        super().on_end(span)


class VerbositySampledSpanProcessor(_ChainedSpanProcessor):
    def __init__(self, span_processor: SpanProcessor, level=logging.DEBUG):
        self._level = level
        super().__init__(span_processor)

    def on_end(self, span: 'Span') -> None:
        level = span.attributes.get(VERBOSITY, logging.NOTSET)
        if isinstance(level, str):
            level = getattr(logging, level.upper(), logging.NOTSET)  # type: int
        if level >= self._level:
            super().on_end(span)


class AmlContextSpanProcessor(_ChainedSpanProcessor):
    def __init__(self, span_processor: SpanProcessor):
        super().__init__(span_processor)

    def on_end(self, span: 'Span') -> None:
        self.__class__._add_aml_context(span)
        super().on_end(span)

    @staticmethod
    def _add_aml_context(span: 'Span'):
        from .._loggerfactory import session_id

        span.set_attribute('sessionId', session_id)
        span.set_attribute('runId', _run_id)


def _clone_span(span: 'Span') -> 'Span':
    from ._span import Span
    from ._event import Event

    def clone_event(event: Event) -> Event:
        return Event(event.name, event.timestamp, event.attributes.copy())

    cloned = Span(span.name, span.parent, span._span_processors)
    cloned._trace_id = span.trace_id
    cloned._span_id = span.span_id
    cloned._start_time = span.start_time
    cloned._end_time = span.end_time
    cloned._attributes = span.attributes.copy()
    cloned._events = [clone_event(event) for event in span.events]
    cloned._status = span.status
    return cloned


def _get_run_id():
    global _run_id
    if _run_id is not None:
        return _run_id
    try:
        from azureml._run_impl.constants import RunEnvVars
        _run_id = os.environ.get(RunEnvVars.ID)
        if not _run_id:
            # We first check the environment variable instead of doing Run.get_context as Run.get_context can try
            # to initialize a PythonFS which in a multithreaded scenario has the potential to cause a deadlock.
            from azureml.core import Run
            _run_id = Run.get_context().id
        return _run_id
    except:
        _run_id = '[Unavailable]'


def _add_aml_context(span: 'Span'):
    from .._loggerfactory import session_id
    run_id = _get_run_id()

    span.set_user_facing_attribute('session_id', session_id)
    span.set_user_facing_attribute('run_id', run_id)
