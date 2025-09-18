from azureml.dataprep.api.engineapi.typedefinitions import ActivityTraceFlags


class Context:
    def __init__(self, trace_id: int, span_id: int):
        self.trace_id = trace_id
        self.span_id = span_id
        self.is_remote = False
        self.trace_flags = ActivityTraceFlags.RECORDED
        self.trace_state = {}
        self._cached_traceparent = None

    def to_w3c_traceparent(self) -> str:
        # https://www.w3.org/TR/trace-context/#traceparent-header
        if not self._cached_traceparent:
            self._cached_traceparent = '00-{}-{}-{}'.format(
                self.trace_id.to_bytes(16, 'big').hex(),
                self.span_id.to_bytes(8, 'big').hex(),
                '01' if self.trace_flags == ActivityTraceFlags.RECORDED else '00'
            )
        return self._cached_traceparent
