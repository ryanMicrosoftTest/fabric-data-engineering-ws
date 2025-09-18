from azureml.dataprep.native import StreamInfo
from typing import Optional
from datetime import datetime, timezone


def get_stream_info_value(si: StreamInfo):
    return {
        'streaminfo': {
            'handler': si.handler,
            'resourceidentifier': si.resource_identifier,
            'arguments': {k: get_value(v) for k, v in si.arguments.items()}
        }
    }

def get_value(value):
    if isinstance(value, StreamInfo):
        return get_stream_info_value(value)
    elif isinstance(value, str):
        return {'string': value}
    elif isinstance(value, bool):
        return {'boolean': value}
    elif isinstance(value, float):
        return {'double': value}
    elif isinstance(value, datetime):
        if not value.tzinfo:
            base = datetime(1, 1, 1)
        else:
            base = datetime(1, 1, 1, tzinfo=timezone.utc)
        diff = value - base
        ticks = diff.days * 864000000000 + diff.seconds * 10000000 + diff.microseconds * 10
        return {'datetime': ticks}
    elif isinstance(value, int):
        return {'long': value}
    elif isinstance(value, dict):
        return {'record': {k: get_value(v) for k, v in value.items()}}
    elif isinstance(value, list):
        return {'list': [get_value(v) for v in value]}
    elif value is None:
        return {'null': ''}  # value needs to be string as rslex defines `enum ValueDto { Null(String), }`
    else:
        raise TypeError('Unexpected type "{}"'.format(type(value)))


class StreamDetails:
    def __init__(self,
                 stream_info: StreamInfo,
                 portable_path: str,
                 size: Optional[int],
                 last_modified: Optional[datetime],
                 can_seek: bool):
        self.stream_info = stream_info
        self.portable_path = portable_path
        self.size = size
        self.last_modified = last_modified
        self.can_seek = can_seek
        self.can_stream = self.size is not None and self.can_seek
