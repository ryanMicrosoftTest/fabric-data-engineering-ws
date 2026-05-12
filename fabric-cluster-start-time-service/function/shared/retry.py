import logging

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    retry_if_result,
    stop_after_attempt,
    wait_exponential_jitter,
    before_sleep_log,
)

_FABRIC_RETRY_STATUS = {408, 429, 500, 502, 503, 504}
_WAREHOUSE_RETRY_STATUS = {500, 502, 503, 504}

_log = logging.getLogger("fcsps.retry")


def _is_retryable_response(status_codes: set[int]):
    def _check(result: object) -> bool:
        if isinstance(result, httpx.Response):
            return result.status_code in status_codes
        return False

    return _check


def fabric_retry():
    return retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential_jitter(initial=1, max=30),
        retry=(
            retry_if_exception_type((httpx.NetworkError, httpx.TimeoutException))
            | retry_if_result(_is_retryable_response(_FABRIC_RETRY_STATUS))
        ),
        before_sleep=before_sleep_log(_log, logging.WARNING),
        reraise=True,
    )


def warehouse_retry():
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=15),
        retry=(
            retry_if_exception_type((httpx.NetworkError, httpx.TimeoutException, ConnectionError, TimeoutError))
            | retry_if_result(_is_retryable_response(_WAREHOUSE_RETRY_STATUS))
        ),
        before_sleep=before_sleep_log(_log, logging.WARNING),
        reraise=True,
    )
