import threading
import time
from collections import deque

from sentry_sdk.utils import AnnotatedValue, get_type_name

if False:  # TYPE_CHECKING
    from typing import Optional
    from sentry_sdk._types import Event, Hint


class ErrorThrottle:
    def __init__(
        self,
        per_error_interval_seconds: float = 60.0,
        global_interval_seconds: float = 60.0,
        global_limit: int = 10,
    ) -> None:
        self._per_error_interval_seconds = per_error_interval_seconds
        self._global_interval_seconds = global_interval_seconds
        self._global_limit = global_limit
        self._per_error_last_sent: "dict[str, float]" = {}
        self._global_sent_times: "deque[float]" = deque()
        self._lock = threading.Lock()

    def _get_error_name(self, event: "Event", hint: "Hint") -> "Optional[str]":
        exc_info = hint.get("exc_info")
        if exc_info is not None:
            error_type = exc_info[0]
            error_type_name = get_type_name(error_type)
            error_module = getattr(error_type, "__module__", None)
            if error_module:
                return "%s.%s" % (error_module, error_type_name)
            return error_type_name

        exceptions = (event.get("exception") or {}).get("values") or []
        if exceptions:
            first_exception = exceptions[0]
            if isinstance(first_exception, AnnotatedValue):
                first_exception = first_exception.value or {}
            error_type = first_exception.get("type")
            error_module = first_exception.get("module")
            if error_module and error_type:
                return "%s.%s" % (error_module, error_type)
            if error_type:
                return error_type
            error_value = first_exception.get("value")
            if error_value:
                return error_value

        message = event.get("message") or (event.get("logentry") or {}).get("message")
        if message:
            return str(message)

        return None

    def should_throttle(self, event: "Event", hint: "Hint") -> bool:
        now = time.monotonic()
        with self._lock:
            while self._global_sent_times and (
                now - self._global_sent_times[0]
            ) >= self._global_interval_seconds:
                self._global_sent_times.popleft()
            if len(self._global_sent_times) >= self._global_limit:
                return True
            self._global_sent_times.append(now)

            error_name = self._get_error_name(event, hint)
            if not error_name:
                return False

            last_sent = self._per_error_last_sent.get(error_name)
            if last_sent is not None and (
                now - last_sent
            ) < self._per_error_interval_seconds:
                return True

            self._per_error_last_sent[error_name] = now

        return False
