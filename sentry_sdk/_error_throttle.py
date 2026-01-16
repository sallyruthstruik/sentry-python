import threading
import traceback
import time
from collections import deque

from sentry_sdk.utils import AnnotatedValue, get_type_name

if False:  # TYPE_CHECKING
    from typing import Optional
    from sentry_sdk._types import Event, Hint


class ErrorThrottle:
    def __init__(
        self,
        max_total_errors_per_hour = 120,
        max_same_errors_per_hour = 60,
    ) -> None:
        self._max_total_errors_per_hour = int(max_total_errors_per_hour)
        self._max_same_errors_per_hour = int(max_same_errors_per_hour)
        self._last_sendings: "deque[tuple[float, str]]" = deque(maxlen=5000)
        self._lock = threading.Lock()
        self._interval_seconds = 3600       # 1 hour

    def _cleanup(self):
        now = time.time()
        with self._lock:
            while self._last_sendings and self._last_sendings[0][0] < now - self._interval_seconds:
                self._last_sendings.popleft()

    def _get_error_name(self, event: "Event", hint: "Hint") -> "Optional[str]":
        file_location = None
        exc_info = hint.get("exc_info")
        if exc_info is not None:
            error_type = exc_info[0]
            error_type_name = get_type_name(error_type)
            error_module = getattr(error_type, "__module__", None)
            try:
                tb = exc_info[2]
                if tb is not None:
                    last_frame = traceback.extract_tb(tb)[-1]
                    file_location = "%s:%s" % (last_frame.filename, last_frame.lineno)
            except Exception:
                file_location = None
            if error_module:
                error_name = "%s.%s" % (error_module, error_type_name)
            else:
                error_name = error_type_name
            if file_location:
                return "%s@%s" % (error_name, file_location)
            return error_name

        exceptions = (event.get("exception") or {}).get("values") or []
        if exceptions:
            first_exception = exceptions[0]
            if isinstance(first_exception, AnnotatedValue):
                first_exception = first_exception.value or {}
            error_type = first_exception.get("type")
            error_module = first_exception.get("module")
            stacktrace = first_exception.get("stacktrace") or {}
            frames = stacktrace.get("frames") or []
            if frames:
                last_frame = frames[-1] or {}
                filename = last_frame.get("filename") or last_frame.get("abs_path")
                lineno = last_frame.get("lineno")
                if filename and lineno:
                    file_location = "%s:%s" % (filename, lineno)
            if error_module and error_type:
                error_name = "%s.%s" % (error_module, error_type)
            elif error_type:
                error_name = error_type
            else:
                error_name = None
            if error_name and file_location:
                return "%s@%s" % (error_name, file_location)
            if error_name:
                return error_name
            error_value = first_exception.get("value")
            if error_value:
                return error_value

        message = event.get("message") or (event.get("logentry") or {}).get("message")
        if message:
            return str(message)

        return None

    def should_throttle(self, event: "Event", hint: "Hint") -> bool:
        now = time.time()

        event_name = self._get_error_name(event, hint)

        count_total_errors = 0
        count_same_errors = 0

        self._cleanup()

        for ts, sent_event_name in self._last_sendings:
            if ts < now - self._interval_seconds:
                continue

            count_total_errors += 1
            if event_name == sent_event_name:
                count_same_errors += 1

        if count_total_errors >= self._max_total_errors_per_hour:
            return True

        if count_same_errors >= self._max_same_errors_per_hour:
            return True

        self._last_sendings.append((now, event_name))
        return False
