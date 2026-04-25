from __future__ import annotations

import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

class JSONFormatter(logging.Formatter):
    """Emit each log record as a single JSON object."""

    _EXCLUDE_FIELDS = {
        "name",
        "msg",
        "args",
        "created",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "module",
        "msecs",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "thread",
        "threadName",
        "exc_info",
        "exc_text",
        "taskName",
    }

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, Any] = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        for key, value in record.__dict__.items():
            if key not in self._EXCLUDE_FIELDS:
                log_entry[key] = value

        if record.exc_info and record.exc_info[0] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str)


# Configure the logger to emit JSON-formatted logs to stdout.
_handler = logging.StreamHandler()
# Set the formatter to the JSON formatter
_handler.setFormatter(JSONFormatter())
# Set the handlers to the handler
logger.handlers = [_handler]
# Set the propagate to False
logger.propagate = False

# Default logger for all log messages in this module, configured to emit JSON-formatted logs to stdout.
logger = logging.getLogger(__name__)
# Set the log level from the environment variable (set by Lambda) or default to INFO.
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())