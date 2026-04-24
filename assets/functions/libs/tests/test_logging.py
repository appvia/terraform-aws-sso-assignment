from __future__ import annotations

import json
import logging

from libs.logging import JSONFormatter


class TestJSONFormatter:
    def test_format_emits_json_with_message_level_and_extras(self):
        fmt = JSONFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="x.py",
            lineno=1,
            msg="hello",
            args=(),
            exc_info=None,
        )
        record.custom_field = "extra"
        data = json.loads(fmt.format(record))
        assert data["message"] == "hello"
        assert data["level"] == "INFO"
        assert data["custom_field"] == "extra"

