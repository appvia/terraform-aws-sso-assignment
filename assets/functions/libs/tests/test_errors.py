from __future__ import annotations

from libs.errors import HandlerError


class TestHandlerError:
    def test_is_runtime_error(self):
        err = HandlerError("boom")
        assert isinstance(err, RuntimeError)
        assert str(err) == "boom"

