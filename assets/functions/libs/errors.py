from __future__ import annotations


class HandlerError(RuntimeError):
    """Raised for expected handler failures that should mark the workflow failed."""

