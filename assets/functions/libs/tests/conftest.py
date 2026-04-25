from __future__ import annotations

import os
import sys

import pytest


@pytest.fixture(autouse=True)
def _boto3_test_env() -> None:
    """
    Prevent boto3/botocore from attempting to resolve credentials during import.

    Many of the libs unit tests patch boto3 clients/resources, but boto3 still
    tries to resolve credentials eagerly in some environments.
    """
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
    os.environ.setdefault("AWS_SESSION_TOKEN", "test")
    os.environ.setdefault("AWS_DEFAULT_REGION", "eu-west-1")
    os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")


def pytest_configure() -> None:
    # Ensure `assets/functions` is on sys.path so `import libs.*` works no matter
    # where pytest is invoked from.
    this_dir = os.path.dirname(os.path.abspath(__file__))  # .../libs/tests
    functions_dir = os.path.dirname(os.path.dirname(this_dir))  # .../functions
    if functions_dir not in sys.path:
        sys.path.insert(0, functions_dir)

