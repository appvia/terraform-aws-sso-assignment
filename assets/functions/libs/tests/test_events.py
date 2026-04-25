from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import boto3

from libs.events import Event, Publisher


class TestEvent:
    def test_to_json_contains_type_timestamp_and_detail(self):
        evt = Event(
            event_type="AccountAssignmentCreated",
            timestamp="2026-01-01T00:00:00+00:00",
            detail={"k": "v"},
        )
        assert json.loads(evt.to_json()) == {
            "event_type": "AccountAssignmentCreated",
            "timestamp": "2026-01-01T00:00:00+00:00",
            "detail": {"k": "v"},
        }


class TestPublisher:
    def test_publish_is_noop_when_topic_unset(self):
        pub = Publisher(None)
        with patch.object(boto3, "client") as bc:
            pub.publish("AccountAssignmentCreated", {"x": 1})
            bc.assert_not_called()

    def test_publish_calls_sns_publish_with_envelope(self):
        fake_client = MagicMock()
        with patch.object(boto3, "client", return_value=fake_client):
            pub = Publisher("arn:aws:sns:eu-west-1:123:topic")
            pub.publish("AccountAssignmentCreated", {"x": 1})

        fake_client.publish.assert_called_once()
        kwargs = fake_client.publish.call_args.kwargs
        assert kwargs["TopicArn"] == "arn:aws:sns:eu-west-1:123:topic"
        body = json.loads(kwargs["Message"])
        assert body["event_type"] == "AccountAssignmentCreated"
        assert body["detail"] == {"x": 1}
        assert "timestamp" in body

