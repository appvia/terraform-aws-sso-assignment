from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
from datetime import datetime, timezone
from typing import Any
from .logging import logger
import boto3
from botocore.exceptions import ClientError


@dataclass
class Event:
    """
    Generic SNS event envelope.

    The SNS topic is expected to exist already. This module only *publishes*.
    """

    # The type of event
    event_type: str
    # The timestamp of the event
    timestamp: str
    # The detail of the event
    detail: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)

@dataclass
class Publisher:
    """
    Small publisher for assignment lifecycle SNS events.

    Args:
        topic_arn: The ARN of the SNS topic to publish the events to
    """
    # The ARN of the SNS topic to publish the events to
    topic_arn: str = field(default_factory=lambda: "")
    # The client for the SNS API
    client: boto3.client = field(default_factory=lambda: None)


    def __init__(self, topic_arn: str, region_name: str = "eu-west-2"):
        # Set the topic ARN
        self.topic_arn = topic_arn
        # Create a client for the SNS API
        self.client = boto3.client("sns", region_name=region_name)


    def publish(self, 
        event_type: str,
        detail: dict[str, Any],
    ) -> None:
        """
        Publish an assignment lifecycle event to the SNS topic.

        Args:
            event_type: The type of event to publish
            detail: The detail of the event to publish

        Returns:
            None
        """

        # Check if the topic ARN is set
        if not self.topic_arn:
            return

        # Create an event object
        evt = Event(
            event_type=event_type,
            timestamp=datetime.now(timezone.utc).isoformat(),
            detail=detail,
        )

        try:
            self.client.publish(
                TopicArn=self.topic_arn,
                Message=evt.to_json(),
                MessageAttributes={
                    "event_type": {
                      "DataType": "String",
                      "StringValue": event_type,
                    },
                },
            )

        except ClientError as e:
            # Publishing events should not break the assignment workflow.
            logger.warning(
                "Failed to publish assignment event to SNS",
                extra={
                    "action": "assignment_event_publish",
                    "event_type": event_type,
                    "topic_arn": self.topic_arn,
                    "error": str(e),
                },
            )