from __future__ import annotations
from dataclasses import asdict, dataclass
import boto3
from botocore.exceptions import ClientError
import json
from datetime import datetime, timezone

from .logging import logger
from .errors import HandlerError
from .types import Assignment


@dataclass
class Tracking:
    # The name of the DynamoDB table used to track assignments
    table_name: str
    # The client for the tracking table
    client: boto3.client

    def __init__(self, table_name: str, region_name: str = "eu-west-2"):
        # Set the table name
        self.table_name = table_name
        # Create a client for the tracking table
        self.client = boto3.resource("dynamodb", region_name=region_name).Table(
            table_name
        )


    def get_assignment_id(
        self, account_id: str, principal_id: str, permission_set_arn: str
    ) -> str:
        """
        Get the assignment ID for a given account, principal, and permission set.

        Args:
            account_id: AWS account ID
            principal_id: Identity Center principal ID (group or user)
            permission_set_arn: ARN of the permission set

        Returns:
            The assignment ID
        """

        return f"{account_id}#{principal_id}#{permission_set_arn}"


    def list(self) -> list[Assignment]:
        """
        Returns all the tracking assignments from the tracking table.

        Returns:
            List of tracking assignments
        """

        assignments: list[Assignment] = []

        try:
            logger.debug(
                "Getting tracking assignments from tracking table",
                extra={
                    "action": "list",
                    "table_name": self.table_name,
                },
            )

            scan_kwargs = {}
            while True:
                resp = self.client.scan(**scan_kwargs)

                for item in resp.get("Items", []):
                    assignment = Assignment(
                        account_id=item.get("account_id", ""),
                        assignment_id=item.get("assignment_id", ""),
                        created_at=item.get("created_at", ""),
                        group_name=item.get("group_name", ""),
                        last_seen=item.get("last_seen", ""),
                        permission_set_arn=item.get("permission_set_arn", ""),
                        permission_set_name=item.get("permission_set_name", ""),
                        principal_id=item.get("principal_id", ""),
                        principal_type=item.get("principal_type", ""),
                        template_name=item.get("template_name", ""),
                    )
                    assignments.append(assignment)

                last_evaluated_key = resp.get("LastEvaluatedKey")
                if not last_evaluated_key:
                    break

                scan_kwargs["ExclusiveStartKey"] = last_evaluated_key
        except ClientError as e:
            logger.error(
                "Failed to list tracking assignments from tracking table",
                extra={
                    "action": "list",
                    "table_name": self.table_name,
                },
            )
            raise HandlerError(
                f"Could not list tracking assignments from tracking table: {e}"
            ) from e

        return assignments


    def create(
        self,
        account_id: str,
        permission_set_arn: str,
        permission_set_name: str,
        principal_id: str,
        principal_type: str,
        template_name: str,
        group_name: str,
    ) -> None:
        """
        Record a new assignment in the tracking table.

        Args:
            account_id: AWS account ID where the assignment exists
            permission_set_arn: ARN of the permission set
            permission_set_name: Name of the permission set
            principal_id: Identity Center principal ID (group or user)
            principal_type: Type of principal ("GROUP" or "USER")
            template_name: Name of the template this assignment came from
            group_name: Display name of the group

        Returns:
            None
        Raises:
            HandlerError: If the assignment creation fails
        """

        # Get the assignment ID
        assignment_id = self.get_assignment_id(
            account_id, principal_id, permission_set_arn
        )
        # Get the current timestamp
        now = datetime.now(timezone.utc).isoformat()

        # Create the assignment
        tracked = Assignment(
            account_id=account_id,
            assignment_id=assignment_id,
            created_at=now,
            group_name=group_name,
            last_seen=now,
            permission_set_arn=permission_set_arn,
            permission_set_name=permission_set_name,
            principal_id=principal_id,
            principal_type=principal_type,
            template_name=template_name,
        )

        # Put the assignment in the tracking table
        try:
            self.client.put_item(Item=asdict(tracked))

            logger.debug(
                "Recorded assignment in tracking table",
                extra={
                    "action": "create",
                    "assignment_id": assignment_id,
                    "table_name": self.table_name,
                },
            )
        except ClientError as e:
            logger.error(
                "Failed to record assignment in tracking table",
                extra={
                    "action": "create",
                    "account_id": account_id,
                    "assignment_id": assignment_id,
                    "error": str(e),
                },
            )
            raise HandlerError(
                f"Could not record assignment in tracking table: {e}"
            ) from e


    def delete(
        self,
        assignment_id: str,
    ) -> None:

        logger.debug(
            "Deleting tracking assignment from tracking table",
            extra={
                "action": "delete",
                "assignment_id": assignment_id,
            },
        )
        try:
            self.client.delete_item(Key={"assignment_id": assignment_id})

        except ClientError as e:
            logger.error(
                "Failed to delete tracking assignment from tracking table",
                extra={
                    "action": "delete",
                    "assignment_id": assignment_id,
                    "error": str(e),
                },
            )
            raise HandlerError(
                f"Could not delete tracking assignment from tracking table: {e}"
            ) from e