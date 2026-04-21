"""
AWS SSO Group Assignment Lambda Handler

This Lambda function orchestrates the assignment of AWS SSO groups to accounts.
It reads group configurations from DynamoDB and manages account assignments
across the organization.

The function is designed to be invoked by a Step Function in two modes:

- `source=account_creation` with a single `account_id`
- `source=cron_schedule` to process all active organization accounts

On each account, the Lambda looks for tags whose key is `{prefix}/{template_name}`,
default prefix `sso` (e.g. `sso/default`). The **value** is a comma-separated list of
IAM Identity Center **group display names**. For each pair, the template named
`template_name` must exist in DynamoDB under `group_name` and lists **permission set
names**; those permission sets are assigned to each listed group for that account.
"""

import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

# Lambda sets AWS_REGION; unit tests and other import contexts may not — botocore requires a region.
_AWS_REGION = (
    os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"
)

# Initialize the DynamoDB client
dynamodb = boto3.resource("dynamodb", region_name=_AWS_REGION)
# Initialize the Organizations client
organizations = boto3.client("organizations", region_name=_AWS_REGION)
# Initialize the SSO Admin client
sso_admin = boto3.client("sso-admin", region_name=_AWS_REGION)
# Initialize the Identity Store client
identitystore = boto3.client("identitystore", region_name=_AWS_REGION)

# Default logger for all log messages in this module, configured to emit JSON-formatted logs to stdout.
logger = logging.getLogger(__name__)
# Set the log level from the environment variable (set by Lambda) or default to INFO.
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())


@dataclass
class Group:
    # The group name
    name: str = field(default_factory=lambda: "")
    # The group ID
    id: str = field(default_factory=lambda: "")

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


# A binding of a permission set to a group
@dataclass
class Binding:
    # The account ID
    account_id: str = field(default_factory=lambda: "")
    # The permission set name
    permission_set_name: str = field(default_factory=lambda: "")
    # The permission set ARN
    permission_set_arn: str = field(default_factory=lambda: "")
    # The groups to assign the permission set to
    groups: list[Group] = field(default_factory=lambda: [])
    # The name of the template this binding came from
    template_name: str = field(default_factory=lambda: "")

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


@dataclass
class Permission:
    # The permission set name
    name: str = field(default_factory=lambda: "")
    # The groups to assign the permission set to
    groups: list[str] = field(default_factory=lambda: [])

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


@dataclass
class GroupConfiguration:
    # The permission sets to assign to the group
    permission_sets: list[str] = field(default_factory=lambda: [])
    # The enabled flag
    enabled: bool = field(default_factory=lambda: True)
    # The description
    description: str = field(default_factory=lambda: "")

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


@dataclass
class Configuration:
    # The group configurations
    groups: dict[str, GroupConfiguration] = field(default_factory=lambda: {})

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


@dataclass
class TrackedAssignment:
    """Represents an assignment tracked in the assignments_tracking DynamoDB table."""

    # Composite key: {account_id}#{principal_id}#{permission_set_arn}
    assignment_id: str = field(default_factory=lambda: "")
    # AWS account ID where assignment exists
    account_id: str = field(default_factory=lambda: "")
    # ARN of the permission set
    permission_set_arn: str = field(default_factory=lambda: "")
    # Name of the permission set (for logging)
    permission_set_name: str = field(default_factory=lambda: "")
    # Identity Center principal ID (group or user)
    principal_id: str = field(default_factory=lambda: "")
    # Type of principal: "GROUP" or "USER"
    principal_type: str = field(default_factory=lambda: "")
    # Name of the template this assignment came from
    template_name: str = field(default_factory=lambda: "")
    # Display name of the group (for logging)
    group_name: str = field(default_factory=lambda: "")
    # ISO 8601 timestamp when assignment was created
    created_at: str = field(default_factory=lambda: "")
    # ISO 8601 timestamp when assignment was last seen during reconciliation
    last_seen: str = field(default_factory=lambda: "")

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


class _JSONFormatter(logging.Formatter):
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
_handler.setFormatter(_JSONFormatter())
logger.handlers = [_handler]
logger.propagate = False


def record_tracking_assignment(
    tracking_table_name: str,
    assignment_id: str,
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
        tracking_table_name: Name of the DynamoDB tracking table
        assignment_id: Composite ID {account_id}#{principal_id}#{permission_set_arn}
        account_id: AWS account ID
        permission_set_arn: ARN of the permission set
        permission_set_name: Name of the permission set
        principal_id: Identity Center principal ID
        principal_type: Type of principal ("GROUP" or "USER")
        template_name: Name of the template this came from
        group_name: Display name of the group
    """

    now = datetime.now(timezone.utc).isoformat()
    tracked = TrackedAssignment(
        assignment_id=assignment_id,
        account_id=account_id,
        permission_set_arn=permission_set_arn,
        permission_set_name=permission_set_name,
        principal_id=principal_id,
        principal_type=principal_type,
        template_name=template_name,
        group_name=group_name,
        created_at=now,
        last_seen=now,
    )

    try:
        table = dynamodb.Table(tracking_table_name)
        table.put_item(Item=asdict(tracked))

        logger.info(
            "Recorded assignment in tracking table",
            extra={
                "action": "record_tracking_assignment",
                "assignment_id": assignment_id,
                "account_id": account_id,
                "permission_set_name": permission_set_name,
                "principal_id": principal_id,
                "template_name": template_name,
            },
        )
    except ClientError as e:
        logger.error(
            "Failed to record assignment in tracking table",
            extra={
                "action": "record_tracking_assignment",
                "assignment_id": assignment_id,
                "account_id": account_id,
            },
        )
        raise HandlerError(f"Could not record assignment in tracking table: {e}") from e


def delete_tracking_assignment(
    account_id: str,
    assignment_id: str,
    permission_set_name: str,
    tracking_table_name: str,
) -> None:
    """
    Delete a tracking assignment from the tracking table.

    Args:
        assignment_id: Composite ID {account_id}#{principal_id}#{permission_set_arn}
        account_id: AWS account ID
        permission_set_name: Name of the permission set
    """

    logger.info(
        "Deleting tracking assignment from tracking table",
        extra={
            "action": "delete_tracking_assignment",
            "assignment_id": assignment_id,
            "account_id": account_id,
            "permission_set_name": permission_set_name,
        },
    )

    try:
        table = dynamodb.Table(tracking_table_name)
        table.delete_item(Key={"assignment_id": assignment_id})
    except ClientError as e:
        logger.error(
            "Failed to delete tracking assignment from tracking table",
            extra={
                "action": "delete_tracking_assignment",
                "assignment_id": assignment_id,
                "account_id": account_id,
                "permission_set_name": permission_set_name,
            },
        )
        raise HandlerError(
            f"Could not delete tracking assignment from tracking table: {e}"
        ) from e


def get_tracking_assignments(
    tracking_table_name: str,
) -> list[TrackedAssignment]:
    """
    Returns all the tracking assignments from the tracking table.

    Args:
        tracking_table_name: Name of the DynamoDB tracking table

    Returns:
        List of tracking assignments
    """

    assignments: list[TrackedAssignment] = []
    try:
        logger.info(
            "Getting tracking assignments from tracking table",
            extra={
                "action": "get_tracking_assignments",
                "tracking_table_name": tracking_table_name,
            },
        )
        table = dynamodb.Table(tracking_table_name)
        resp = table.scan()

        for item in resp.get("Items", []):
            assignments.append(
                TrackedAssignment(
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
            )
        logger.debug(
            "Retrieved assignments",
            extra={
                "action": "get_tracking_assignments",
                "count": len(assignments),
            },
        )

        return assignments

    except ClientError as e:
        logger.error(
            "Failed to get tracked assignments",
            extra={
                "action": "get_tracking_assignments",
            },
        )
        raise HandlerError(f"Could not get assignments: {e}") from e


def delete_permission(
    instance_arn: str,
    account_id: str,
    permission_set_arn: str,
    principal_id: str,
    principal_type: str,
    poll_timeout_seconds: int = 60,
    poll_interval_seconds: float = 1.5,
) -> None:
    """
    Delete a permission set assignment and wait for completion.

    Args:
        instance_arn: The ARN of the SSO Instance
        account_id: The ID of the target account
        permission_set_arn: The ARN of the permission set
        principal_id: The ID of the principal
        principal_type: The type of principal ("GROUP" or "USER")
        poll_timeout_seconds: The timeout for the poll
        poll_interval_seconds: The interval for the poll

    Returns:
        None
    Raises:
        HandlerError: If the account assignment deletion fails
    """

    logger.info(
        "Deleting account assignment",
        extra={
            "action": "delete_permission",
            "instance_arn": instance_arn,
            "account_id": account_id,
            "permission_set_arn": permission_set_arn,
            "principal_id": principal_id,
            "principal_type": principal_type,
        },
    )

    # Initiate the deletion
    resp = sso_admin.delete_account_assignment(
        InstanceArn=instance_arn,
        PermissionSetArn=permission_set_arn,
        PrincipalId=principal_id,
        PrincipalType=principal_type,
        TargetId=account_id,
        TargetType="AWS_ACCOUNT",
    )
    request_id = resp["AccountAssignmentDeletionStatus"]["RequestId"]

    deadline = time.time() + poll_timeout_seconds

    while True:
        # Describe the account assignment deletion status
        status = sso_admin.describe_account_assignment_deletion_status(
            InstanceArn=instance_arn,
            AccountAssignmentDeletionRequestId=request_id,
        )["AccountAssignmentDeletionStatus"]

        # Get the status of the account assignment deletion
        state = status.get("Status")
        # If the status is SUCCEEDED, return
        if state == "SUCCEEDED":
            logger.info(
                "Successfully deleted permission set assignment",
                extra={
                    "action": "delete_permission",
                    "account_id": account_id,
                    "permission_set_arn": permission_set_arn,
                    "principal_id": principal_id,
                },
            )
            return
        # If the status is FAILED, raise an error
        if state == "FAILED":
            # Get the failure reason
            failure_reason = status.get("FailureReason", "unknown")
            # Raise an error with the failure reason
            raise HandlerError(
                f"Assignment deletion failed (account={account_id}, "
                f"permission_set_arn={permission_set_arn}, principal_id={principal_id}): {failure_reason}"
            )

        # If the time has expired, raise an error
        if time.time() >= deadline:
            # Raise an error with the request ID, account ID, permission set ARN, and principal ID
            raise HandlerError(
                f"Timed out waiting for assignment deletion (request_id={request_id}, "
                f"account={account_id}, permission_set_arn={permission_set_arn}, principal_id={principal_id})"
            )

        # Sleep for the poll interval
        time.sleep(poll_interval_seconds)


def has_matching_binding(
    assignment: TrackedAssignment,
    bindings: list[Binding],
) -> bool:
    """
    Check if the assignment has a matching binding.

    Args:
        assignment: The assignment to check
        binding: The list of bindings to check against

    Returns:
        True if the assignment has a matching binding, False otherwise
    """

    # Iterate over the bindings and check if the assignment has a matching binding
    for binding in bindings:
        logger.debug(
            "Checking if binding has a matching assignment",
            extra={
                "action": "has_matching_binding",
                "assignment.account_id": assignment.account_id,
                "assignment.group_name": assignment.group_name,
                "assignment.permission_set_name": assignment.permission_set_name,
                "binding.account_id": binding.account_id,
                "binding.groups": binding.groups,
                "binding.permission_set_name": binding.permission_set_name,
            },
        )
        # Ensure the assignment is for the correct account
        if assignment.account_id != binding.account_id:
            continue
        # Ensure the assignment is for the correct permission set
        if assignment.permission_set_name != binding.permission_set_name:
            continue
        # Ensure the assignment is for a group that is in the binding
        for group in binding.groups:
            if assignment.principal_id == group.id:
                logger.info(
                    "Found matching binding",
                    extra={
                        "action": "has_matching_binding",
                        "assignment.account_id": assignment.account_id,
                        "assignment.permission_set_name": assignment.permission_set_name,
                        "assignment.group_name": assignment.group_name,
                    },
                )
                return True

    return False


def reconcile_assignments(
    instance_arn: str,
    desired_bindings: list[Binding],
    tracking_table_name: Optional[str] = None,
    accounts_to_reconcile: Optional[list[str]] = None,
) -> Tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Reconcile provisioned assignments against the desired configuration.

    For each account, compare what is currently provisioned in AWS IAM Identity Center
    against what *should* exist based on the provided bindings. If an active assignment
    exists in AWS but is not present in the desired bindings, delete it.

    Args:
        instance_arn: The ARN of the SSO Instance
        desired_bindings: Full set of desired bindings (typically across all target accounts)
        tracking_table_name: Optional name of DynamoDB tracking table for marking deletions
        accounts_to_reconcile: Optional list of account IDs to reconcile.

    Returns:
        (deleted_assignments, deletion_failures) - lists of dictionaries documenting deletions
    """

    logger.info(
        "Starting assignment reconciliation",
        extra={
            "action": "reconcile_assignments",
            "instance_arn": instance_arn,
            "desired_bindings_count": len(desired_bindings),
            "tracking_table_name": tracking_table_name,
            "accounts_to_reconcile": accounts_to_reconcile,
        },
    )

    successful_deletions: list[dict[str, Any]] = []
    failed_deletions: list[dict[str, Any]] = []

    try:
        # Retrieve all active assignments from the tracking table
        assignments = get_tracking_assignments(tracking_table_name)

        logger.info(
            "Retrieved all tracking assignments from tracking table",
            extra={
                "action": "reconcile_assignments",
                "assignments_count": len(assignments),
            },
        )
        # If there are no assignments, we can return an empty list
        if len(assignments) == 0:
            logger.info(
                "No tracking assignments to reconcile",
                extra={
                    "action": "reconcile_assignments",
                },
            )
            return [], []

        # For each of the assignments, we need to find a corresponding binding in t
        # he desired_bindings list. If no matching binding is found, we need to delete
        # the assignment.
        for assignment in assignments:
            if not has_matching_binding(assignment, desired_bindings):
                logger.info(
                    "Deleting tracking assignment, no matching binding found for account",
                    extra={
                        "action": "reconcile_assignments",
                        "account_id": assignment.account_id,
                        "assignment_id": assignment.assignment_id,
                        "permission_set_name": assignment.permission_set_name,
                    },
                )
                try:
                    # Attempt to delete the assignment from the account
                    delete_permission(
                        instance_arn=instance_arn,
                        account_id=assignment.account_id,
                        permission_set_arn=assignment.permission_set_arn,
                        principal_id=assignment.principal_id,
                        principal_type=assignment.principal_type.upper(),
                    )
                    successful_deletions.append(
                        {
                            "assignment_id": assignment.assignment_id,
                            "account_id": assignment.account_id,
                            "permission_set_name": assignment.permission_set_name,
                        }
                    )
                except Exception as e:
                    # If the deletion failed because the assignment does not exist, we can ignore it
                    if "Assignment does not exist" in str(e):
                        logger.info(
                            "Assignment does not exist, skipping",
                            extra={
                                "action": "reconcile_assignments",
                            },
                        )
                    else:
                        logger.error(
                            "Error trying to delete assignment",
                            extra={
                                "action": "reconcile_assignments",
                                "assignment_id": assignment.assignment_id,
                                "account_id": assignment.account_id,
                                "permission_set_name": assignment.permission_set_name,
                                "error": str(e),
                            },
                        )
                        failed_deletions.append(
                            {
                                "assignment_id": assignment.assignment_id,
                                "account_id": assignment.account_id,
                                "permission_set_name": assignment.permission_set_name,
                                "error": str(e),
                            }
                        )

                # We need to delete the assignment from the tracking table
                delete_tracking_assignment(
                    account_id=assignment.account_id,
                    assignment_id=assignment.assignment_id,
                    permission_set_name=assignment.permission_set_name,
                    tracking_table_name=tracking_table_name,
                )
                logger.info(
                    "Deleted tracking assignment",
                    extra={
                        "action": "reconcile_assignments",
                        "failed_deletions_count": len(failed_deletions),
                        "successful_deletions_count": len(successful_deletions),
                    },
                )

            return successful_deletions, failed_deletions

    except Exception as e:
        logger.error(
            "Unhandled error during tracking assignment reconciliation",
            extra={
                "action": "reconcile_assignments",
                "error": str(e),
            },
        )
        raise


class HandlerError(RuntimeError):
    """Raised for expected handler failures that should mark the workflow failed."""


def get_identity_store_id(instance_arn: str) -> str:
    """
    Get the Identity Store ID for the given SSO Instance ARN.

    Args:
        instance_arn: The ARN of the SSO Instance
    Returns:
        The Identity Store ID
    Raises:
        HandlerError: If the Identity Store ID is not found
    """

    paginator = sso_admin.get_paginator("list_instances")
    for page in paginator.paginate():
        for inst in page.get("Instances", []):
            if inst.get("InstanceArn") == instance_arn:
                return inst["IdentityStoreId"]

    raise HandlerError(f"SSO Instance ARN not found in list_instances: {instance_arn}")


def get_permission_sets(instance_arn: str) -> dict[str, str]:
    """
    Get the permission sets in the SSO Instance.

    Args:
        instance_arn: The ARN of the SSO Instance
    Returns:
        A map of permission set names to permission set ARNs
    """

    logger.info(
        "Getting permission sets",
        extra={
            "action": "get_permission_sets",
            "instance_arn": instance_arn,
        },
    )

    # Initialize the map to store the permission sets
    permission_sets: dict[str, str] = {}
    # Create a paginator to list the permission sets
    paginator = sso_admin.get_paginator("list_permission_sets")

    for page in paginator.paginate(InstanceArn=instance_arn):
        for permission_set_arn in page.get("PermissionSets", []):
            details = sso_admin.describe_permission_set(
                InstanceArn=instance_arn,
                PermissionSetArn=permission_set_arn,
            )
            name = details.get("PermissionSet", {}).get("Name")
            if name:
                permission_sets[name] = permission_set_arn

    logger.info(
        "Found the following permission sets",
        extra={
            "action": "get_permission_sets",
            "count": len(permission_sets),
        },
    )

    return permission_sets


def get_identity_store_groups(identity_store_id: str) -> dict[str, str]:
    """
    Get the groups in the Identity Store with their IDs and names.

    Args:
        identity_store_id: The Identity Store ID

    Returns:
        A map of group display names to group IDs
    """

    logger.info(
        "Getting identity store groups",
        extra={
            "action": "get_identity_store_groups",
            "identity_store_id": identity_store_id,
        },
    )

    # Initialize a map of group IDs to names
    groups: dict[str, str] = {}
    # Create a paginator to list the groups
    paginator = identitystore.get_paginator("list_groups")

    # Iterate over the pages of the paginator
    for page in paginator.paginate(IdentityStoreId=identity_store_id):
        for group in page.get("Groups", []):
            display_name = group.get("DisplayName")
            group_id = group.get("GroupId")
            if display_name and group_id:
                groups[display_name] = group_id

    logger.info(
        "Found the following groups",
        extra={
            "action": "get_identity_store_groups",
            "identity_store_id": identity_store_id,
            "groups": groups,
            "count": len(groups),
        },
    )

    return groups


def list_active_accounts() -> list[str]:
    """
    List all active accounts in the organization.

    Returns:
        A list of account IDs
    """

    logger.info(
        "Listing active accounts",
        extra={
            "action": "list_active_accounts",
        },
    )

    # Create a paginator to list all accounts
    paginator = organizations.get_paginator("list_accounts")

    account_ids: list[str] = []
    for page in paginator.paginate():
        for acct in page.get("Accounts", []):
            if acct.get("Status") == "ACTIVE":
                account_ids.append(acct["Id"])

    logger.info(
        "Found the following active accounts",
        extra={
            "action": "list_active_accounts",
            "account_ids": account_ids,
            "count": len(account_ids),
        },
    )

    return account_ids


def get_account_tags(account_id: str) -> dict[str, str]:
    """
    Load AWS Organizations resource tags for the member account.

    Args:
        account_id: The 12-digit account ID
    Returns:
        Map of tag key to tag value
    Raises:
        HandlerError: If tags cannot be read
    """

    try:
        resp = organizations.list_tags_for_resource(ResourceId=account_id)
    except ClientError as e:
        logger.error(
            "Failed to list tags for account",
            extra={
                "action": "get_account_tags",
                "account_id": account_id,
            },
        )
        raise HandlerError(f"Could not list tags for account {account_id}") from e

    return {t["Key"]: t["Value"] for t in resp.get("Tags", [])}


def ensure_account_exists(account_id: str) -> None:
    """
    Ensure the given account exists and is accessible.

    Args:
        account_id: The ID of the account
    Raises:
        HandlerError: If the account is not found/accessible
    """

    try:
        organizations.describe_account(AccountId=account_id)

    except ClientError as e:
        logger.error(
            "Error ensuring account exists",
            extra={
                "action": "ensure_account_exists",
                "account_id": account_id,
            },
        )
        raise HandlerError(f"Account not found/accessible: {account_id}") from e


def get_account_permission_tags(
    account_tags: dict[str, str],
    tag_prefix: str,
) -> list[Permission]:
    """
    Build ``Permission`` entries from account tags whose keys are ``{prefix}/{template_name}``.

    Args:
        account_tags: Organization account tags (key -> value).
        tag_prefix: Configured prefix (e.g. ``sso`` from ``SSO_ACCOUNT_TAG_PREFIX``); defaults to ``sso``.
    Returns:
        One ``Permission`` per matching tag; ``name`` is the full tag key (template key / DynamoDB ``group_name``).
    """

    # Initialize the list to store the permission tags
    tags: list[Permission] = []

    for key, value in account_tags.items():
        if key.startswith(tag_prefix):
            permission_tag: Permission = Permission(
                name=key.split("/")[1],
                groups=[group.strip() for group in value.split(",") if group.strip()],
            )
            tags.append(permission_tag)

    return tags


def load_configuration(table_name: str) -> Configuration:
    """
    Load all enabled group configurations from DynamoDB.

    Note: the table's hash key is `group_name`. The current Terraform module
    only grants GetItem/Query, so we use a Scan via Query patterns are not possible
    without a GSI; if IAM does not allow Scan, the policy must be updated.

    Args:
        table_name: The name of the DynamoDB table
    Returns:
        A configuration object
    """

    logger.info(
        "Loading group configurations from DynamoDB",
        extra={
            "action": "load_configuration",
            "table_name": table_name,
        },
    )

    # Get the table from DynamoDB
    table = dynamodb.Table(table_name)
    # Initialize the list to store the group configurations
    configuration: Configuration = Configuration()
    # Initialize the last evaluated key to None
    last_evaluated_key: Optional[dict[str, Any]] = None

    while True:
        kwargs: dict[str, Any] = {}
        if last_evaluated_key:
            kwargs["ExclusiveStartKey"] = last_evaluated_key

        # DynamoDB Scan is the simplest way to enumerate all group configs.
        resp = table.scan(**kwargs)
        for item in resp.get("Items", []):
            group_configuration: GroupConfiguration = GroupConfiguration(
                permission_sets=item.get("permission_sets", []),
                enabled=item.get("enabled", True),
                description=item.get("description", ""),
            )
            configuration.groups[item.get("group_name")] = group_configuration

        last_evaluated_key = resp.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    logger.debug(
        "Loaded the following group configurations",
        extra={
            "action": "load_configuration",
            "groups": len(configuration.groups),
            "table_name": table_name,
        },
    )

    return configuration


def get_bindings(
    account_id: str,
    identity_store_groups: dict[str, str],
    permission_sets: dict[str, str],
    request: Permission,
    template: GroupConfiguration,
) -> Tuple[list[Binding], list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Build a list of bindings for the given request.

    Args:
        account_id: The ID of the account
        identity_store_groups: A map of all the available identity store groups
        permission_sets: A map of all the available permission sets
        request: The request to build the bindings for
        template: The group configuration template to use
    Returns:
        A tuple of (bindings, successes, failures)
    """

    # Initialize the list to store the bindings
    bindings: list[Binding] = []
    # Initialize the lists to store the successes and failures
    successes: list[dict[str, Any]] = []
    # Initialize the list to store the failures
    failures: list[dict[str, Any]] = []

    logger.info(
        "Building bindings for account",
        extra={
            "action": "get_bindings",
            "account_id": account_id,
            "permission": request.name,
        },
    )

    available_groups: list[Group] = []

    # Check all the groups exist in the identity store
    for group in request.groups:
        if group not in identity_store_groups:
            logger.warning(
                "Group not found in identity store, skipping",
                extra={
                    "action": "get_bindings",
                    "account_id": account_id,
                    "permission": request.name,
                    "group": group,
                },
            )
            failures.append(
                {
                    "account_id": account_id,
                    "permission": request.name,
                    "group": group,
                    "error": "Group not found in identity store",
                }
            )
            continue

        logger.info(
            "Group found in identity store",
            extra={
                "action": "get_bindings",
                "account_id": account_id,
                "permission": request.name,
                "group": group,
            },
        )

        # Add the group to the list of available groups
        available_groups.append(Group(name=group, id=identity_store_groups[group]))

    for ps_name in template.permission_sets:
        if ps_name not in permission_sets:
            logger.warning(
                "Permission set not found in identity store, skipping",
                extra={
                    "action": "get_bindings",
                    "account_id": account_id,
                    "permission": ps_name,
                },
            )
            failures.append(
                {
                    "account_id": account_id,
                    "permission": ps_name,
                    "error": "Permission set not found in identity store",
                }
            )
            continue

        # Build a binding for the permission set
        binding: Binding = Binding(
            account_id=account_id,
            permission_set_name=ps_name,
            permission_set_arn=permission_sets[ps_name],
            groups=available_groups,
            template_name=request.name,
        )
        bindings.append(binding)

    logger.info(
        "Built the following bindings",
        extra={
            "action": "get_bindings",
            "account_id": account_id,
            "bindings": len(bindings),
            "template_name": request.name,
        },
    )

    return bindings, successes, failures


def create_account_assignment(
    instance_arn: str,
    target_account_id: str,
    permission_set_arn: str,
    permission_set_name: str,
    principal_type: str,
    principal_id: str,
    tracking_table_name: Optional[str] = None,
    template_name: str = "",
    group_name: str = "",
    poll_timeout_seconds: int = 60,
    poll_interval_seconds: float = 1.5,
) -> None:
    """
    Create an account assignment.

    Args:
        instance_arn: The ARN of the SSO Instance
        target_account_id: The ID of the target account
        permission_set_arn: The ARN of the permission set
        permission_set_name: The name of the permission set
        principal_type: The type of principal
        principal_id: The ID of the principal
        tracking_table_name: Optional name of DynamoDB tracking table (if provided, assignment will be recorded)
        template_name: Name of the template this assignment came from
        group_name: Display name of the group
        poll_timeout_seconds: The timeout for the poll
        poll_interval_seconds: The interval for the poll

    Returns:
        None
    Raises:
        HandlerError: If the account assignment creation fails
    """

    logger.info(
        "Creating account assignment",
        extra={
            "action": "create_account_assignment",
            "instance_arn": instance_arn,
            "target_account_id": target_account_id,
            "permission_set_name": permission_set_name,
            "permission_set_arn": permission_set_arn,
            "principal_type": principal_type,
            "principal_id": principal_id,
        },
    )

    # Check if the account assignment already exists
    resp = sso_admin.list_account_assignments(
        InstanceArn=instance_arn,
        AccountId=target_account_id,
        PermissionSetArn=permission_set_arn,
    )

    # Filter the results to check if this specific principal has the assignment
    existing_assignment = any(
        assignment.get("PrincipalId") == principal_id
        and assignment.get("PrincipalType") == principal_type
        for assignment in resp.get("AccountAssignments", [])
    )

    if existing_assignment:
        logger.info(
            "Account assignment already exists, skipping",
            extra={
                "action": "create_account_assignment",
                "instance_arn": instance_arn,
                "principal_id": principal_id,
                "principal_type": principal_type,
                "target_account_id": target_account_id,
            },
        )
        return

    logger.info(
        "Account assignment does not exist, creating",
        extra={
            "action": "create_account_assignment",
            "instance_arn": instance_arn,
            "principal_id": principal_id,
            "principal_type": principal_type,
            "target_account_id": target_account_id,
        },
    )

    # Assign the permission set to the principal in the target account
    resp = sso_admin.create_account_assignment(
        InstanceArn=instance_arn,
        PermissionSetArn=permission_set_arn,
        PrincipalId=principal_id,
        PrincipalType=principal_type,
        TargetId=target_account_id,
        TargetType="AWS_ACCOUNT",
    )
    request_id = resp["AccountAssignmentCreationStatus"]["RequestId"]

    deadline = time.time() + poll_timeout_seconds

    while True:
        # Describe the account assignment creation status
        status = sso_admin.describe_account_assignment_creation_status(
            InstanceArn=instance_arn,
            AccountAssignmentCreationRequestId=request_id,
        )["AccountAssignmentCreationStatus"]

        # Get the status of the account assignment creation
        state = status.get("Status")
        # If the status is SUCCEEDED, record the assignment and return
        if state == "SUCCEEDED":
            # If tracking is enabled, record this assignment
            if tracking_table_name:
                assignment_id = (
                    f"{target_account_id}#{principal_id}#{permission_set_arn}"
                )
                try:
                    record_tracking_assignment(
                        tracking_table_name=tracking_table_name,
                        assignment_id=assignment_id,
                        account_id=target_account_id,
                        permission_set_arn=permission_set_arn,
                        permission_set_name=permission_set_name,
                        principal_id=principal_id,
                        principal_type=principal_type,
                        template_name=template_name,
                        group_name=group_name,
                    )
                except HandlerError as e:
                    logger.warning(
                        "Failed to record assignment in tracking table (but assignment was created)",
                        extra={
                            "action": "create_account_assignment",
                            "assignment_id": assignment_id,
                            "error": str(e),
                        },
                    )
            return
        # If the status is FAILED, raise an error
        if state == "FAILED":
            # Get the failure reason
            failure_reason = status.get("FailureReason", "unknown")
            # Raise an error with the failure reason
            raise HandlerError(
                f"Assignment creation failed (account={target_account_id}, "
                f"permission_set_arn={permission_set_arn}, principal_id={principal_id}): {failure_reason}"
            )

        # If the time has expired, raise an error
        if time.time() >= deadline:
            # Raise an error with the request ID, account ID, permission set ARN, and principal ID
            raise HandlerError(
                f"Timed out waiting for assignment creation (request_id={request_id}, "
                f"account={target_account_id}, permission_set_arn={permission_set_arn}, principal_id={principal_id})"
            )

        # Sleep for the poll interval
        time.sleep(poll_interval_seconds)


def assign_permissions(
    bindings: list[Binding],
    instance_arn: str,
    tracking_table_name: Optional[str] = None,
) -> Tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Assign each binding's permission set to every listed Identity Center group.

    Args:
        bindings: Per-permission-set bindings for one account
        instance_arn: The ARN of the SSO Instance
        tracking_table_name: Optional name of DynamoDB tracking table for recording assignments
    Returns:
        ``(successes, failures)`` for each attempted assignment
    """

    logger.info(
        "Assigning permission sets to groups",
        extra={
            "action": "assign_permissions",
            "bindings": bindings,
            "instance_arn": instance_arn,
        },
    )

    if len(bindings) == 0:
        logger.warning(
            "No bindings to assign",
            extra={
                "action": "assign_permissions",
                "bindings": bindings,
            },
        )
        return [], []

    # Initialize the lists to store the successes and failures
    successes: list[dict[str, Any]] = []
    # Initialize the list to store the failures
    failures: list[dict[str, Any]] = []

    # Assign the permission set to the groups
    for binding in bindings:
        # Iterate over the groups in the binding
        for group in binding.groups:
            try:
                create_account_assignment(
                    instance_arn=instance_arn,
                    permission_set_arn=binding.permission_set_arn,
                    permission_set_name=binding.permission_set_name,
                    principal_id=group.id,
                    principal_type="GROUP",
                    target_account_id=binding.account_id,
                    tracking_table_name=tracking_table_name,
                    template_name=binding.template_name,
                    group_name=group.name,
                )
                # Add the success to the list
                successes.append(
                    {
                        "account_id": binding.account_id,
                        "group_name": group.name,
                        "permission_set_arn": binding.permission_set_arn,
                        "permission_set_name": binding.permission_set_name,
                    }
                )
            except HandlerError as e:
                failures.append(
                    {
                        "account_id": binding.account_id,
                        "group_name": group.name,
                        "permission_set_arn": binding.permission_set_arn,
                        "permission_set_name": binding.permission_set_name,
                        "error": str(e),
                    }
                )

    logger.debug(
        "Completed assigning permission set to groups",
        extra={
            "action": "assign_permissions",
            "bindings_count": len(bindings),
            "successes": successes,
            "failures": failures,
        },
    )

    return successes, failures


def validate_environment() -> None:
    """
    Validate the environment variables.

    Raises:
        HandlerError: If a required environment variable is missing
    """

    required_variables = [
        "DYNAMODB_CONFIG_TABLE",
        "DYNAMODB_TRACKING_TABLE",
        "SSO_INSTANCE_ARN",
    ]
    for var in required_variables:
        if not os.environ.get(var):
            raise HandlerError(f"Missing required environment variable: {var}")


def lambda_handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    """
    Main Lambda handler for SSO group assignment.

    Args:
        event: EventBridge or Step Function event containing:
        - source: "account_creation" or "cron_schedule"
        - account_id: (optional) specific account ID for single-account mode
        _context: Lambda context object (unused, required by Lambda signature)

    Returns:
        Dictionary with status and optional error details
    """

    logger.info(
        "Starting SSO group assignment run",
        extra={
            "action": "lambda_handler",
            "event": event,
        },
    )

    # Get the current UTC timestamp
    started_at = datetime.now(timezone.utc).isoformat()
    wall_start = time.time()

    try:
        # Ensure we have a valid environment
        validate_environment()
        # Get the environment variables
        tracking_table_name = os.environ.get("DYNAMODB_TRACKING_TABLE")
        # Get the config table name
        config_table_name = os.environ.get("DYNAMODB_CONFIG_TABLE")
        # Get the SSO Instance ARN
        instance_arn = os.environ.get("SSO_INSTANCE_ARN")
        # Get the tagging prefix (module doc default: ``sso``)
        tag_prefix = os.environ.get("SSO_ACCOUNT_TAG_PREFIX") or "sso"

        logger.info(
            "Using the following environment variables",
            extra={
                "action": "lambda_handler",
                "instance_arn": instance_arn,
                "config_table_name": config_table_name,
                "tracking_table_name": tracking_table_name,
            },
        )

        # Supports the ability to assign to a single account - for debugging purposes
        account_id = event.get("account_id")
        # Determine the source of the execution
        source = event.get("source", "cron_schedule")
        # Retrieve the Identity Store ID for the given SSO Instance ARN
        identity_store_id = get_identity_store_id(instance_arn)

        if account_id:
            # Ensure the account exists and is accessible
            ensure_account_exists(account_id)
            # Set the target accounts to the single account
            target_accounts = [account_id]
        else:
            # List all active accounts
            target_accounts = list_active_accounts()

        logger.info(
            "Resolved execution targets",
            extra={
                "action": "lambda_handler",
                "config_table_name": config_table_name,
                "source": source,
                "target_accounts_count": len(target_accounts),
                "target_accounts": target_accounts,
                "tracking_table_name": tracking_table_name,
            },
        )

        # Initialize the lists to store the successes and failures
        all_successes: list[dict[str, Any]] = []
        all_failures: list[dict[str, Any]] = []

        # Load the group configurations from DynamoDB
        configuration = load_configuration(config_table_name)
        # Get the groups in the Identity Store
        identity_store_groups = get_identity_store_groups(identity_store_id)
        # Get the permission sets in the SSO Instance
        permission_sets = get_permission_sets(instance_arn)
        ## Build a list of bindings for the account
        all_bindings: list[Binding] = []

        # Iterate over the target accounts and assign the groups
        # Logic:
        # 1. Load the group configurations from DynamoDB
        # 2. Iterate over the target accounts
        # 4. We build a list of bindings for the account
        # 5. We assign the groups for the account
        for account_id in target_accounts:
            # Retrieve the account tags
            account_tags = get_account_tags(account_id)
            # Get any permission tags for the account tags
            request_permissions = get_account_permission_tags(account_tags, tag_prefix)

            # Iterate over the requests and build the bindings
            for request in request_permissions:
                # Get the template for the request
                template = configuration.groups.get(request.name)
                # If the template is not found, skip the request
                if template is None:
                    logger.warning(
                        "Permission template not found in configuration (skipping)",
                        extra={
                            "action": "lambda_handler",
                            "account_id": account_id,
                            "template_name": request.name,
                            "available_templates": configuration.groups.keys(),
                        },
                    )
                    all_failures.append(
                        {
                            "account_id": account_id,
                            "template_name": request.name,
                            "error": "Permission template not found in configuration",
                        }
                    )
                    continue

                # Check if the permission template is enabled
                if not template.enabled:
                    logger.warning(
                        "Permission template is not enabled, skipping",
                        extra={
                            "action": "lambda_handler",
                            "account_id": account_id,
                            "template_name": request.name,
                        },
                    )
                    all_failures.append(
                        {
                            "account_id": account_id,
                            "template_name": request.name,
                            "error": "Permission template is not enabled",
                        }
                    )
                    continue

                # Get all the bindings for this request
                bindings, successes, failures = get_bindings(
                    account_id=account_id,
                    identity_store_groups=identity_store_groups,
                    permission_sets=permission_sets,
                    request=request,
                    template=template,
                )
                # Add the bindings to the list
                all_bindings.extend(bindings)
                # Add the successes to the list
                all_successes.extend(successes)
                # Add the failures to the list
                all_failures.extend(failures)

        # We should at this point have all the bindings for all the accounts - we should
        # iterate over the bindings and assign the permissions to the groups
        if len(all_bindings) > 0:
            successes, failures = assign_permissions(
                bindings=all_bindings,
                instance_arn=instance_arn,
                tracking_table_name=tracking_table_name,
            )
            # Add the successes to the list
            all_successes.extend(successes)
            # Add the failures to the list
            all_failures.extend(failures)

        # Run reconciliation if tracking is enabled
        reconciliation_deleted: list[dict[str, Any]] = []
        reconciliation_failures: list[dict[str, Any]] = []

        if tracking_table_name:
            try:
                reconciliation_deleted, reconciliation_failures = reconcile_assignments(
                    instance_arn=instance_arn,
                    desired_bindings=all_bindings,
                    tracking_table_name=tracking_table_name,
                    accounts_to_reconcile=target_accounts,
                )
                logger.info(
                    "Completed assignment reconciliation",
                    extra={
                        "action": "lambda_handler",
                        "deleted_count": len(reconciliation_deleted),
                        "failure_count": len(reconciliation_failures),
                    },
                )
            except Exception as e:
                logger.error(
                    "Reconciliation failed",
                    extra={
                        "action": "lambda_handler",
                        "error": str(e),
                    },
                )
                # Log reconciliation failures but don't fail the whole handler
                reconciliation_failures.append(
                    {"error": f"Reconciliation failed: {str(e)}"}
                )

        # At the end of the loop, we should have all the bindings for the account
        status = (
            "success" if not all_failures and not reconciliation_failures else "failed"
        )

        logger.info(
            "Completed SSO group assignment run",
            extra={
                "action": "lambda_handler",
                "assignments_failed": len(all_failures),
                "assignments_succeeded": len(all_successes),
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "reconciliation_deleted": len(reconciliation_deleted),
                "reconciliation_failures": len(reconciliation_failures),
                "started_at": started_at,
                "status": status,
            },
        )

        return {
            "account_id": account_id,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "source": source,
            "started_at": started_at,
            "status": status,
            "results": {
                "succeeded": all_successes,
                "failed": all_failures,
                "reconciliation_deleted": reconciliation_deleted,
                "reconciliation_failures": reconciliation_failures,
            },
            "errors": (
                None
                if not all_failures and not reconciliation_failures
                else {
                    "count": len(all_failures) + len(reconciliation_failures),
                    "items": all_failures + reconciliation_failures,
                }
            ),
        }

    except Exception as e:
        logger.error(
            "Unhandled error during SSO group assignment run",
            extra={
                "action": "lambda_handler",
                "error": str(e),
            },
            exc_info=True,
        )
        return {
            "account_id": (event or {}).get("account_id"),
            "errors": {"message": str(e)},
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "results": None,
            "source": (event or {}).get("source", "unknown"),
            "started_at": started_at,
            "status": "error",
            "time_taken": time.time() - wall_start,
        }
