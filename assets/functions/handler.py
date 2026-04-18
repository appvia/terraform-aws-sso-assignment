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

from math import log
import tempfile
from zoneinfo import available_timezones
import boto3
import json
import logging
import os
import time
from botocore.exceptions import ClientError
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, TypedDict
from dataclasses import asdict, dataclass, field

# Lambda sets AWS_REGION; unit tests and other import contexts may not — botocore requires a region.
_AWS_REGION = (
    os.environ.get("AWS_REGION")
    or os.environ.get("AWS_DEFAULT_REGION")
    or "us-east-1"
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
    groups: List[Group] = field(default_factory=lambda: [])

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


@dataclass
class Permission:
    # The permission set name
    name: str = field(default_factory=lambda: "")
    # The groups to assign the permission set to
    groups: List[str] = field(default_factory=lambda: [])

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)

@dataclass
class GroupConfiguration:
    # The permission sets to assign to the group
    permission_sets: List[str] = field(default_factory=lambda: [])
    # The enabled flag
    enabled: bool = field(default_factory=lambda: True)
    # The description
    description: str = field(default_factory=lambda: "")

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


@dataclass
class Configuration:
    # The group configurations
    groups: Dict[str, GroupConfiguration] = field(default_factory=lambda: {})

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
        log_entry: Dict[str, Any] = {
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


_handler = logging.StreamHandler()
_handler.setFormatter(_JSONFormatter())
logger.handlers = [_handler]
logger.propagate = False


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


def get_permission_sets(instance_arn: str) -> Dict[str, str]:
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
    permission_sets: Dict[str, str] = {}
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


def get_identity_store_groups(identity_store_id: str) -> Dict[str, str]:
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
    groups: Dict[str, str] = {}
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


def list_active_accounts() -> List[str]:
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

    account_ids: List[str] = []
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


def get_account_tags(account_id: str) -> Dict[str, str]:
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
    account_tags: Dict[str, str],
    tag_prefix: str,
) -> List[Permission]:
    """
    Build ``Permission`` entries from account tags whose keys are ``{prefix}/{template_name}``.

    Args:
        account_tags: Organization account tags (key -> value).
        tag_prefix: Configured prefix (e.g. ``sso`` from ``SSO_ACCOUNT_TAG_PREFIX``); defaults to ``sso``.
    Returns:
        One ``Permission`` per matching tag; ``name`` is the full tag key (template key / DynamoDB ``group_name``).
    """

    # Initialize the list to store the permission tags
    tags: List[Permission] = []

    for key, value in account_tags.items():
        if key.startswith(tag_prefix):
            permission_tag: Permission = Permission(
                name=key,
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
    last_evaluated_key: Optional[Dict[str, Any]] = None

    while True:
        kwargs: Dict[str, Any] = {}
        if last_evaluated_key:
            kwargs["ExclusiveStartKey"] = last_evaluated_key

        # DynamoDB Scan is the simplest way to enumerate all group configs.
        resp = table.scan(**kwargs)
        for item in resp.get("Items", []):
            group_configuration: GroupConfiguration = GroupConfiguration(
                permission_sets=item.get("permission_sets", []),
                enabled=item.get("enabled", True),
                description=item.get("description", ""))
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
    identity_store_groups: Dict[str, str],
    permission_sets: Dict[str, str],
    request: Permission,
    template: GroupConfiguration,
) -> Tuple[List[Binding], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Build a list of bindings for the given request.

    Args:
        account_id: The ID of the account
        identity_store_groups: A Dict of all the available identity store groups
        permission_sets: A Dict of all the available permission sets
        request: The request to build the bindings for
        template: The group configuration template to use
    Returns:
        A tuple of (bindings, successes, failures)
    """

    # Initialize the list to store the bindings
    bindings: List[Binding] = []
    # Initialize the lists to store the successes and failures
    successes: List[Dict[str, Any]] = []
    # Initialize the list to store the failures
    failures: List[Dict[str, Any]] = []

    logger.info(
        "Building bindings for account",
        extra={
            "action": "get_bindings",
            "account_id": account_id,
            "permission": request.name,
        },
    )

    available_groups: List[Group] = []

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
        PrincipalId=principal_id,
        PrincipalType=principal_type,
        TargetId=target_account_id,
        TargetType="AWS_ACCOUNT",
    )
    if resp.get("AccountAssignments"):
        logger.info(
            "Account assignment already exists, skipping",
            extra={
                "action": "create_account_assignment",
                "instance_arn": instance_arn,
            },
        )
        return
    else:
        logger.info(
            "Account assignment does not exist, creating",
            extra={
                "action": "create_account_assignment",
                "instance_arn": instance_arn,
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
        # If the status is SUCCEEDED, return
        if state == "SUCCEEDED":
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
    bindings: List[Binding],
    instance_arn: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Assign each binding's permission set to every listed Identity Center group.

    Args:
        bindings: Per-permission-set bindings for one account
        instance_arn: The ARN of the SSO Instance
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
    successes: List[Dict[str, Any]] = []
    # Initialize the list to store the failures
    failures: List[Dict[str, Any]] = []

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
        "DYNAMODB_TABLE_NAME",
        "SSO_INSTANCE_ARN",
    ]
    for var in required_variables:
        if not os.environ.get(var):
            raise HandlerError(f"Missing required environment variable: {var}")


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for SSO group assignment.

    Args:
        event: EventBridge or Step Function event containing:
            - source: "account_creation" or "cron_schedule"
            - account_id: (optional) specific account ID for single-account mode
        context: Lambda context object

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
        table_name = os.environ.get("DYNAMODB_TABLE_NAME")
        # Get the SSO Instance ARN
        instance_arn = os.environ.get("SSO_INSTANCE_ARN")
        # Get the tagging prefix (module doc default: ``sso``)
        tag_prefix = os.environ.get("SSO_ACCOUNT_TAG_PREFIX") or "sso"

        logger.info(
            "Using the following environment variables",
            extra={
                "action": "lambda_handler",
                "instance_arn": instance_arn,
                "table_name": table_name,
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
                "source": source,
                "target_accounts": target_accounts,
                "target_accounts_count": len(target_accounts),
            },
        )

        # Initialize the lists to store the successes and failures
        all_successes: List[Dict[str, Any]] = []
        all_failures: List[Dict[str, Any]] = []

        # Load the group configurations from DynamoDB
        configuration = load_configuration(table_name)
        # Get the groups in the Identity Store
        identity_store_groups = get_identity_store_groups(identity_store_id)
        # Get the permission sets in the SSO Instance
        permission_sets = get_permission_sets(instance_arn)

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
            ## Build a list of bindings for the account
            all_bindings: List[Binding] = []

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

            # We should at this point have all the bindings for the account
            successes, failures = assign_permissions(
                bindings=all_bindings,
                instance_arn=instance_arn,
            )
            # Add the successes to the list
            all_successes.extend(successes)
            # Add the failures to the list
            all_failures.extend(failures)

        # At the end of the loop, we should have all the bindings for the account
        status = "success" if not all_failures else "failed"

        logger.info(
            "Completed SSO group assignment run",
            extra={
                "action": "lambda_handler",
                "status": status,
                "started_at": started_at,
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "assignments_succeeded": len(all_successes),
                "assignments_failed": len(all_failures),
            },
        )

        return {
            "status": status,
            "source": source,
            "account_id": account_id,
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "results": {
                "succeeded": all_successes,
                "failed": all_failures,
            },
            "errors": None if not all_failures else {"count": len(all_failures), "items": all_failures},
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
            "status": "error",
            "source": (event or {}).get("source", "unknown"),
            "account_id": (event or {}).get("account_id"),
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "time_taken": time.time() - wall_start,
            "results": None,
            "errors": {"message": str(e)},
        }
