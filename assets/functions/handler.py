"""
AWS SSO Group Assignment Lambda Handler

This Lambda function orchestrates the assignment of AWS SSO groups to accounts.
It reads group configurations from DynamoDB and manages account assignments
across the organization.
"""

import fnmatch
import json
import logging
import os
import re
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

# Default logger for all log messages in this module, configured to emit JSON-formatted logs to stdout.
logger = logging.getLogger(__name__)
# Set the log level from the environment variable (set by Lambda) or default to INFO.
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())


class HandlerError(RuntimeError):
    """Raised for expected handler failures that should mark the workflow failed."""


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


@dataclass
class Group:
    # The group name
    name: str = field(default_factory=lambda: "")
    # The group ID
    id: str = field(default_factory=lambda: "")

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


@dataclass
class PermissionSet:
    # The permission set name
    name: str = field(default_factory=lambda: "")
    # The permission set ARN
    arn: str = field(default_factory=lambda: "")

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
class Account:
    # The account ID
    id: str = field(default_factory=lambda: "")
    # The account name
    name: str = field(default_factory=lambda: "")
    # The account tags
    tags: dict[str, str] = field(default_factory=lambda: {})
    # The account organizational unit path
    organizational_unit_path: str = field(default_factory=lambda: "")

    def get_permission_tags(self, prefix: str) -> list[Permission]:
        """
        Return a list of permissions for the account.

        Args:
            prefix: The prefix of the permission tags

        Returns:
            A list of permissions for the account
        """

        # Initialize the list to store the permissions
        permissions: list[Permission] = []

        for key, value in self.tags.items():
            if key.startswith(prefix):
                permission: Permission = Permission(
                    name=key.split("/")[1],
                    groups=[
                        group.strip() for group in value.split(",") if group.strip()
                    ],
                )
                permissions.append(permission)

        return permissions

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


@dataclass
class Template:
    # The permission sets to assign to the group
    permission_sets: list[str] = field(default_factory=lambda: [])
    # A human-readable description of the template
    description: str = field(default_factory=lambda: "")

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


@dataclass
class Configuration:
    # The account-level templates used to auto-provision assignments
    account_templates: dict[str, AccountTemplate] = field(default_factory=lambda: {})
    # The permission-set templates referenced by account tags or account templates
    templates: dict[str, Template] = field(default_factory=lambda: {})

    def __init__(self, table_name: str):
        # Set the table name
        self.table_name = table_name
        # Initialize in-memory configuration maps (dataclass fields are not set
        # automatically because we implement a custom __init__).
        self.account_templates = {}
        self.templates = {}
        # Create a client for the tracking table
        self.client = boto3.resource("dynamodb", region_name=_AWS_REGION).Table(
            table_name
        )

    def load(self) -> None:
        """
        Load the configuration from the DynamoDB table.
        """
        resp = self.client.scan()

        logger.info(
            "Loading configuration from DynamoDB",
            extra={
                "action": "load",
                "table_name": self.table_name,
            },
        )

        for item in resp.get("Items", []):
            item_type = item.get("type", "template")
            if item_type == "account_template":
                self.account_templates[item.get("group_name")] = AccountTemplate(
                    name=item.get("group_name", ""),
                    matcher=AccountTemplateMatcher(
                        organizational_units=item.get("matcher", {}).get(
                            "organizational_units", []
                        ),
                        name_pattern=item.get("matcher", {}).get("name_pattern"),
                        name_patterns=item.get("matcher", {}).get("name_patterns", []),
                        account_tags=item.get("matcher", {}).get("account_tags", {}),
                    ),
                    excluded=item.get("excluded", []) or [],
                    template_names=item.get("template_names", []) or [],
                    groups=item.get("groups", []) or [],
                    description=item.get("description", "") or "",
                )
            else:
                self.templates[item.get("group_name")] = Template(
                    permission_sets=item.get("permission_sets", []),
                    description=item.get("description", ""),
                )

        logger.info(
            "Successfully loaded configuration from DynamoDB",
            extra={
                "action": "load",
                "account_templates": len(self.account_templates),
                "table_name": self.table_name,
                "templates": len(self.templates),
            },
        )

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


@dataclass
class Assignment:
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


@dataclass
class AccountTemplateMatcher:
    """
    Represents conditions for matching accounts in account-level templates.
    """

    # Match by organizational unit trailing path with glob patterns
    # e.g., ["production/accounts/*", "prod"]
    organizational_units: list[str] = field(default_factory=list)
    # Match by account name with glob pattern (e.g., "prod-*")
    name_pattern: Optional[str] = None
    # Match by account name with glob patterns (ANY can match, Python fnmatch)
    name_patterns: list[str] = field(default_factory=list)
    # Match by account tags - all must exist and match (AND logic)
    # e.g., {"Environment": "Production", "CostCenter": "Engineering"}
    account_tags: dict[str, str] = field(default_factory=dict)

    def matches(self, account: Account) -> bool:
        """
        Check if an account matches all specified conditions (AND logic).
        If a condition is not specified, it's considered a pass.

        Args:
            account: The account to check

        Returns:
            True if all specified conditions match, False otherwise
        """
        # Check organizational units (if specified)
        if self.organizational_units:
            if not self.matches_organizational_unit(
                account.organizational_unit_path, self.organizational_units
            ):
                return False
        # Check account name pattern (if specified)
        if self.name_pattern:
            if not self.matches_account_name(account.name, self.name_pattern):
                return False
        # Check account name patterns (if specified)
        if self.name_patterns:
            if not self.matches_account_name_patterns(account.name, self.name_patterns):
                return False
        # Check account tags (if specified, all must match)
        if self.account_tags:
            if not self.matches_account_tags(account.tags, self.account_tags):
                return False

        return True

    def matches_organizational_unit(
        self, account_ou: Optional[str], patterns: list[str]
    ) -> bool:
        """
        Match account organizational unit against glob patterns (trailing path matching).

        The account_ou is typically in format: r-xxxx/ou-prod/ou-workloads
        The patterns are trailing paths like: "prod/workloads/*" or "production/accounts/*"

        Args:
            account_ou: The full OU path from AWS Organizations
            patterns: List of glob patterns to match against

        Returns:
            True if account_ou matches ANY pattern, False otherwise
        """
        if not account_ou or not patterns:
            return False

        # Extract the trailing path from the full OU path
        # Example: "r-xxxx/ou-prod/ou-workloads" -> "prod/workloads"
        ou_parts = account_ou.split("/")[1:]  # Skip root identifier
        ou_trailing = "/".join(ou_parts)

        # Match against any of the patterns
        for pattern in patterns:
            if fnmatch.fnmatch(ou_trailing, pattern):
                logger.debug(
                    "OU matched",
                    extra={
                        "action": "match_organizational_unit",
                        "ou_trailing": ou_trailing,
                        "pattern": pattern,
                        "matched": True,
                    },
                )
                return True

        logger.debug(
            "OU did not match any patterns",
            extra={
                "action": "match_organizational_unit",
                "ou_trailing": ou_trailing,
                "patterns": patterns,
                "matched": False,
            },
        )
        return False

    def matches_account_name(self, account_name: str, pattern: str) -> bool:
        """
        Match account name against a glob pattern.

        Args:
            account_name: The account name from AWS Organizations
            pattern: Glob pattern to match against (e.g., "prod-*")

        Returns:
            True if account_name matches pattern, False otherwise
        """
        if not account_name or not pattern:
            return False

        matched = fnmatch.fnmatch(account_name, pattern)
        logger.debug(
            "Account name match result",
            extra={
                "action": "match_account_name",
                "account_name": account_name,
                "pattern": pattern,
                "matched": matched,
            },
        )
        return matched

    def matches_account_name_patterns(
        self, account_name: str, patterns: list[str]
    ) -> bool:
        if not account_name or not patterns:
            return False
        for pattern in patterns:
            if not pattern:
                continue
            # Support both glob patterns (fnmatch) and regex patterns.
            # Terraform users commonly supply regexes like ".*" to mean "match all".
            matched = fnmatch.fnmatch(account_name, pattern)
            if not matched:
                try:
                    matched = re.search(pattern, account_name) is not None
                except re.error:
                    matched = False
            if matched:
                logger.debug(
                    "Account name pattern matched",
                    extra={
                        "action": "match_account_name_patterns",
                        "account_name": account_name,
                        "pattern": pattern,
                        "matched": True,
                    },
                )
                return True

        logger.debug(
            "Account name did not match any patterns",
            extra={
                "action": "match_account_name_patterns",
                "account_name": account_name,
                "patterns": patterns,
                "matched": False,
            },
        )
        return False

    def matches_account_tags(
        self, account_tags: dict[str, str], required_tags: dict[str, str]
    ) -> bool:
        if not required_tags:
            return True  # No required tags means pass

        for tag_key, tag_value in required_tags.items():
            if tag_key not in account_tags:
                logger.debug(
                    "Required tag not found on account",
                    extra={
                        "action": "match_account_tags",
                        "required_tag_key": tag_key,
                        "matched": False,
                    },
                )
                return False

            if account_tags[tag_key] != tag_value:
                logger.debug(
                    "Account tag value does not match required value",
                    extra={
                        "action": "match_account_tags",
                        "tag_key": tag_key,
                        "account_value": account_tags[tag_key],
                        "required_value": tag_value,
                        "matched": False,
                    },
                )
                return False

        return True


@dataclass
class AccountTemplate:
    """Represents an account-level template matcher configuration."""

    # Name of the account template matcher (from DynamoDB group_name)
    name: str = field(default_factory=lambda: "")
    # Matcher conditions (logical AND)
    matcher: AccountTemplateMatcher = field(default_factory=AccountTemplateMatcher)
    # Excluded account regex patterns (Python ``re`` syntax). If any pattern matches
    # the account ID or account name, the template is not applied.
    excluded: list[str] = field(default_factory=list)
    # List of template names to apply
    template_names: list[str] = field(default_factory=list)
    # List of groups from those templates to assign
    groups: list[str] = field(default_factory=list)
    # Human-readable description
    description: str = field(default_factory=lambda: "")

    def is_excluded(self, account: Account) -> bool:
        if not self.excluded:
            return False

        for pattern in self.excluded:
            if not pattern:
                continue
            try:
                if re.search(pattern, account.id) or re.search(pattern, account.name):
                    logger.info(
                        "Account excluded by account template",
                        extra={
                            "action": "account_template_excluded",
                            "account_id": account.id,
                            "account_name": account.name,
                            "account_template_name": self.name,
                            "pattern": pattern,
                        },
                    )
                    return True
            except re.error as e:
                raise HandlerError(
                    f"Invalid excluded regex in account template '{self.name}': {pattern} ({e})"
                ) from e

        return False

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


@dataclass
class IdentityCenter:
    # The client for the tracking table
    client: boto3.client = field(default_factory=lambda: None)
    # Dedicated client for the Identity Store API (groups/users live here)
    identitystore_client: boto3.client = field(default_factory=lambda: None)
    # The ARN of the SSO Instance
    instance_arn: str = field(default_factory=lambda: "")
    # The groups in the Identity Store
    groups: list[Group] = field(default_factory=lambda: [])
    # The permission sets in the Identity Store
    permission_sets: list[PermissionSet] = field(default_factory=lambda: [])
    # The polling timeout in seconds
    poll_timeout_seconds: int = field(default_factory=lambda: 60)
    # The polling interval in seconds
    poll_interval_seconds: float = field(default_factory=lambda: 1.5)
    # The Identity Store ID
    identity_store_id: str = field(default_factory=lambda: "")

    def __init__(self, instance_arn: str):
        # Set the instance ARN
        self.instance_arn = instance_arn
        # Create a client for the Identity Center (sso-admin)
        self.client = boto3.client("sso-admin", region_name=_AWS_REGION)
        # Create a client for the Identity Store (identitystore)
        self.identitystore_client = boto3.client(
            "identitystore", region_name=_AWS_REGION
        )
        # Initialize the Identity Store ID
        self.poll_timeout_seconds = 60
        # Initialize the polling interval
        self.poll_interval_seconds = 1.5
        # Initialize caches (dataclass defaults are bypassed by custom __init__)
        self.groups = []
        # Initialize the cache for the permission sets
        self.permission_sets = []
        # Initialize the Identity Store ID
        self.identity_store_id = ""
        # Initialize the Identity Store ID
        self.identity_store_id = self.get_identity_store_id()
        # Cache the groups and permission sets
        self.groups = self.list_groups()
        # Cache the permission sets
        self.permission_sets = self.list_permission_sets()

    def get_identity_store_id(self) -> str:
        """
        Get the Identity Store ID from the instance ARN.
        """

        if self.identity_store_id:
            return self.identity_store_id

        logger.info(
            "Getting Identity Store ID from instance ARN",
            extra={
                "action": "get_identity_store_id",
                "instance_arn": self.instance_arn,
            },
        )

        paginator = self.client.get_paginator("list_instances")
        for page in paginator.paginate():
            for inst in page.get("Instances", []):
                if inst.get("InstanceArn") == self.instance_arn:
                    self.identity_store_id = inst["IdentityStoreId"]

                    logger.info(
                        "Successfully got Identity Store ID from instance ARN",
                        extra={
                            "action": "get_identity_store_id",
                            "instance_arn": self.instance_arn,
                            "identity_store_id": self.identity_store_id,
                        },
                    )
                    return self.identity_store_id

        raise HandlerError(
            f"SSO Instance ARN not found in list_instances: {self.instance_arn}"
        )

    def has_group(self, group_name: str) -> bool:
        """
        Check if a group exists in the Identity Store.

        Args:
            group_name: The name of the group to check

        Returns:
            True if the group exists, False otherwise
        """
        # Populate cache on-demand if needed
        if not self.groups:
            self.groups = self.list_groups()
        # Check if the group exists in the list of groups
        return any(group.name == group_name for group in self.groups)

    def get_group(self, group_name: str) -> Group:
        """
        Get the ID of a group in the Identity Store.

        Args:
            group_name: The name of the group to get the ID of

        Returns:
            The ID of the group
        """
        # Populate cache on-demand if needed
        if not self.groups:
            self.groups = self.list_groups()

        # Get the group from the list of groups
        return next((group for group in self.groups if group.name == group_name), None)

    def get_permission_set(self, permission_set_name: str) -> PermissionSet:
        """
        Get the ARN of a permission set in the Identity Store.

        Args:
            permission_set_name: The name of the permission set to get the ARN of

        Returns:
            The ARN of the permission set
        """
        # Populate cache on-demand if needed
        if not self.permission_sets:
            self.permission_sets = self.list_permission_sets()

        # Get the permission set from the list of permission sets
        return next(
            (
                permission_set
                for permission_set in self.permission_sets
                if permission_set.name == permission_set_name
            ),
            None,
        )

    def has_permission_set(self, permission_set_name: str) -> bool:
        """
        Check if a permission set exists in the Identity Store.

        Args:
            permission_set_name: The name of the permission set to check

        Returns:
            True if the permission set exists, False otherwise
        """
        # Populate cache on-demand if needed
        if not self.permission_sets:
            self.permission_sets = self.list_permission_sets()
        # Check if the permission set exists in the list of permission sets
        return any(
            permission_set.name == permission_set_name
            for permission_set in self.permission_sets
        )

    def list_permission_sets(self) -> list[PermissionSet]:
        """
        List all the permission sets in the Identity Center.
        """

        # Use the cached permission sets if they exist
        if self.permission_sets:
            return self.permission_sets

        logger.info(
            "Listing all permission sets in the Identity Center",
            extra={
                "action": "list_permission_sets",
                "instance_arn": self.instance_arn,
            },
        )

        # Initialize the map to store the permission sets
        permission_sets: list[PermissionSet] = []
        # Create a paginator to list the permission sets
        paginator = self.client.get_paginator("list_permission_sets")

        for page in paginator.paginate(InstanceArn=self.instance_arn):
            for permission_set_arn in page.get("PermissionSets", []):
                details = self.client.describe_permission_set(
                    InstanceArn=self.instance_arn,
                    PermissionSetArn=permission_set_arn,
                )
                name = details.get("PermissionSet", {}).get("Name")
                arn = details.get("PermissionSet", {}).get("PermissionSetArn")
                if name:
                    permission_sets.append(PermissionSet(name=name, arn=arn))

        # Cache the permission sets
        self.permission_sets = permission_sets

        return permission_sets

    def list_groups(self) -> list[Group]:
        """
        List the groups in the Identity Store.
        """

        # Use the cached groups if they exist
        if self.groups:
            return self.groups

        logger.info(
            "Getting all groups in the Identity Center",
            extra={
                "action": "list_groups",
                "identity_store_id": self.identity_store_id,
            },
        )

        # Initialize the list to store the groups
        groups: list[Group] = []
        # Create a paginator to list the groups
        paginator = self.identitystore_client.get_paginator("list_groups")

        # Iterate over the pages of the paginator
        for page in paginator.paginate(IdentityStoreId=self.identity_store_id):
            for group in page.get("Groups", []):
                display_name = group.get("DisplayName")
                group_id = group.get("GroupId")
                if display_name and group_id:
                    groups.append(Group(name=display_name, id=group_id))

        # Cache the groups
        self.groups = groups

        logger.info(
            "Successfully listed groups in Identity Center",
            extra={
                "action": "list_groups",
                "identity_store_id": self.identity_store_id,
                "groups": len(groups),
            },
        )

        return groups

    def delete_assignment(
        self,
        account_id: str,
        permission_set_arn: str,
        permission_set_name: str,
        principal_id: str,
        principal_type: str,
    ) -> None:
        """
        Delete a permission set assignment and wait for completion.

        Args:
            account_id: The ID of the target account
            permission_set_arn: The ARN of the permission set
            permission_set_name: The name of the permission set
            principal_id: The ID of the principal
            principal_type: The type of principal ("GROUP" or "USER")

        Returns:
            None
        Raises:
            HandlerError: If the account assignment deletion fails
        """

        logger.info(
            "Deleting account assignment",
            extra={
                "action": "delete_assignment",
                "instance_arn": self.instance_arn,
                "account_id": account_id,
                "permission_set_arn": permission_set_arn,
                "permission_set_name": permission_set_name,
                "principal_id": principal_id,
                "principal_type": principal_type,
            },
        )

        # Initiate the deletion
        resp = self.client.delete_account_assignment(
            InstanceArn=self.instance_arn,
            PermissionSetArn=permission_set_arn,
            PrincipalId=principal_id,
            PrincipalType=principal_type,
            TargetId=account_id,
            TargetType="AWS_ACCOUNT",
        )
        request_id = resp["AccountAssignmentDeletionStatus"]["RequestId"]

        deadline = time.time() + self.poll_timeout_seconds

        while True:
            # Describe the account assignment deletion status
            status = self.client.describe_account_assignment_deletion_status(
                InstanceArn=self.instance_arn,
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
                        "permission_set_name": permission_set_name,
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
            time.sleep(self.poll_interval_seconds)

    def create_assignment(
        self,
        account_id: str,
        permission_set_arn: str,
        permission_set_name: str,
        principal_type: str,
        principal_id: str,
    ) -> None:
        """
        Create an account assignment with Identity Center

        Args:
            account_id: The ID of the target account
            permission_set_arn: The ARN of the permission set
            permission_set_name: The name of the permission set
            principal_type: The type of principal
            principal_id: The ID of the principal

        Returns:
            None
        Raises:
            HandlerError: If the account assignment creation fails
        """

        logger.info(
            "Creating account assignment",
            extra={
                "action": "create_assignment",
                "account_id": account_id,
                "instance_arn": self.instance_arn,
                "permission_set_arn": permission_set_arn,
                "permission_set_name": permission_set_name,
                "principal_id": principal_id,
                "principal_type": principal_type,
            },
        )

        # Check if the account assignment already exists
        resp = self.client.list_account_assignments(
            InstanceArn=self.instance_arn,
            AccountId=account_id,
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
                    "action": "create_assignment",
                    "account_id": account_id,
                    "instance_arn": self.instance_arn,
                    "principal_id": principal_id,
                    "principal_type": principal_type,
                },
            )
            return

        logger.info(
            "Account assignment does not exist, creating",
            extra={
                "action": "create_assignment",
                "account_id": account_id,
                "instance_arn": self.instance_arn,
                "principal_id": principal_id,
                "principal_type": principal_type,
            },
        )

        # Assign the permission set to the principal in the target account
        resp = self.client.create_account_assignment(
            InstanceArn=self.instance_arn,
            PermissionSetArn=permission_set_arn,
            PrincipalId=principal_id,
            PrincipalType=principal_type,
            TargetId=account_id,
            TargetType="AWS_ACCOUNT",
        )
        request_id = resp["AccountAssignmentCreationStatus"]["RequestId"]

        deadline = time.time() + self.poll_timeout_seconds

        while True:
            # Describe the account assignment creation status
            status = self.client.describe_account_assignment_creation_status(
                InstanceArn=self.instance_arn,
                AccountAssignmentCreationRequestId=request_id,
            )["AccountAssignmentCreationStatus"]

            # Get the status of the account assignment creation
            state = status.get("Status")
            # If the status is SUCCEEDED, record the assignment and return
            if state == "SUCCEEDED":
                logger.info(
                    "Successfully created permission set assignment",
                    extra={
                        "action": "create_assignment",
                        "instance_arn": self.instance_arn,
                        "account_id": account_id,
                        "permission_set_name": permission_set_name,
                        "permission_set_arn": permission_set_arn,
                    },
                )
                return

            # If the status is FAILED, raise an error
            if state == "FAILED":
                # Get the failure reason
                failure_reason = status.get("FailureReason", "unknown")
                # Raise an error with the failure reason
                raise HandlerError(
                    f"Failed to create assignment with Identity Center: {failure_reason}"
                )

            # If the time has expired, raise an error
            if time.time() >= deadline:
                # Raise an error with the request ID, account ID, permission set ARN, and principal ID
                raise HandlerError(
                    f"Timed out waiting for assignment creation with Identity Center (request_id={request_id})"
                )

            # Sleep for the poll interval
            time.sleep(self.poll_interval_seconds)


@dataclass
class Organizations:
    # The client for the organizations API
    client: boto3.client = field(default_factory=lambda: None)

    def __init__(self):
        # Create a client for the organizations API
        self.client = boto3.client("organizations", region_name=_AWS_REGION)
        # Cache of Organizational Unit ID -> full path (OU names joined by "/").
        # This avoids repeated parent-walks when multiple accounts share the same OU.
        self._ou_path_cache: dict[str, str] = {}
        # Cache of Organizational Unit ID -> normalized (lowercased) name.
        self._ou_name_cache: dict[str, str] = {}

    def get_organizational_unit_name(self, organizational_unit_id: str) -> str:
        """
        Resolve an Organizational Unit ID to a normalized name segment.

        Args:
            organizational_unit_id: The ID of the organizational unit to resolve

        Returns:
            The normalized name of the organizational unit
        """
        logger.debug(
            "Getting organizational unit name with the unit id",
            extra={
                "action": "get_organizational_unit_name",
                "organizational_unit_id": organizational_unit_id,
            },
        )
        # Check in the cache for the organizational unit name
        if organizational_unit_id in self._ou_name_cache:
            return self._ou_name_cache[organizational_unit_id]

        # Describe the organizational unit
        resp = self.client.describe_organizational_unit(
            OrganizationalUnitId=organizational_unit_id
        )
        ou_name = resp.get("OrganizationalUnit", {}).get("Name", "").strip()
        # Normalize to a stable path segment.
        normalized = (
            re.sub(r"\s+", "-", ou_name.lower()) if ou_name else organizational_unit_id
        )
        # Cache the organizational unit name
        self._ou_name_cache[organizational_unit_id] = normalized

        # Return the normalized organizational unit name
        logger.debug(
            "Successfully got the organizational unit name",
            extra={
                "action": "get_organizational_unit_name",
                "normalized_name": normalized,
                "organizational_unit_id": organizational_unit_id,
                "organizational_unit_name": ou_name,
            },
        )

        return normalized

    def list_accounts(self) -> list[Account]:
        """
        List all active accounts in the organization.

        Returns:
            A list of Account objects
        """

        logger.info(
            "Listing accounts with the Organization",
            extra={
                "action": "list_accounts",
            },
        )

        accounts: list[Account] = []

        try:
            account_ids: list[str] = []

            # List the accounts and account ids
            paginator = self.client.get_paginator("list_accounts")
            for page in paginator.paginate():
                for record in page.get("Accounts", []):
                    if record.get("Status") != "ACTIVE":
                        continue
                    account_ids.append(record.get("Id"))

            # Next we need to get the account details for each account id
            for account_id in account_ids:
                accounts.append(self.get_account(account_id))

            return accounts

        except ClientError as e:
            logger.error(
                "Failed to list accounts with the Organization",
                extra={
                    "action": "list_accounts",
                    "error": str(e),
                },
            )
            raise HandlerError(f"Could not list active accounts: {e}") from e

    def get_account_organizational_path(self, account_id: str) -> str:
        """
        Get the organizational path for a given account ID.

        Args:
            account_id: The account ID to get the organizational path for

        Returns:
            The organizational path for the account
        """

        # Get the account organizational unit path
        resp = self.client.list_parents(ChildId=account_id)
        # Get the parents
        parents = resp.get("Parents", [])
        # If there are no parents, return an empty path
        if not parents:
            logger.debug(
                "No parents found for account",
                extra={
                    "action": "get_account_organizational_path",
                    "account_id": account_id,
                },
            )
            return ""

        # Get the first parent and type
        parent_id = parents[0].get("Id")
        # Get the parent type
        parent_type = parents[0].get("Type")
        # The base path for the account
        base_path = ""
        # If the parent is the root, no OU path exists
        if parent_type == "ROOT":
            logger.debug(
                "Account is a root account, no OU path exists",
                extra={
                    "action": "get_account_organizational_path",
                    "account_id": account_id,
                },
            )
            return base_path

        # Walk up OU parents until ROOT
        paths: list[str] = []
        # Set the current id to the first parent
        current_id = parent_id

        for _i in range(6):
            # Check in the cache for the current id - we should OU-ID/OU-ID etc
            if current_id in self._ou_path_cache:
                logger.info(
                    "Using cached organizational unit path",
                    extra={
                        "action": "get_account_organizational_path",
                        "account_id": account_id,
                        "cached_path": self._ou_path_cache[current_id],
                    },
                )
                return self._ou_path_cache[current_id]

            # Add the current id to the accumulated organizational unit ids
            paths.append(current_id)

            logger.debug(
                "Getting account organizational unit path",
                extra={
                    "action": "get_account_organizational_path",
                    "account_id": account_id,
                    "current_id": current_id,
                    "paths": paths,
                },
            )

            # List the parents for the current id
            resp = self.client.list_parents(ChildId=current_id)
            # Get the parents
            parents = resp.get("Parents", [])
            logger.debug(
                "Got parents for the current id",
                extra={
                    "action": "get_account_organizational_path",
                    "account_id": account_id,
                    "current_id": current_id,
                    "parents": parents,
                },
            )
            # No more parents, break
            if not parents:
                logger.debug(
                    "No more parents, breaking",
                    extra={
                        "action": "get_account_organizational_path",
                        "account_id": account_id,
                        "current_id": current_id,
                    },
                )
                break
            # Get the parent type
            parent_type = parents[0].get("Type", None)
            # Get the parent id
            parent_id = parents[0].get("Id", None)
            # If the parent type is ROOT, break
            if parent_type == "ROOT" or parent_id is None or parent_type is None:
                logger.debug(
                    "Parent type is ROOT, breaking",
                    extra={
                        "action": "get_account_organizational_path",
                        "account_id": account_id,
                        "parent_type": parent_type,
                        "parent_id": parent_id,
                    },
                )
                break

            # If the parent type is ORGANIZATIONAL_UNIT or OU, set current id to parent id and continue
            if parent_type in ["ORGANIZATIONAL_UNIT", "OU"]:
                current_id = parent_id
                continue

            break

        # Reverse the paths
        paths.reverse()

        logger.debug(
            "Found the following organizational unit paths",
            extra={
                "action": "get_account_organizational_path",
                "account_id": account_id,
                "paths": paths,
            },
        )

        # We should have the accumulated paths in the correct order
        for path in paths:
            # Get the organizational unit name
            ou_name = self.get_organizational_unit_name(path)
            # Add the organizational unit name to the base path
            base_path = "/".join([base_path, ou_name])
            # Cache the path
            self._ou_path_cache[path] = base_path
            # Successfully cached the path
            logger.info(
                "Added organizational path segment to the base path to the cache",
                extra={
                    "action": "get_account_organizational_path",
                    "path": path,
                    "base_path": base_path,
                },
            )

        return base_path

    def get_account_name(self, account_id: str) -> str:
        """
        Get the account name for a given account ID.

        Args:
            account_id: The account ID to get the name for

        Returns:
            The account name
        """

        # Get the account name
        resp = self.client.describe_account(AccountId=account_id)
        # Return the account name
        return resp.get("Account", {}).get("Name", None)

    def get_account_tags(self, account_id: str) -> dict[str, str]:
        """
        Get the account tags for a given account ID.

        Args:
            account_id: The account ID to get the tags for

        Returns:
            The account tags
        """

        # Get the account tags
        resp = self.client.list_tags_for_resource(ResourceId=account_id)
        # If there are no tags, return an empty dictionary
        if not resp.get("Tags", []):
            return {}

        # Return the account tags
        return {tag.get("Key"): tag.get("Value") for tag in resp.get("Tags", [])}

    def get_account(self, account_id: str) -> Account:
        """
        Get the account with the Organization for a given account ID.

        Returns:
            An Account object with the account details
        """

        # Initialize the account object
        account = Account()
        account.id = account_id
        account.name = ""

        try:
            # Get the account name
            account.name = self.get_account_name(account_id=account.id)
            # Get the account tags
            account.tags = self.get_account_tags(account_id=account.id)
            # Get the account organizational unit path
            account.organizational_unit_path = self.get_account_organizational_path(
                account_id=account.id
            )
            # Successfully got the account organizational unit path
            logger.info(
                "Successfully got the account organizational unit path",
                extra={
                    "action": "get_account",
                    "account_id": account.id,
                    "account.name": account.name,
                    "account.tags": account.tags,
                    "account.organizational_unit_path": account.organizational_unit_path,
                },
            )
        except Exception as e:
            logger.warning(
                "Could not get account details from the Organization",
                extra={
                    "action": "get_account",
                    "account_id": account.id,
                    "error": str(e),
                },
            )
            raise HandlerError(
                f"Could not get account details from the Organization: {e}"
            ) from e

        return account


class Tracking:
    # The name of the DynamoDB table used to track assignments
    table_name: str
    # The client for the tracking table
    client: boto3.client

    def __init__(self, table_name: str):
        # Set the table name
        self.table_name = table_name
        # Create a client for the tracking table
        self.client = boto3.resource("dynamodb", region_name=_AWS_REGION).Table(
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
            logger.info(
                "Getting tracking assignments from tracking table",
                extra={
                    "action": "list",
                    "table_name": self.table_name,
                },
            )
            resp = self.client.scan()
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

            logger.info(
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

        logger.info(
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

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


def has_matching_binding(
    assignment: Assignment,
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


def reconcile_creations(
    bindings: list[Binding],
    identity_center: IdentityCenter,
    tracking: Tracking,
) -> Tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Assign each binding's permission set to every listed Identity Center group.

    Args:
        bindings: Per-permission-set bindings for one account
        identity_center: The Identity Center client used to create the assignments
        tracking: The tracking client used to manipulate the tracking table

    Returns:
        ``(successes, failures)`` for each attempted assignment
    """

    if len(bindings) == 0:
        logger.warning(
            "No bindings to assign",
            extra={
                "action": "assign_permissions",
                "bindings": bindings,
            },
        )
        return [], []

    logger.info(
        "Assigning permissions to the accounts",
        extra={
            "action": "assign_permissions",
            "bindings": bindings,
        },
    )

    # Initialize the lists to store the successes and failures
    successes: list[dict[str, Any]] = []
    # Initialize the list to store the failures
    failures: list[dict[str, Any]] = []

    # Assign the permission set to the groups
    for binding in bindings:
        # Iterate over the groups in the binding
        for group in binding.groups:
            try:
                identity_center.create_assignment(
                    account_id=binding.account_id,
                    permission_set_arn=binding.permission_set_arn,
                    permission_set_name=binding.permission_set_name,
                    principal_id=group.id,
                    principal_type="GROUP",
                )
                # Create the item in the tracking table
                tracking.create(
                    account_id=binding.account_id,
                    group_name=group.name,
                    permission_set_arn=binding.permission_set_arn,
                    permission_set_name=binding.permission_set_name,
                    principal_id=group.id,
                    principal_type="GROUP",
                    template_name=binding.template_name,
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
            "successes": successes,
            "failures": failures,
        },
    )

    return successes, failures


def reconcile_deletions(
    desired_bindings: list[Binding],
    tracking: Tracking,
    identity_center: IdentityCenter,
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

    Returns:
        (deleted_assignments, deletion_failures) - lists of dictionaries documenting deletions
    """

    logger.info(
        "Starting deletion reconciliation",
        extra={
            "action": "reconcile_deletions",
            "instance_arn": identity_center.instance_arn,
            "desired_bindings_count": len(desired_bindings),
        },
    )

    successful_deletions: list[dict[str, Any]] = []
    failed_deletions: list[dict[str, Any]] = []

    try:
        # Retrieve all active assignments from the tracking table
        assignments = tracking.list()

        logger.info(
            "Retrieved all tracking deletions from tracking table",
            extra={
                "action": "reconcile_deletions",
                "assignments_count": len(assignments),
            },
        )
        # If there are no assignments, we can return an empty list
        if len(assignments) == 0:
            logger.info(
                "No tracking deletions to reconcile",
                extra={
                    "action": "reconcile_deletions",
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
                    identity_center.delete_assignment(
                        account_id=assignment.account_id,
                        permission_set_arn=assignment.permission_set_arn,
                        permission_set_name=assignment.permission_set_name,
                        principal_id=assignment.principal_id,
                        principal_type=assignment.principal_type,
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
                tracking.delete(assignment.assignment_id)
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


def build_permission_bindings(
    account: Account,
    configuration: Configuration,
    identity_center: IdentityCenter,
    permission: Permission,
) -> Tuple[list[Binding], list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Build a list of bindings from a given permission request.

    Args:
        account: The account to build the bindings for
        configuration: The configuration to use
        identity_center: The identity center to use
        permission: The permission to build the bindings for
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
        "Building bindings from account permissions",
        extra={
            "action": "build_bindings",
            "account_id": account.id,
            "permission": permission,
        },
    )

    template = configuration.templates.get(permission.name)
    if not template:
        failures.append(
            {
                "account_id": account.id,
                "permission": permission.name,
                "error": "Permission template not found in configuration",
            }
        )
        return [], successes, failures

    logger.info(
        "Found permission template",
        extra={
            "action": "build_bindings",
            "account_id": account.id,
            "permission": permission.name,
            "template": template,
        },
    )

    # Used to hold the groups that are available in the identity store
    available_groups: list[Group] = []

    # Check all the groups exist in the identity store
    for group in permission.groups:
        logger.info(
            "Checking if group exists in Identity Center",
            extra={
                "action": "build_bindings",
                "account_id": account.id,
                "group": group,
                "permission": permission.name,
            },
        )

        if not identity_center.has_group(group):
            logger.warning(
                "Group not found in Identity Center, skipping",
                extra={
                    "action": "build_bindings",
                    "account_id": account.id,
                    "group": group,
                    "permission": permission.name,
                },
            )
            failures.append(
                {
                    "account_id": account.id,
                    "error": "Group not found in identity store",
                    "group": group,
                    "permission": permission.name,
                }
            )
            continue

        logger.info(
            "Group found in Identity Center",
            extra={
                "action": "build_permission_bindings",
                "account_id": account.id,
                "permission": permission.name,
                "group": group,
            },
        )

        # Add the group to the list of available groups
        available_groups.append(identity_center.get_group(group))

    # Check all the permission sets exist in the identity store
    for permission_set_name in template.permission_sets:
        # Get the ARN of the permission set
        permission_set = identity_center.get_permission_set(permission_set_name)
        # If the permission set is not found, add a failure to the list
        if not permission_set:
            logger.warning(
                "Permission set not found in identity store, skipping",
                extra={
                    "action": "build_permission_bindings",
                    "account_id": account.id,
                    "permission": permission_set_name,
                },
            )
            failures.append(
                {
                    "account_id": account.id,
                    "permission": permission_set_name,
                    "error": "Permission set not found in identity store",
                }
            )
            continue

        # Build a binding for the permission set
        binding: Binding = Binding(
            account_id=account.id,
            groups=available_groups,
            permission_set_arn=permission_set.arn,
            permission_set_name=permission_set_name,
            template_name=permission.name,
        )
        bindings.append(binding)

    logger.info(
        "Built the following permission bindings",
        extra={
            "action": "build_permission_bindings",
            "account_id": account.id,
            "bindings": len(bindings),
            "template_name": permission.name,
        },
    )

    return bindings, successes, failures


def build_account_bindings(
    account: Account,
    configuration: Configuration,
    identity_center: IdentityCenter,
) -> Tuple[list[Binding], list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Evaluate which account templates match the given account and return Permission objects.

    Uses AND logic: all specified conditions in a matcher must match for the account template to apply.

    Args:
        account: The account to evaluate
        configuration: The configuration to use

    Returns:
        A tuple of (bindings, successes, failures)
    """

    logger.info(
        "Building account bindings from account templates",
        extra={
            "action": "build_account_bindings",
            "account.name": account.name,
        },
    )

    # Initialize the lists to store the bindings, successes, and failures
    all_bindings: list[Binding] = []
    all_successes: list[dict[str, Any]] = []
    all_failures: list[dict[str, Any]] = []

    for name, template in configuration.account_templates.items():
        # Check if account matches this template's matcher
        if not template.matcher.matches(account):
            logger.info(
                "Account did not match account template, skipping",
                extra={
                    "action": "build_account_bindings",
                    "account.name": account.name,
                    "account_template_name": name,
                },
            )
            continue

        # If excluded patterns are configured and the account matches any of them,
        # do not apply this account template.
        try:
            if template.is_excluded(account):
                continue
        except HandlerError as e:
            all_failures.append(
                {
                    "account.name": account.name,
                    "account_template_name": name,
                    "error": str(e),
                }
            )
            continue

        logger.info(
            "Found a matching account template for account",
            extra={
                "action": "build_account_bindings",
                "account.name": account.name,
                "template.groups": template.groups,
                "template.matcher": template.matcher,
                "template.name": name,
                "template.template_names": template.template_names,
            },
        )

        # Create a permission for each of the template's groups
        for template_ref in template.template_names:
            permission = Permission(
                name=template_ref,
                groups=template.groups,
            )
            logger.info(
                "Building Permission object from account template",
                extra={
                    "action": "build_account_bindings",
                    "account.name": account.name,
                    "template.name": name,
                    "template.template_names": template.template_names,
                    "permission.name": permission.name,
                    "permission.groups": permission.groups,
                },
            )
            # Build the permission bindings
            bindings, successes, failures = build_permission_bindings(
                account=account,
                configuration=configuration,
                identity_center=identity_center,
                permission=permission,
            )

            logger.info(
                "Built the following permission bindings",
                extra={
                    "action": "build_account_bindings",
                    "account.name": account.name,
                    "bindings": len(bindings),
                    "successes": len(successes),
                    "failures": len(failures),
                },
            )

            all_bindings.extend(bindings)
            all_successes.extend(successes)
            all_failures.extend(failures)

    return all_bindings, all_successes, all_failures


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
    started_at = time.time()

    try:
        # Ensure we have a valid environment
        validate_environment()
        # Initialize the configuration
        configuration = Configuration(
            table_name=os.environ.get("DYNAMODB_CONFIG_TABLE")
        )
        # Initialize the tracking client
        tracking = Tracking(table_name=os.environ.get("DYNAMODB_TRACKING_TABLE"))
        # Get the SSO Instance ARN
        identity_center = IdentityCenter(
            instance_arn=os.environ.get("SSO_INSTANCE_ARN")
        )
        # Get the Organizations client
        organizations = Organizations()
        # Get the tagging prefix (module doc default: ``sso``)
        tag_prefix = os.environ.get("SSO_ACCOUNT_TAG_PREFIX") or "sso"

        logger.info(
            "Using the following environment variables",
            extra={
                "action": "lambda_handler",
                "config_table_name": configuration.table_name,
                "instance_arn": identity_center.instance_arn,
                "tracking_table_name": tracking.table_name,
            },
        )

        # Supports the ability to assign to a single account - for debugging purposes
        account_id = event.get("account_id")
        if account_id is not None:
            account_id = str(account_id).strip()
        # Initialize the list of target accounts
        target_accounts: list[Account] = []
        # Get the target accounts, either a single account or all accounts
        if account_id:
            # Set the target accounts to the single account
            target_accounts = [organizations.get_account(account_id)]
        else:
            # List all active accounts
            target_accounts = organizations.list_accounts()

        logger.info(
            "Resolved execution targets",
            extra={
                "action": "lambda_handler",
                "accounts": [account.id for account in target_accounts],
            },
        )

        # Initialize the lists to store the successes and failures
        all_successes: list[dict[str, Any]] = []
        all_failures: list[dict[str, Any]] = []

        # Load the group configurations and account templates from DynamoDB
        configuration.load()
        ## Build a list of bindings for the account
        all_bindings: list[Binding] = []

        # Iterate over the target accounts and build a list of bindings based on
        # the accounts tags and account templates.
        for account in target_accounts:
            # Does the account have permission tags?
            permissions = account.get_permission_tags(tag_prefix)
            # If the account has permission tags, add them to the list of bindings
            if permissions or len(permissions) > 0:
                # Get all the bindings from each permission tags
                for permission in permissions:
                    bindings, successes, failures = build_permission_bindings(
                        account=account,
                        configuration=configuration,
                        identity_center=identity_center,
                        permission=permission,
                    )
                    # Add the bindings to the list
                    all_bindings.extend(bindings)
                    # Add the successes to the list
                    all_successes.extend(successes)
                    # Add the failures to the list
                    all_failures.extend(failures)
            else:
                logger.info(
                    "Skipping account as it has no permission tags",
                    extra={
                        "action": "lambda_handler",
                        "account.name": account.name,
                    },
                )

            # Does the account conform to any account templates?
            bindings, successes, failures = build_account_bindings(
                account=account,
                configuration=configuration,
                identity_center=identity_center,
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
            successes, failures = reconcile_creations(
                bindings=all_bindings,
                identity_center=identity_center,
                tracking=tracking,
            )
            # Add the successes to the list
            all_successes.extend(successes)
            # Add the failures to the list
            all_failures.extend(failures)

        # Run reconciliation if tracking is enabled
        reconciliation_deleted: list[dict[str, Any]] = []
        reconciliation_failures: list[dict[str, Any]] = []

        try:
            reconciliation_deleted, reconciliation_failures = reconcile_deletions(
                desired_bindings=all_bindings,
                identity_center=identity_center,
                tracking=tracking,
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
                "started_at": started_at,
                "status": status,
            },
        )

        return {
            "account_id": account_id,
            "finished_at": datetime.now(timezone.utc).isoformat(),
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
        )
        return {
            "account_id": (event or {}).get("account_id"),
            "errors": {"message": str(e)},
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "results": None,
            "source": (event or {}).get("source", "unknown"),
            "started_at": started_at,
            "status": "error",
            "time_taken_seconds": time.time() - started_at,
        }
