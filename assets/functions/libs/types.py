from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
import boto3
import fnmatch
import re
from typing import Optional

from .errors import HandlerError
from .logging import logger

@dataclass
class Group:
    # The group name
    name: str = field(default_factory=lambda: "")
    # The group ID
    id: str = field(default_factory=lambda: "")

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


@dataclass
class User:
    # The user identifier as configured (e.g., email or username)
    name: str = field(default_factory=lambda: "")
    # The user ID in the Identity Store
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
    # The users to assign the permission set to (account_templates only)
    users: list[User] = field(default_factory=lambda: [])
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
    # The users to assign the permission set to (account_templates only)
    users: list[str] = field(default_factory=lambda: [])

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

    def __init__(self, table_name: str, region_name: str = "eu-west-2"):
        # Set the table name
        self.table_name = table_name
        # Initialize in-memory configuration maps (dataclass fields are not set
        # automatically because we implement a custom __init__).
        self.account_templates = {}
        self.templates = {}
        # Create a client for the tracking table
        self.client = boto3.resource("dynamodb", region_name=region_name).Table(
            table_name
        )

    def load(self) -> None:
        """
        Load the configuration from the DynamoDB table.
        """
        logger.info(
            "Loading configuration from DynamoDB",
            extra={
                "action": "load",
                "table_name": self.table_name,
            },
        )

        scan_kwargs: dict = {}
        while True:
            resp = self.client.scan(**scan_kwargs)

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
                        users=item.get("users", []) or [],
                        description=item.get("description", "") or "",
                    )
                else:
                    self.templates[item.get("group_name")] = Template(
                        permission_sets=item.get("permission_sets", []),
                        description=item.get("description", ""),
                    )

            last_evaluated_key = resp.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break

            scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

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

        `account_ou` is expected to be a leading-slash OU path like "/data" or
        "/data/development".

        Patterns are glob patterns (Python `fnmatch`) and must include a leading "/",
        e.g. "/data/*".

        Args:
            account_ou: The full OU path from AWS Organizations
            patterns: List of glob patterns to match against

        Returns:
            True if account_ou matches ANY pattern, False otherwise
        """
        if not account_ou or not patterns:
            return False

        account_ou = account_ou.strip()
        if not account_ou.startswith("/"):
            return False

        # Iterate over the patterns
        for pattern in patterns:
            if not pattern or not pattern.startswith("/"):
                logger.debug(
                    "Skipping OU pattern without leading slash",
                    extra={
                        "action": "match_organizational_unit",
                        "account_ou": account_ou,
                        "pattern": pattern,
                        "matched": False,
                    },
                )
                continue

            matched = fnmatch.fnmatch(account_ou, pattern)
            if matched:
                logger.debug(
                    "Account organizational unit matched",
                    extra={
                        "action": "match_organizational_unit",
                        "account_ou": account_ou,
                        "pattern": pattern,
                        "matched": True,
                    },
                )
                return True

        logger.debug(
            "Account organizational unit did not match any patterns",
            extra={
                "action": "match_organizational_unit",
                "account_ou": account_ou,
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
    # List of users (identifiers) to assign (account_templates only)
    users: list[str] = field(default_factory=list)
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
                    logger.debug(
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
