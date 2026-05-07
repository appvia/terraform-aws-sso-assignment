from __future__ import annotations
from dataclasses import dataclass, field
import boto3
import time

from .logging import logger
from .errors import HandlerError
from .types import Group, PermissionSet, User


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
    # Cache of resolved users keyed by configured identifier
    users_by_identifier: dict[str, User] = field(default_factory=dict)

    def __init__(self, instance_arn: str, region_name: str = "eu-west-2"):
        # Set the instance ARN
        self.instance_arn = instance_arn
        # Create a client for the Identity Center (sso-admin)
        self.client = boto3.client("sso-admin", region_name=region_name)
        # Create a client for the Identity Store (identitystore)
        self.identitystore_client = boto3.client(
            "identitystore", region_name=region_name
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
        # Initialize the user cache
        self.users_by_identifier = {}
        # Initialize the Identity Store ID
        self.identity_store_id = self.get_identity_store_id()
        # Cache the groups and permission sets
        self.groups = self.list_groups()
        # Cache the permission sets
        self.permission_sets = self.list_permission_sets()


    def has_user(self, user_identifier: str) -> bool:
        """
        Check if a user exists in the Identity Store by identifier.

        The identifier should follow the module convention (e.g., email or username).
        """
        return self.get_user(user_identifier) is not None


    def get_user(self, user_identifier: str) -> User | None:
        """
        Resolve a user identifier to an Identity Store user id.

        This uses `identitystore:GetUserId` with a small set of supported
        attribute paths to avoid directory-wide scans.
        """
        user_identifier = (user_identifier or "").strip()
        if not user_identifier:
            return None

        if user_identifier in self.users_by_identifier:
            return self.users_by_identifier[user_identifier]

        # Try common identity store attribute paths. Which one works depends on
        # how the Identity Center directory is populated.
        attribute_paths = [
            # Username
            "UserName",
            # Primary email (common for SCIM-synced identities)
            "Emails.Value",
        ]

        for attribute_path in attribute_paths:
            try:
                resp = self.identitystore_client.get_user_id(
                    IdentityStoreId=self.identity_store_id,
                    AlternateIdentifier={
                        "UniqueAttribute": {
                            "AttributePath": attribute_path,
                            "AttributeValue": user_identifier,
                        }
                    },
                )
                user_id = resp.get("UserId")
                if user_id:
                    user = User(name=user_identifier, id=user_id)
                    self.users_by_identifier[user_identifier] = user
                    return user
            except Exception:
                continue

        return None


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

        Returns:
            A list of permission sets
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

        Returns:
            A list of groups
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

        logger.debug(
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

        logger.debug(
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
        paginator = self.client.get_paginator("list_account_assignments")
        existing_assignment = False

        for page in paginator.paginate(
            InstanceArn=self.instance_arn,
            AccountId=account_id,
            PermissionSetArn=permission_set_arn,
        ):
            # Filter the results to check if this specific principal has the assignment
            existing_assignment = any(
                assignment.get("PrincipalId") == principal_id
                and assignment.get("PrincipalType") == principal_type
                for assignment in page.get("AccountAssignments", [])
            )
            if existing_assignment:
                break
        if existing_assignment:
            logger.debug(
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

        logger.debug(
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
