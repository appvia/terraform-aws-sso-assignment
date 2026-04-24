from __future__ import annotations
from dataclasses import dataclass, field
import boto3
from botocore.exceptions import ClientError
import re
from .logging import logger
from .errors import HandlerError
from .types import Account

@dataclass
class Organizations:
    # The client for the organizations API
    client: boto3.client = field(default_factory=lambda: None)

    def __init__(self, region_name: str = "eu-west-2"):
        # Create a client for the organizations API
        self.client = boto3.client("organizations", region_name=region_name)
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

        logger.debug(
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
                logger.debug(
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
            logger.debug(
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
            logger.debug(
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

