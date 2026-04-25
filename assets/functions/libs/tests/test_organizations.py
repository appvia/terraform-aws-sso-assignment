from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from libs.errors import HandlerError
from libs.organizations import Organizations
from libs.types import Account


class TestOrganizations:
    def test_list_accounts_returns_active_accounts_from_get_account(self):
        org = Organizations.__new__(Organizations)
        org.client = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "Accounts": [
                    {"Id": "1", "Status": "ACTIVE"},
                    {"Id": "2", "Status": "SUSPENDED"},
                ]
            }
        ]
        org.client.get_paginator.return_value = paginator
        org.get_account = MagicMock(side_effect=lambda account_id: Account(id=account_id, name=f"n-{account_id}"))

        accounts = org.list_accounts()
        assert [a.id for a in accounts] == ["1"]
        org.get_account.assert_called_once_with("1")

    def test_get_account_sets_tags_and_ou_path_empty_when_no_parents(self):
        org = Organizations.__new__(Organizations)
        org.client = MagicMock()
        org.client.list_tags_for_resource.return_value = {"Tags": [{"Key": "k", "Value": "v"}]}
        org.client.list_parents.return_value = {"Parents": []}

        acct = org.get_account("123")
        assert acct.id == "123"
        assert acct.tags == {"k": "v"}
        assert acct.organizational_unit_path == ""
        org.client.list_tags_for_resource.assert_called_once_with(ResourceId="123")
        org.client.list_parents.assert_called_once_with(ChildId="123")

    def test_get_account_sets_tags_and_builds_ou_path_from_parent_chain(self):
        org = Organizations.__new__(Organizations)
        org.client = MagicMock()
        org._ou_path_cache = {}
        org._ou_name_cache = {}
        org.client.list_tags_for_resource.return_value = {
            "Tags": [{"Key": "Environment", "Value": "Production"}]
        }

        def describe_ou_side_effect(*, OrganizationalUnitId: str):
            if OrganizationalUnitId == "ou-0":
                return {"OrganizationalUnit": {"Name": "Workloads"}}
            if OrganizationalUnitId == "ou-1":
                return {"OrganizationalUnit": {"Name": "Development"}}
            raise AssertionError(f"Unexpected OrganizationalUnitId={OrganizationalUnitId}")

        def list_parents_side_effect(*, ChildId: str):
            if ChildId == "123":
                return {"Parents": [{"Id": "ou-1", "Type": "OU"}]}
            if ChildId == "ou-1":
                return {"Parents": [{"Id": "ou-0", "Type": "OU"}]}
            if ChildId == "ou-0":
                return {"Parents": [{"Id": "r-root", "Type": "ROOT"}]}
            raise AssertionError(f"Unexpected ChildId={ChildId}")

        org.client.describe_organizational_unit.side_effect = describe_ou_side_effect
        org.client.list_parents.side_effect = list_parents_side_effect

        acct = org.get_account("123")
        assert acct.id == "123"
        assert acct.tags == {"Environment": "Production"}
        assert acct.organizational_unit_path == "/workloads/development"

        org.client.list_tags_for_resource.assert_called_once_with(ResourceId="123")
        assert org.client.list_parents.call_count == 3
        org.client.list_parents.assert_any_call(ChildId="123")
        org.client.list_parents.assert_any_call(ChildId="ou-1")
        org.client.list_parents.assert_any_call(ChildId="ou-0")

    def test_get_account_reuses_cached_ou_path_when_accounts_share_parent_ou(self):
        org = Organizations.__new__(Organizations)
        org.client = MagicMock()
        org._ou_path_cache = {}
        org._ou_name_cache = {}

        org.client.list_tags_for_resource.return_value = {
            "Tags": [{"Key": "Environment", "Value": "Production"}]
        }

        def describe_ou_side_effect(*, OrganizationalUnitId: str):
            if OrganizationalUnitId == "ou-0":
                return {"OrganizationalUnit": {"Name": "Workloads"}}
            if OrganizationalUnitId == "ou-1":
                return {"OrganizationalUnit": {"Name": "Development"}}
            raise AssertionError(f"Unexpected OrganizationalUnitId={OrganizationalUnitId}")

        def list_parents_side_effect(*, ChildId: str):
            if ChildId in {"111", "222"}:
                return {"Parents": [{"Id": "ou-1", "Type": "OU"}]}
            if ChildId == "ou-1":
                return {"Parents": [{"Id": "ou-0", "Type": "OU"}]}
            if ChildId == "ou-0":
                return {"Parents": [{"Id": "r-root", "Type": "ROOT"}]}
            raise AssertionError(f"Unexpected ChildId={ChildId}")

        org.client.describe_organizational_unit.side_effect = describe_ou_side_effect
        org.client.list_parents.side_effect = list_parents_side_effect

        acct1 = org.get_account("111")
        acct2 = org.get_account("222")

        assert acct1.organizational_unit_path == "/workloads/development"
        assert acct2.organizational_unit_path == "/workloads/development"

        # First account: list_parents for account + ou-1 + ou-0 = 3 calls
        # Second account: list_parents for account only (ou-1 is cached) = +1 call
        assert org.client.list_parents.call_count == 4

    def test_get_account_returns_account_on_client_error(self):
        org = Organizations.__new__(Organizations)
        org.client = MagicMock()
        org.client.list_tags_for_resource.side_effect = ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "nope"}},
            "ListTagsForResource",
        )
        with pytest.raises(HandlerError, match="Could not get account details"):
            org.get_account("123")

