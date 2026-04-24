"""
Class-focused unit tests for handler.py.

These tests intentionally avoid the end-to-end Lambda workflow for now and
instead validate each class in isolation using mocks (no AWS calls).
"""

from __future__ import annotations

import json
import logging
import os
import sys
from unittest.mock import MagicMock, call, patch

import pytest  # pylint: disable=import-error
from botocore.exceptions import ClientError

# Prevent boto3/botocore from attempting to resolve credentials during import.
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
os.environ.setdefault("AWS_SESSION_TOKEN", "test")
os.environ.setdefault("AWS_DEFAULT_REGION", "eu-west-1")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")

# Import the module under test from the same directory (no package layout).
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import handler  # noqa: E402 pylint: disable=wrong-import-position


class TestHandlerError:
    def test_is_runtime_error(self):
        err = handler.HandlerError("boom")
        assert isinstance(err, RuntimeError)
        assert str(err) == "boom"


class TestJSONFormatter:
    def test_format_emits_json_with_message_level_and_extras(self):
        fmt = handler._JSONFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="x.py",
            lineno=1,
            msg="hello",
            args=(),
            exc_info=None,
        )
        record.custom_field = "extra"
        data = json.loads(fmt.format(record))
        assert data["message"] == "hello"
        assert data["level"] == "INFO"
        assert data["custom_field"] == "extra"


class TestGroup:
    def test_to_json(self):
        g = handler.Group(name="TeamA", id="g-1")
        assert json.loads(g.to_json()) == {"name": "TeamA", "id": "g-1"}


class TestPermissionSet:
    def test_to_json(self):
        ps = handler.PermissionSet(name="Admin", arn="arn:ps:1")
        assert json.loads(ps.to_json()) == {"name": "Admin", "arn": "arn:ps:1"}


class TestBinding:
    def test_to_json_includes_nested_groups(self):
        b = handler.Binding(
            account_id="123456789012",
            permission_set_name="Admin",
            permission_set_arn="arn:ps:1",
            groups=[handler.Group(name="TeamA", id="g-1")],
            template_name="tpl",
        )
        data = json.loads(b.to_json())
        assert data["account_id"] == "123456789012"
        assert data["template_name"] == "tpl"
        assert data["groups"] == [{"name": "TeamA", "id": "g-1"}]


class TestPermission:
    def test_to_json(self):
        p = handler.Permission(name="default", groups=["A", "B"])
        assert json.loads(p.to_json()) == {"name": "default", "groups": ["A", "B"]}


class TestAccount:
    def test_get_permission_tags_filters_and_splits(self):
        acct = handler.Account(
            id="123456789012",
            name="acct",
            tags={
                "sso/default": " Alpha , Beta , ,",
                "other": "x",
                "sso/security": "Gamma",
            },
        )
        perms = acct.get_permission_tags("sso")
        by_name = {p.name: p.groups for p in perms}
        assert by_name == {
            "default": ["Alpha", "Beta"],
            "security": ["Gamma"],
        }

    def test_to_json(self):
        acct = handler.Account(
            id="1", name="n", tags={"k": "v"}, organizational_unit_path="ou/x"
        )
        data = json.loads(acct.to_json())
        assert data["id"] == "1"
        assert data["tags"] == {"k": "v"}


class TestTemplate:
    def test_to_json(self):
        t = handler.Template(permission_sets=["Admin"], description="desc")
        assert json.loads(t.to_json()) == {
            "permission_sets": ["Admin"],
            "description": "desc",
        }


class TestAssignment:
    def test_to_json(self):
        a = handler.Assignment(
            assignment_id="a#b#c",
            account_id="123",
            permission_set_arn="arn:ps",
            permission_set_name="Admin",
            principal_id="g-1",
            principal_type="GROUP",
            template_name="tpl",
            group_name="TeamA",
            created_at="t1",
            last_seen="t2",
        )
        data = json.loads(a.to_json())
        assert data["assignment_id"] == "a#b#c"
        assert data["principal_type"] == "GROUP"


class TestAccountTemplateMatcher:
    def test_matches_all_conditions(self):
        matcher = handler.AccountTemplateMatcher(
            organizational_units=["ou-prod/ou-workloads*"],
            name_pattern="prod-*",
            account_tags={"Environment": "Production"},
        )
        acct = handler.Account(
            id="1",
            name="prod-app-1",
            tags={"Environment": "Production", "CostCenter": "Eng"},
            organizational_unit_path="r-abc/ou-prod/ou-workloads/ou-team1",
        )
        assert matcher.matches(acct) is True

    def test_matches_fails_on_tag_mismatch(self):
        matcher = handler.AccountTemplateMatcher(
            account_tags={"Environment": "Production"}
        )
        acct = handler.Account(id="1", name="x", tags={"Environment": "Dev"})
        assert matcher.matches(acct) is False

    def test_matches_tags_single_and_multiple_conditions(self):
        matcher = handler.AccountTemplateMatcher(
            account_tags={"Environment": "Production", "CostCenter": "Eng"}
        )
        acct_ok = handler.Account(
            id="1",
            name="x",
            tags={"Environment": "Production", "CostCenter": "Eng", "Owner": "me"},
        )
        acct_missing = handler.Account(
            id="2", name="x", tags={"Environment": "Production"}
        )
        acct_value_mismatch = handler.Account(
            id="3",
            name="x",
            tags={"Environment": "Production", "CostCenter": "Finance"},
        )
        assert matcher.matches(acct_ok) is True
        assert matcher.matches(acct_missing) is False
        assert matcher.matches(acct_value_mismatch) is False

    def test_matches_organizational_unit_trailing_path(self):
        matcher = handler.AccountTemplateMatcher(
            organizational_units=["ou-prod/ou-workloads"]
        )
        assert (
            matcher.matches_organizational_unit(
                "r-abc/ou-prod/ou-workloads", ["ou-prod/ou-workloads"]
            )
            is True
        )
        assert (
            matcher.matches_organizational_unit(
                "r-abc/ou-dev/ou-workloads", ["ou-prod/*"]
            )
            is False
        )

    def test_matches_organizational_unit_for_leading_slash_paths_and_globs(self):
        matcher = handler.AccountTemplateMatcher(organizational_units=["workloads/*"])
        acct = handler.Account(
            id="1",
            name="TestAccount",
            tags={},
            organizational_unit_path="/workloads/development",
        )
        assert matcher.matches(acct) is True

    def test_matches_organizational_unit_for_exact_trailing_path(self):
        matcher = handler.AccountTemplateMatcher(
            organizational_units=["workloads/development"]
        )
        acct = handler.Account(
            id="1",
            name="TestAccount",
            tags={},
            organizational_unit_path="/workloads/development",
        )
        assert matcher.matches(acct) is True

    def test_matches_organizational_unit_negative_when_path_does_not_match(self):
        matcher = handler.AccountTemplateMatcher(organizational_units=["workspaces/*"])
        acct = handler.Account(
            id="1",
            name="TestAccount",
            tags={},
            organizational_unit_path="/workloads/development",
        )
        assert matcher.matches(acct) is False

    def test_matches_name_patterns(self):
        matcher = handler.AccountTemplateMatcher(
            name_patterns=["prod-[a-z]*-[0-9][0-9]", "shared-*"]
        )
        assert matcher.matches(handler.Account(id="1", name="prod-app-12")) is True
        assert matcher.matches(handler.Account(id="2", name="shared-services")) is True
        assert matcher.matches(handler.Account(id="3", name="prod-APP-12")) is False

    def test_matches_combined_tags_and_name_conditions(self):
        matcher = handler.AccountTemplateMatcher(
            account_tags={"Environment": "Production"},
            name_patterns=["prod-[a-z]*-[0-9]"],
        )
        acct_ok = handler.Account(
            id="1",
            name="prod-app-1",
            tags={"Environment": "Production"},
        )
        acct_bad_tag = handler.Account(
            id="2",
            name="prod-app-1",
            tags={"Environment": "Dev"},
        )
        acct_bad_name = handler.Account(
            id="3",
            name="prod-app-x",
            tags={"Environment": "Production"},
        )
        assert matcher.matches(acct_ok) is True
        assert matcher.matches(acct_bad_tag) is False
        assert matcher.matches(acct_bad_name) is False


class TestAccountTemplate:
    def test_to_json(self):
        at = handler.AccountTemplate(
            name="baseline",
            matcher=handler.AccountTemplateMatcher(name_pattern="prod-*"),
            template_names=["default"],
            groups=["TeamA"],
            description="d",
        )
        data = json.loads(at.to_json())
        assert data["name"] == "baseline"
        assert data["template_names"] == ["default"]


class TestConfiguration:
    def test_load_populates_templates_and_account_templates(self):
        fake_table = MagicMock()
        fake_table.scan.return_value = {
            "Items": [
                {
                    "type": "template",
                    "group_name": "default",
                    "permission_sets": ["Admin"],
                    "description": "Default",
                },
                {
                    "type": "account_template",
                    "group_name": "prod-baseline",
                    "matcher": {
                        "name_pattern": "prod-*",
                        "name_patterns": ["prod-*-*"],
                        "organizational_units": ["ou-prod/*"],
                        "account_tags": {"Environment": "Production"},
                    },
                    "excluded": [r"^111111111111$", r"^prod-secret-.*$"],
                },
            ]
        }
        fake_ddb = MagicMock()
        fake_ddb.Table.return_value = fake_table

        with patch.object(handler.boto3, "resource", return_value=fake_ddb):
            cfg = handler.Configuration("cfg-table")
            cfg.load()

        assert "default" in cfg.templates
        assert cfg.templates["default"].permission_sets == ["Admin"]
        assert "prod-baseline" in cfg.account_templates
        assert cfg.account_templates["prod-baseline"].matcher.name_pattern == "prod-*"
        assert cfg.account_templates["prod-baseline"].matcher.name_patterns == [
            "prod-*-*"
        ]
        assert cfg.account_templates["prod-baseline"].excluded == [
            r"^111111111111$",
            r"^prod-secret-.*$",
        ]


class TestIdentityCenter:
    def test_init_calls_list_groups_and_list_permission_sets_and_caches_results(self):
        mock_admin_client = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "Instances": [
                    {
                        "InstanceArn": "arn:aws:sso:::instance/ssoins-1234567890abcdef",
                        "IdentityStoreId": "d-1234567890",
                    }
                ]
            }
        ]
        mock_admin_client.get_paginator.return_value = paginator
        mock_identitystore_client = MagicMock()

        with (
            patch.object(
                handler.boto3,
                "client",
                side_effect=[mock_admin_client, mock_identitystore_client],
            ) as p_client,
            patch.object(
                handler.IdentityCenter,
                "list_groups",
                return_value=[handler.Group(name="TeamA", id="g-1")],
            ) as p_list_groups,
            patch.object(
                handler.IdentityCenter,
                "list_permission_sets",
                return_value=[handler.PermissionSet(name="Admin", arn="arn:ps:1")],
            ) as p_list_permission_sets,
        ):
            ic = handler.IdentityCenter(
                instance_arn="arn:aws:sso:::instance/ssoins-1234567890abcdef"
            )

        assert p_client.call_args_list == [
            call("sso-admin", region_name=handler._AWS_REGION),
            call("identitystore", region_name=handler._AWS_REGION),
        ]
        p_list_groups.assert_called_once()
        p_list_permission_sets.assert_called_once()
        assert ic.groups == [handler.Group(name="TeamA", id="g-1")]
        assert ic.permission_sets == [
            handler.PermissionSet(name="Admin", arn="arn:ps:1")
        ]
        assert ic.identity_store_id == "d-1234567890"

    def test_has_group_and_get_group_use_cached_groups(self):
        ic = handler.IdentityCenter.__new__(handler.IdentityCenter)
        ic.instance_arn = "arn:i"
        ic.client = MagicMock()
        ic.identitystore_client = MagicMock()
        ic.permission_sets = []
        ic.groups = [handler.Group(name="TeamA", id="g-1")]
        assert ic.has_group("TeamA") is True
        assert ic.has_group("Missing") is False
        assert ic.get_group("TeamA").id == "g-1"
        assert ic.get_group("Missing") is None

    def test_list_groups_populates_cache_and_is_used_by_has_group_and_get_group(self):
        ic = handler.IdentityCenter.__new__(handler.IdentityCenter)
        ic.instance_arn = "arn:aws:sso:::instance/ssoins-1234567890abcdef"
        ic.identity_store_id = "d-1234567890"
        ic.groups = []
        ic.permission_sets = []

        mock_identitystore_client = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"Groups": [{"DisplayName": "TeamA", "GroupId": "g-1"}]}
        ]
        mock_identitystore_client.get_paginator.return_value = paginator
        ic.identitystore_client = mock_identitystore_client
        ic.client = MagicMock()

        assert ic.has_group("TeamA") is True
        grp = ic.get_group("TeamA")
        assert grp is not None
        assert grp.id == "g-1"
        # Ensure list_groups cached results (second call should not hit paginator)
        assert ic.list_groups() == [handler.Group(name="TeamA", id="g-1")]
        mock_identitystore_client.get_paginator.assert_called_once_with("list_groups")

    def test_list_permission_sets_builds_objects(self):
        ic = handler.IdentityCenter.__new__(handler.IdentityCenter)
        ic.instance_arn = "arn:i"
        ic.groups = []
        ic.permission_sets = []
        ic.poll_timeout_seconds = 1
        ic.poll_interval_seconds = 0.01

        mock_client = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [{"PermissionSets": ["arn:ps:1"]}]
        mock_client.get_paginator.return_value = paginator
        mock_client.describe_permission_set.return_value = {
            "PermissionSet": {"Name": "Admin", "PermissionSetArn": "arn:ps:1"}
        }
        ic.client = mock_client

        out = ic.list_permission_sets()
        assert [(p.name, p.arn) for p in out] == [("Admin", "arn:ps:1")]
        # Ensure list_permission_sets cached results
        assert ic.permission_sets == out

    def test_list_permission_sets_cache_hit_does_not_call_paginator(self):
        ic = handler.IdentityCenter.__new__(handler.IdentityCenter)
        ic.instance_arn = "arn:i"
        ic.groups = []
        ic.permission_sets = [handler.PermissionSet(name="Cached", arn="arn:cached")]
        ic.client = MagicMock()

        out = ic.list_permission_sets()
        assert out == ic.permission_sets
        ic.client.get_paginator.assert_not_called()

    def test_list_groups_cache_hit_does_not_call_paginator(self):
        ic = handler.IdentityCenter.__new__(handler.IdentityCenter)
        ic.instance_arn = "arn:i"
        ic.groups = [handler.Group(name="TeamA", id="g-1")]
        ic.permission_sets = []
        ic.client = MagicMock()

        out = ic.list_groups()
        assert out == ic.groups
        ic.client.get_paginator.assert_not_called()

    def test_create_assignment_skips_when_preexisting_assignment_found(self):
        ic = handler.IdentityCenter.__new__(handler.IdentityCenter)
        ic.instance_arn = "arn:i"
        ic.poll_timeout_seconds = 1
        ic.poll_interval_seconds = 0.01
        ic.groups = []
        ic.permission_sets = []

        mock_client = MagicMock()
        mock_client.list_account_assignments.return_value = {
            "AccountAssignments": [
                {"PrincipalId": "g-1", "PrincipalType": "GROUP"},
            ]
        }
        ic.client = mock_client

        ic.create_assignment(
            account_id="123",
            permission_set_arn="arn:ps",
            permission_set_name="Admin",
            principal_type="GROUP",
            principal_id="g-1",
        )

        mock_client.list_account_assignments.assert_called_once_with(
            InstanceArn="arn:i",
            AccountId="123",
            PermissionSetArn="arn:ps",
        )
        mock_client.create_account_assignment.assert_not_called()
        mock_client.describe_account_assignment_creation_status.assert_not_called()

    def test_create_assignment_creates_when_nonexistent_and_polls_to_succeeded(self):
        ic = handler.IdentityCenter.__new__(handler.IdentityCenter)
        ic.instance_arn = "arn:i"
        ic.poll_timeout_seconds = 5
        ic.poll_interval_seconds = 0.01
        ic.groups = []
        ic.permission_sets = []

        mock_client = MagicMock()
        mock_client.list_account_assignments.return_value = {"AccountAssignments": []}
        mock_client.create_account_assignment.return_value = {
            "AccountAssignmentCreationStatus": {"RequestId": "req-1"}
        }
        mock_client.describe_account_assignment_creation_status.return_value = {
            "AccountAssignmentCreationStatus": {"Status": "SUCCEEDED"}
        }
        ic.client = mock_client

        ic.create_assignment(
            account_id="123",
            permission_set_arn="arn:ps",
            permission_set_name="Admin",
            principal_type="GROUP",
            principal_id="g-1",
        )

        mock_client.create_account_assignment.assert_called_once_with(
            InstanceArn="arn:i",
            PermissionSetArn="arn:ps",
            PrincipalId="g-1",
            PrincipalType="GROUP",
            TargetId="123",
            TargetType="AWS_ACCOUNT",
        )
        mock_client.describe_account_assignment_creation_status.assert_called_once()

    def test_create_assignment_raises_when_creation_failed(self):
        ic = handler.IdentityCenter.__new__(handler.IdentityCenter)
        ic.instance_arn = "arn:i"
        ic.poll_timeout_seconds = 5
        ic.poll_interval_seconds = 0.01
        ic.groups = []
        ic.permission_sets = []

        mock_client = MagicMock()
        mock_client.list_account_assignments.return_value = {"AccountAssignments": []}
        mock_client.create_account_assignment.return_value = {
            "AccountAssignmentCreationStatus": {"RequestId": "req-1"}
        }
        mock_client.describe_account_assignment_creation_status.return_value = {
            "AccountAssignmentCreationStatus": {
                "Status": "FAILED",
                "FailureReason": "boom",
            }
        }
        ic.client = mock_client

        with pytest.raises(handler.HandlerError, match="boom"):
            ic.create_assignment(
                account_id="123",
                permission_set_arn="arn:ps",
                permission_set_name="Admin",
                principal_type="GROUP",
                principal_id="g-1",
            )

    def test_delete_assignment_polls_to_succeeded(self):
        ic = handler.IdentityCenter.__new__(handler.IdentityCenter)
        ic.instance_arn = "arn:i"
        ic.poll_timeout_seconds = 5
        ic.poll_interval_seconds = 0.01
        ic.groups = []
        ic.permission_sets = []

        mock_client = MagicMock()
        mock_client.delete_account_assignment.return_value = {
            "AccountAssignmentDeletionStatus": {"RequestId": "req-1"}
        }
        mock_client.describe_account_assignment_deletion_status.return_value = {
            "AccountAssignmentDeletionStatus": {"Status": "SUCCEEDED"}
        }
        ic.client = mock_client

        ic.delete_assignment(
            account_id="123",
            permission_set_arn="arn:ps",
            permission_set_name="Admin",
            principal_id="g-1",
            principal_type="GROUP",
        )

        mock_client.delete_account_assignment.assert_called_once_with(
            InstanceArn="arn:i",
            PermissionSetArn="arn:ps",
            PrincipalId="g-1",
            PrincipalType="GROUP",
            TargetId="123",
            TargetType="AWS_ACCOUNT",
        )
        mock_client.describe_account_assignment_deletion_status.assert_called_once()

    def test_delete_assignment_raises_when_deletion_failed(self):
        ic = handler.IdentityCenter.__new__(handler.IdentityCenter)
        ic.instance_arn = "arn:i"
        ic.poll_timeout_seconds = 5
        ic.poll_interval_seconds = 0.01
        ic.groups = []
        ic.permission_sets = []

        mock_client = MagicMock()
        mock_client.delete_account_assignment.return_value = {
            "AccountAssignmentDeletionStatus": {"RequestId": "req-1"}
        }
        mock_client.describe_account_assignment_deletion_status.return_value = {
            "AccountAssignmentDeletionStatus": {
                "Status": "FAILED",
                "FailureReason": "nope",
            }
        }
        ic.client = mock_client

        with pytest.raises(handler.HandlerError, match="nope"):
            ic.delete_assignment(
                account_id="123",
                permission_set_arn="arn:ps",
                permission_set_name="Admin",
                principal_id="g-1",
                principal_type="GROUP",
            )


class TestOrganizations:
    def test_list_accounts_returns_active_accounts_from_get_account(self):
        org = handler.Organizations.__new__(handler.Organizations)
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
        org.get_account = MagicMock(
            side_effect=lambda account_id: handler.Account(
                id=account_id, name=f"n-{account_id}"
            )
        )

        accounts = org.list_accounts()
        assert [a.id for a in accounts] == ["1"]
        org.get_account.assert_called_once_with("1")

    def test_get_account_sets_tags_and_ou_path_empty_when_no_parents(self):
        org = handler.Organizations.__new__(handler.Organizations)
        org.client = MagicMock()
        org.client.list_tags_for_resource.return_value = {
            "Tags": [{"Key": "k", "Value": "v"}]
        }
        org.client.list_parents.return_value = {"Parents": []}

        acct = org.get_account("123")
        assert acct.id == "123"
        assert acct.tags == {"k": "v"}
        assert acct.organizational_unit_path == ""
        org.client.list_tags_for_resource.assert_called_once_with(ResourceId="123")
        org.client.list_parents.assert_called_once_with(ChildId="123")

    def test_get_account_sets_tags_and_builds_ou_path_from_parent_chain(self):
        org = handler.Organizations.__new__(handler.Organizations)
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
            raise AssertionError(
                f"Unexpected OrganizationalUnitId={OrganizationalUnitId}"
            )

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
        org = handler.Organizations.__new__(handler.Organizations)
        org.client = MagicMock()
        org._ou_path_cache = {}
        org._ou_name_cache = {}

        # Same tags response for both accounts (not important for this test).
        org.client.list_tags_for_resource.return_value = {
            "Tags": [{"Key": "Environment", "Value": "Production"}]
        }

        def describe_ou_side_effect(*, OrganizationalUnitId: str):
            if OrganizationalUnitId == "ou-0":
                return {"OrganizationalUnit": {"Name": "Workloads"}}
            if OrganizationalUnitId == "ou-1":
                return {"OrganizationalUnit": {"Name": "Development"}}
            raise AssertionError(
                f"Unexpected OrganizationalUnitId={OrganizationalUnitId}"
            )

        def list_parents_side_effect(*, ChildId: str):
            # Two accounts in the same OU (ou-1).
            if ChildId in {"111", "222"}:
                return {"Parents": [{"Id": "ou-1", "Type": "OU"}]}
            # Walk the OU chain once.
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
        org = handler.Organizations.__new__(handler.Organizations)
        org.client = MagicMock()
        org.client.list_tags_for_resource.side_effect = ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "nope"}},
            "ListTagsForResource",
        )
        with pytest.raises(handler.HandlerError, match="Could not get account details"):
            org.get_account("123")


class TestTracking:
    def test_get_assignment_id(self):
        tr = handler.Tracking.__new__(handler.Tracking)
        assert tr.get_assignment_id("a", "p", "arn:ps") == "a#p#arn:ps"

    def test_list_returns_assignments(self):
        fake_table = MagicMock()
        fake_table.scan.return_value = {
            "Items": [
                {
                    "assignment_id": "a#p#arn",
                    "account_id": "a",
                    "permission_set_arn": "arn",
                    "permission_set_name": "Admin",
                    "principal_id": "p",
                    "principal_type": "GROUP",
                    "template_name": "t",
                    "group_name": "g",
                    "created_at": "c",
                    "last_seen": "l",
                }
            ]
        }
        fake_ddb = MagicMock()
        fake_ddb.Table.return_value = fake_table

        with patch.object(handler.boto3, "resource", return_value=fake_ddb):
            tr = handler.Tracking("tracking-table")
            out = tr.list()

        assert len(out) == 1
        assert out[0].assignment_id == "a#p#arn"
        assert out[0].account_id == "a"
        assert out[0].permission_set_arn == "arn"
        assert out[0].permission_set_name == "Admin"
        assert out[0].principal_id == "p"
        assert out[0].principal_type == "GROUP"
        assert out[0].template_name == "t"
        assert out[0].group_name == "g"
        assert out[0].created_at == "c"
        assert out[0].last_seen == "l"

    def test_create_puts_item(self):
        fake_table = MagicMock()
        fake_ddb = MagicMock()
        fake_ddb.Table.return_value = fake_table

        with patch.object(handler.boto3, "resource", return_value=fake_ddb):
            tr = handler.Tracking("tracking-table")
            tr.create(
                account_id="a",
                permission_set_arn="arn:ps",
                permission_set_name="Admin",
                principal_id="p",
                principal_type="GROUP",
                template_name="t",
                group_name="g",
            )

        fake_table.put_item.assert_called_once()

    def test_delete_calls_delete_item(self):
        fake_table = MagicMock()
        fake_ddb = MagicMock()
        fake_ddb.Table.return_value = fake_table
        with patch.object(handler.boto3, "resource", return_value=fake_ddb):
            tr = handler.Tracking("tracking-table")
            tr.delete("a#p#arn")
        fake_table.delete_item.assert_called_once_with(Key={"assignment_id": "a#p#arn"})


class TestReconcileCreations:
    def test_returns_empty_when_no_bindings(self):
        ok, bad = handler.reconcile_creations(
            [], identity_center=MagicMock(), tracking=MagicMock()
        )
        assert not ok
        assert not bad

    def test_records_success_and_failure_per_group(self):
        identity_center = MagicMock()
        tracking = MagicMock()

        def create_side_effect(*_args, **kwargs):
            if kwargs["principal_id"] == "g-1":
                raise handler.HandlerError("boom")

        identity_center.create_assignment.side_effect = create_side_effect

        bindings = [
            handler.Binding(
                account_id="123",
                permission_set_name="Admin",
                permission_set_arn="arn:ps",
                groups=[
                    handler.Group(name="A", id="g-1"),
                    handler.Group(name="B", id="g-2"),
                ],
                template_name="tpl",
            )
        ]

        successes, failures = handler.reconcile_creations(
            bindings, identity_center=identity_center, tracking=tracking
        )
        assert [s["group_name"] for s in successes] == ["B"]
        assert [f["group_name"] for f in failures] == ["A"]
        tracking.create.assert_called_once()


class TestReconcileDeletions:
    def test_returns_empty_when_no_tracked_assignments(self):
        tracking = MagicMock()
        tracking.list.return_value = []
        identity_center = MagicMock()
        deleted, failed = handler.reconcile_deletions(
            desired_bindings=[], tracking=tracking, identity_center=identity_center
        )
        assert not deleted
        assert not failed
        identity_center.delete_assignment.assert_not_called()
        tracking.delete.assert_not_called()

    def test_deletes_only_unmatched_assignments_and_processes_all(self):
        tracking = MagicMock()
        identity_center = MagicMock()

        a1 = handler.Assignment(
            assignment_id="1#p1#arn",
            account_id="1",
            permission_set_arn="arn",
            permission_set_name="Admin",
            principal_id="p1",
            principal_type="GROUP",
            template_name="t",
            group_name="G1",
        )
        a2 = handler.Assignment(
            assignment_id="2#p2#arn",
            account_id="2",
            permission_set_arn="arn",
            permission_set_name="Admin",
            principal_id="p2",
            principal_type="GROUP",
            template_name="t",
            group_name="G2",
        )

        tracking.list.return_value = [a1, a2]

        desired = [
            handler.Binding(
                account_id="2",
                permission_set_name="Admin",
                permission_set_arn="arn",
                groups=[handler.Group(name="G2", id="p2")],
                template_name="t",
            )
        ]

        deleted, failed = handler.reconcile_deletions(
            desired_bindings=desired, tracking=tracking, identity_center=identity_center
        )
        assert not failed
        assert [d["assignment_id"] for d in deleted] == ["1#p1#arn"]
        identity_center.delete_assignment.assert_called_once()
        tracking.delete.assert_called_once_with("1#p1#arn")

    def test_ignores_assignment_missing_in_aws_but_still_deletes_tracking(self):
        tracking = MagicMock()
        identity_center = MagicMock()

        a1 = handler.Assignment(
            assignment_id="1#p1#arn",
            account_id="1",
            permission_set_arn="arn",
            permission_set_name="Admin",
            principal_id="p1",
            principal_type="GROUP",
            template_name="t",
            group_name="G1",
        )
        tracking.list.return_value = [a1]
        identity_center.delete_assignment.side_effect = RuntimeError(
            "Assignment does not exist"
        )

        deleted, failed = handler.reconcile_deletions(
            desired_bindings=[], tracking=tracking, identity_center=identity_center
        )
        assert not failed
        assert not deleted
        tracking.delete.assert_called_once_with("1#p1#arn")


class _ConfigStub:
    def __init__(self, templates: dict[str, handler.Template], account_templates=None):
        self.templates = templates
        self.account_templates = account_templates or {}


class _IdentityCenterStub:
    def __init__(self, groups: dict[str, str], permission_sets: dict[str, str]):
        self._groups = groups
        self._permission_sets = permission_sets

    def has_group(self, group_name: str) -> bool:
        return group_name in self._groups

    def get_group(self, group_name: str) -> handler.Group | None:
        if group_name not in self._groups:
            return None
        return handler.Group(name=group_name, id=self._groups[group_name])

    def get_permission_set(self, name: str) -> handler.PermissionSet | None:
        arn = self._permission_sets.get(name)
        if not arn:
            return None
        return handler.PermissionSet(name=name, arn=arn)


class TestBuildPermissionBindings:
    def test_fails_when_template_missing(self):
        acct = handler.Account(id="1", tags={})
        cfg = _ConfigStub(templates={})
        ic = _IdentityCenterStub(
            groups={"G1": "g1"}, permission_sets={"PS1": "arn:ps1"}
        )

        bindings, successes, failures = handler.build_permission_bindings(
            account=acct,
            configuration=cfg,
            identity_center=ic,
            permission=handler.Permission(name="tpl", groups=["G1"]),
        )
        assert not bindings
        assert not successes
        assert (
            failures
            and failures[0]["error"] == "Permission template not found in configuration"
        )

    def test_records_failure_for_missing_group_and_skips_it(self):
        acct = handler.Account(id="1", tags={})
        cfg = _ConfigStub(templates={"tpl": handler.Template(permission_sets=["PS1"])})
        ic = _IdentityCenterStub(
            groups={"G2": "g2"}, permission_sets={"PS1": "arn:ps1"}
        )

        bindings, _successes, failures = handler.build_permission_bindings(
            account=acct,
            configuration=cfg,
            identity_center=ic,
            permission=handler.Permission(name="tpl", groups=["Missing", "G2"]),
        )
        assert len(failures) == 1
        assert failures[0]["group"] == "Missing"
        assert len(bindings) == 1
        assert [g.name for g in bindings[0].groups] == ["G2"]

    def test_records_failure_for_missing_permission_set_and_omits_binding(self):
        acct = handler.Account(id="1", tags={})
        cfg = _ConfigStub(
            templates={"tpl": handler.Template(permission_sets=["MissingPS", "PS1"])}
        )
        ic = _IdentityCenterStub(
            groups={"G1": "g1"}, permission_sets={"PS1": "arn:ps1"}
        )

        bindings, _successes, failures = handler.build_permission_bindings(
            account=acct,
            configuration=cfg,
            identity_center=ic,
            permission=handler.Permission(name="tpl", groups=["G1"]),
        )
        assert len(bindings) == 1
        assert bindings[0].permission_set_name == "PS1"
        assert any(f["permission"] == "MissingPS" for f in failures)

    def test_creates_one_binding_per_permission_set(self):
        acct = handler.Account(id="1", tags={})
        cfg = _ConfigStub(
            templates={"tpl": handler.Template(permission_sets=["PS1", "PS2"])}
        )
        ic = _IdentityCenterStub(
            groups={"G1": "g1"}, permission_sets={"PS1": "arn:ps1", "PS2": "arn:ps2"}
        )

        bindings, _successes, failures = handler.build_permission_bindings(
            account=acct,
            configuration=cfg,
            identity_center=ic,
            permission=handler.Permission(name="tpl", groups=["G1"]),
        )
        assert not failures
        assert {b.permission_set_name for b in bindings} == {"PS1", "PS2"}
        assert all(b.template_name == "tpl" for b in bindings)

    def test_builds_expected_bindings_across_multiple_tagged_accounts(self):
        accounts = [
            handler.Account(
                id="111111111111",
                name="a1",
                tags={"x/tpl": "G1,G2"},
                organizational_unit_path="r-root/ou-1",
            ),
            handler.Account(
                id="222222222222",
                name="a2",
                tags={"x/tpl": "G2"},
                organizational_unit_path="r-root/ou-2",
            ),
        ]
        cfg = _ConfigStub(
            templates={"tpl": handler.Template(permission_sets=["PS1", "PS2"])}
        )
        ic = _IdentityCenterStub(
            groups={"G1": "g1", "G2": "g2"},
            permission_sets={"PS1": "arn:ps1", "PS2": "arn:ps2"},
        )

        all_bindings: list[handler.Binding] = []
        all_failures: list[dict[str, object]] = []
        for acct in accounts:
            for permission in acct.get_permission_tags("x"):
                bindings, _successes, failures = handler.build_permission_bindings(
                    account=acct,
                    configuration=cfg,
                    identity_center=ic,
                    permission=permission,
                )
                all_bindings.extend(bindings)
                all_failures.extend(failures)

        assert not all_failures
        assert len(all_bindings) == 2 * 2  # 2 accounts * 2 permission sets
        assert {b.account_id for b in all_bindings} == {"111111111111", "222222222222"}
        assert {b.template_name for b in all_bindings} == {"tpl"}
        assert {b.permission_set_name for b in all_bindings} == {"PS1", "PS2"}
        # Account 1 bindings should contain both groups
        acct1_groups = [
            {g.name for g in b.groups}
            for b in all_bindings
            if b.account_id == "111111111111"
        ]
        assert all(gs == {"G1", "G2"} for gs in acct1_groups)
        # Account 2 bindings should contain only G2
        acct2_groups = [
            {g.name for g in b.groups}
            for b in all_bindings
            if b.account_id == "222222222222"
        ]
        assert all(gs == {"G2"} for gs in acct2_groups)

    def test_records_failures_across_accounts_for_missing_template_group_and_permission_set(
        self,
    ):
        accounts = [
            # Missing template
            handler.Account(
                id="1",
                name="a1",
                tags={"x/missing": "G1"},
                organizational_unit_path="r-root/ou-1",
            ),
            # Missing group referenced in tag
            handler.Account(
                id="2",
                name="a2",
                tags={"x/tpl": "MissingGroup,G1"},
                organizational_unit_path="r-root/ou-2",
            ),
            # Missing permission set referenced by template
            handler.Account(
                id="3",
                name="a3",
                tags={"x/tpl_missing_ps": "G1"},
                organizational_unit_path="r-root/ou-3",
            ),
        ]
        cfg = _ConfigStub(
            templates={
                "tpl": handler.Template(permission_sets=["PS1"]),
                "tpl_missing_ps": handler.Template(
                    permission_sets=["MissingPS", "PS1"]
                ),
            }
        )
        ic = _IdentityCenterStub(
            groups={"G1": "g1"}, permission_sets={"PS1": "arn:ps1"}
        )

        all_bindings: list[handler.Binding] = []
        all_failures: list[dict[str, object]] = []
        for acct in accounts:
            for permission in acct.get_permission_tags("x"):
                bindings, _successes, failures = handler.build_permission_bindings(
                    account=acct,
                    configuration=cfg,
                    identity_center=ic,
                    permission=permission,
                )
                all_bindings.extend(bindings)
                all_failures.extend(failures)

        # We should still have bindings for account 2 (G1) and account 3 (PS1)
        assert {b.account_id for b in all_bindings} == {"2", "3"}
        assert all(b.permission_set_name == "PS1" for b in all_bindings)

        assert any(
            f.get("account_id") == "1"
            and f.get("permission") == "missing"
            and f.get("error") == "Permission template not found in configuration"
            for f in all_failures
        )
        assert any(
            f.get("account_id") == "2"
            and f.get("permission") == "tpl"
            and f.get("group") == "MissingGroup"
            and f.get("error") == "Group not found in identity store"
            for f in all_failures
        )
        assert any(
            f.get("account_id") == "3"
            and f.get("permission") == "MissingPS"
            and f.get("error") == "Permission set not found in identity store"
            for f in all_failures
        )


class TestBuildAccountBindings:
    def test_returns_empty_when_no_account_templates(self):
        acct = handler.Account(
            id="1", name="a", tags={}, organizational_unit_path="r-root/ou-1"
        )
        cfg = _ConfigStub(
            templates={"tpl": handler.Template(permission_sets=["PS1"])},
            account_templates={},
        )
        ic = _IdentityCenterStub(
            groups={"G1": "g1"}, permission_sets={"PS1": "arn:ps1"}
        )

        bindings, successes, failures = handler.build_account_bindings(
            account=acct,
            configuration=cfg,
            identity_center=ic,
        )
        assert not bindings
        assert not successes
        assert not failures

    def test_skips_when_matcher_does_not_match(self):
        acct = handler.Account(
            id="1", name="dev-app", tags={}, organizational_unit_path="r-root/ou-dev"
        )
        cfg = _ConfigStub(
            templates={"tpl": handler.Template(permission_sets=["PS1"])},
            account_templates={
                "prod": handler.AccountTemplate(
                    name="prod",
                    matcher=handler.AccountTemplateMatcher(name_pattern="prod-*"),
                    template_names=["tpl"],
                    groups=["G1"],
                )
            },
        )
        ic = _IdentityCenterStub(
            groups={"G1": "g1"}, permission_sets={"PS1": "arn:ps1"}
        )

        bindings, successes, failures = handler.build_account_bindings(acct, cfg, ic)
        assert not bindings
        assert not successes
        assert not failures

    def test_builds_bindings_for_matching_template_and_multiple_template_refs(self):
        acct = handler.Account(
            id="1", name="prod-app", tags={}, organizational_unit_path="r-root/ou-prod"
        )
        cfg = _ConfigStub(
            templates={
                "tpl1": handler.Template(permission_sets=["PS1"]),
                "tpl2": handler.Template(permission_sets=["PS2"]),
            },
            account_templates={
                "prod": handler.AccountTemplate(
                    name="prod",
                    matcher=handler.AccountTemplateMatcher(name_pattern="prod-*"),
                    template_names=["tpl1", "tpl2"],
                    groups=["G1"],
                )
            },
        )
        ic = _IdentityCenterStub(
            groups={"G1": "g1"},
            permission_sets={"PS1": "arn:ps1", "PS2": "arn:ps2"},
        )

        bindings, _successes, failures = handler.build_account_bindings(acct, cfg, ic)
        assert not failures
        # One binding per referenced template's permission_sets -> 2 total
        assert len(bindings) == 2
        assert {b.template_name for b in bindings} == {"tpl1", "tpl2"}
        assert {b.permission_set_name for b in bindings} == {"PS1", "PS2"}

    def test_account_template_name_patterns_dot_star_matches_all_accounts(self):
        # Mirrors a Terraform account template:
        # baseline = { template_names=["platform"], groups=["Cloud Solutions"], excluded=[...],
        #              matcher={ name_patterns=[".*"] } }
        acct = handler.Account(
            id="123456789012",
            name="TestAccount",
            tags={},
            organizational_unit_path="r-root/ou-any",
        )
        cfg = _ConfigStub(
            templates={"platform": handler.Template(permission_sets=["PS1"])},
            account_templates={
                "baseline": handler.AccountTemplate(
                    name="baseline",
                    matcher=handler.AccountTemplateMatcher(name_patterns=[".*"]),
                    template_names=["platform"],
                    groups=["Cloud Solutions"],
                    excluded=["Management", "Audit", "LogArchive"],
                    description="Every account receives the platform template",
                )
            },
        )
        ic = _IdentityCenterStub(
            groups={"Cloud Solutions": "g-cloud-solutions"},
            permission_sets={"PS1": "arn:ps1"},
        )

        bindings, successes, failures = handler.build_account_bindings(acct, cfg, ic)
        assert not failures
        assert not successes
        assert len(bindings) == 1
        assert bindings[0].account_id == "123456789012"
        assert bindings[0].template_name == "platform"
        assert bindings[0].permission_set_name == "PS1"
        assert [g.name for g in bindings[0].groups] == ["Cloud Solutions"]

    def test_excluded_filters_out_account_by_id(self):
        acct = handler.Account(
            id="111111111111",
            name="prod-app",
            tags={},
            organizational_unit_path="r-root/ou-prod",
        )
        cfg = _ConfigStub(
            templates={"tpl": handler.Template(permission_sets=["PS1"])},
            account_templates={
                "prod": handler.AccountTemplate(
                    name="prod",
                    matcher=handler.AccountTemplateMatcher(name_pattern="prod-*"),
                    excluded=[r"^111111111111$"],
                    template_names=["tpl"],
                    groups=["G1"],
                )
            },
        )
        ic = _IdentityCenterStub(
            groups={"G1": "g1"}, permission_sets={"PS1": "arn:ps1"}
        )

        bindings, successes, failures = handler.build_account_bindings(acct, cfg, ic)
        assert not bindings
        assert not successes
        assert not failures

    def test_excluded_filters_out_account_by_name(self):
        acct = handler.Account(
            id="999999999999",
            name="prod-secret-app",
            tags={},
            organizational_unit_path="r-root/ou-prod",
        )
        cfg = _ConfigStub(
            templates={"tpl": handler.Template(permission_sets=["PS1"])},
            account_templates={
                "prod": handler.AccountTemplate(
                    name="prod",
                    matcher=handler.AccountTemplateMatcher(name_pattern="prod-*"),
                    excluded=[r"^prod-secret-.*$"],
                    template_names=["tpl"],
                    groups=["G1"],
                )
            },
        )
        ic = _IdentityCenterStub(
            groups={"G1": "g1"}, permission_sets={"PS1": "arn:ps1"}
        )

        bindings, successes, failures = handler.build_account_bindings(acct, cfg, ic)
        assert not bindings
        assert not successes
        assert not failures

    def test_invalid_excluded_regex_is_reported_as_failure(self):
        acct = handler.Account(
            id="111111111111",
            name="prod-app",
            tags={},
            organizational_unit_path="r-root/ou-prod",
        )
        cfg = _ConfigStub(
            templates={"tpl": handler.Template(permission_sets=["PS1"])},
            account_templates={
                "prod": handler.AccountTemplate(
                    name="prod",
                    matcher=handler.AccountTemplateMatcher(name_pattern="prod-*"),
                    excluded=[r"("],
                    template_names=["tpl"],
                    groups=["G1"],
                )
            },
        )
        ic = _IdentityCenterStub(
            groups={"G1": "g1"}, permission_sets={"PS1": "arn:ps1"}
        )

        bindings, successes, failures = handler.build_account_bindings(acct, cfg, ic)
        assert not bindings
        assert not successes
        assert len(failures) == 1
        assert failures[0]["account.name"] == "prod-app"
        assert failures[0]["account_template_name"] == "prod"
        assert "Invalid excluded regex" in failures[0]["error"]

    def test_propagates_failure_when_group_missing(self):
        acct = handler.Account(
            id="1", name="prod-app", tags={}, organizational_unit_path="r-root/ou-prod"
        )
        cfg = _ConfigStub(
            templates={"tpl": handler.Template(permission_sets=["PS1"])},
            account_templates={
                "prod": handler.AccountTemplate(
                    name="prod",
                    matcher=handler.AccountTemplateMatcher(name_pattern="prod-*"),
                    template_names=["tpl"],
                    groups=["Missing", "G1"],
                )
            },
        )
        ic = _IdentityCenterStub(
            groups={"G1": "g1"}, permission_sets={"PS1": "arn:ps1"}
        )

        bindings, successes, failures = handler.build_account_bindings(acct, cfg, ic)
        assert not successes
        assert len(failures) == 1
        assert failures[0]["error"] == "Group not found in identity store"
        assert failures[0]["group"] == "Missing"
        assert failures[0]["permission"] == "tpl"
        assert len(bindings) == 1
        assert [g.name for g in bindings[0].groups] == ["G1"]

    def test_propagates_failure_when_permission_set_missing(self):
        acct = handler.Account(
            id="1", name="prod-app", tags={}, organizational_unit_path="r-root/ou-prod"
        )
        cfg = _ConfigStub(
            templates={
                "tpl": handler.Template(permission_sets=["MissingPS", "PS1"]),
            },
            account_templates={
                "prod": handler.AccountTemplate(
                    name="prod",
                    matcher=handler.AccountTemplateMatcher(name_pattern="prod-*"),
                    template_names=["tpl"],
                    groups=["G1"],
                )
            },
        )
        ic = _IdentityCenterStub(
            groups={"G1": "g1"}, permission_sets={"PS1": "arn:ps1"}
        )

        bindings, successes, failures = handler.build_account_bindings(acct, cfg, ic)
        assert not successes
        assert any(
            f.get("error") == "Permission set not found in identity store"
            and f.get("permission") == "MissingPS"
            for f in failures
        )
        assert len(bindings) == 1
        assert bindings[0].permission_set_name == "PS1"


class TestBindingCreationAcrossAccounts:
    def test_lambda_handler_builds_expected_bindings_from_account_tags(self):
        os.environ["DYNAMODB_CONFIG_TABLE"] = "cfg"
        os.environ["DYNAMODB_TRACKING_TABLE"] = "tracking"
        os.environ["SSO_INSTANCE_ARN"] = "arn:i"
        os.environ["SSO_ACCOUNT_TAG_PREFIX"] = "x"

        accounts = [
            handler.Account(
                id="111111111111",
                name="a1",
                tags={"x/tpl": "G1,G2"},
                organizational_unit_path="r-root/ou-1",
            ),
            handler.Account(
                id="222222222222",
                name="a2",
                tags={"x/tpl": "G2"},
                organizational_unit_path="r-root/ou-2",
            ),
        ]

        class FakeConfig:
            def __init__(self, table_name: str):
                self.table_name = table_name
                self.templates = {
                    "tpl": handler.Template(permission_sets=["PS1", "PS2"])
                }
                self.account_templates = {}

            def load(self) -> None:
                return None

        class FakeTracking:
            def __init__(self, table_name: str):
                self.table_name = table_name

        class FakeIdentityCenter:
            def __init__(self, instance_arn: str):
                self.instance_arn = instance_arn

            def has_group(self, group_name: str) -> bool:
                return group_name in {"G1", "G2"}

            def get_group(self, group_name: str) -> handler.Group:
                return handler.Group(name=group_name, id=f"id-{group_name}")

            def get_permission_set(self, name: str) -> handler.PermissionSet | None:
                if name not in {"PS1", "PS2"}:
                    return None
                return handler.PermissionSet(name=name, arn=f"arn:{name}")

        class FakeOrgs:
            def list_accounts(self) -> list[handler.Account]:
                return accounts

        captured: dict[str, object] = {}

        def fake_reconcile_creations(
            *, bindings, **_kwargs
        ):  # pylint: disable=unused-argument
            captured["bindings"] = bindings
            return [], []

        with (
            patch.object(handler, "Configuration", FakeConfig),
            patch.object(handler, "Tracking", FakeTracking),
            patch.object(handler, "IdentityCenter", FakeIdentityCenter),
            patch.object(handler, "Organizations", FakeOrgs),
            patch.object(
                handler, "reconcile_creations", side_effect=fake_reconcile_creations
            ) as mock_reconcile,
            patch.object(handler, "reconcile_deletions", return_value=([], [])),
        ):
            out = handler.lambda_handler({"source": "cron_schedule"}, None)

        assert out["status"] == "success"
        mock_reconcile.assert_called_once()

        bindings = captured["bindings"]
        assert bindings, "Expected bindings to be created"

        # Expect one Binding per account per permission set.
        assert len(bindings) == 4
        by_account = {}
        for b in bindings:
            by_account.setdefault(b.account_id, []).append(b)

        assert {b.permission_set_name for b in by_account["111111111111"]} == {
            "PS1",
            "PS2",
        }
        assert {b.permission_set_name for b in by_account["222222222222"]} == {
            "PS1",
            "PS2",
        }

        # Groups resolved from tag values.
        a1_groups = {g.name for g in by_account["111111111111"][0].groups}
        a2_groups = {g.name for g in by_account["222222222222"][0].groups}
        assert a1_groups == {"G1", "G2"}
        assert a2_groups == {"G2"}

        # Template name propagated.
        assert all(b.template_name == "tpl" for b in bindings)
