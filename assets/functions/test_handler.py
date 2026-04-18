"""
Unit tests for handler.py — AWS SSO assignment Lambda logic.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

# Import the module under test from the same directory (no package layout).
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import handler  # noqa: E402


@pytest.fixture(autouse=True)
def restore_env():
    """Snapshot env vars touched by tests and restore after each test."""
    keys = (
        "DYNAMODB_TABLE_NAME",
        "SSO_INSTANCE_ARN",
        "SSO_ACCOUNT_TAG_PREFIX",
        "LOG_LEVEL",
    )
    before = {k: os.environ.get(k) for k in keys}
    yield
    for k in keys:
        v = before[k]
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


class TestConfigurationModel:
    def test_groups_map_contains_templates(self):
        cfg = handler.Configuration(
            groups={"sso/tpl": handler.GroupConfiguration(permission_sets=["Admin"])}
        )
        assert "sso/tpl" in cfg.groups
        assert cfg.groups["sso/tpl"].permission_sets == ["Admin"]


class TestPermissionAndBindingModels:
    def test_permission_holds_template_key_and_groups(self):
        p = handler.Permission(name="sso/x", groups=[" a ", "b"])
        assert p.name == "sso/x"
        assert p.groups == [" a ", "b"]

    def test_binding_holds_groups(self):
        b = handler.Binding(
            account_id="123456789012",
            permission_set_name="PS",
            permission_set_arn="arn:ps",
            groups=[handler.Group(name="G", id="id-1")],
        )
        assert b.account_id == "123456789012"
        assert b.groups[0].id == "id-1"


class TestDataclassToJson:
    def test_group_and_permission_json_roundtrip(self):
        g = handler.Group(name="G", id="gid")
        data = json.loads(g.to_json())
        assert data == {"name": "G", "id": "gid"}

        p = handler.Permission(name="sso/t", groups=["A"])
        assert json.loads(p.to_json()) == {"name": "sso/t", "groups": ["A"]}

    def test_binding_nested_groups_serialize(self):
        b = handler.Binding(
            account_id="123456789012",
            permission_set_name="PS",
            permission_set_arn="arn:ps",
            groups=[handler.Group(name="G", id="gid")],
        )
        data = json.loads(b.to_json())
        assert data["account_id"] == "123456789012"
        assert data["groups"] == [{"name": "G", "id": "gid"}]

    def test_configuration_nested_json(self):
        cfg = handler.Configuration(
            groups={"sso/x": handler.GroupConfiguration(permission_sets=["P"], description="d")}
        )
        data = json.loads(cfg.to_json())
        assert data["groups"]["sso/x"]["permission_sets"] == ["P"]
        assert data["groups"]["sso/x"]["description"] == "d"


class TestHandlerError:
    def test_is_runtime_error(self):
        err = handler.HandlerError("x")
        assert isinstance(err, RuntimeError)
        assert str(err) == "x"


class TestGetIdentityStoreId:
    def test_returns_matching_identity_store_id(self):
        mock_sso = MagicMock()
        paginator = MagicMock()
        mock_sso.get_paginator.return_value = paginator
        paginator.paginate.return_value = [
            {
                "Instances": [
                    {"InstanceArn": "arn:aws:sso:::instance/other", "IdentityStoreId": "d-WRONG"},
                    {
                        "InstanceArn": "arn:aws:sso:::instance/want",
                        "IdentityStoreId": "d-12345",
                    },
                ]
            }
        ]
        with patch.object(handler, "sso_admin", mock_sso):
            assert handler.get_identity_store_id("arn:aws:sso:::instance/want") == "d-12345"

    def test_raises_when_not_found(self):
        mock_sso = MagicMock()
        paginator = MagicMock()
        mock_sso.get_paginator.return_value = paginator
        paginator.paginate.return_value = [{"Instances": []}]
        with patch.object(handler, "sso_admin", mock_sso):
            with pytest.raises(handler.HandlerError, match="not found"):
                handler.get_identity_store_id("arn:aws:sso:::instance/nope")

    def test_finds_instance_across_paginated_pages(self):
        mock_sso = MagicMock()
        paginator = MagicMock()
        mock_sso.get_paginator.return_value = paginator
        paginator.paginate.return_value = [
            {"Instances": [{"InstanceArn": "arn:aws:sso:::instance/a", "IdentityStoreId": "d-A"}]},
            {
                "Instances": [
                    {"InstanceArn": "arn:aws:sso:::instance/want", "IdentityStoreId": "d-WANT"},
                ]
            },
        ]
        with patch.object(handler, "sso_admin", mock_sso):
            assert handler.get_identity_store_id("arn:aws:sso:::instance/want") == "d-WANT"


class TestGetPermissionSets:
    def test_maps_names_to_arns(self):
        mock_sso = MagicMock()
        paginator = MagicMock()
        mock_sso.get_paginator.return_value = paginator
        paginator.paginate.return_value = [{"PermissionSets": ["arn:ps:1", "arn:ps:2"]}]
        mock_sso.describe_permission_set.side_effect = [
            {"PermissionSet": {"Name": "Admin", "PermissionSetArn": "arn:ps:1"}},
            {"PermissionSet": {"Name": "ReadOnly", "PermissionSetArn": "arn:ps:2"}},
        ]
        with patch.object(handler, "sso_admin", mock_sso):
            result = handler.get_permission_sets("arn:aws:sso:::instance/x")
        assert result == {"Admin": "arn:ps:1", "ReadOnly": "arn:ps:2"}


class TestGetIdentityStoreGroups:
    def test_maps_display_names_to_ids(self):
        mock_ids = MagicMock()
        paginator = MagicMock()
        mock_ids.get_paginator.return_value = paginator
        paginator.paginate.return_value = [
            {
                "Groups": [
                    {"DisplayName": "TeamA", "GroupId": "g-1"},
                    {"DisplayName": None, "GroupId": "g-orphan"},
                    {"DisplayName": "TeamB", "GroupId": "g-2"},
                ]
            }
        ]
        with patch.object(handler, "identitystore", mock_ids):
            out = handler.get_identity_store_groups("d-abc")
        assert out == {"TeamA": "g-1", "TeamB": "g-2"}

    def test_merges_groups_across_pages(self):
        mock_ids = MagicMock()
        paginator = MagicMock()
        mock_ids.get_paginator.return_value = paginator
        paginator.paginate.return_value = [
            {"Groups": [{"DisplayName": "A", "GroupId": "g-a"}]},
            {"Groups": [{"DisplayName": "B", "GroupId": "g-b"}]},
        ]
        with patch.object(handler, "identitystore", mock_ids):
            out = handler.get_identity_store_groups("d-abc")
        assert out == {"A": "g-a", "B": "g-b"}


class TestListActiveAccounts:
    def test_only_active_ids(self):
        mock_org = MagicMock()
        paginator = MagicMock()
        mock_org.get_paginator.return_value = paginator
        paginator.paginate.return_value = [
            {
                "Accounts": [
                    {"Id": "111111111111", "Status": "ACTIVE"},
                    {"Id": "222222222222", "Status": "SUSPENDED"},
                    {"Id": "333333333333", "Status": "ACTIVE"},
                ]
            }
        ]
        with patch.object(handler, "organizations", mock_org):
            assert handler.list_active_accounts() == ["111111111111", "333333333333"]

    def test_concatenates_accounts_across_pages(self):
        mock_org = MagicMock()
        paginator = MagicMock()
        mock_org.get_paginator.return_value = paginator
        paginator.paginate.return_value = [
            {"Accounts": [{"Id": "111111111111", "Status": "ACTIVE"}]},
            {"Accounts": [{"Id": "222222222222", "Status": "ACTIVE"}]},
        ]
        with patch.object(handler, "organizations", mock_org):
            assert handler.list_active_accounts() == ["111111111111", "222222222222"]


class TestGetAccountTags:
    def test_returns_key_value_map(self):
        mock_org = MagicMock()
        mock_org.list_tags_for_resource.return_value = {
            "Tags": [{"Key": "sso/default", "Value": "g1,g2"}, {"Key": "other", "Value": "x"}]
        }
        with patch.object(handler, "organizations", mock_org):
            tags = handler.get_account_tags("123456789012")
        assert tags == {"sso/default": "g1,g2", "other": "x"}
        mock_org.list_tags_for_resource.assert_called_once_with(ResourceId="123456789012")

    def test_raises_handler_error_on_client_error(self):
        mock_org = MagicMock()
        mock_org.list_tags_for_resource.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "nope"}}, "ListTagsForResource"
        )
        with patch.object(handler, "organizations", mock_org):
            with pytest.raises(handler.HandlerError, match="Could not list tags"):
                handler.get_account_tags("123456789012")


class TestEnsureAccountExists:
    def test_ok_when_describe_succeeds(self):
        mock_org = MagicMock()
        with patch.object(handler, "organizations", mock_org):
            handler.ensure_account_exists("123456789012")
        mock_org.describe_account.assert_called_once_with(AccountId="123456789012")

    def test_raises_handler_error_on_client_error(self):
        mock_org = MagicMock()
        mock_org.describe_account.side_effect = ClientError(
            {"Error": {"Code": "AccountNotFound", "Message": "missing"}}, "DescribeAccount"
        )
        with patch.object(handler, "organizations", mock_org):
            with pytest.raises(handler.HandlerError, match="not found"):
                handler.ensure_account_exists("999999999999")


class TestGetAccountPermissionTags:
    def test_builds_permissions_from_prefixed_tags(self):
        tags = {
            "sso/default": " Alpha , Beta ",
            "sso/other": "Gamma",
            "unrelated": "x",
        }
        perms = handler.get_account_permission_tags(tags, "sso")
        by_name = {p.name: p.groups for p in perms}
        assert by_name["sso/default"] == ["Alpha", "Beta"]
        assert by_name["sso/other"] == ["Gamma"]
        assert len(perms) == 2

    def test_empty_when_no_matching_prefix(self):
        assert handler.get_account_permission_tags({"a": "b"}, "sso") == []


class TestLoadConfiguration:
    def test_scan_pagination_and_fields(self):
        mock_table = MagicMock()
        mock_table.scan.side_effect = [
            {
                "Items": [
                    {
                        "group_name": "sso/a",
                        "permission_sets": ["P1"],
                        "enabled": False,
                        "description": "d1",
                    }
                ],
                "LastEvaluatedKey": {"group_name": "sso/a"},
            },
            {
                "Items": [
                    {"group_name": "sso/b"},
                ],
            },
        ]
        mock_resource = MagicMock()
        mock_resource.Table.return_value = mock_table
        with patch.object(handler, "dynamodb", mock_resource):
            cfg = handler.load_configuration("my-table")
        mock_resource.Table.assert_called_once_with("my-table")
        assert set(cfg.groups) == {"sso/a", "sso/b"}
        assert cfg.groups["sso/a"].permission_sets == ["P1"]
        assert cfg.groups["sso/a"].enabled is False
        assert cfg.groups["sso/a"].description == "d1"
        assert cfg.groups["sso/b"].permission_sets == []
        assert cfg.groups["sso/b"].enabled is True


class TestGetBindings:
    def test_builds_bindings_for_resolved_groups_and_permission_sets(self):
        req = handler.Permission(name="sso/tpl", groups=["G1", "G2"])
        template = handler.GroupConfiguration(permission_sets=["Admin", "ReadOnly"])
        bindings, successes, failures = handler.get_bindings(
            account_id="123456789012",
            identity_store_groups={"G1": "id-1", "G2": "id-2"},
            permission_sets={"Admin": "arn:a", "ReadOnly": "arn:r"},
            request=req,
            template=template,
        )
        assert successes == [] and failures == []
        assert len(bindings) == 2
        assert {b.permission_set_name for b in bindings} == {"Admin", "ReadOnly"}
        assert all(b.account_id == "123456789012" for b in bindings)
        assert len(bindings[0].groups) == 2
        assert {g.name for g in bindings[0].groups} == {"G1", "G2"}

    def test_unknown_group_appends_failure_and_omits_group(self):
        req = handler.Permission(name="sso/tpl", groups=["Missing", "G2"])
        template = handler.GroupConfiguration(permission_sets=["Admin"])
        bindings, successes, failures = handler.get_bindings(
            account_id="123456789012",
            identity_store_groups={"G2": "id-2"},
            permission_sets={"Admin": "arn:a"},
            request=req,
            template=template,
        )
        assert successes == []
        assert len(failures) == 1
        assert failures[0]["group"] == "Missing"
        assert len(bindings) == 1
        assert len(bindings[0].groups) == 1
        assert bindings[0].groups[0].name == "G2"

    def test_unknown_permission_set_appends_failure(self):
        req = handler.Permission(name="sso/tpl", groups=["G1"])
        template = handler.GroupConfiguration(permission_sets=["Nope"])
        bindings, successes, failures = handler.get_bindings(
            account_id="123456789012",
            identity_store_groups={"G1": "id-1"},
            permission_sets={"Admin": "arn:a"},
            request=req,
            template=template,
        )
        assert bindings == []
        assert len(failures) == 1
        assert failures[0]["permission"] == "Nope"


class TestGetPermissionSetsEdgeCases:
    def test_skips_permission_sets_without_a_name(self):
        mock_sso = MagicMock()
        paginator = MagicMock()
        mock_sso.get_paginator.return_value = paginator
        paginator.paginate.return_value = [{"PermissionSets": ["arn:ps:1"]}]
        mock_sso.describe_permission_set.return_value = {"PermissionSet": {"Name": None}}
        with patch.object(handler, "sso_admin", mock_sso):
            result = handler.get_permission_sets("arn:aws:sso:::instance/x")
        assert result == {}


class TestCreateAccountAssignment:
    """``list_account_assignments`` must be stubbed: a bare MagicMock is truthy and would always skip create."""

    @staticmethod
    def _no_existing_assignments(mock_sso: MagicMock) -> None:
        mock_sso.list_account_assignments.return_value = {"AccountAssignments": []}

    def test_returns_when_status_succeeded(self):
        mock_sso = MagicMock()
        self._no_existing_assignments(mock_sso)
        mock_sso.create_account_assignment.return_value = {
            "AccountAssignmentCreationStatus": {"RequestId": "req-1"}
        }
        mock_sso.describe_account_assignment_creation_status.return_value = {
            "AccountAssignmentCreationStatus": {"Status": "SUCCEEDED"}
        }
        with patch.object(handler, "sso_admin", mock_sso):
            handler.create_account_assignment(
                instance_arn="arn:i",
                target_account_id="123456789012",
                permission_set_arn="arn:ps",
                permission_set_name="Admin",
                principal_type="GROUP",
                principal_id="g-1",
                poll_timeout_seconds=5,
                poll_interval_seconds=0.01,
            )
        mock_sso.list_account_assignments.assert_called_once_with(
            InstanceArn="arn:i",
            PrincipalId="g-1",
            PrincipalType="GROUP",
            TargetId="123456789012",
            TargetType="AWS_ACCOUNT",
        )
        mock_sso.create_account_assignment.assert_called_once()

    def test_skips_create_when_assignment_already_listed(self):
        mock_sso = MagicMock()
        mock_sso.list_account_assignments.return_value = {
            "AccountAssignments": [
                {
                    "AccountId": "123456789012",
                    "PermissionSetArn": "arn:ps",
                    "PrincipalId": "g-1",
                    "PrincipalType": "GROUP",
                }
            ]
        }
        with patch.object(handler, "sso_admin", mock_sso):
            handler.create_account_assignment(
                instance_arn="arn:i",
                target_account_id="123456789012",
                permission_set_arn="arn:ps",
                permission_set_name="Admin",
                principal_type="GROUP",
                principal_id="g-1",
                poll_timeout_seconds=5,
                poll_interval_seconds=0.01,
            )
        mock_sso.list_account_assignments.assert_called_once_with(
            InstanceArn="arn:i",
            PrincipalId="g-1",
            PrincipalType="GROUP",
            TargetId="123456789012",
            TargetType="AWS_ACCOUNT",
        )
        mock_sso.create_account_assignment.assert_not_called()
        mock_sso.describe_account_assignment_creation_status.assert_not_called()

    def test_raises_on_failed_status(self):
        mock_sso = MagicMock()
        self._no_existing_assignments(mock_sso)
        mock_sso.create_account_assignment.return_value = {
            "AccountAssignmentCreationStatus": {"RequestId": "req-1"}
        }
        mock_sso.describe_account_assignment_creation_status.return_value = {
            "AccountAssignmentCreationStatus": {"Status": "FAILED", "FailureReason": "boom"}
        }
        with patch.object(handler, "sso_admin", mock_sso):
            with pytest.raises(handler.HandlerError, match="boom"):
                handler.create_account_assignment(
                    instance_arn="arn:i",
                    target_account_id="123456789012",
                    permission_set_arn="arn:ps",
                    permission_set_name="Admin",
                    principal_type="GROUP",
                    principal_id="g-1",
                    poll_timeout_seconds=5,
                    poll_interval_seconds=0.01,
                )

    def test_raises_on_timeout(self):
        mock_sso = MagicMock()
        self._no_existing_assignments(mock_sso)
        mock_sso.create_account_assignment.return_value = {
            "AccountAssignmentCreationStatus": {"RequestId": "req-1"}
        }
        mock_sso.describe_account_assignment_creation_status.return_value = {
            "AccountAssignmentCreationStatus": {"Status": "IN_PROGRESS"}
        }
        with patch.object(handler, "sso_admin", mock_sso):
            with patch("handler.time.time", side_effect=[0, 100]):
                with patch("handler.time.sleep"):
                    with pytest.raises(handler.HandlerError, match="Timed out"):
                        handler.create_account_assignment(
                            instance_arn="arn:i",
                            target_account_id="123456789012",
                            permission_set_arn="arn:ps",
                            permission_set_name="Admin",
                            principal_type="GROUP",
                            principal_id="g-1",
                            poll_timeout_seconds=1,
                            poll_interval_seconds=0.01,
                        )


class TestAssignPermissions:
    def test_empty_bindings(self, caplog):
        caplog.set_level(logging.WARNING)
        ok, bad = handler.assign_permissions([], "arn:i")
        assert ok == [] and bad == []

    def test_records_successes_and_failures(self):
        calls = {"n": 0}

        def fake_create(**kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise handler.HandlerError("first fails")
            return None

        bindings = [
            handler.Binding(
                account_id="123456789012",
                permission_set_name="PS",
                permission_set_arn="arn:ps",
                groups=[
                    handler.Group(name="G1", id="id-1"),
                    handler.Group(name="G2", id="id-2"),
                ],
            )
        ]
        with patch.object(handler, "create_account_assignment", side_effect=fake_create):
            successes, failures = handler.assign_permissions(bindings, "arn:i")
        assert len(successes) == 1
        assert successes[0]["group_name"] == "G2"
        assert len(failures) == 1
        assert "first fails" in failures[0]["error"]


class TestValidateEnvironment:
    def test_raises_when_missing(self):
        os.environ.pop("DYNAMODB_TABLE_NAME", None)
        os.environ.pop("SSO_INSTANCE_ARN", None)
        with pytest.raises(handler.HandlerError, match="DYNAMODB_TABLE_NAME"):
            handler.validate_environment()

    def test_ok_when_set(self):
        os.environ["DYNAMODB_TABLE_NAME"] = "t"
        os.environ["SSO_INSTANCE_ARN"] = "arn:aws:sso:::instance/x"
        handler.validate_environment()


class TestJSONFormatter:
    def test_format_emits_json_with_message(self):
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
        line = fmt.format(record)
        data = json.loads(line)
        assert data["message"] == "hello"
        assert data["level"] == "INFO"
        assert data["custom_field"] == "extra"


class TestLambdaHandler:
    def test_error_status_when_required_env_missing(self):
        os.environ.pop("DYNAMODB_TABLE_NAME", None)
        os.environ.pop("SSO_INSTANCE_ARN", None)
        out = handler.lambda_handler({"source": "account_creation"}, None)
        assert out["status"] == "error"
        assert "Missing required environment variable" in out["errors"]["message"]

    @patch.object(handler, "assign_permissions", return_value=([], []))
    @patch.object(handler, "get_permission_sets", return_value={"Admin": "arn:ps"})
    @patch.object(handler, "get_identity_store_groups", return_value={"MyGroup": "g-1"})
    @patch.object(handler, "load_configuration")
    @patch.object(handler, "get_account_tags", return_value={"sso/tpl": "MyGroup"})
    @patch.object(handler, "ensure_account_exists")
    @patch.object(handler, "list_active_accounts")
    @patch.object(handler, "get_identity_store_id", return_value="d-store")
    def test_single_account_success_path(
        self,
        mock_get_identity_store_id,
        mock_list_active,
        mock_ensure,
        mock_get_account_tags,
        mock_load_cfg,
        mock_get_groups,
        mock_get_ps,
        mock_assign,
    ):
        os.environ["DYNAMODB_TABLE_NAME"] = "tbl"
        os.environ["SSO_INSTANCE_ARN"] = "arn:aws:sso:::instance/x"
        os.environ["SSO_ACCOUNT_TAG_PREFIX"] = "sso"

        cfg = handler.Configuration(
            groups={
                "sso/tpl": handler.GroupConfiguration(permission_sets=["Admin"], enabled=True),
            }
        )
        mock_load_cfg.return_value = cfg

        event = {"source": "account_creation", "account_id": "123456789012"}
        out = handler.lambda_handler(event, None)

        assert out["status"] == "success"
        assert out["account_id"] == "123456789012"
        mock_ensure.assert_called_once_with("123456789012")
        mock_list_active.assert_not_called()
        mock_assign.assert_called_once()
        args, kwargs = mock_assign.call_args
        assert kwargs["instance_arn"] == "arn:aws:sso:::instance/x"
        bound = kwargs["bindings"]
        assert len(bound) == 1
        assert bound[0].account_id == "123456789012"
        assert bound[0].permission_set_name == "Admin"
        assert bound[0].groups[0].name == "MyGroup"

    @patch.object(handler, "assign_permissions", return_value=([], []))
    @patch.object(handler, "get_permission_sets", return_value={"Admin": "arn:ps"})
    @patch.object(handler, "get_identity_store_groups", return_value={"MyGroup": "g-1"})
    @patch.object(handler, "load_configuration")
    @patch.object(handler, "get_account_tags", return_value={"sso/tpl": "MyGroup"})
    @patch.object(handler, "ensure_account_exists")
    @patch.object(handler, "list_active_accounts", return_value=["123456789012"])
    @patch.object(handler, "get_identity_store_id", return_value="d-store")
    def test_cron_uses_list_active_accounts(
        self,
        mock_get_identity_store_id,
        mock_list_active,
        mock_ensure,
        mock_get_account_tags,
        mock_load_cfg,
        mock_get_groups,
        mock_get_ps,
        mock_assign,
    ):
        os.environ["DYNAMODB_TABLE_NAME"] = "tbl"
        os.environ["SSO_INSTANCE_ARN"] = "arn:aws:sso:::instance/x"
        os.environ["SSO_ACCOUNT_TAG_PREFIX"] = "sso"

        mock_load_cfg.return_value = handler.Configuration(
            groups={"sso/tpl": handler.GroupConfiguration(permission_sets=["Admin"])}
        )

        out = handler.lambda_handler({"source": "cron_schedule"}, None)
        assert out["status"] == "success"
        mock_list_active.assert_called_once()
        mock_ensure.assert_not_called()

    @patch.object(handler, "assign_permissions", return_value=([], []))
    @patch.object(handler, "get_permission_sets", return_value={"Admin": "arn:ps"})
    @patch.object(handler, "get_identity_store_groups", return_value={"MyGroup": "g-1"})
    @patch.object(handler, "load_configuration")
    @patch.object(handler, "get_account_tags", return_value={"sso/tpl": "MyGroup"})
    @patch.object(handler, "ensure_account_exists")
    @patch.object(handler, "list_active_accounts")
    @patch.object(handler, "get_identity_store_id", return_value="d-store")
    def test_default_tag_prefix_sso_when_env_unset(
        self,
        mock_get_identity_store_id,
        mock_list_active,
        mock_ensure,
        mock_get_account_tags,
        mock_load_cfg,
        mock_get_groups,
        mock_get_ps,
        mock_assign,
    ):
        os.environ["DYNAMODB_TABLE_NAME"] = "tbl"
        os.environ["SSO_INSTANCE_ARN"] = "arn:aws:sso:::instance/x"
        os.environ.pop("SSO_ACCOUNT_TAG_PREFIX", None)

        mock_load_cfg.return_value = handler.Configuration(
            groups={"sso/tpl": handler.GroupConfiguration(permission_sets=["Admin"])}
        )

        out = handler.lambda_handler(
            {"source": "account_creation", "account_id": "123456789012"},
            None,
        )
        assert out["status"] == "success"
        mock_get_account_tags.assert_called()
        mock_assign.assert_called_once()

    @patch.object(handler, "assign_permissions", return_value=([], []))
    @patch.object(handler, "get_permission_sets", return_value={"Admin": "arn:ps"})
    @patch.object(handler, "get_identity_store_groups", return_value={"MyGroup": "g-1"})
    @patch.object(handler, "load_configuration")
    @patch.object(handler, "get_account_tags", return_value={"sso/unknown": "MyGroup"})
    @patch.object(handler, "ensure_account_exists")
    @patch.object(handler, "list_active_accounts")
    @patch.object(handler, "get_identity_store_id", return_value="d-store")
    def test_unknown_template_records_failure_and_failed_status(
        self,
        mock_get_identity_store_id,
        mock_list_active,
        mock_ensure,
        mock_get_account_tags,
        mock_load_cfg,
        mock_get_groups,
        mock_get_ps,
        mock_assign,
    ):
        os.environ["DYNAMODB_TABLE_NAME"] = "tbl"
        os.environ["SSO_INSTANCE_ARN"] = "arn:aws:sso:::instance/x"
        os.environ["SSO_ACCOUNT_TAG_PREFIX"] = "sso"

        mock_load_cfg.return_value = handler.Configuration(groups={})

        out = handler.lambda_handler(
            {"source": "account_creation", "account_id": "123456789012"},
            None,
        )
        assert out["status"] == "failed"
        assert out["errors"]["count"] == 1
        assert out["results"]["failed"][0]["error"] == "Permission template not found in configuration"
        mock_assign.assert_called_once()
        assert mock_assign.call_args.kwargs["bindings"] == []

    @patch.object(handler, "assign_permissions", return_value=([], []))
    @patch.object(handler, "get_permission_sets", return_value={"Admin": "arn:ps"})
    @patch.object(handler, "get_identity_store_groups", return_value={"MyGroup": "g-1"})
    @patch.object(handler, "load_configuration")
    @patch.object(handler, "get_account_tags", return_value={"sso/tpl": "MyGroup"})
    @patch.object(handler, "ensure_account_exists")
    @patch.object(handler, "list_active_accounts")
    @patch.object(handler, "get_identity_store_id", return_value="d-store")
    def test_disabled_template_records_failure(
        self,
        mock_get_identity_store_id,
        mock_list_active,
        mock_ensure,
        mock_get_account_tags,
        mock_load_cfg,
        mock_get_groups,
        mock_get_ps,
        mock_assign,
    ):
        os.environ["DYNAMODB_TABLE_NAME"] = "tbl"
        os.environ["SSO_INSTANCE_ARN"] = "arn:aws:sso:::instance/x"
        os.environ["SSO_ACCOUNT_TAG_PREFIX"] = "sso"

        mock_load_cfg.return_value = handler.Configuration(
            groups={"sso/tpl": handler.GroupConfiguration(permission_sets=["Admin"], enabled=False)}
        )

        out = handler.lambda_handler(
            {"source": "account_creation", "account_id": "123456789012"},
            None,
        )
        assert out["status"] == "failed"
        assert "not enabled" in out["results"]["failed"][0]["error"]

    @patch.object(handler, "assign_permissions", return_value=([], []))
    @patch.object(handler, "get_permission_sets", return_value={"Admin": "arn:ps"})
    @patch.object(handler, "get_identity_store_groups", return_value={"MyGroup": "g-1"})
    @patch.object(handler, "load_configuration")
    @patch.object(handler, "get_account_tags", return_value={"sso/tpl": "MyGroup"})
    @patch.object(handler, "ensure_account_exists")
    @patch.object(handler, "list_active_accounts", return_value=["111111111111", "222222222222"])
    @patch.object(handler, "get_identity_store_id", return_value="d-store")
    def test_cron_accumulates_bindings_across_accounts(
        self,
        mock_get_identity_store_id,
        mock_list_active,
        mock_ensure,
        mock_get_account_tags,
        mock_load_cfg,
        mock_get_groups,
        mock_get_ps,
        mock_assign,
    ):
        os.environ["DYNAMODB_TABLE_NAME"] = "tbl"
        os.environ["SSO_INSTANCE_ARN"] = "arn:aws:sso:::instance/x"
        os.environ["SSO_ACCOUNT_TAG_PREFIX"] = "sso"

        mock_load_cfg.return_value = handler.Configuration(
            groups={"sso/tpl": handler.GroupConfiguration(permission_sets=["Admin"])}
        )

        handler.lambda_handler({"source": "cron_schedule"}, None)

        # assign_permissions runs once per target account (inside the account loop).
        assert mock_assign.call_count == 2
        accts = [
            mock_assign.call_args_list[i].kwargs["bindings"][0].account_id
            for i in range(2)
        ]
        assert set(accts) == {"111111111111", "222222222222"}

    @patch.object(
        handler,
        "assign_permissions",
        return_value=(
            [],
            [{"account_id": "123456789012", "error": "assignment boom"}],
        ),
    )
    @patch.object(handler, "get_permission_sets", return_value={"Admin": "arn:ps"})
    @patch.object(handler, "get_identity_store_groups", return_value={"MyGroup": "g-1"})
    @patch.object(handler, "load_configuration")
    @patch.object(handler, "get_account_tags", return_value={"sso/tpl": "MyGroup"})
    @patch.object(handler, "ensure_account_exists")
    @patch.object(handler, "list_active_accounts")
    @patch.object(handler, "get_identity_store_id", return_value="d-store")
    def test_failed_status_when_assign_permissions_reports_failures(
        self,
        mock_get_identity_store_id,
        mock_list_active,
        mock_ensure,
        mock_get_account_tags,
        mock_load_cfg,
        mock_get_groups,
        mock_get_ps,
        mock_assign,
    ):
        os.environ["DYNAMODB_TABLE_NAME"] = "tbl"
        os.environ["SSO_INSTANCE_ARN"] = "arn:aws:sso:::instance/x"
        os.environ["SSO_ACCOUNT_TAG_PREFIX"] = "sso"

        mock_load_cfg.return_value = handler.Configuration(
            groups={"sso/tpl": handler.GroupConfiguration(permission_sets=["Admin"])}
        )

        out = handler.lambda_handler(
            {"source": "account_creation", "account_id": "123456789012"},
            None,
        )
        assert out["status"] == "failed"
        assert "assignment boom" in out["results"]["failed"][-1]["error"]

    @patch.object(handler, "get_identity_store_id", side_effect=RuntimeError("boom"))
    def test_unhandled_exception_returns_error_payload(self, _mock):
        os.environ["DYNAMODB_TABLE_NAME"] = "tbl"
        os.environ["SSO_INSTANCE_ARN"] = "arn:aws:sso:::instance/x"
        out = handler.lambda_handler({"source": "x", "account_id": "1"}, None)
        assert out["status"] == "error"
        assert out["errors"]["message"] == "boom"
        assert isinstance(out["time_taken"], float)
        assert out["time_taken"] >= 0
