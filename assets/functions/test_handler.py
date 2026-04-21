"""
Unit tests for handler.py — AWS SSO assignment Lambda logic.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from unittest.mock import MagicMock, patch

import pytest  # pylint: disable=import-error
from botocore.exceptions import ClientError

# Prevent boto3/botocore from attempting to resolve credentials via local
# credential_process helpers during import (e.g. `granted`), which is not
# available/allowed in some test environments.
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
os.environ.setdefault("AWS_SESSION_TOKEN", "test")
os.environ.setdefault("AWS_DEFAULT_REGION", "eu-west-1")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")

# Import the module under test from the same directory (no package layout).
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import handler  # noqa: E402 pylint: disable=wrong-import-position


@pytest.fixture(autouse=True)
def restore_env():
    """Snapshot env vars touched by tests and restore after each test."""
    keys = (
        "DYNAMODB_TRACKING_TABLE",
        "DYNAMODB_CONFIG_TABLE",
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
            groups={"tpl": handler.GroupConfiguration(permission_sets=["Admin"])}
        )
        assert "tpl" in cfg.groups
        assert cfg.groups["tpl"].permission_sets == ["Admin"]


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
            groups={
                "x": handler.GroupConfiguration(permission_sets=["P"], description="d")
            }
        )
        data = json.loads(cfg.to_json())
        assert data["groups"]["x"]["permission_sets"] == ["P"]
        assert data["groups"]["x"]["description"] == "d"


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
                    {
                        "InstanceArn": "arn:aws:sso:::instance/other",
                        "IdentityStoreId": "d-WRONG",
                    },
                    {
                        "InstanceArn": "arn:aws:sso:::instance/want",
                        "IdentityStoreId": "d-12345",
                    },
                ]
            }
        ]
        with patch.object(handler, "sso_admin", mock_sso):
            assert (
                handler.get_identity_store_id("arn:aws:sso:::instance/want")
                == "d-12345"
            )

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
            {
                "Instances": [
                    {
                        "InstanceArn": "arn:aws:sso:::instance/a",
                        "IdentityStoreId": "d-A",
                    }
                ]
            },
            {
                "Instances": [
                    {
                        "InstanceArn": "arn:aws:sso:::instance/want",
                        "IdentityStoreId": "d-WANT",
                    },
                ]
            },
        ]
        with patch.object(handler, "sso_admin", mock_sso):
            assert (
                handler.get_identity_store_id("arn:aws:sso:::instance/want") == "d-WANT"
            )


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
            "Tags": [
                {"Key": "sso/default", "Value": "g1,g2"},
                {"Key": "other", "Value": "x"},
            ]
        }
        with patch.object(handler, "organizations", mock_org):
            tags = handler.get_account_tags("123456789012")
        assert tags == {"sso/default": "g1,g2", "other": "x"}
        mock_org.list_tags_for_resource.assert_called_once_with(
            ResourceId="123456789012"
        )

    def test_raises_handler_error_on_client_error(self):
        mock_org = MagicMock()
        mock_org.list_tags_for_resource.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "nope"}},
            "ListTagsForResource",
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
            {"Error": {"Code": "AccountNotFound", "Message": "missing"}},
            "DescribeAccount",
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
        assert by_name["default"] == ["Alpha", "Beta"]
        assert by_name["other"] == ["Gamma"]
        assert len(perms) == 2

    def test_empty_when_no_matching_prefix(self):
        assert not handler.get_account_permission_tags({"a": "b"}, "sso")


class TestLoadConfiguration:
    def test_scan_pagination_and_fields(self):
        mock_table = MagicMock()
        mock_table.scan.side_effect = [
            {
                "Items": [
                    {
                        "group_name": "a",
                        "permission_sets": ["P1"],
                        "enabled": False,
                        "description": "d1",
                    }
                ],
                "LastEvaluatedKey": {"group_name": "a"},
            },
            {
                "Items": [
                    {"group_name": "b"},
                ],
            },
        ]
        mock_resource = MagicMock()
        mock_resource.Table.return_value = mock_table
        with patch.object(handler, "dynamodb", mock_resource):
            cfg = handler.load_configuration("my-table")
        mock_resource.Table.assert_called_once_with("my-table")
        assert set(cfg.groups) == {"a", "b"}
        assert cfg.groups["a"].permission_sets == ["P1"]
        assert cfg.groups["a"].enabled is False
        assert cfg.groups["a"].description == "d1"
        assert cfg.groups["b"].permission_sets == []
        assert cfg.groups["b"].enabled is True


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
        assert not successes and not failures
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
        assert not successes
        assert len(failures) == 1
        assert failures[0]["group"] == "Missing"
        assert len(bindings) == 1
        assert len(bindings[0].groups) == 1
        assert bindings[0].groups[0].name == "G2"

    def test_unknown_permission_set_appends_failure(self):
        req = handler.Permission(name="sso/tpl", groups=["G1"])
        template = handler.GroupConfiguration(permission_sets=["Nope"])
        bindings, _successes, failures = handler.get_bindings(
            account_id="123456789012",
            identity_store_groups={"G1": "id-1"},
            permission_sets={"Admin": "arn:a"},
            request=req,
            template=template,
        )
        assert not bindings
        assert len(failures) == 1
        assert failures[0]["permission"] == "Nope"


class TestGetPermissionSetsEdgeCases:
    def test_skips_permission_sets_without_a_name(self):
        mock_sso = MagicMock()
        paginator = MagicMock()
        mock_sso.get_paginator.return_value = paginator
        paginator.paginate.return_value = [{"PermissionSets": ["arn:ps:1"]}]
        mock_sso.describe_permission_set.return_value = {
            "PermissionSet": {"Name": None}
        }
        with patch.object(handler, "sso_admin", mock_sso):
            result = handler.get_permission_sets("arn:aws:sso:::instance/x")
        assert not result


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
            AccountId="123456789012",
            PermissionSetArn="arn:ps",
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
            AccountId="123456789012",
            PermissionSetArn="arn:ps",
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
            "AccountAssignmentCreationStatus": {
                "Status": "FAILED",
                "FailureReason": "boom",
            }
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
        assert not ok and not bad

    def test_records_successes_and_failures(self):
        calls = {"n": 0}

        def fake_create(**_kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise handler.HandlerError("first fails")

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
        with patch.object(
            handler, "create_account_assignment", side_effect=fake_create
        ):
            successes, failures = handler.assign_permissions(bindings, "arn:i")
        assert len(successes) == 1
        assert successes[0]["group_name"] == "G2"
        assert len(failures) == 1
        assert "first fails" in failures[0]["error"]


class TestValidateEnvironment:
    def test_raises_when_missing(self):
        os.environ.pop("DYNAMODB_CONFIG_TABLE", None)
        os.environ.pop("SSO_INSTANCE_ARN", None)
        os.environ.pop("DYNAMODB_TRACKING_TABLE", None)
        with pytest.raises(handler.HandlerError, match="DYNAMODB_CONFIG_TABLE"):
            handler.validate_environment()

    def test_ok_when_set(self):
        os.environ["DYNAMODB_CONFIG_TABLE"] = "t"
        os.environ["DYNAMODB_TRACKING_TABLE"] = "tracking"
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
        os.environ.pop("DYNAMODB_CONFIG_TABLE", None)
        os.environ.pop("DYNAMODB_TRACKING_TABLE", None)
        os.environ.pop("SSO_INSTANCE_ARN", None)
        out = handler.lambda_handler({"source": "account_creation"}, None)
        assert out["status"] == "error"
        assert "Missing required environment variable" in out["errors"]["message"]

    @patch.object(handler, "reconcile_assignments", return_value=([], []))
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
        _mock_get_identity_store_id,
        mock_list_active,
        mock_ensure,
        _mock_get_account_tags,
        mock_load_cfg,
        _mock_get_groups,
        _mock_get_ps,
        mock_assign,
        _mock_reconcile,
    ):
        os.environ["DYNAMODB_CONFIG_TABLE"] = "cfg"
        os.environ["DYNAMODB_TRACKING_TABLE"] = "tracking"
        os.environ["SSO_INSTANCE_ARN"] = "arn:aws:sso:::instance/x"
        os.environ["SSO_ACCOUNT_TAG_PREFIX"] = "sso"

        cfg = handler.Configuration(
            groups={
                "tpl": handler.GroupConfiguration(
                    permission_sets=["Admin"], enabled=True
                ),
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
        _args, kwargs = mock_assign.call_args
        assert kwargs["instance_arn"] == "arn:aws:sso:::instance/x"
        bound = kwargs["bindings"]
        assert len(bound) == 1
        assert bound[0].account_id == "123456789012"
        assert bound[0].permission_set_name == "Admin"
        assert bound[0].groups[0].name == "MyGroup"

    @patch.object(handler, "reconcile_assignments", return_value=([], []))
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
        _mock_get_identity_store_id,
        mock_list_active,
        mock_ensure,
        _mock_get_account_tags,
        mock_load_cfg,
        _mock_get_groups,
        _mock_get_ps,
        _mock_assign,
        _mock_reconcile,
    ):
        os.environ["DYNAMODB_CONFIG_TABLE"] = "cfg"
        os.environ["DYNAMODB_TRACKING_TABLE"] = "tracking"
        os.environ["SSO_INSTANCE_ARN"] = "arn:aws:sso:::instance/x"
        os.environ["SSO_ACCOUNT_TAG_PREFIX"] = "sso"

        mock_load_cfg.return_value = handler.Configuration(
            groups={"tpl": handler.GroupConfiguration(permission_sets=["Admin"])}
        )

        out = handler.lambda_handler({"source": "cron_schedule"}, None)
        assert out["status"] == "success"
        mock_list_active.assert_called_once()
        mock_ensure.assert_not_called()

    @patch.object(handler, "reconcile_assignments", return_value=([], []))
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
        _mock_get_identity_store_id,
        _mock_list_active,
        _mock_ensure,
        mock_get_account_tags,
        mock_load_cfg,
        _mock_get_groups,
        _mock_get_ps,
        mock_assign,
        _mock_reconcile,
    ):
        os.environ["DYNAMODB_CONFIG_TABLE"] = "cfg"
        os.environ["DYNAMODB_TRACKING_TABLE"] = "tracking"
        os.environ["SSO_INSTANCE_ARN"] = "arn:aws:sso:::instance/x"
        os.environ.pop("SSO_ACCOUNT_TAG_PREFIX", None)

        mock_load_cfg.return_value = handler.Configuration(
            groups={"tpl": handler.GroupConfiguration(permission_sets=["Admin"])}
        )

        out = handler.lambda_handler(
            {"source": "account_creation", "account_id": "123456789012"},
            None,
        )
        assert out["status"] == "success"
        mock_get_account_tags.assert_called()
        mock_assign.assert_called_once()

    @patch.object(handler, "reconcile_assignments", return_value=([], []))
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
        _mock_get_identity_store_id,
        _mock_list_active,
        _mock_ensure,
        _mock_get_account_tags,
        mock_load_cfg,
        _mock_get_groups,
        _mock_get_ps,
        mock_assign,
        _mock_reconcile,
    ):
        os.environ["DYNAMODB_CONFIG_TABLE"] = "cfg"
        os.environ["DYNAMODB_TRACKING_TABLE"] = "tracking"
        os.environ["SSO_INSTANCE_ARN"] = "arn:aws:sso:::instance/x"
        os.environ["SSO_ACCOUNT_TAG_PREFIX"] = "sso"

        mock_load_cfg.return_value = handler.Configuration(groups={})

        out = handler.lambda_handler(
            {"source": "account_creation", "account_id": "123456789012"},
            None,
        )
        assert out["status"] == "failed"
        assert out["errors"]["count"] == 1
        assert (
            out["results"]["failed"][0]["error"]
            == "Permission template not found in configuration"
        )
        # No bindings are produced for an unknown template, so we should not
        # attempt to call assign_permissions at all.
        mock_assign.assert_not_called()

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
        _mock_get_identity_store_id,
        _mock_list_active,
        _mock_ensure,
        _mock_get_account_tags,
        mock_load_cfg,
        _mock_get_groups,
        _mock_get_ps,
        _mock_assign,
    ):
        os.environ["DYNAMODB_CONFIG_TABLE"] = "cfg"
        os.environ["DYNAMODB_TRACKING_TABLE"] = "tracking"
        os.environ["SSO_INSTANCE_ARN"] = "arn:aws:sso:::instance/x"
        os.environ["SSO_ACCOUNT_TAG_PREFIX"] = "sso"

        mock_load_cfg.return_value = handler.Configuration(
            groups={
                "tpl": handler.GroupConfiguration(
                    permission_sets=["Admin"], enabled=False
                )
            }
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
    @patch.object(handler, "reconcile_assignments", return_value=([], []))
    @patch.object(handler, "ensure_account_exists")
    @patch.object(
        handler, "list_active_accounts", return_value=["111111111111", "222222222222"]
    )
    @patch.object(handler, "get_identity_store_id", return_value="d-store")
    def test_cron_accumulates_bindings_across_accounts(
        self,
        _mock_get_identity_store_id,
        _mock_list_active,
        _mock_ensure,
        mock_reconcile,
        _mock_get_account_tags,
        mock_load_cfg,
        _mock_get_groups,
        _mock_get_ps,
        mock_assign,
    ):
        os.environ["DYNAMODB_CONFIG_TABLE"] = "tbl"
        os.environ["DYNAMODB_TRACKING_TABLE"] = "tracking"
        os.environ["SSO_INSTANCE_ARN"] = "arn:aws:sso:::instance/x"
        os.environ["SSO_ACCOUNT_TAG_PREFIX"] = "sso"

        mock_load_cfg.return_value = handler.Configuration(
            groups={"tpl": handler.GroupConfiguration(permission_sets=["Admin"])}
        )

        handler.lambda_handler({"source": "cron_schedule"}, None)

        # assign_permissions runs once with all bindings across accounts.
        mock_assign.assert_called_once()
        bindings = mock_assign.call_args.kwargs["bindings"]
        assert len(bindings) == 2
        assert {b.account_id for b in bindings} == {"111111111111", "222222222222"}

        # Reconciliation runs once with the full desired bindings.
        mock_reconcile.assert_called_once()
        assert (
            mock_reconcile.call_args.kwargs["desired_bindings"] == bindings
        ), "Reconcile should receive all desired bindings"

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
        _mock_get_identity_store_id,
        _mock_list_active,
        _mock_ensure,
        _mock_get_account_tags,
        mock_load_cfg,
        _mock_get_groups,
        _mock_get_ps,
        _mock_assign,
    ):
        os.environ["DYNAMODB_CONFIG_TABLE"] = "tbl"
        os.environ["DYNAMODB_TRACKING_TABLE"] = "tracking"
        os.environ["SSO_INSTANCE_ARN"] = "arn:aws:sso:::instance/x"
        os.environ["SSO_ACCOUNT_TAG_PREFIX"] = "sso"

        mock_load_cfg.return_value = handler.Configuration(
            groups={"tpl": handler.GroupConfiguration(permission_sets=["Admin"])}
        )

        out = handler.lambda_handler(
            {"source": "account_creation", "account_id": "123456789012"},
            None,
        )
        assert out["status"] == "failed"
        assert "assignment boom" in out["results"]["failed"][-1]["error"]

    @patch.object(handler, "get_identity_store_id", side_effect=RuntimeError("boom"))
    def test_unhandled_exception_returns_error_payload(self, _mock):
        os.environ["DYNAMODB_CONFIG_TABLE"] = "cfg"
        os.environ["DYNAMODB_TRACKING_TABLE"] = "tracking"
        os.environ["SSO_INSTANCE_ARN"] = "arn:aws:sso:::instance/x"
        out = handler.lambda_handler({"source": "x", "account_id": "1"}, None)
        assert out["status"] == "error"
        assert out["errors"]["message"] == "boom"
        assert isinstance(out["time_taken"], float)
        assert out["time_taken"] >= 0


class TestRecordAssignment:
    """Test assignment tracking record functionality."""

    def test_records_assignment_to_dynamodb(self):
        """Test that a successful assignment is recorded to the tracking table."""
        mock_dynamodb = MagicMock()
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table

        with patch.object(handler, "dynamodb", mock_dynamodb):
            handler.record_tracking_assignment(
                tracking_table_name="tracking_tbl",
                assignment_id="123456789012#g-1#arn:ps",
                account_id="123456789012",
                permission_set_arn="arn:ps",
                permission_set_name="Admin",
                principal_id="g-1",
                principal_type="GROUP",
                template_name="default",
                group_name="test-group",
            )

        mock_dynamodb.Table.assert_called_once_with("tracking_tbl")
        # Verify put_item was called
        mock_table.put_item.assert_called_once()
        item = mock_table.put_item.call_args[1]["Item"]
        assert item["assignment_id"] == "123456789012#g-1#arn:ps"
        assert item["account_id"] == "123456789012"
        assert item["permission_set_arn"] == "arn:ps"
        assert item["permission_set_name"] == "Admin"
        assert item["principal_id"] == "g-1"
        assert item["principal_type"] == "GROUP"
        assert item["template_name"] == "default"
        assert item["group_name"] == "test-group"

    def test_raises_on_dynamodb_error(self):
        """Test that a ClientError is properly handled and re-raised."""
        mock_dynamodb = MagicMock()
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table
        mock_table.put_item.side_effect = ClientError(
            {"Error": {"Code": "ValidationException", "Message": "boom"}}, "PutItem"
        )

        with patch.object(handler, "dynamodb", mock_dynamodb):
            with pytest.raises(
                handler.HandlerError, match="Could not record assignment"
            ):
                handler.record_tracking_assignment(
                    tracking_table_name="tracking_tbl",
                    assignment_id="123456789012#g-1#arn:ps",
                    account_id="123456789012",
                    permission_set_arn="arn:ps",
                    permission_set_name="Admin",
                    principal_id="g-1",
                    principal_type="GROUP",
                    template_name="default",
                    group_name="test-group",
                )


class TestGetTrackingAssignments:
    """Test retrieval of tracked assignments."""

    def test_retrieves_assignments(self):
        """Test that assignments are retrieved via scan."""
        mock_dynamodb = MagicMock()
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table

        mock_table.scan.return_value = {
            "Items": [
                {
                    "assignment_id": "123456789012#g-1#arn:ps1",
                    "account_id": "123456789012",
                    "permission_set_arn": "arn:ps1",
                    "permission_set_name": "Admin",
                    "principal_id": "g-1",
                    "principal_type": "GROUP",
                    "template_name": "default",
                    "group_name": "test-group",
                    "created_at": "2026-01-01T00:00:00+00:00",
                    "last_seen": "2026-01-01T00:00:00+00:00",
                }
            ],
        }

        with patch.object(handler, "dynamodb", mock_dynamodb):
            result = handler.get_tracking_assignments("tracking")

        assert len(result) == 1
        assert result[0].assignment_id == "123456789012#g-1#arn:ps1"
        assert result[0].account_id == "123456789012"

        mock_table.scan.assert_called_once()

    def test_raises_on_dynamodb_error(self):
        """Test that a ClientError is properly handled and re-raised."""
        mock_dynamodb = MagicMock()
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table
        mock_table.scan.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "boom"}}, "Scan"
        )

        with patch.object(handler, "dynamodb", mock_dynamodb):
            with pytest.raises(handler.HandlerError, match="Could not get assignments"):
                handler.get_tracking_assignments("tracking")


class TestDeletePermission:
    """Test assignment deletion functionality."""

    def test_successfully_deletes_assignment(self):
        """Test that an assignment is successfully deleted."""
        mock_sso = MagicMock()
        mock_sso.delete_account_assignment.return_value = {
            "AccountAssignmentDeletionStatus": {"RequestId": "req-1"}
        }
        mock_sso.describe_account_assignment_deletion_status.return_value = {
            "AccountAssignmentDeletionStatus": {"Status": "SUCCEEDED"}
        }

        with patch.object(handler, "sso_admin", mock_sso):
            handler.delete_permission(
                instance_arn="arn:i",
                account_id="123456789012",
                permission_set_arn="arn:ps",
                principal_id="g-1",
                principal_type="GROUP",
                poll_timeout_seconds=5,
                poll_interval_seconds=0.01,
            )

        mock_sso.delete_account_assignment.assert_called_once_with(
            InstanceArn="arn:i",
            PermissionSetArn="arn:ps",
            PrincipalId="g-1",
            PrincipalType="GROUP",
            TargetId="123456789012",
            TargetType="AWS_ACCOUNT",
        )
        mock_sso.describe_account_assignment_deletion_status.assert_called_once()

    def test_raises_on_failed_deletion(self):
        """Test that a failed deletion raises an error."""
        mock_sso = MagicMock()
        mock_sso.delete_account_assignment.return_value = {
            "AccountAssignmentDeletionStatus": {"RequestId": "req-1"}
        }
        mock_sso.describe_account_assignment_deletion_status.return_value = {
            "AccountAssignmentDeletionStatus": {
                "Status": "FAILED",
                "FailureReason": "boom",
            }
        }

        with patch.object(handler, "sso_admin", mock_sso):
            with pytest.raises(handler.HandlerError, match="boom"):
                handler.delete_permission(
                    instance_arn="arn:i",
                    account_id="123456789012",
                    permission_set_arn="arn:ps",
                    principal_id="g-1",
                    principal_type="GROUP",
                    poll_timeout_seconds=5,
                    poll_interval_seconds=0.01,
                )

    def test_raises_on_timeout(self):
        """Test that a timeout raises an error."""
        mock_sso = MagicMock()
        mock_sso.delete_account_assignment.return_value = {
            "AccountAssignmentDeletionStatus": {"RequestId": "req-1"}
        }
        mock_sso.describe_account_assignment_deletion_status.return_value = {
            "AccountAssignmentDeletionStatus": {"Status": "IN_PROGRESS"}
        }

        with patch.object(handler, "sso_admin", mock_sso):
            with patch("handler.time.time", side_effect=[0, 100]):
                with patch("handler.time.sleep"):
                    with pytest.raises(handler.HandlerError, match="Timed out"):
                        handler.delete_permission(
                            instance_arn="arn:i",
                            account_id="123456789012",
                            permission_set_arn="arn:ps",
                            principal_id="g-1",
                            principal_type="GROUP",
                            poll_timeout_seconds=5,
                            poll_interval_seconds=0.01,
                        )


class TestHasMatchingBinding:
    def test_returns_true_when_binding_contains_matching_group_among_multiple(self):
        assignment = handler.TrackedAssignment(
            assignment_id="123456789012#g-2#arn:ps",
            account_id="123456789012",
            permission_set_arn="arn:ps",
            permission_set_name="Admin",
            principal_id="g-2",
            principal_type="GROUP",
            template_name="t",
            group_name="TeamB",
        )
        binding = handler.Binding(
            account_id="123456789012",
            permission_set_name="Admin",
            permission_set_arn="arn:ps",
            groups=[
                handler.Group(name="TeamA", id="g-1"),
                handler.Group(name="TeamB", id="g-2"),
                handler.Group(name="TeamC", id="g-3"),
            ],
            template_name="t",
        )

        assert (
            handler.has_matching_binding(assignment=assignment, bindings=[binding])
            is True
        )


class TestReconcileAssignments:
    """Test assignment reconciliation functionality."""

    def test_deletes_provisioned_assignment_not_in_desired_bindings(self):
        mock_sso = MagicMock()
        mock_dynamodb = MagicMock()
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table

        # Create a tracked assignment that is NOT in desired bindings
        tracked_assignment = handler.TrackedAssignment(
            assignment_id="123456789012#g-extra#arn:ps-extra",
            account_id="123456789012",
            permission_set_arn="arn:ps-extra",
            permission_set_name="Extra",
            principal_id="g-extra",
            principal_type="GROUP",
            template_name="t",
            group_name="extra",
            created_at="2025-01-01T00:00:00Z",
            last_seen="2025-01-01T00:00:00Z",
        )

        # Deletion succeeds
        mock_sso.delete_account_assignment.return_value = {
            "AccountAssignmentDeletionStatus": {"RequestId": "req-1"}
        }
        mock_sso.describe_account_assignment_deletion_status.return_value = {
            "AccountAssignmentDeletionStatus": {"Status": "SUCCEEDED"}
        }

        desired = [
            handler.Binding(
                account_id="123456789012",
                permission_set_name="Admin",
                permission_set_arn="arn:ps-desired",
                groups=[handler.Group(name="desired", id="g-desired")],
                template_name="t",
            )
        ]

        with patch.object(handler, "sso_admin", mock_sso):
            with patch.object(handler, "dynamodb", mock_dynamodb):
                with patch.object(
                    handler,
                    "get_tracking_assignments",
                    return_value=[tracked_assignment],
                ):
                    deleted, failures = handler.reconcile_assignments(
                        instance_arn="arn:i",
                        desired_bindings=desired,
                        tracking_table_name="tracking",
                        accounts_to_reconcile=["123456789012"],
                    )

        assert not failures
        assert len(deleted) == 1
        assert deleted[0]["assignment_id"] == "123456789012#g-extra#arn:ps-extra"

    def test_does_not_delete_when_assignment_is_desired(self):
        mock_sso = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "AccountAssignments": [
                    {
                        "AccountId": "123456789012",
                        "PermissionSetArn": "arn:ps-desired",
                        "PrincipalId": "g-desired",
                        "PrincipalType": "GROUP",
                    }
                ]
            }
        ]
        mock_sso.get_paginator.return_value = paginator

        desired = [
            handler.Binding(
                account_id="123456789012",
                permission_set_name="Admin",
                permission_set_arn="arn:ps-desired",
                groups=[handler.Group(name="desired", id="g-desired")],
                template_name="t",
            )
        ]

        with patch.object(handler, "sso_admin", mock_sso):
            with patch.object(handler, "get_tracking_assignments", return_value=[]):
                deleted, failures = handler.reconcile_assignments(
                    instance_arn="arn:i",
                    desired_bindings=desired,
                    tracking_table_name=None,
                    accounts_to_reconcile=["123456789012"],
                )

        assert not deleted
        assert not failures
        mock_sso.delete_account_assignment.assert_not_called()

    def test_skips_non_group_assignments(self):
        mock_sso = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "AccountAssignments": [
                    {
                        "AccountId": "123456789012",
                        "PermissionSetArn": "arn:ps-user",
                        "PrincipalId": "u-1",
                        "PrincipalType": "USER",
                    }
                ]
            }
        ]
        mock_sso.get_paginator.return_value = paginator

        with patch.object(handler, "sso_admin", mock_sso):
            with patch.object(handler, "get_tracking_assignments", return_value=[]):
                deleted, failures = handler.reconcile_assignments(
                    instance_arn="arn:i",
                    desired_bindings=[],
                    tracking_table_name=None,
                    accounts_to_reconcile=["123456789012"],
                )

        assert not deleted
        assert not failures
        mock_sso.delete_account_assignment.assert_not_called()

    def test_records_failure_when_deletion_raises(self):
        mock_sso = MagicMock()

        # Create a tracked assignment that will be deleted (with no matching desired binding)
        tracked_assignment = handler.TrackedAssignment(
            assignment_id="123456789012#g-extra#arn:ps-extra",
            account_id="123456789012",
            permission_set_arn="arn:ps-extra",
            permission_set_name="Extra",
            principal_id="g-extra",
            principal_type="GROUP",
            template_name="t",
            group_name="extra",
            created_at="2025-01-01T00:00:00Z",
            last_seen="2025-01-01T00:00:00Z",
        )

        mock_sso.delete_account_assignment.side_effect = handler.HandlerError("boom")

        with patch.object(handler, "sso_admin", mock_sso):
            with patch.object(
                handler, "get_tracking_assignments", return_value=[tracked_assignment]
            ):
                with patch.object(handler, "delete_tracking_assignment"):
                    deleted, failures = handler.reconcile_assignments(
                        instance_arn="arn:i",
                        desired_bindings=[],
                        tracking_table_name=None,
                        accounts_to_reconcile=["123456789012"],
                    )

        assert not deleted
        assert len(failures) == 1
        assert failures[0]["assignment_id"] == "123456789012#g-extra#arn:ps-extra"
