from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import boto3
import pytest

from libs.errors import HandlerError
from libs.identity_center import IdentityCenter
from libs.types import Group, PermissionSet


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
                boto3,
                "client",
                side_effect=[mock_admin_client, mock_identitystore_client],
            ) as p_client,
            patch.object(
                IdentityCenter,
                "list_groups",
                return_value=[Group(name="TeamA", id="g-1")],
            ) as p_list_groups,
            patch.object(
                IdentityCenter,
                "list_permission_sets",
                return_value=[PermissionSet(name="Admin", arn="arn:ps:1")],
            ) as p_list_permission_sets,
        ):
            ic = IdentityCenter(instance_arn="arn:aws:sso:::instance/ssoins-1234567890abcdef")

        assert p_client.call_args_list == [
            call("sso-admin", region_name="eu-west-2"),
            call("identitystore", region_name="eu-west-2"),
        ]
        p_list_groups.assert_called_once()
        p_list_permission_sets.assert_called_once()
        assert ic.groups == [Group(name="TeamA", id="g-1")]
        assert ic.permission_sets == [PermissionSet(name="Admin", arn="arn:ps:1")]
        assert ic.identity_store_id == "d-1234567890"

    def test_has_group_and_get_group_use_cached_groups(self):
        ic = IdentityCenter.__new__(IdentityCenter)
        ic.instance_arn = "arn:i"
        ic.client = MagicMock()
        ic.identitystore_client = MagicMock()
        ic.permission_sets = []
        ic.groups = [Group(name="TeamA", id="g-1")]
        assert ic.has_group("TeamA") is True
        assert ic.has_group("Missing") is False
        assert ic.get_group("TeamA").id == "g-1"
        assert ic.get_group("Missing") is None

    def test_list_groups_populates_cache_and_is_used_by_has_group_and_get_group(self):
        ic = IdentityCenter.__new__(IdentityCenter)
        ic.instance_arn = "arn:aws:sso:::instance/ssoins-1234567890abcdef"
        ic.identity_store_id = "d-1234567890"
        ic.groups = []
        ic.permission_sets = []

        mock_identitystore_client = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [{"Groups": [{"DisplayName": "TeamA", "GroupId": "g-1"}]}]
        mock_identitystore_client.get_paginator.return_value = paginator
        ic.identitystore_client = mock_identitystore_client
        ic.client = MagicMock()

        assert ic.has_group("TeamA") is True
        grp = ic.get_group("TeamA")
        assert grp is not None
        assert grp.id == "g-1"
        # Ensure list_groups cached results (second call should not hit paginator)
        assert ic.list_groups() == [Group(name="TeamA", id="g-1")]
        mock_identitystore_client.get_paginator.assert_called_once_with("list_groups")

    def test_list_permission_sets_builds_objects(self):
        ic = IdentityCenter.__new__(IdentityCenter)
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
        ic = IdentityCenter.__new__(IdentityCenter)
        ic.instance_arn = "arn:i"
        ic.groups = []
        ic.permission_sets = [PermissionSet(name="Cached", arn="arn:cached")]
        ic.client = MagicMock()

        out = ic.list_permission_sets()
        assert out == ic.permission_sets
        ic.client.get_paginator.assert_not_called()

    def test_list_groups_cache_hit_does_not_call_paginator(self):
        ic = IdentityCenter.__new__(IdentityCenter)
        ic.instance_arn = "arn:i"
        ic.groups = [Group(name="TeamA", id="g-1")]
        ic.permission_sets = []
        ic.client = MagicMock()

        out = ic.list_groups()
        assert out == ic.groups
        ic.client.get_paginator.assert_not_called()

    def test_create_assignment_skips_when_preexisting_assignment_found(self):
        ic = IdentityCenter.__new__(IdentityCenter)
        ic.instance_arn = "arn:i"
        ic.poll_timeout_seconds = 1
        ic.poll_interval_seconds = 0.01
        ic.groups = []
        ic.permission_sets = []

        mock_client = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"AccountAssignments": [{"PrincipalId": "g-1", "PrincipalType": "GROUP"}]}
        ]
        mock_client.get_paginator.return_value = paginator
        ic.client = mock_client

        ic.create_assignment(
            account_id="123",
            permission_set_arn="arn:ps",
            permission_set_name="Admin",
            principal_type="GROUP",
            principal_id="g-1",
        )

        mock_client.get_paginator.assert_called_once_with("list_account_assignments")
        paginator.paginate.assert_called_once_with(
            InstanceArn="arn:i",
            AccountId="123",
            PermissionSetArn="arn:ps",
        )
        mock_client.create_account_assignment.assert_not_called()
        mock_client.describe_account_assignment_creation_status.assert_not_called()

    def test_create_assignment_creates_when_nonexistent_and_polls_to_succeeded(self):
        ic = IdentityCenter.__new__(IdentityCenter)
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
        ic = IdentityCenter.__new__(IdentityCenter)
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

        with pytest.raises(HandlerError, match="boom"):
            ic.create_assignment(
                account_id="123",
                permission_set_arn="arn:ps",
                permission_set_name="Admin",
                principal_type="GROUP",
                principal_id="g-1",
            )

    def test_delete_assignment_polls_to_succeeded(self):
        ic = IdentityCenter.__new__(IdentityCenter)
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
        ic = IdentityCenter.__new__(IdentityCenter)
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

        with pytest.raises(HandlerError, match="nope"):
            ic.delete_assignment(
                account_id="123",
                permission_set_arn="arn:ps",
                permission_set_name="Admin",
                principal_id="g-1",
                principal_type="GROUP",
            )

