"""
Unit tests for `handler.py`.

Tests for `assets/functions/libs/*` live under `assets/functions/libs/tests/`.
"""

from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock, patch

import pytest  # pylint: disable=import-error

from libs.errors import HandlerError
from libs.types import (
    Account,
    AccountTemplate,
    AccountTemplateMatcher,
    Assignment,
    Binding,
    Group,
    Permission,
    PermissionSet,
    Template,
)

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
os.environ.setdefault("AWS_SESSION_TOKEN", "test")
os.environ.setdefault("AWS_DEFAULT_REGION", "eu-west-1")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import handler  # noqa: E402 pylint: disable=wrong-import-position


class TestReconcileCreations:
    def test_returns_empty_when_no_bindings(self):
        ok, bad = handler.reconcile_creations(
            [], identity_center=MagicMock(), tracking=MagicMock()
        )
        assert not ok
        assert not bad

    def test_returns_empty_and_does_not_call_dependencies_when_zero_bindings(self):
        identity_center = MagicMock()
        tracking = MagicMock()
        handler.events_publisher = MagicMock()

        successes, failures = handler.reconcile_creations(
            bindings=[],
            identity_center=identity_center,
            tracking=tracking,
        )

        assert not successes
        assert not failures
        identity_center.create_assignment.assert_not_called()
        tracking.create.assert_not_called()
        handler.events_publisher.publish.assert_not_called()

    def test_failure_to_create_assignment_is_returned_in_failures(self):
        identity_center = MagicMock()
        tracking = MagicMock()
        handler.events_publisher = MagicMock()

        identity_center.create_assignment.side_effect = HandlerError("boom")

        bindings = [
            Binding(
                account_id="111111111111",
                permission_set_name="PS1",
                permission_set_arn="arn:ps1",
                groups=[Group(name="G1", id="g-1")],
                template_name="tpl",
            )
        ]

        successes, failures = handler.reconcile_creations(
            bindings=bindings,
            identity_center=identity_center,
            tracking=tracking,
        )

        assert not successes
        assert failures == [
            {
                "account_id": "111111111111",
                "group_name": "G1",
                "permission_set_arn": "arn:ps1",
                "permission_set_name": "PS1",
                "error": "boom",
            }
        ]
        identity_center.create_assignment.assert_called_once_with(
            account_id="111111111111",
            permission_set_arn="arn:ps1",
            permission_set_name="PS1",
            principal_id="g-1",
            principal_type="GROUP",
            dry_run=False,
        )
        tracking.create.assert_not_called()
        handler.events_publisher.publish.assert_not_called()

    def test_failure_to_create_tracking_is_returned_in_failures_and_publish_is_not_called(
        self,
    ):
        identity_center = MagicMock()
        tracking = MagicMock()
        handler.events_publisher = MagicMock()

        tracking.create.side_effect = HandlerError("tracking failed")

        bindings = [
            Binding(
                account_id="111111111111",
                permission_set_name="PS1",
                permission_set_arn="arn:ps1",
                groups=[Group(name="G1", id="g-1")],
                template_name="tpl",
            )
        ]

        successes, failures = handler.reconcile_creations(
            bindings=bindings,
            identity_center=identity_center,
            tracking=tracking,
        )

        assert not successes
        assert failures == [
            {
                "account_id": "111111111111",
                "group_name": "G1",
                "permission_set_arn": "arn:ps1",
                "permission_set_name": "PS1",
                "error": "tracking failed",
            }
        ]
        identity_center.create_assignment.assert_called_once()
        tracking.create.assert_called_once_with(
            account_id="111111111111",
            dry_run=False,
            group_name="G1",
            permission_set_arn="arn:ps1",
            permission_set_name="PS1",
            principal_id="g-1",
            principal_type="GROUP",
            template_name="tpl",
        )
        handler.events_publisher.publish.assert_not_called()

    def test_creation_of_multiple_bindings_calls_all_dependencies_and_returns_consistent_results(
        self,
    ):
        identity_center = MagicMock()
        tracking = MagicMock()
        publisher = MagicMock()
        handler.events_publisher = publisher

        tracking.get_assignment_id.side_effect = [
            "a-1",
            "a-2",
            "a-3",
        ]

        bindings = [
            Binding(
                account_id="111111111111",
                permission_set_name="PS1",
                permission_set_arn="arn:ps1",
                groups=[Group(name="G1", id="g-1"), Group(name="G2", id="g-2")],
                template_name="tpl-1",
            ),
            Binding(
                account_id="222222222222",
                permission_set_name="PS2",
                permission_set_arn="arn:ps2",
                groups=[Group(name="G3", id="g-3")],
                template_name="tpl-2",
            ),
        ]

        successes, failures = handler.reconcile_creations(
            bindings=bindings,
            identity_center=identity_center,
            tracking=tracking,
        )

        assert not failures
        assert successes == [
            {
                "account_id": "111111111111",
                "group_name": "G1",
                "permission_set_arn": "arn:ps1",
                "permission_set_name": "PS1",
            },
            {
                "account_id": "111111111111",
                "group_name": "G2",
                "permission_set_arn": "arn:ps1",
                "permission_set_name": "PS1",
            },
            {
                "account_id": "222222222222",
                "group_name": "G3",
                "permission_set_arn": "arn:ps2",
                "permission_set_name": "PS2",
            },
        ]

        assert identity_center.create_assignment.call_count == 3
        identity_center.create_assignment.assert_any_call(
            account_id="111111111111",
            permission_set_arn="arn:ps1",
            permission_set_name="PS1",
            principal_id="g-1",
            principal_type="GROUP",
            dry_run=False,
        )
        identity_center.create_assignment.assert_any_call(
            account_id="111111111111",
            permission_set_arn="arn:ps1",
            permission_set_name="PS1",
            principal_id="g-2",
            principal_type="GROUP",
            dry_run=False,
        )
        identity_center.create_assignment.assert_any_call(
            account_id="222222222222",
            permission_set_arn="arn:ps2",
            permission_set_name="PS2",
            principal_id="g-3",
            principal_type="GROUP",
            dry_run=False,
        )

        assert tracking.create.call_count == 3
        tracking.create.assert_any_call(
            account_id="111111111111",
            dry_run=False,
            group_name="G1",
            permission_set_arn="arn:ps1",
            permission_set_name="PS1",
            principal_id="g-1",
            principal_type="GROUP",
            template_name="tpl-1",
        )
        tracking.create.assert_any_call(
            account_id="111111111111",
            dry_run=False,
            group_name="G2",
            permission_set_arn="arn:ps1",
            permission_set_name="PS1",
            principal_id="g-2",
            principal_type="GROUP",
            template_name="tpl-1",
        )
        tracking.create.assert_any_call(
            account_id="222222222222",
            dry_run=False,
            group_name="G3",
            permission_set_arn="arn:ps2",
            permission_set_name="PS2",
            principal_id="g-3",
            principal_type="GROUP",
            template_name="tpl-2",
        )

        assert publisher.publish.call_count == 3
        publisher.publish.assert_any_call(
            dry_run=False,
            event_type="AccountAssignmentCreated",
            detail={
                "account_id": "111111111111",
                "assignment_id": "a-1",
                "group": {"id": "g-1", "name": "G1"},
                "permission_set": {"arn": "arn:ps1", "name": "PS1"},
                "principal_type": "GROUP",
                "template_name": "tpl-1",
            },
        )
        publisher.publish.assert_any_call(
            dry_run=False,
            event_type="AccountAssignmentCreated",
            detail={
                "account_id": "111111111111",
                "assignment_id": "a-2",
                "group": {"id": "g-2", "name": "G2"},
                "permission_set": {"arn": "arn:ps1", "name": "PS1"},
                "principal_type": "GROUP",
                "template_name": "tpl-1",
            },
        )
        publisher.publish.assert_any_call(
            dry_run=False,
            event_type="AccountAssignmentCreated",
            detail={
                "account_id": "222222222222",
                "assignment_id": "a-3",
                "group": {"id": "g-3", "name": "G3"},
                "permission_set": {"arn": "arn:ps2", "name": "PS2"},
                "principal_type": "GROUP",
                "template_name": "tpl-2",
            },
        )


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

    def test_does_not_delete_when_assignment_has_matching_binding_and_is_tracked(self):
        tracking = MagicMock()
        identity_center = MagicMock()
        identity_center.instance_arn = "arn:i"
        publisher = MagicMock()
        handler.events_publisher = publisher

        tracked = Assignment(
            assignment_id="a-1",
            account_id="111111111111",
            permission_set_arn="arn:ps1",
            permission_set_name="PS1",
            principal_id="g-1",
            principal_type="GROUP",
            group_name="G1",
            template_name="tpl",
        )
        tracking.list.return_value = [tracked]

        desired_bindings = [
            Binding(
                account_id="111111111111",
                permission_set_name="PS1",
                permission_set_arn="arn:ps1",
                groups=[Group(name="G1", id="g-1")],
                template_name="tpl",
            )
        ]

        deleted, failed = handler.reconcile_deletions(
            desired_bindings=desired_bindings,
            tracking=tracking,
            identity_center=identity_center,
        )

        assert not deleted
        assert not failed
        identity_center.delete_assignment.assert_not_called()
        tracking.delete.assert_not_called()
        publisher.publish.assert_not_called()

    def test_deletes_when_assignment_is_tracked_but_not_in_desired_bindings(self):
        tracking = MagicMock()
        identity_center = MagicMock()
        identity_center.instance_arn = "arn:i"
        publisher = MagicMock()
        handler.events_publisher = publisher

        tracked = Assignment(
            assignment_id="a-1",
            account_id="111111111111",
            permission_set_arn="arn:ps1",
            permission_set_name="PS1",
            principal_id="g-1",
            principal_type="GROUP",
            group_name="G1",
            template_name="tpl",
        )
        tracking.list.return_value = [tracked]

        deleted, failed = handler.reconcile_deletions(
            desired_bindings=[],
            tracking=tracking,
            identity_center=identity_center,
        )

        assert deleted == [
            {
                "assignment_id": "a-1",
                "account_id": "111111111111",
                "permission_set_name": "PS1",
            }
        ]
        assert not failed

        identity_center.delete_assignment.assert_called_once_with(
            account_id="111111111111",
            dry_run=False,
            permission_set_arn="arn:ps1",
            permission_set_name="PS1",
            principal_id="g-1",
            principal_type="GROUP",
        )
        publisher.publish.assert_called_once_with(
            event_type="AccountAssignmentDeleted",
            dry_run=False,
            detail={
                "account_id": "111111111111",
                "assignment_id": "a-1",
                "group": {"id": "g-1", "name": "G1"},
                "permission_set": {"arn": "arn:ps1", "name": "PS1"},
                "principal_type": "GROUP",
                "template_name": "tpl",
            },
        )
        tracking.delete.assert_called_once_with("a-1")

    def test_records_failure_when_delete_assignment_raises_and_still_deletes_from_tracking(
        self,
    ):
        tracking = MagicMock()
        identity_center = MagicMock()
        identity_center.instance_arn = "arn:i"
        publisher = MagicMock()
        handler.events_publisher = publisher

        identity_center.delete_assignment.side_effect = Exception("nope")

        tracked = Assignment(
            assignment_id="a-1",
            account_id="111111111111",
            permission_set_arn="arn:ps1",
            permission_set_name="PS1",
            principal_id="g-1",
            principal_type="GROUP",
            group_name="G1",
            template_name="tpl",
        )
        tracking.list.return_value = [tracked]

        deleted, failed = handler.reconcile_deletions(
            desired_bindings=[],
            tracking=tracking,
            identity_center=identity_center,
        )

        assert not deleted
        assert failed == [
            {
                "assignment_id": "a-1",
                "account_id": "111111111111",
                "permission_set_name": "PS1",
                "error": "nope",
            }
        ]
        publisher.publish.assert_not_called()
        tracking.delete.assert_called_once_with("a-1")

    def test_treats_assignment_does_not_exist_as_success_and_deletes_from_tracking(
        self,
    ):
        tracking = MagicMock()
        identity_center = MagicMock()
        identity_center.instance_arn = "arn:i"
        publisher = MagicMock()
        handler.events_publisher = publisher

        identity_center.delete_assignment.side_effect = Exception(
            "Assignment does not exist"
        )

        tracked = Assignment(
            assignment_id="a-1",
            account_id="111111111111",
            permission_set_arn="arn:ps1",
            permission_set_name="PS1",
            principal_id="g-1",
            principal_type="GROUP",
            group_name="G1",
            template_name="tpl",
        )
        tracking.list.return_value = [tracked]

        deleted, failed = handler.reconcile_deletions(
            desired_bindings=[],
            tracking=tracking,
            identity_center=identity_center,
        )

        assert deleted == [
            {
                "assignment_id": "a-1",
                "account_id": "111111111111",
                "permission_set_name": "PS1",
            }
        ]
        assert not failed
        publisher.publish.assert_not_called()
        tracking.delete.assert_called_once_with("a-1")


class _ConfigStub:
    def __init__(self, templates: dict[str, Template], account_templates=None):
        self.templates = templates
        self.account_templates = account_templates or {}


class _IdentityCenterStub:
    def __init__(self, groups: dict[str, str], permission_sets: dict[str, str]):
        self._groups = groups
        self._permission_sets = permission_sets

    def has_group(self, group_name: str) -> bool:
        return group_name in self._groups

    def get_group(self, group_name: str) -> Group | None:
        if group_name not in self._groups:
            return None
        return Group(name=group_name, id=self._groups[group_name])

    def get_permission_set(self, name: str) -> PermissionSet | None:
        arn = self._permission_sets.get(name)
        if not arn:
            return None
        return PermissionSet(name=name, arn=arn)


class TestBuildPermissionBindings:
    def test_fails_when_template_missing(self):
        acct = Account(id="1", tags={})
        cfg = _ConfigStub(templates={})
        ic = _IdentityCenterStub(
            groups={"G1": "g1"}, permission_sets={"PS1": "arn:ps1"}
        )
        bindings, successes, failures = handler.build_permission_bindings(
            account=acct,
            configuration=cfg,
            identity_center=ic,
            permission=Permission(name="tpl", groups=["G1"]),
        )
        assert not bindings
        assert not successes
        assert (
            failures
            and failures[0]["error"] == "Permission template not found in configuration"
        )


class TestBuildPermissionBindingsFromAccountTags:
    def test_creates_expected_bindings_for_multiple_accounts_and_templates_from_tags(
        self,
    ):
        cfg = _ConfigStub(
            templates={
                "tplA": Template(permission_sets=["PS1", "PS2"]),
                "tplB": Template(permission_sets=["PS2"]),
            }
        )
        ic = _IdentityCenterStub(
            groups={"G1": "g-1", "G2": "g-2", "G3": "g-3"},
            permission_sets={"PS1": "arn:ps1", "PS2": "arn:ps2"},
        )

        a1 = Account(
            id="111111111111",
            name="a1",
            tags={"sso/tplA": "G1,G2"},
            organizational_unit_path="r/ou",
        )
        a2 = Account(
            id="222222222222",
            name="a2",
            tags={"sso/tplB": "G3"},
            organizational_unit_path="r/ou",
        )

        all_bindings: list[Binding] = []
        all_failures: list[dict] = []

        for acct in (a1, a2):
            for perm in acct.get_permission_tags(prefix="sso"):
                bindings, successes, failures = handler.build_permission_bindings(
                    account=acct, configuration=cfg, identity_center=ic, permission=perm
                )
                assert not successes
                all_bindings.extend(bindings)
                all_failures.extend(failures)

        assert not all_failures
        assert [
            (
                b.account_id,
                b.permission_set_name,
                [g.id for g in b.groups],
                b.template_name,
            )
            for b in all_bindings
        ] == [
            ("111111111111", "PS1", ["g-1", "g-2"], "tplA"),
            ("111111111111", "PS2", ["g-1", "g-2"], "tplA"),
            ("222222222222", "PS2", ["g-3"], "tplB"),
        ]

    def test_missing_group_in_tag_value_produces_failure_but_still_builds_bindings_with_existing_groups(
        self,
    ):
        cfg = _ConfigStub(templates={"tplA": Template(permission_sets=["PS1"])})
        ic = _IdentityCenterStub(
            groups={"G1": "g-1"}, permission_sets={"PS1": "arn:ps1"}
        )
        acct = Account(
            id="111111111111",
            name="a1",
            tags={"sso/tplA": "G1,MissingGroup"},
            organizational_unit_path="r/ou",
        )

        perms = acct.get_permission_tags(prefix="sso")
        assert len(perms) == 1

        bindings, successes, failures = handler.build_permission_bindings(
            account=acct, configuration=cfg, identity_center=ic, permission=perms[0]
        )

        assert not successes
        assert failures == [
            {
                "account_id": "111111111111",
                "error": "Group not found in identity store",
                "group": "MissingGroup",
                "permission": "tplA",
            }
        ]
        assert len(bindings) == 1
        assert bindings[0].account_id == "111111111111"
        assert bindings[0].permission_set_name == "PS1"
        assert bindings[0].permission_set_arn == "arn:ps1"
        assert [g.id for g in bindings[0].groups] == ["g-1"]
        assert bindings[0].template_name == "tplA"

    def test_missing_permission_set_in_template_produces_failure_and_skips_that_binding(
        self,
    ):
        cfg = _ConfigStub(
            templates={"tplA": Template(permission_sets=["PS1", "PS-MISSING"])}
        )
        ic = _IdentityCenterStub(
            groups={"G1": "g-1"}, permission_sets={"PS1": "arn:ps1"}
        )
        acct = Account(
            id="111111111111",
            name="a1",
            tags={"sso/tplA": "G1"},
            organizational_unit_path="r/ou",
        )

        perm = acct.get_permission_tags(prefix="sso")[0]
        bindings, successes, failures = handler.build_permission_bindings(
            account=acct, configuration=cfg, identity_center=ic, permission=perm
        )

        assert not successes
        assert bindings and [b.permission_set_name for b in bindings] == ["PS1"]
        assert failures == [
            {
                "account_id": "111111111111",
                "permission": "PS-MISSING",
                "error": "Permission set not found in identity store",
            }
        ]

    def test_missing_template_referenced_by_account_tags_returns_no_bindings_and_failure(
        self,
    ):
        cfg = _ConfigStub(templates={"tplA": Template(permission_sets=["PS1"])})
        ic = _IdentityCenterStub(
            groups={"G1": "g-1"}, permission_sets={"PS1": "arn:ps1"}
        )
        acct = Account(
            id="111111111111",
            name="a1",
            tags={"sso/does-not-exist": "G1"},
            organizational_unit_path="r/ou",
        )

        perm = acct.get_permission_tags(prefix="sso")[0]
        bindings, successes, failures = handler.build_permission_bindings(
            account=acct, configuration=cfg, identity_center=ic, permission=perm
        )

        assert not bindings
        assert not successes
        assert failures == [
            {
                "account_id": "111111111111",
                "permission": "does-not-exist",
                "error": "Permission template not found in configuration",
            }
        ]


class TestBuildAccountBindings:
    def test_returns_empty_when_no_account_templates(self):
        acct = Account(
            id="1", name="a", tags={}, organizational_unit_path="r-root/ou-1"
        )
        cfg = _ConfigStub(
            templates={"tpl": Template(permission_sets=["PS1"])}, account_templates={}
        )
        ic = _IdentityCenterStub(
            groups={"G1": "g1"}, permission_sets={"PS1": "arn:ps1"}
        )
        bindings, successes, failures = handler.build_account_bindings(
            account=acct, configuration=cfg, identity_center=ic
        )
        assert not bindings
        assert not successes
        assert not failures


class TestBuildAccountBindingsFromAccountTemplates:
    def test_creates_bindings_for_accounts_matching_ou_name_and_tag_templates(self):
        cfg = _ConfigStub(
            templates={
                "tplA": Template(permission_sets=["PS1"]),
                "tplB": Template(permission_sets=["PS2"]),
            },
            account_templates={
                "prod-by-ou": AccountTemplate(
                    name="prod-by-ou",
                    matcher=AccountTemplateMatcher(organizational_units=["prod/*"]),
                    template_names=["tplA"],
                    groups=["G1"],
                ),
                "sandbox-by-name-and-tag": AccountTemplate(
                    name="sandbox-by-name-and-tag",
                    matcher=AccountTemplateMatcher(
                        name_pattern="sandbox-*",
                        account_tags={"Environment": "Sandbox"},
                    ),
                    template_names=["tplB"],
                    groups=["G2"],
                ),
            },
        )
        ic = _IdentityCenterStub(
            groups={"G1": "g-1", "G2": "g-2"},
            permission_sets={"PS1": "arn:ps1", "PS2": "arn:ps2"},
        )

        prod_acct = Account(
            id="111111111111",
            name="prod-app",
            tags={},
            organizational_unit_path="r-root/prod/apps",
        )
        sandbox_acct = Account(
            id="222222222222",
            name="sandbox-foo",
            tags={"Environment": "Sandbox"},
            organizational_unit_path="r-root/sandbox",
        )
        unmatched = Account(
            id="333333333333",
            name="dev-foo",
            tags={"Environment": "Sandbox"},
            organizational_unit_path="r-root/dev",
        )

        b1, s1, f1 = handler.build_account_bindings(
            account=prod_acct, configuration=cfg, identity_center=ic
        )
        b2, s2, f2 = handler.build_account_bindings(
            account=sandbox_acct, configuration=cfg, identity_center=ic
        )
        b3, s3, f3 = handler.build_account_bindings(
            account=unmatched, configuration=cfg, identity_center=ic
        )

        assert not s1 and not f1
        assert [
            (
                b.account_id,
                b.permission_set_name,
                [g.id for g in b.groups],
                b.template_name,
            )
            for b in b1
        ] == [("111111111111", "PS1", ["g-1"], "tplA")]

        assert not s2 and not f2
        assert [
            (
                b.account_id,
                b.permission_set_name,
                [g.id for g in b.groups],
                b.template_name,
            )
            for b in b2
        ] == [("222222222222", "PS2", ["g-2"], "tplB")]

        assert not b3
        assert not s3
        assert not f3

    def test_missing_template_referenced_by_account_template_populates_failures(self):
        cfg = _ConfigStub(
            templates={"tplA": Template(permission_sets=["PS1"])},
            account_templates={
                "prod": AccountTemplate(
                    name="prod",
                    matcher=AccountTemplateMatcher(organizational_units=["prod/*"]),
                    template_names=["tplA", "tplMissing"],
                    groups=["G1"],
                )
            },
        )
        ic = _IdentityCenterStub(
            groups={"G1": "g-1"}, permission_sets={"PS1": "arn:ps1"}
        )
        acct = Account(
            id="111111111111",
            name="prod-app",
            tags={},
            organizational_unit_path="r-root/prod/apps",
        )

        bindings, successes, failures = handler.build_account_bindings(
            account=acct, configuration=cfg, identity_center=ic
        )

        assert not successes
        assert [(b.permission_set_name, b.template_name) for b in bindings] == [
            ("PS1", "tplA")
        ]
        assert failures == [
            {
                "account_id": "111111111111",
                "permission": "tplMissing",
                "error": "Permission template not found in configuration",
            }
        ]

    def test_missing_group_and_permission_set_from_account_template_propagates_failures_and_skips_missing_permission_set(
        self,
    ):
        cfg = _ConfigStub(
            templates={"tplA": Template(permission_sets=["PS1", "PS-MISSING"])},
            account_templates={
                "prod": AccountTemplate(
                    name="prod",
                    matcher=AccountTemplateMatcher(organizational_units=["prod/*"]),
                    template_names=["tplA"],
                    groups=["G1", "MissingGroup"],
                )
            },
        )
        ic = _IdentityCenterStub(
            groups={"G1": "g-1"}, permission_sets={"PS1": "arn:ps1"}
        )
        acct = Account(
            id="111111111111",
            name="prod-app",
            tags={},
            organizational_unit_path="r-root/prod/apps",
        )

        bindings, successes, failures = handler.build_account_bindings(
            account=acct, configuration=cfg, identity_center=ic
        )

        assert not successes
        assert [
            (b.permission_set_name, [g.id for g in b.groups]) for b in bindings
        ] == [("PS1", ["g-1"])]
        assert failures == [
            {
                "account_id": "111111111111",
                "error": "Group not found in identity store",
                "group": "MissingGroup",
                "permission": "tplA",
            },
            {
                "account_id": "111111111111",
                "permission": "PS-MISSING",
                "error": "Permission set not found in identity store",
            },
        ]

    def test_invalid_excluded_regex_populates_failures_and_skips_template(self):
        cfg = _ConfigStub(
            templates={"tplA": Template(permission_sets=["PS1"])},
            account_templates={
                "bad-exclude": AccountTemplate(
                    name="bad-exclude",
                    matcher=AccountTemplateMatcher(organizational_units=["prod/*"]),
                    excluded=["*("],
                    template_names=["tplA"],
                    groups=["G1"],
                )
            },
        )
        ic = _IdentityCenterStub(
            groups={"G1": "g-1"}, permission_sets={"PS1": "arn:ps1"}
        )
        acct = Account(
            id="111111111111",
            name="prod-app",
            tags={},
            organizational_unit_path="r-root/prod/apps",
        )

        bindings, successes, failures = handler.build_account_bindings(
            account=acct, configuration=cfg, identity_center=ic
        )

        assert not bindings
        assert not successes
        assert len(failures) == 1
        assert failures[0]["account.name"] == "prod-app"
        assert failures[0]["account_template_name"] == "bad-exclude"
        assert "Invalid excluded regex in account template" in failures[0]["error"]


class TestBindingCreationAcrossAccounts:
    def test_lambda_handler_builds_expected_bindings_from_account_tags(self):
        os.environ["DYNAMODB_CONFIG_TABLE"] = "cfg"
        os.environ["DYNAMODB_TRACKING_TABLE"] = "tracking"
        os.environ["SSO_INSTANCE_ARN"] = "arn:i"
        os.environ["SSO_ACCOUNT_TAG_PREFIX"] = "x"

        accounts = [
            Account(
                id="111111111111",
                name="a1",
                tags={"x/tpl": "G1,G2"},
                organizational_unit_path="r-root/ou-1",
            ),
            Account(
                id="222222222222",
                name="a2",
                tags={"x/tpl": "G2"},
                organizational_unit_path="r-root/ou-2",
            ),
        ]

        class FakeConfig:
            def __init__(self, table_name: str, **_kwargs):
                self.table_name = table_name
                self.templates = {"tpl": Template(permission_sets=["PS1", "PS2"])}
                self.account_templates = {}

            def load(self) -> None:
                return None

        class FakeTracking:
            def __init__(self, table_name: str, **_kwargs):
                self.table_name = table_name

        class FakeIdentityCenter:
            def __init__(self, instance_arn: str, **_kwargs):
                self.instance_arn = instance_arn

            def has_group(self, group_name: str) -> bool:
                return group_name in {"G1", "G2"}

            def get_group(self, group_name: str) -> Group:
                return Group(name=group_name, id=f"id-{group_name}")

            def get_permission_set(self, name: str) -> PermissionSet | None:
                if name not in {"PS1", "PS2"}:
                    return None
                return PermissionSet(name=name, arn=f"arn:{name}")

        class FakeOrgs:
            def __init__(self, **_kwargs):
                return None

            def list_accounts(self) -> list[Account]:
                return accounts

        captured: dict[str, object] = {}

        def fake_reconcile_creations(*, bindings, **_kwargs):
            captured["bindings"] = bindings
            return [], []

        with (
            patch.object(handler, "Configuration", FakeConfig),
            patch.object(handler, "Tracking", FakeTracking),
            patch.object(handler, "IdentityCenter", FakeIdentityCenter),
            patch.object(handler, "Organizations", FakeOrgs),
            patch.object(
                handler, "reconcile_creations", side_effect=fake_reconcile_creations
            ),
            patch.object(handler, "reconcile_deletions", return_value=([], [])),
        ):
            out = handler.lambda_handler({"source": "cron_schedule"}, None)

        assert out["status"] == "success"
        assert captured["bindings"]


class TestHasMatchingBinding:
    def test_returns_true_when_account_permission_set_and_group_match(self):
        bindings = [
            Binding(
                account_id="111111111111",
                permission_set_name="PS1",
                permission_set_arn="arn:ps1",
                groups=[Group(name="G1", id="g-1"), Group(name="G2", id="g-2")],
                template_name="tpl",
            )
        ]
        assignment = Assignment(
            account_id="111111111111",
            permission_set_name="PS1",
            permission_set_arn="arn:ps1",
            principal_id="g-2",
            principal_type="GROUP",
            group_name="G2",
        )

        assert (
            handler.has_matching_binding(assignment=assignment, bindings=bindings)
            is True
        )

    @pytest.mark.parametrize(
        "assignment,bindings",
        [
            pytest.param(
                Assignment(
                    account_id="111111111111",
                    permission_set_name="PS1",
                    principal_id="g-1",
                ),
                [],
                id="no-bindings",
            ),
            pytest.param(
                Assignment(
                    account_id="222222222222",
                    permission_set_name="PS1",
                    principal_id="g-1",
                ),
                [
                    Binding(
                        account_id="111111111111",
                        permission_set_name="PS1",
                        groups=[Group(name="G1", id="g-1")],
                    )
                ],
                id="account-mismatch",
            ),
            pytest.param(
                Assignment(
                    account_id="111111111111",
                    permission_set_name="PS2",
                    principal_id="g-1",
                ),
                [
                    Binding(
                        account_id="111111111111",
                        permission_set_name="PS1",
                        groups=[Group(name="G1", id="g-1")],
                    )
                ],
                id="permission-set-mismatch",
            ),
            pytest.param(
                Assignment(
                    account_id="111111111111",
                    permission_set_name="PS1",
                    principal_id="g-999",
                ),
                [
                    Binding(
                        account_id="111111111111",
                        permission_set_name="PS1",
                        groups=[Group(name="G1", id="g-1")],
                    )
                ],
                id="principal-not-in-binding-groups",
            ),
            pytest.param(
                Assignment(
                    account_id="111111111111",
                    permission_set_name="PS1",
                    principal_id="g-1",
                ),
                [
                    Binding(
                        account_id="111111111111",
                        permission_set_name="PS2",
                        groups=[Group(name="G1", id="g-1")],
                    ),
                    Binding(
                        account_id="111111111111",
                        permission_set_name="PS1",
                        groups=[Group(name="G2", id="g-2")],
                    ),
                ],
                id="no-match-across-multiple-bindings",
            ),
        ],
    )
    def test_returns_false_when_any_required_field_does_not_match(
        self, assignment, bindings
    ):
        assert (
            handler.has_matching_binding(assignment=assignment, bindings=bindings)
            is False
        )


class TestValidateEnvironmentBDD:
    def test_given_all_required_env_vars_when_validate_environment_then_does_not_raise(
        self, monkeypatch
    ):
        monkeypatch.setenv("DYNAMODB_CONFIG_TABLE", "cfg")
        monkeypatch.setenv("DYNAMODB_TRACKING_TABLE", "tracking")
        monkeypatch.setenv("SSO_INSTANCE_ARN", "arn:i")

        handler.validate_environment()

    @pytest.mark.parametrize(
        "missing_var",
        [
            pytest.param("DYNAMODB_CONFIG_TABLE", id="missing-config-table"),
            pytest.param("DYNAMODB_TRACKING_TABLE", id="missing-tracking-table"),
            pytest.param("SSO_INSTANCE_ARN", id="missing-sso-instance-arn"),
        ],
    )
    def test_given_a_required_env_var_is_missing_when_validate_environment_then_raises_handler_error(
        self, monkeypatch, missing_var
    ):
        monkeypatch.setenv("DYNAMODB_CONFIG_TABLE", "cfg")
        monkeypatch.setenv("DYNAMODB_TRACKING_TABLE", "tracking")
        monkeypatch.setenv("SSO_INSTANCE_ARN", "arn:i")
        monkeypatch.delenv(missing_var, raising=True)

        with pytest.raises(
            HandlerError, match=rf"Missing required environment variable: {missing_var}"
        ):
            handler.validate_environment()
