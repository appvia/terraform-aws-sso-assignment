from __future__ import annotations

import json
from unittest.mock import MagicMock, call, patch

import boto3

from libs.types import (
    Account,
    AccountTemplate,
    AccountTemplateMatcher,
    Assignment,
    Binding,
    Configuration,
    Group,
    User,
    Permission,
    PermissionSet,
    Template,
)


class TestGroup:
    def test_to_json(self):
        g = Group(name="TeamA", id="g-1")
        assert json.loads(g.to_json()) == {"name": "TeamA", "id": "g-1"}


class TestPermissionSet:
    def test_to_json(self):
        ps = PermissionSet(name="Admin", arn="arn:ps:1")
        assert json.loads(ps.to_json()) == {"name": "Admin", "arn": "arn:ps:1"}


class TestBinding:
    def test_to_json_includes_nested_groups(self):
        b = Binding(
            account_id="123456789012",
            permission_set_name="Admin",
            permission_set_arn="arn:ps:1",
            groups=[Group(name="TeamA", id="g-1")],
            users=[User(name="alice@example.com", id="u-1")],
            template_name="tpl",
        )
        data = json.loads(b.to_json())
        assert data["account_id"] == "123456789012"
        assert data["template_name"] == "tpl"
        assert data["groups"] == [{"name": "TeamA", "id": "g-1"}]
        assert data["users"] == [{"name": "alice@example.com", "id": "u-1"}]


class TestPermission:
    def test_to_json(self):
        p = Permission(name="default", groups=["A", "B"], users=["u1"])
        assert json.loads(p.to_json()) == {
            "name": "default",
            "groups": ["A", "B"],
            "users": ["u1"],
        }


class TestAccount:
    def test_to_json(self):
        acct = Account(id="1", name="n", tags={"k": "v"}, organizational_unit_path="ou/x")
        data = json.loads(acct.to_json())
        assert data["id"] == "1"
        assert data["tags"] == {"k": "v"}


class TestTemplate:
    def test_to_json(self):
        t = Template(permission_sets=["Admin"], description="desc")
        assert json.loads(t.to_json()) == {
            "permission_sets": ["Admin"],
            "description": "desc",
        }


class TestAssignment:
    def test_to_json(self):
        a = Assignment(
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
        matcher = AccountTemplateMatcher(
            organizational_units=["/ou-prod/ou-workloads*"],
            name_pattern="prod-*",
            account_tags={"Environment": "Production"},
        )
        acct = Account(
            id="1",
            name="prod-app-1",
            tags={"Environment": "Production", "CostCenter": "Eng"},
            organizational_unit_path="/ou-prod/ou-workloads/ou-team1",
        )
        assert matcher.matches(acct) is True

    def test_matches_fails_on_tag_mismatch(self):
        matcher = AccountTemplateMatcher(account_tags={"Environment": "Production"})
        acct = Account(id="1", name="x", tags={"Environment": "Dev"})
        assert matcher.matches(acct) is False

    def test_matches_tags_single_and_multiple_conditions(self):
        matcher = AccountTemplateMatcher(account_tags={"Environment": "Production", "CostCenter": "Eng"})
        acct_ok = Account(
            id="1",
            name="x",
            tags={"Environment": "Production", "CostCenter": "Eng", "Owner": "me"},
        )
        acct_missing = Account(id="2", name="x", tags={"Environment": "Production"})
        acct_value_mismatch = Account(
            id="3",
            name="x",
            tags={"Environment": "Production", "CostCenter": "Finance"},
        )
        assert matcher.matches(acct_ok) is True
        assert matcher.matches(acct_missing) is False
        assert matcher.matches(acct_value_mismatch) is False

    def test_matches_organizational_unit_trailing_path(self):
        matcher = AccountTemplateMatcher(organizational_units=["/ou-prod/ou-workloads"])
        assert (
            matcher.matches_organizational_unit(
                "/ou-prod/ou-workloads", ["/ou-prod/ou-workloads"]
            )
            is True
        )
        assert (
            matcher.matches_organizational_unit("/ou-dev/ou-workloads", ["/ou-prod/*"])
            is False
        )

    def test_matches_organizational_unit_for_leading_slash_paths_and_globs(self):
        matcher = AccountTemplateMatcher(organizational_units=["/workloads/*"])
        acct = Account(
            id="1",
            name="TestAccount",
            tags={},
            organizational_unit_path="/workloads/development",
        )
        assert matcher.matches(acct) is True

    def test_matches_organizational_unit_for_exact_trailing_path(self):
        matcher = AccountTemplateMatcher(organizational_units=["/workloads/development"])
        acct = Account(
            id="1",
            name="TestAccount",
            tags={},
            organizational_unit_path="/workloads/development",
        )
        assert matcher.matches(acct) is True

    def test_matches_organizational_unit_negative_when_path_does_not_match(self):
        matcher = AccountTemplateMatcher(organizational_units=["/workspaces/*"])
        acct = Account(
            id="1",
            name="TestAccount",
            tags={},
            organizational_unit_path="/workloads/development",
        )
        assert matcher.matches(acct) is False

    def test_matches_name_patterns(self):
        matcher = AccountTemplateMatcher(name_patterns=["prod-[a-z]*-[0-9][0-9]", "shared-*"])
        assert matcher.matches(Account(id="1", name="prod-app-12")) is True
        assert matcher.matches(Account(id="2", name="shared-services")) is True
        assert matcher.matches(Account(id="3", name="prod-APP-12")) is False

    def test_matches_combined_tags_and_name_conditions(self):
        matcher = AccountTemplateMatcher(
            account_tags={"Environment": "Production"},
            name_patterns=["prod-[a-z]*-[0-9]"],
        )
        acct_ok = Account(id="1", name="prod-app-1", tags={"Environment": "Production"})
        acct_bad_tag = Account(id="2", name="prod-app-1", tags={"Environment": "Dev"})
        acct_bad_name = Account(id="3", name="prod-app-x", tags={"Environment": "Production"})
        assert matcher.matches(acct_ok) is True
        assert matcher.matches(acct_bad_tag) is False
        assert matcher.matches(acct_bad_name) is False


class TestAccountTemplate:
    def test_to_json(self):
        at = AccountTemplate(
            name="baseline",
            matcher=AccountTemplateMatcher(name_pattern="prod-*"),
            template_names=["default"],
            groups=["TeamA"],
            users=["alice@example.com"],
            description="d",
        )
        data = json.loads(at.to_json())
        assert data["name"] == "baseline"
        assert data["template_names"] == ["default"]
        assert data["users"] == ["alice@example.com"]


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
                        "organizational_units": ["/ou-prod/*"],
                        "account_tags": {"Environment": "Production"},
                    },
                    "excluded": [r"^111111111111$", r"^prod-secret-.*$"],
                    "users": ["alice@example.com", "bob@example.com"],
                },
            ]
        }
        fake_ddb = MagicMock()
        fake_ddb.Table.return_value = fake_table

        with patch.object(boto3, "resource", return_value=fake_ddb):
            cfg = Configuration("cfg-table")
            cfg.load()

        assert "default" in cfg.templates
        assert cfg.templates["default"].permission_sets == ["Admin"]
        assert "prod-baseline" in cfg.account_templates
        assert cfg.account_templates["prod-baseline"].matcher.name_pattern == "prod-*"
        assert cfg.account_templates["prod-baseline"].matcher.name_patterns == ["prod-*-*"]
        assert cfg.account_templates["prod-baseline"].excluded == [r"^111111111111$", r"^prod-secret-.*$"]
        assert cfg.account_templates["prod-baseline"].users == [
            "alice@example.com",
            "bob@example.com",
        ]

    def test_load_paginates_until_last_evaluated_key_absent(self):
        fake_table = MagicMock()
        fake_table.scan.side_effect = [
            {
                "Items": [
                    {
                        "type": "template",
                        "group_name": "page1-template",
                        "permission_sets": ["ReadOnly"],
                        "description": "Page 1",
                    }
                ],
                "LastEvaluatedKey": {"assignment_id": "page1-last"},
            },
            {
                "Items": [
                    {
                        "type": "template",
                        "group_name": "page2-template",
                        "permission_sets": ["Admin"],
                        "description": "Page 2",
                    }
                ],
            },
        ]
        fake_ddb = MagicMock()
        fake_ddb.Table.return_value = fake_table

        with patch.object(boto3, "resource", return_value=fake_ddb):
            cfg = Configuration("cfg-table")
            cfg.load()

        assert fake_table.scan.call_count == 2
        assert fake_table.scan.call_args_list[1] == call(ExclusiveStartKey={"assignment_id": "page1-last"})
        assert "page1-template" in cfg.templates
        assert "page2-template" in cfg.templates

