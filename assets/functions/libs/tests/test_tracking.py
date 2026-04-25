from __future__ import annotations

from unittest.mock import MagicMock, patch

import boto3

from libs.tracking import Tracking


class TestTracking:
    def test_get_assignment_id(self):
        tr = Tracking.__new__(Tracking)
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

        with patch.object(boto3, "resource", return_value=fake_ddb):
            tr = Tracking("tracking-table")
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

        with patch.object(boto3, "resource", return_value=fake_ddb):
            tr = Tracking("tracking-table")
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
        with patch.object(boto3, "resource", return_value=fake_ddb):
            tr = Tracking("tracking-table")
            tr.delete("a#p#arn")
        fake_table.delete_item.assert_called_once_with(Key={"assignment_id": "a#p#arn"})

