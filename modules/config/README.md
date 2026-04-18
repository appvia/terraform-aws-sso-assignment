# Config Module

This module populates a DynamoDB table with **permission-set templates**: each key is a **template name** that must match the suffix in the account tag `sso/<template_name>` (see root module README). The Lambda assigns each template’s permission sets to the Identity Center groups named in the tag’s comma-separated value.

## Usage

```hcl
module "config" {
  source = "./modules/config"

  dynamodb_table_name  = module.sso_assignment.dynamodb_table_name
  groups_configuration = var.groups_configuration
}
```

## Features

- **Automated Configuration**: Converts group configuration maps into DynamoDB items
- **Metadata Tracking**: Stores description and `updated_at` for each template
- **Flexible Schema**: Optional `enabled` per template
- **Reusable**: Can be called independently to update configurations

## Configuration format

The **map key** (e.g. `default`, `finance`) is the `group_name` / template name; use it in the account tag: `sso/<key>` (with the root module’s tag prefix, default `sso`).

For each map entry:
- `permission_sets` (required): IAM Identity Center **permission set names** to apply when this template is selected on an account
- `description` (required): Human-readable text (documentation only)
- `enabled` (optional, default: true): When false, this template is skipped

Example:
```hcl
groups_configuration = {
  default = {
    permission_sets = ["ReadOnly", "PowerUser"]
    description     = "On each account, tag: sso/default = Team-A, Team-B (IC group display names)"
  }
  finance = {
    permission_sets = ["FinanceAdmin", "FinanceReadOnly"]
    description     = "sso/finance = Global-Finance (example)"
  }
}
```

## DynamoDB item structure

- `group_name` (hash key): Template name (same as the Terraform map key)
- `permission_sets`: String set of permission set names
- `enabled`, `description`, `updated_at`

<!-- BEGIN_TF_DOCS -->
## Providers

| Name | Version |
|------|---------|
| <a name="provider_aws"></a> [aws](#provider\_aws) | >= 6.0.0 |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_dynamodb_table_name"></a> [dynamodb\_table\_name](#input\_dynamodb\_table\_name) | Name of the DynamoDB table for storing group configurations | `string` | n/a | yes |
| <a name="input_groups_configuration"></a> [groups\_configuration](#input\_groups\_configuration) | Permission-set templates keyed by template name; each key must match the account tag suffix {prefix}/{key} (e.g. sso/<key> when prefix is sso) | <pre>map(object({<br/>    # List of permission sets to assign to the group<br/>    permission_sets = list(string)<br/>    # Description of the group<br/>    description = string<br/>    # Whether the group is enabled<br/>    enabled = optional(bool, true)<br/>  }))</pre> | n/a | yes |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_items_created"></a> [items\_created](#output\_items\_created) | Count of DynamoDB items created for group configurations |
| <a name="output_table_arn"></a> [table\_arn](#output\_table\_arn) | ARN of the DynamoDB table |
| <a name="output_table_name"></a> [table\_name](#output\_table\_name) | Name of the DynamoDB table |
<!-- END_TF_DOCS -->