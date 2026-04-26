# Config Module

This module populates the root module’s DynamoDB **configuration** table with:

- **Permission-set templates**: template name → permission set names (and description)
- **Account templates** (optional): matchers that auto-apply templates to accounts based on OU path, account name patterns, and/or account tags

## Usage

```hcl
module "config" {
  source = "./modules/config"

  dynamodb_table_arn = module.sso_assignment.config_dynamodb_table_arn
  configuration      = var.configuration
}
```

## Features

- **Templates**: Converts `configuration.templates` into DynamoDB items (`type = "template"`)
- **Account templates (optional)**: Converts `configuration.account_templates` into DynamoDB items (`type = "account_template"`)
- **Idempotent updates**: Re-applying updates items in-place

## Configuration format

The **template map key** (e.g. `default`, `finance`) is the template name.

Accounts can reference templates in two ways (see root `README.md` for full flow):

- **Account tags**: an Organizations account tag key `<prefix>/<template_name>` where `<prefix>` is the root module input `sso_account_tag_prefix` (Terraform default is `"Grant"`).
- **Account templates**: matchers stored in DynamoDB that the Lambda evaluates for each account.

### Templates

Example:

```hcl
configuration = {
  templates = {
    default = {
      permission_sets = ["ReadOnly", "PowerUser"]
      description     = "Baseline access (tag example: Grant/default = Team-A,Team-B)"
    }
    finance = {
      permission_sets = ["FinanceAdmin", "FinanceReadOnly"]
      description     = "Finance access (tag example: Grant/finance = Global-Finance)"
    }
  }

  # Optional
  account_templates = {}
}
```

## DynamoDB item structure

- **Primary key**: `group_name` (hash key) + `type` (range key)
- **Templates** (`type = "template"`):
  - `group_name`: template name (same as the `configuration.templates` map key)
  - `permission_sets`: string set of permission set names
  - `description`
- **Account templates** (`type = "account_template"`):
  - `group_name`: account template name (same as the `configuration.account_templates` map key)
  - `matcher`: matcher object (OU/name patterns/account tags)
  - `template_names`: which templates to apply
  - `groups`: which Identity Center group display names should receive those templates’ permission sets
  - `excluded` (optional): list of exclusion regexes

<!-- BEGIN_TF_DOCS -->
## Providers

| Name | Version |
|------|---------|
| <a name="provider_aws"></a> [aws](#provider\_aws) | >= 6.0.0 |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_configuration"></a> [configuration](#input\_configuration) | SSO configuration containing templates and account-level template matchers | <pre>object({<br/>    # Permission-set templates keyed by template name<br/>    # Each key can be referenced by account tags ({prefix}/{key}) or account templates<br/>    templates = map(object({<br/>      # List of permission sets to assign to the group<br/>      permission_sets = list(string)<br/>      # Description of the template<br/>      description = string<br/>    }))<br/><br/>    # Account-level templates: auto-provision accounts matching conditions<br/>    # Optional - default empty (no account-level matching)<br/>    account_templates = optional(map(object({<br/>      # Description of this account template matcher<br/>      description = string<br/>      # Exclude accounts that match this pattern - supports python re syntax<br/>      excluded = optional(list(string))<br/>      # List of template names to apply to matching accounts<br/>      template_names = list(string)<br/>      # List of groups from those templates to assign<br/>      # These groups will receive the permission sets defined in the templates<br/>      groups = list(string)<br/>      # Matcher conditions (logical AND: all specified conditions must match)<br/>      matcher = object({<br/>        # Match by organizational unit trailing path with glob patterns<br/>        # e.g., ["production/accounts/*", "prod"]<br/>        organizational_units = optional(list(string))<br/>        # Match by account name with glob pattern<br/>        # e.g., "prod-*"<br/>        name_patterns = optional(list(string))<br/>        # Match by account tags (all specified tags must exist and match)<br/>        # e.g., { Environment = "Production", CostCenter = "Engineering" }<br/>        account_tags = optional(map(string))<br/>      })<br/>    })), {})<br/>  })</pre> | n/a | yes |
| <a name="input_dynamodb_table_arn"></a> [dynamodb\_table\_arn](#input\_dynamodb\_table\_arn) | ARN of the DynamoDB table for storing configuration | `string` | n/a | yes |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_configuration"></a> [configuration](#output\_configuration) | Count of DynamoDB items created for group configurations |
| <a name="output_dynamodb_table_arn"></a> [dynamodb\_table\_arn](#output\_dynamodb\_table\_arn) | ARN of the DynamoDB table where configurations are stored |
| <a name="output_dynamodb_table_name"></a> [dynamodb\_table\_name](#output\_dynamodb\_table\_name) | Name of the DynamoDB table where configurations are stored |
<!-- END_TF_DOCS -->