# Terraform AWS SSO Assignment

This Terraform module deploys automation for **AWS IAM Identity Center (SSO)** account assignments. You define **permission-set templates** in **DynamoDB** (via [`modules/config`](./modules/config/)): template name → list of permission set names. On each **member account**, you set an Organizations tag whose key is **`{prefix}/{template_name}`** (default prefix `sso`, e.g. `sso/default`) and whose **value** is a **comma-separated list of Identity Center group *display names***. The Lambda applies that template's permission sets to each listed group for that account only.

**New in this version:** **Account-level templates** allow you to auto-provision SSO assignments based on account **naming conventions**, **organizational units**, or **tags**—without requiring per-account tags. Combine this with tag-based assignment for flexible, scalable SSO management.

**EventBridge** runs the flow on a schedule and on new account creation; **Step Functions** adds retries and optional **SNS** failure notifications.

Use it when you want repeatable, infrastructure-as-code driven SSO assignments across many accounts without hand-managing each assignment in the console.

## Features

- **Templates + account tags** (existing): Define permission sets as **named templates** (e.g. `default`, `finance`). Member accounts get tags `sso/<template>` (prefix configurable) listing which **IC groups** receive that template's permission sets on that account.
- **Account-level templates** (NEW): Automatically apply templates to accounts matching **organizational unit paths**, **account name patterns**, or **account tags**. Uses logical AND matching: all conditions in a matcher must match for the template to apply.
- **Declarative templates**: The [`modules/config`](./modules/config/) submodule writes templates and account matchers to DynamoDB.
- **Tag precedence**: Explicit per-account tags override account-level matchers, allowing fine-grained control.
- **Two trigger modes**: Scheduled reconciliation (default `rate(10 minutes)`) and organization account-creation events (CloudTrail on `CreateAccount`).
- **Resilient orchestration**: Step Functions retries Lambda tasks (3 attempts, exponential backoff).
- **Optional alerting**: If you set `sns_topic_arn`, failed runs can publish error details to your existing SNS topic.
- **Flexible operations**: Tune Lambda memory, timeout, runtime, CloudWatch log retention, and DynamoDB billing mode.
- **Packaged handler**: Python Lambda source lives under [`assets/functions/`](./assets/functions/) (JSON logging, unit tests alongside the handler).

## Architecture

```
EventBridge (account creation / schedule)
           ↓
       Step Functions (retries, optional SNS on failure)
           ↓
       Lambda (reads DynamoDB + account tags, calls SSO + Organizations)
           ↓
DynamoDB (templates + account matchers) + account tags (which IC groups get the template)
           ↓
       IAM Identity Center + Organizations
```

### Components

| Piece | Role |
|-------|------|
| Root module | DynamoDB table, Lambda (via [terraform-aws-modules/lambda/aws](https://github.com/terraform-aws-modules/terraform-aws-lambda)), Step Functions, EventBridge rules and IAM. |
| `modules/config` | Populates DynamoDB: stores templates and account matchers. |
| Lambda | Reads account details (OU, name, tags), evaluates account matchers, loads `sso/*` tags, merges with tag precedence, assigns permission sets to named IC groups (`assets/functions/handler.py`). |

## Usage

### Prerequisites

- Terraform **>= 1.0** and AWS provider **>= 6.0** (see `terraform.tf`).
- IAM Identity Center enabled; permission sets and groups already exist (this module assigns groups to accounts—it does not create permission sets or IdP groups).
- Credentials with rights to deploy Lambda, DynamoDB, Step Functions, EventBridge, IAM, and (for the handler) SSO Admin, Organizations, and Identity Store actions.
- SSO instance ARN, for example:

  ```bash
  aws sso-admin list-instances --query 'Instances[0].InstanceArn' --output text
  ```

### Example 1 — Minimal stack (root module + config)

The root module creates the runtime; you almost always pair it with `modules/config` so DynamoDB contains your groups.

```hcl
locals {
  # Configuration with templates
  configuration = {
    templates = {
      default = {
        permission_sets = ["OrgReadOnly", "Developer-ReadOnly"]
        description     = "Baseline access — e.g. sso/default = Platform-ReadOnly,App-Developers"
      }
      breakglass = {
        permission_sets = ["BreakGlassAdmin"]
        description     = "Use tag sso/breakglass = SRE-Lead only where needed"
      }
    }
    # Optional: Account-level template matchers
    account_templates = {}
  }

  sso_instance_arn = "arn:aws:sso:::instance/ssoins-xxxxxxxx"
}

module "sso_assignment" {
  # Pin a ref in production, e.g. ?ref=v1.0.0 — or use a relative path as in examples/basic.
  source = "git::https://github.com/appvia/terraform-aws-sso-assignment.git"

  sso_instance_arn = local.sso_instance_arn
  # The module default is "Grant". Set this if you want tags like `sso/default`.
  sso_account_tag_prefix = "sso"
  tags = {
    Project = "sso-assignment"
  }
}

module "config" {
  source = "git::https://github.com/appvia/terraform-aws-sso-assignment.git//modules/config"

  dynamodb_table_name = module.sso_assignment.dynamodb_table_name
  configuration       = local.configuration
}
```

### Example 2 — Account-level templates with OU matching

Auto-provision production accounts with baseline permissions based on organizational unit:

```hcl
locals {
  configuration = {
    templates = {
      production = {
        permission_sets = ["Administrator", "ReadOnly"]
        description     = "Production account base permissions"
      }
      development = {
        permission_sets = ["Developer", "ReadOnly"]
        description     = "Development account permissions"
      }
    }
    account_templates = {
      prod_baseline = {
        description = "Auto-provision production accounts by OU"
        matcher = {
          # Matches accounts in organizational unit paths like:
          # "production/accounts/prod-workload-1"
          organizational_units = ["production/accounts/*"]
        }
        template_names = ["production"]
        groups         = ["ProdEngineers"]
      }
      
      dev_baseline = {
        description = "Auto-provision development accounts"
        matcher = {
          name_patterns = ["dev-.*]"
        }
        template_names = ["development"]
        groups         = ["DevEngineers"]
      }
    }
  }

  sso_instance_arn = "arn:aws:sso:::instance/ssoins-xxxxxxxx"
}

module "sso_assignment" {
  source = "git::https://github.com/appvia/terraform-aws-sso-assignment.git"

  sso_instance_arn = local.sso_instance_arn
  sso_account_tag_prefix = "sso"
  tags = {
    Project = "sso-assignment"
  }
}

module "config" {
  source = "git::https://github.com/appvia/terraform-aws-sso-assignment.git//modules/config"

  dynamodb_table_name = module.sso_assignment.dynamodb_table_name
  configuration       = local.configuration
}
```

### Example 3 — Account-level templates with tag matching

Auto-provision any account with specific tags (all conditions must match):

```hcl
locals {
  configuration = {
    templates = {
      managed_baseline = {
        permission_sets = ["ReadOnly", "Audit"]
        description     = "Baseline managed permissions"
      }
    }
    account_templates = {
      managed_by_tags = {
        description = "Auto-provision accounts with management tags (AND logic)"
        matcher = {
          account_tags = {
            Environment = "Production"
            ManagedBySSO = "true"
          }
        }
        template_names = ["managed_baseline"]
        groups         = ["Operations"]
      }
    }
  }

  sso_instance_arn = "arn:aws:sso:::instance/ssoins-xxxxxxxx"
}
```

### Example 4 — Named resources, schedule, and failure notifications

Use `name` to prefix resources (default is `lz-sso`). Point `sns_topic_arn` at a topic you already manage; the state machine publishes there when the Lambda response includes errors.

```hcl
module "sso_assignment" {
  source = "git::https://github.com/appvia/terraform-aws-sso-assignment.git"

  name             = "my-org-sso"
  sso_instance_arn = local.sso_instance_arn
  sso_account_tag_prefix = "sso"

  lambda_schedule = "rate(30 minutes)"
  lambda_timeout  = 120
  lambda_memory   = 1024

  sns_topic_arn = aws_sns_topic.sso_alerts.arn

  tags = {
    Environment = "production"
  }
}

module "config" {
  source = "git::https://github.com/appvia/terraform-aws-sso-assignment.git//modules/config"

  dynamodb_table_name = module.sso_assignment.dynamodb_table_name
  configuration       = local.configuration
}
```

### Example 5 — Run the repository example

From the clone:

```bash
cd examples/basic
# Set local.sso_instance_arn in main.tf (or extend the example with tfvars)
terraform init
terraform plan
terraform apply
```

See [examples/basic/README.md](./examples/basic/README.md) for more detail.

## Configuration

### Templates and Account Templates

The `configuration` variable (on `modules/config`) contains:

```hcl
configuration = {
  templates = {
    template_name = {
      permission_sets = ["PermissionSet1", "PermissionSet2"]
      description     = "Description of this template"
      enabled         = true  # optional, defaults to true
    }
  }
  
  account_templates = {
    matcher_name = {
      description = "Description of this account matcher"
      matcher = {
        # At least ONE of the following must be specified (others are optional)
        # All specified conditions must match (logical AND)
        
        organizational_units = ["trailing/ou/path/*", "prod"]  # OU trailing path patterns
        name_pattern         = "prod-*"                          # Account name glob pattern
        account_tags         = {                                 # Account tags (all must match)
          Environment = "Production"
          ManagedBySSO = "true"
        }
      }
      template_names = ["template_name"]  # Which templates to apply
      groups         = ["GroupName"]      # Which groups from those templates
    }
  }
}
```

### Assignment Precedence

When an account matches both **account templates** and has explicit **account tags**:

1. **Account tags take precedence** — explicit per-account configuration always wins
2. **Same template, different groups** — both sources' groups receive the permission sets
3. **Same template, same groups** — tag configuration is used, template configuration is skipped

Example:
- Account template matches and would assign: `(template="prod", groups=["ProdEngineers"])`
- Account tag says: `sso/prod = ProdEngineers,ProdAdmins`
- Result: Both `ProdEngineers` and `ProdAdmins` get the `prod` template's permission sets (tag augments the account template assignment)

### How member-account tags work

1. **DynamoDB / Terraform** — `configuration.templates` is a map of **template name → permission set names** (and description / optional `enabled`). The map key is stored as `group_name` (e.g. `default`, `finance`).

2. **Member account (Organizations)** — set a tag on the **account** resource (12-digit account ID) with:
   - **Key**: `<prefix>/<template_name>` where `<prefix>` is the module input `sso_account_tag_prefix` (default **`Grant`**).
     - If you want tags like `sso/default`, set `sso_account_tag_prefix = "sso"` on the root module (recommended for clarity).
   - **Value**: comma-separated **Identity Center group *display names*** (must match the Identity Store `DisplayName` exactly) that should receive **all** permission sets from that template on **this** account.

   You can set multiple template tags on one account, e.g. `sso/default` and `sso/finance` with different group lists.

**Example tags on an account** (vending / account factory / CLI):

```bash
aws organizations tag-resource \
  --resource-id 123456789012 \
  --tags \
    Key=sso/default,Value=App-Developers,App-ReadOnly-Users \
    Key=sso/finance,Value=Finance-Approvers
```

**Inspect tags the Lambda will read:**

```bash
aws organizations list-tags-for-resource --resource-id 123456789012
```

Accounts with **no** `sso/*` tags and **no matching account templates** get **no** assignments from this mechanism (the run still succeeds for other accounts). Add or adjust tags / matchers, then let the next scheduled run (or a Step Functions execution) reconcile assignments.

### Account Template Matcher Details

**Organizational Units** — Match by trailing OU path with glob patterns:
- OU path format: `/root-ou/prod/workloads` → trailing path: `prod/workloads`
- Patterns support `*` for wildcards (e.g. `prod/workloads/*`)
- At least one pattern must match for the condition to pass

**Account Name** — Match by account name with glob pattern:
- Single pattern string (e.g. `prod-*`)
- Uses fnmatch glob syntax

**Account Tags** — Match by account tags with AND logic:
- All specified tags must exist on the account
- All values must match exactly (case-sensitive)
- If no tags specified, this condition is skipped

### Migration from old `groups_configuration`

If you have existing Terraform using `groups_configuration` directly:

```hcl
# OLD (no longer supported)
module "config" {
  groups_configuration = {
    default = { ... }
  }
}

# NEW
module "config" {
  configuration = {
    templates = {
      default = { ... }
    }
    account_templates = {}  # Add account matchers here if desired
  }
}
```

### Configuration notes

- **`configuration`** (on `modules/config`): Contains `templates` (top-level keys are template names) and optional `account_templates` (name → matcher + template references). See [modules/config/README.md](./modules/config/README.md).
- **`sso_account_tag_prefix`** (root module, default `sso`): tag keys on accounts are `<prefix>/<template_name>`.
- **Backwards compatibility**: Older configurations using only tag-based assignment continue to work—just use `account_templates = {}` (or omit it, defaults to empty).

## Outputs

| Name | Description |
|------|-------------|
| `dynamodb_table_name` | Configuration table name (pass to `modules/config`). |
| `dynamodb_table_arn` | Table ARN. |
| `lambda_function_name` | Assignment Lambda name. |
| `lambda_function_arn` | Assignment Lambda ARN. |
| `step_function_arn` | State machine ARN (e.g. manual `start-execution`). |
| `eventbridge_rule_arns` | Map with `account_creation` and `cron_schedule` rule ARNs. |

## Module layout

```
terraform-aws-sso-assignment/
├── main.tf, variables.tf, outputs.tf, locals.tf, data.tf, terraform.tf
├── dynamodb.tf, lambda.tf, step_function.tf, eventbridge.tf
├── assets/functions/          # Lambda (handler.py, tests)
├── modules/config/            # DynamoDB item population
└── examples/basic/            # End-to-end sample
```

## Workflow behavior

1. **Schedule**: EventBridge invokes the Step Functions workflow on `lambda_schedule`; the Lambda lists target accounts, reads each account's organizational unit and tags, evaluates account templates, reads each account's `sso/*` tags, merges assignments with tag precedence, and creates assignments for each matched template against the matching DynamoDB template.
2. **New account**: An EventBridge rule matches Organizations `CreateAccount` CloudTrail events and starts the same state machine so new accounts can be included in the next reconciliation.

For low-level steps (retries, SNS on failure), inspect `step_function.tf` and `assets/functions/handler.py`.

## Deployment

```bash
terraform init
terraform plan
terraform apply
```

### Verify

```bash
TABLE_NAME=$(terraform output -raw dynamodb_table_name)
aws dynamodb describe-table --table-name "$TABLE_NAME"

FN=$(terraform output -raw lambda_function_name)
aws lambda get-function --function-name "$FN"

SF=$(terraform output -raw step_function_arn)
aws stepfunctions describe-state-machine --state-machine-arn "$SF"
```

## IAM (high level)

The module defines IAM for Lambda (DynamoDB read, SSO/Identity Store/Organizations APIs including `ListTagsForResource` on member accounts, logs), Step Functions (invoke Lambda, optional SNS publish), and EventBridge (start execution). Exact policies are in `data.tf` and `step_function.tf`.

## Troubleshooting

### No assignments created

- Confirm `module.config` has been applied and `aws dynamodb scan --table-name <name>` shows items for your templates.
- Check CloudWatch logs for the Lambda function to see if templates are loading correctly.
- Verify account templates matcher conditions are correct: all conditions in a matcher must match for the template to apply.

### Account template not matching

- Check account OU path: Use `aws organizations list-parents --child-id <account-id>` and build the full path
- Check account name: Use `aws organizations describe-account --account-id <account-id>`
- Check account tags: Use `aws organizations list-tags-for-resource --resource-id <account-id>`
- Enable DEBUG logging: Check Lambda CloudWatch logs for detailed matcher debug output

### No assignments for an account

- It may have no `sso/*` tags or matching account templates
- If using account templates, verify the matcher conditions all pass
- Confirm tag value group names match Identity Center **DisplayName** exactly (case-sensitive)
- Confirm with `aws identitystore list-groups` / console

### Timeouts

- Increase `lambda_timeout` (and possibly `lambda_memory`) for large organizations.

### SNS

- Ensure `sns_topic_arn` is set, the topic exists, and the Step Functions role can publish to it.

## Contributing

Contributions are welcome via issues and pull requests.

## License

See [LICENSE](./LICENSE).

<!-- BEGIN_TF_DOCS -->
## Providers

| Name | Version |
|------|---------|
| <a name="provider_aws"></a> [aws](#provider\_aws) | >= 6.0.0 |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_sso_instance_arn"></a> [sso\_instance\_arn](#input\_sso\_instance\_arn) | ARN of the AWS SSO instance | `string` | n/a | yes |
| <a name="input_cloudwatch_logs_kms_key_id"></a> [cloudwatch\_logs\_kms\_key\_id](#input\_cloudwatch\_logs\_kms\_key\_id) | KMS key ID for CloudWatch logs | `string` | `null` | no |
| <a name="input_cloudwatch_logs_log_group_class"></a> [cloudwatch\_logs\_log\_group\_class](#input\_cloudwatch\_logs\_log\_group\_class) | The class of the CloudWatch log group | `string` | `"STANDARD"` | no |
| <a name="input_cloudwatch_logs_retention_in_days"></a> [cloudwatch\_logs\_retention\_in\_days](#input\_cloudwatch\_logs\_retention\_in\_days) | The number of days to retain the CloudWatch logs | `number` | `30` | no |
| <a name="input_dynamodb_billing_mode"></a> [dynamodb\_billing\_mode](#input\_dynamodb\_billing\_mode) | DynamoDB billing mode (PAY\_PER\_REQUEST or PROVISIONED) | `string` | `"PAY_PER_REQUEST"` | no |
| <a name="input_enable_config_triggers"></a> [enable\_config\_triggers](#input\_enable\_config\_triggers) | Enable EventBridge Pipes to trigger Lambda when config table is updated | `bool` | `true` | no |
| <a name="input_lambda_memory"></a> [lambda\_memory](#input\_lambda\_memory) | Lambda function memory allocation in MB | `number` | `512` | no |
| <a name="input_lambda_runtime"></a> [lambda\_runtime](#input\_lambda\_runtime) | Lambda function runtime | `string` | `"python3.14"` | no |
| <a name="input_lambda_schedule"></a> [lambda\_schedule](#input\_lambda\_schedule) | EventBridge cron/rate schedule for Lambda execution | `string` | `"rate(60 minutes)"` | no |
| <a name="input_lambda_timeout"></a> [lambda\_timeout](#input\_lambda\_timeout) | Lambda function timeout in seconds | `number` | `300` | no |
| <a name="input_name"></a> [name](#input\_name) | Name for all resources i.e. handler, lambda, step function, event bridge, etc. | `string` | `"lz-sso"` | no |
| <a name="input_sns_topic_arn"></a> [sns\_topic\_arn](#input\_sns\_topic\_arn) | ARN of SNS topic for Step Function notifications (if null, notifications disabled) | `string` | `null` | no |
| <a name="input_sso_account_tag_prefix"></a> [sso\_account\_tag\_prefix](#input\_sso\_account\_tag\_prefix) | Account tag key prefix for permission-set templates. Keys are {prefix}/{template\_name} (e.g. sso/default) — see module README | `string` | `"Grant"` | no |
| <a name="input_tags"></a> [tags](#input\_tags) | Common tags to apply to all resources | `map(string)` | `{}` | no |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_assignments_tracking_table_arn"></a> [assignments\_tracking\_table\_arn](#output\_assignments\_tracking\_table\_arn) | ARN of the DynamoDB table tracking managed SSO assignments |
| <a name="output_assignments_tracking_table_name"></a> [assignments\_tracking\_table\_name](#output\_assignments\_tracking\_table\_name) | Name of the DynamoDB table tracking managed SSO assignments |
| <a name="output_dynamodb_table_arn"></a> [dynamodb\_table\_arn](#output\_dynamodb\_table\_arn) | ARN of the DynamoDB table storing group configurations |
| <a name="output_dynamodb_table_name"></a> [dynamodb\_table\_name](#output\_dynamodb\_table\_name) | Name of the DynamoDB table storing group configurations |
| <a name="output_eventbridge_rule_arns"></a> [eventbridge\_rule\_arns](#output\_eventbridge\_rule\_arns) | ARNs of EventBridge rules for account creation and cron schedule |
| <a name="output_lambda_function_arn"></a> [lambda\_function\_arn](#output\_lambda\_function\_arn) | ARN of the Lambda function for SSO group assignment |
| <a name="output_lambda_function_name"></a> [lambda\_function\_name](#output\_lambda\_function\_name) | Name of the Lambda function for SSO group assignment |
| <a name="output_step_function_arn"></a> [step\_function\_arn](#output\_step\_function\_arn) | ARN of the Step Function state machine orchestrating SSO assignments |
<!-- END_TF_DOCS -->
