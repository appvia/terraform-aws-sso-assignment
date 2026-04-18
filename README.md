# Terraform AWS SSO Assignment

This Terraform module deploys automation for **AWS IAM Identity Center (SSO)** account assignments. You define **permission-set templates** in **DynamoDB** (via [`modules/config`](./modules/config/): template name → list of permission set names). On each **member account**, you set an Organizations tag whose key is **`{prefix}/{template_name}`** (default prefix `sso`, e.g. `sso/default`) and whose **value** is a **comma-separated list of Identity Center group *display names***. The Lambda applies that template’s permission sets to each listed group for that account only. **EventBridge** runs the flow on a schedule and on new account creation; **Step Functions** adds retries and optional **SNS** failure notifications.

Use it when you want repeatable, infrastructure-as-code driven SSO assignments across many accounts without hand-managing each assignment in the console.

## Features

- **Template + account tags**: Each config entry is a **named template** (e.g. `default`, `finance`). Member accounts get tags `sso/<template>` (prefix configurable) listing which **IC groups** receive that template’s permission sets on that account.
- **Declarative templates**: The [`modules/config`](./modules/config/) submodule writes `group_name` (template name) and `permission_sets` to DynamoDB.
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
    DynamoDB (template name → permission sets) + account tags (which IC groups get the template)
           ↓
       IAM Identity Center + Organizations
```

### Components

| Piece | Role |
|-------|------|
| Root module | DynamoDB table, Lambda (via [terraform-aws-modules/lambda/aws](https://github.com/terraform-aws-modules/terraform-aws-lambda)), Step Functions, EventBridge rules and IAM. |
| `modules/config` | Populates DynamoDB: each key is a **template name** used in the account tag `sso/<key>`. |
| Lambda | Reads `ListTagsForResource` for each account, matches `sso/*` keys to templates, assigns permission sets to named IC groups (`assets/functions/handler.py`). |

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
  # Map keys = template names. On an account, tag sso/<key> = comma-separated IC group display names.
  groups_configuration = {
    default = {
      permission_sets = ["OrgReadOnly", "Developer-ReadOnly"]
      description     = "Baseline access — e.g. sso/default = Platform-ReadOnly,App-Developers"
    }
    breakglass = {
      permission_sets = ["BreakGlassAdmin"]
      description     = "Use tag sso/breakglass = SRE-Lead only where needed"
    }
  }

  sso_instance_arn = "arn:aws:sso:::instance/ssoins-xxxxxxxx"
}

module "sso_assignment" {
  # Pin a ref in production, e.g. ?ref=v1.0.0 — or use a relative path as in examples/basic.
  source = "git::https://github.com/appvia/terraform-aws-sso-assignment.git"

  sso_instance_arn = local.sso_instance_arn
  tags = {
    Project = "sso-assignment"
  }
}

module "config" {
  source = "git::https://github.com/appvia/terraform-aws-sso-assignment.git//modules/config"

  dynamodb_table_name  = module.sso_assignment.dynamodb_table_name
  groups_configuration = local.groups_configuration
}
```

### Example 2 — Named resources, schedule, and failure notifications

Use `name` to prefix resources (default is `lz-sso-assignment`). Point `sns_topic_arn` at a topic you already manage; the state machine publishes there when the Lambda response includes errors.

```hcl
module "sso_assignment" {
  source = "git::https://github.com/appvia/terraform-aws-sso-assignment.git"

  name             = "my-org-sso"
  sso_instance_arn = local.sso_instance_arn

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

  dynamodb_table_name  = module.sso_assignment.dynamodb_table_name
  groups_configuration = local.groups_configuration
}
```

### Example 3 — Run the repository example

From the clone:

```bash
cd examples/basic
# Set local.sso_instance_arn in main.tf (or extend the example with tfvars)
terraform init
terraform plan
terraform apply
```

See [examples/basic/README.md](./examples/basic/README.md) for more detail.

### How member-account tags work

1. **DynamoDB / Terraform** — `groups_configuration` is a map of **template name → permission set names** (and description / optional `enabled`). The map key is stored as `group_name` (e.g. `default`, `finance`).

2. **Member account (Organizations)** — set a tag on the **account** resource (12-digit account ID) with:
   - **Key**: `sso/<template_name>` (default prefix; override with module input `sso_account_tag_prefix` and env `SSO_ACCOUNT_TAG_PREFIX` on the Lambda).
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

Accounts with **no** `sso/*` tags in scope get **no** assignments from this mechanism (the run still succeeds for other accounts that do have such tags). Add or adjust tags, then let the next scheduled run (or a Step Functions execution) reconcile assignments.

### Configuration notes

- **`groups_configuration`** (on `modules/config`): top-level **keys are template names**; each value has `permission_sets` (list), `description` (string), optional `enabled` (default `true`). See [modules/config/README.md](./modules/config/README.md).
- **`sso_account_tag_prefix`** (root module, default `sso`): tag keys on accounts are `<prefix>/<template_name>`.
- **Not present in current releases:** the older DynamoDB attribute `account_tag_filters` and a Terraform `account_tags` field on `groups_configuration` (used to match arbitrary account tags) are **not** used and should not be written. Assignment is only driven by [`sso/<template>` tags](#how-member-account-tags-work) on each member account.
- **`sns_topic_arn`**: `null` (default) disables SNS steps in the state machine. Set it to an existing topic ARN; the module attaches `sns:Publish` on that ARN to the Step Functions role. Ensure the topic’s resource policy allows that role if your organization uses restrictive topic policies.

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

1. **Schedule**: EventBridge invokes the Step Functions workflow on `lambda_schedule`; the Lambda lists target accounts, reads each account’s `sso/*` tags, and creates assignments for each `sso/<template>` value against the matching DynamoDB template.
2. **New account**: An EventBridge rule matches Organizations `CreateAccount` CloudTrail events and starts the same state machine so new accounts can be included in the next reconciliation path defined by your state machine and Lambda contract.

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

- **Timeouts**: Increase `lambda_timeout` (and possibly `lambda_memory`) for large organizations.
- **SNS**: Ensure `sns_topic_arn` is set, the topic exists, and the Step Functions role can publish to it.
- **No assignments**: Confirm `module.config` has been applied and `aws dynamodb scan --table-name <name>` shows items for your groups.
- **No assignments for an account**: It may have no `sso/*` tags yet, or tag value group names do not **exactly** match Identity Center **DisplayName**; confirm with `aws identitystore list-groups` / console.

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
| <a name="input_lambda_memory"></a> [lambda\_memory](#input\_lambda\_memory) | Lambda function memory allocation in MB | `number` | `512` | no |
| <a name="input_lambda_runtime"></a> [lambda\_runtime](#input\_lambda\_runtime) | Lambda function runtime | `string` | `"python3.14"` | no |
| <a name="input_lambda_schedule"></a> [lambda\_schedule](#input\_lambda\_schedule) | EventBridge cron/rate schedule for Lambda execution | `string` | `"rate(10 minutes)"` | no |
| <a name="input_lambda_timeout"></a> [lambda\_timeout](#input\_lambda\_timeout) | Lambda function timeout in seconds | `number` | `60` | no |
| <a name="input_name"></a> [name](#input\_name) | Name for all resources i.e. handler, lambda, step function, event bridge, etc. | `string` | `"lz-sso-assignment"` | no |
| <a name="input_sns_topic_arn"></a> [sns\_topic\_arn](#input\_sns\_topic\_arn) | ARN of SNS topic for Step Function notifications (if null, notifications disabled) | `string` | `null` | no |
| <a name="input_sso_account_tag_prefix"></a> [sso\_account\_tag\_prefix](#input\_sso\_account\_tag\_prefix) | Account tag key prefix for permission-set templates. Keys are {prefix}/{template\_name} (e.g. sso/default) — see module README | `string` | `"Grants"` | no |
| <a name="input_tags"></a> [tags](#input\_tags) | Common tags to apply to all resources | `map(string)` | `{}` | no |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_dynamodb_table_arn"></a> [dynamodb\_table\_arn](#output\_dynamodb\_table\_arn) | ARN of the DynamoDB table storing group configurations |
| <a name="output_dynamodb_table_name"></a> [dynamodb\_table\_name](#output\_dynamodb\_table\_name) | Name of the DynamoDB table storing group configurations |
| <a name="output_eventbridge_rule_arns"></a> [eventbridge\_rule\_arns](#output\_eventbridge\_rule\_arns) | ARNs of EventBridge rules for account creation and cron schedule |
| <a name="output_lambda_function_arn"></a> [lambda\_function\_arn](#output\_lambda\_function\_arn) | ARN of the Lambda function for SSO group assignment |
| <a name="output_lambda_function_name"></a> [lambda\_function\_name](#output\_lambda\_function\_name) | Name of the Lambda function for SSO group assignment |
| <a name="output_step_function_arn"></a> [step\_function\_arn](#output\_step\_function\_arn) | ARN of the Step Function state machine orchestrating SSO assignments |
<!-- END_TF_DOCS -->
