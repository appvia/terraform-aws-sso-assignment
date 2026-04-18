# Basic example

This example deploys the root module plus `modules/config` with sample **permission-set templates**. Each key under `groups_configuration` (e.g. `default`, `finance`) is a **template name**; on each **member account** you set a tag `sso/<template_name>` whose **value** is a comma-separated list of **IAM Identity Center group display names** that should receive that template’s permission sets on that account. There is no `account_tag_filters` or legacy `account_tags` map on the configuration (those were removed).

## Overview

The example defines four templates: `default`, `finance`, `security`, and `operations`. See `main.tf` for the exact permission set names. After apply, **tag your accounts** (e.g. in your account-vending pipeline) with keys like `sso/default` and values like `My-IC-Group,Another-IC-Group` so the Lambda can create assignments.

## Prerequisites

1. AWS Organizations with a management account that can tag member accounts and deploy the stack.
2. IAM Identity Center enabled; permission sets and groups already exist; group **DisplayName** values must match the comma-separated names in your `sso/*` tag values.
3. Terraform `>= 1.0` and the AWS provider as required by the module.

Find your Identity Center instance ARN:

```bash
aws sso-admin list-instances --query 'Instances[0].InstanceArn' --output text
```

## Usage

1. Edit `main.tf` and set `local.sso_instance_arn` to your instance ARN.
2. Optionally set `local.sns_topic_arn` to an existing SNS topic ARN (or leave `null` to disable failure notifications to SNS).

```bash
terraform init
terraform plan
terraform apply
```

## Tag member accounts

After deploy, set tags on each target account (12-digit account ID as resource id):

```bash
aws organizations tag-resource \
  --resource-id 123456789012 \
  --tags Key=sso/default,Value=App-Developers,App-ReadOnly-Users
```

Use `sso/<template_name>` for each key in `groups_configuration`. Wait for the scheduled run or start the Step Function (below).

## Verify

```bash
TABLE_NAME=$(terraform output -raw dynamodb_table_name)
aws dynamodb describe-table --table-name "$TABLE_NAME"

aws dynamodb scan --table-name "$TABLE_NAME" --projection-expression "group_name,permission_sets"

FUNCTION_NAME=$(terraform output -raw lambda_function_name)
aws lambda get-function --function-name "$FUNCTION_NAME"

aws organizations list-tags-for-resource --resource-id 123456789012
```

## Test the Step Function (reconcile all accounts in org)

```bash
STEP_FUNCTION_ARN=$(terraform output -raw step_function_arn)

aws stepfunctions start-execution \
  --state-machine-arn "$STEP_FUNCTION_ARN" \
  --input '{"source":"cron_schedule"}'

aws stepfunctions list-executions --state-machine-arn "$STEP_FUNCTION_ARN"
```

## Customization

### Add a template

Add an entry in `local.groups_configuration` in `main.tf` (e.g. `data_engineers` with `permission_sets` and `description`), apply, then use the tag `sso/data_engineers` on accounts with a comma-separated list of IC group display names.

### Tune the root module

Pass through on `module "sso_assignment"` in `main.tf`, for example: `lambda_timeout`, `lambda_memory`, `lambda_schedule`, `sso_account_tag_prefix` (default `sso`), `sns_topic_arn` via the module block.

**Notifications:** the root variable is `sns_topic_arn` (bring your own topic), not an email string.

## Cleanup

```bash
terraform destroy
```

## References

- [AWS IAM Identity Center](https://docs.aws.amazon.com/singlesignon/)
- [AWS Organizations tagging](https://docs.aws.amazon.com/organizations/latest/userguide/orgs_tagging.html)
- [Step Functions](https://docs.aws.amazon.com/step-functions/)
- [Repository README](../../README.md) for the full `sso/<template>` model

<!-- BEGIN_TF_DOCS -->
## Providers

No providers.

## Inputs

No inputs.

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_dynamodb_table_arn"></a> [dynamodb\_table\_arn](#output\_dynamodb\_table\_arn) | ARN of the DynamoDB configuration table |
| <a name="output_dynamodb_table_name"></a> [dynamodb\_table\_name](#output\_dynamodb\_table\_name) | Name of the DynamoDB configuration table |
| <a name="output_eventbridge_rule_arns"></a> [eventbridge\_rule\_arns](#output\_eventbridge\_rule\_arns) | ARNs of EventBridge rules |
| <a name="output_lambda_function_arn"></a> [lambda\_function\_arn](#output\_lambda\_function\_arn) | ARN of the Lambda function |
| <a name="output_lambda_function_name"></a> [lambda\_function\_name](#output\_lambda\_function\_name) | Name of the Lambda function |
| <a name="output_step_function_arn"></a> [step\_function\_arn](#output\_step\_function\_arn) | ARN of the Step Function state machine |
<!-- END_TF_DOCS -->
