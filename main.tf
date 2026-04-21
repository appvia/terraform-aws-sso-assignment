## DynamoDB table for storing SSO group to permission set mappings
resource "aws_dynamodb_table" "config" {
  billing_mode = var.dynamodb_billing_mode
  hash_key     = "group_name"
  name         = format("%s-config", var.name)
  tags         = local.tags

  attribute {
    name = "group_name"
    type = "S"
  }
}

## DynamoDB table for tracking managed SSO account assignments
## Used to differentiate between assignments created by this module and external assignments
resource "aws_dynamodb_table" "assignments_tracking" {
  billing_mode = var.dynamodb_billing_mode
  hash_key     = "assignment_id"
  name         = format("%s-tracking", var.name)
  tags         = local.tags

  attribute {
    name = "assignment_id"
    type = "S"
  }
}

## Lambda function for SSO group assignment using terraform-aws-modules/lambda/aws
module "lambda" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "8.7.0"

  architectures = ["arm64"]
  function_name = var.name
  memory_size   = var.lambda_memory
  function_tags = var.tags
  description   = "Lambda function for SSO group assignment"
  handler       = "handler.lambda_handler"
  runtime       = var.lambda_runtime
  timeout       = var.lambda_timeout
  tags          = var.tags

  source_path = [
    {
      path = "${path.module}/assets/functions"
      patterns = [
        "!test_.*\\.py",
      ]
    }
  ]

  environment_variables = {
    DYNAMODB_CONFIG_TABLE   = aws_dynamodb_table.config.name
    DYNAMODB_TRACKING_TABLE = aws_dynamodb_table.assignments_tracking.name
    SSO_ACCOUNT_TAG_PREFIX  = var.sso_account_tag_prefix
    SSO_INSTANCE_ARN        = var.sso_instance_arn
  }

  ## Lambda Role
  create_role                   = true
  role_force_detach_policies    = true
  role_maximum_session_duration = 3600
  role_name                     = var.name
  role_path                     = "/"
  role_permissions_boundary     = null
  role_tags                     = var.tags

  ## IAM Policy
  attach_cloudwatch_logs_policy = true
  attach_network_policy         = false
  attach_policy_json            = true
  attach_tracing_policy         = true
  policy_json                   = data.aws_iam_policy_document.lambda_policy.json

  ## Cloudwatch Logs
  cloudwatch_logs_kms_key_id        = var.cloudwatch_logs_kms_key_id
  cloudwatch_logs_log_group_class   = var.cloudwatch_logs_log_group_class
  cloudwatch_logs_retention_in_days = var.cloudwatch_logs_retention_in_days
  cloudwatch_logs_tags              = var.tags
}

## Provision a IAM role for the EventBridge
resource "aws_iam_role" "eventbridge" {
  assume_role_policy = data.aws_iam_policy_document.eventbridge_assume_role.json
  name               = format("%s-eventbridge", var.name)
  tags               = local.tags
}

## Provide the EventBridge role the ability to invoke the Step Function
resource "aws_iam_role_policy" "eventbridge_step_function" {
  name   = "sso-assignment-eventbridge-step-function"
  role   = aws_iam_role.eventbridge.id
  policy = data.aws_iam_policy_document.eventbridge_invoke_step_function.json
}

## Provision a EventBridge rule for the periodic cron schedule
resource "aws_cloudwatch_event_rule" "cron_schedule" {
  description         = "Used to trigger the SSO assignment Lambda function on a periodic schedule"
  name                = format("%s-cron-schedule", var.name)
  state               = "ENABLED"
  schedule_expression = var.lambda_schedule
  tags                = local.tags
}

## Provision a EventBridge rule for the AWS Organizations account creation events
resource "aws_cloudwatch_event_rule" "account_creation" {
  description    = "Used to trigger the SSO assignment Lambda function when a new account is created in the AWS Organizations"
  event_bus_name = "default"
  name           = format("%s-account-creation", var.name)
  state          = "ENABLED"
  tags           = local.tags

  event_pattern = jsonencode({
    source      = ["aws.organizations"]
    detail-type = ["AWS API Call via CloudTrail"]
    detail = {
      eventName   = ["CreateAccount"]
      eventSource = ["organizations.amazonaws.com"]
    }
  })
}

## Provision an event to trigger the Lambda function when a tracking in the config table is updated
resource "aws_cloudwatch_event_rule" "config_update" {
  description = "Used to trigger the SSO assignment Lambda function when a tracking in the config table is updated"
  name        = format("%s-config-update", var.name)
  state       = "ENABLED"
  tags        = local.tags

  event_pattern = jsonencode({
    source      = ["aws.dynamodb"]
    detail-type = ["Table Update"]
    detail = {
      tableName = [aws_dynamodb_table.config.name]
    }
  })
}

## Provision a EventBridge rule for the AWS Organizations account creation events
resource "aws_cloudwatch_event_target" "account_creation_target" {
  arn      = aws_sfn_state_machine.main.arn
  role_arn = aws_iam_role.eventbridge.arn
  rule     = aws_cloudwatch_event_rule.account_creation.name
}

## Cron schedule event rule target for invoking Step Function
resource "aws_cloudwatch_event_target" "cron_schedule_target" {
  arn      = aws_sfn_state_machine.main.arn
  role_arn = aws_iam_role.eventbridge.arn
  rule     = aws_cloudwatch_event_rule.cron_schedule.name
}

