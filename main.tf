## DynamoDB table for storing SSO group to permission set mappings
## Uses composite key: group_name (hash) + type (range) to support multiple item types
## (templates and account_templates) with the same name
resource "aws_dynamodb_table" "config" {
  billing_mode     = var.dynamodb_billing_mode
  hash_key         = "group_name"
  name             = format("%s-config", var.name)
  range_key        = "type"
  stream_enabled   = var.enable_config_triggers
  stream_view_type = "NEW_AND_OLD_IMAGES"
  tags             = local.tags

  attribute {
    name = "group_name"
    type = "S"
  }

  attribute {
    name = "type"
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
  description   = "Lambda function used for the automation of SSO assignments based on templates and account tagging"
  handler       = "handler.lambda_handler"
  runtime       = var.lambda_runtime
  timeout       = var.lambda_timeout
  tags          = var.tags

  source_path = [
    {
      path = "${path.module}/assets/functions"
      patterns = [
        "!test_.*\\.py",
        "!__pycache__",
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

## Provision a IAM role for the EventBridge Pipes
resource "aws_iam_role" "eventbridge_pipes" {
  count = var.enable_config_triggers ? 1 : 0

  assume_role_policy = data.aws_iam_policy_document.eventbridge_pipes_assume_role.json
  name               = format("%s-eventbridge-pipes", var.name)
  tags               = local.tags
}

## Attach policy to EventBridge Pipes role
resource "aws_iam_role_policy" "eventbridge_pipes_policy" {
  count = var.enable_config_triggers ? 1 : 0

  name   = format("%s-eventbridge-pipes", var.name)
  role   = aws_iam_role.eventbridge_pipes[0].id
  policy = data.aws_iam_policy_document.eventbridge_pipes_policy.json
}

## Provide the EventBridge role the ability to invoke the Step Function
resource "aws_iam_role_policy" "eventbridge_step_function" {
  name   = format("%s-eventbridge-step-function", var.name)
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
## Using EventBridge Pipes for reliable DynamoDB stream-based triggering
resource "aws_pipes_pipe" "config_update" {
  count = var.enable_config_triggers ? 1 : 0

  name        = format("%s-config-update", var.name)
  description = "EventBridge Pipe to trigger SSO assignment when config table is updated"
  role_arn    = aws_iam_role.eventbridge_pipes[0].arn
  source      = aws_dynamodb_table.config.stream_arn
  target      = aws_sfn_state_machine.main.arn
  tags        = local.tags

  source_parameters {
    dynamodb_stream_parameters {
      starting_position = "LATEST"
      batch_size        = 1
      # Explicitly set these to avoid provider/default update bugs and satisfy
      # AWS validation constraints.
      maximum_record_age_in_seconds      = -1
      maximum_batching_window_in_seconds = 0
    }
  }
  target_parameters {
    step_function_state_machine_parameters {
      invocation_type = "FIRE_AND_FORGET"
    }
    input_template = jsonencode({
      account_id = "$.detail.requestParameters.accountId"
      region     = "$.detail.awsRegion"
      source     = "config_update"
      time       = "$.detail.eventTime"
    })
  }

  depends_on = [aws_iam_role_policy.eventbridge_pipes_policy]
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

