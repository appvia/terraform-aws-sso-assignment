locals {
  ## Master switch for every alarm and metric filter in this file
  observability_enabled = var.enable_observability ? 1 : 0

  ## EventBridge rules watched for delivery failures. The account creation rule is
  ## only present when account triggers are enabled.
  monitored_event_rules = var.enable_observability ? merge(
    {
      cron_schedule = aws_cloudwatch_event_rule.cron_schedule.name
    },
    var.enable_account_triggers ? {
      account_creation = aws_cloudwatch_event_rule.account_creation[0].name
    } : {},
  ) : {}

  ## DynamoDB tables watched for throttling. Both are fully scanned on every run,
  ## so throttling here stalls reconciliation.
  monitored_dynamodb_tables = var.enable_observability ? {
    config   = aws_dynamodb_table.config.name
    tracking = aws_dynamodb_table.assignments_tracking.name
  } : {}

  ## Duration at which the Lambda is considered close enough to its timeout to warrant
  ## attention, expressed in milliseconds to match the CloudWatch Duration metric.
  lambda_duration_threshold_ms = var.lambda_timeout * 1000 * var.alarm_duration_threshold_percent / 100
}

## ---------------------------------------------------------------------------
## Application level signal
##
## The handler catches every exception and returns a result document rather than
## raising, so the Lambda Errors metric stays at zero for logic failures. These
## metric filters recover that signal from the structured JSON logs the handler
## already emits, without changing the handler's contract with the Step Function.
## ---------------------------------------------------------------------------

## Extract a metric from any ERROR level log line emitted by the handler
resource "aws_cloudwatch_log_metric_filter" "handler_errors" {
  count = local.observability_enabled

  name           = format("%s-handler-errors", var.name)
  log_group_name = module.lambda.lambda_cloudwatch_log_group_name
  pattern        = "{ $.level = \"ERROR\" }"

  metric_transformation {
    name      = "HandlerErrors"
    namespace = var.alarm_metric_namespace
    value     = "1"
    # Report zero when a batch contains no errors so the alarm has continuous data
    default_value = "0"
    unit          = "Count"
  }
}

## Extract a metric from the warnings raised when a group, user or permission set named
## in configuration cannot be resolved in Identity Center. The run still reports success,
## so this is otherwise invisible.
resource "aws_cloudwatch_log_metric_filter" "identity_resolution_failures" {
  count = local.observability_enabled

  name           = format("%s-identity-resolution-failures", var.name)
  log_group_name = module.lambda.lambda_cloudwatch_log_group_name
  pattern        = "{ $.level = \"WARNING\" && $.action = \"build_permissions\" }"

  metric_transformation {
    name      = "IdentityResolutionFailures"
    namespace = var.alarm_metric_namespace
    value     = "1"
    # Report zero when a batch contains no warnings so the alarm has continuous data
    default_value = "0"
    unit          = "Count"
  }
}

## Alarm when the handler logs an error
resource "aws_cloudwatch_metric_alarm" "handler_errors" {
  count = local.observability_enabled

  alarm_name          = format("%s-handler-errors", var.name)
  alarm_description   = "The SSO assignment handler logged one or more errors. Check the Lambda log group for entries with level ERROR."
  namespace           = var.alarm_metric_namespace
  metric_name         = aws_cloudwatch_log_metric_filter.handler_errors[0].metric_transformation[0].name
  statistic           = "Sum"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = 1
  period              = 300
  evaluation_periods  = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_sns_topic_arns
  ok_actions          = var.alarm_sns_topic_arns
  tags                = local.tags
}

## Alarm when configuration references identities that do not exist in Identity Center
resource "aws_cloudwatch_metric_alarm" "identity_resolution_failures" {
  count = local.observability_enabled

  alarm_name          = format("%s-identity-resolution-failures", var.name)
  alarm_description   = "One or more groups, users or permission sets named in account tags or templates could not be resolved in Identity Center. The affected access has not been granted."
  namespace           = var.alarm_metric_namespace
  metric_name         = aws_cloudwatch_log_metric_filter.identity_resolution_failures[0].metric_transformation[0].name
  statistic           = "Sum"
  comparison_operator = "GreaterThanThreshold"
  threshold           = var.alarm_identity_resolution_threshold
  period              = 300
  evaluation_periods  = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_sns_topic_arns
  ok_actions          = var.alarm_sns_topic_arns
  tags                = local.tags
}

## ---------------------------------------------------------------------------
## Step Function
##
## The state machine routes a non-null errors field to a Fail state, so failed
## executions are the primary signal that a run did not complete cleanly.
## ---------------------------------------------------------------------------

## Alarm when a Step Function execution fails
resource "aws_cloudwatch_metric_alarm" "step_function_executions_failed" {
  count = local.observability_enabled

  alarm_name          = format("%s-step-function-executions-failed", var.name)
  alarm_description   = "A SSO assignment Step Function execution failed. Inspect the execution history for the failing state."
  namespace           = "AWS/States"
  metric_name         = "ExecutionsFailed"
  statistic           = "Sum"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = 1
  period              = 300
  evaluation_periods  = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_sns_topic_arns
  ok_actions          = var.alarm_sns_topic_arns
  tags                = local.tags

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.main.arn
  }
}

## Alarm when a Step Function execution times out
resource "aws_cloudwatch_metric_alarm" "step_function_executions_timed_out" {
  count = local.observability_enabled

  alarm_name          = format("%s-step-function-executions-timed-out", var.name)
  alarm_description   = "A SSO assignment Step Function execution timed out before completing."
  namespace           = "AWS/States"
  metric_name         = "ExecutionsTimedOut"
  statistic           = "Sum"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = 1
  period              = 300
  evaluation_periods  = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_sns_topic_arns
  ok_actions          = var.alarm_sns_topic_arns
  tags                = local.tags

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.main.arn
  }
}

## Alarm when no executions have started for longer than expected. Without this, a
## disabled rule, a broken invoke role or a dead Pipe is indistinguishable from a
## healthy but quiet system - nothing else in this file detects reconciliation
## having silently stopped.
resource "aws_cloudwatch_metric_alarm" "step_function_no_executions" {
  count = local.observability_enabled

  alarm_name          = format("%s-step-function-no-executions", var.name)
  alarm_description   = format("No SSO assignment Step Function executions started in the last %d seconds. Reconciliation has stopped and account access will drift.", var.alarm_execution_staleness_period_seconds)
  namespace           = "AWS/States"
  metric_name         = "ExecutionsStarted"
  statistic           = "Sum"
  comparison_operator = "LessThanThreshold"
  threshold           = 1
  period              = var.alarm_execution_staleness_period_seconds
  evaluation_periods  = 1
  # An absent metric means nothing ran at all, which is exactly the condition we
  # are alarming on, so missing data must breach rather than be ignored.
  treat_missing_data = "breaching"
  alarm_actions      = var.alarm_sns_topic_arns
  ok_actions         = var.alarm_sns_topic_arns
  tags               = local.tags

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.main.arn
  }
}

## ---------------------------------------------------------------------------
## Lambda
##
## These catch the failure modes the handler cannot report on itself, because they
## kill the invocation before any result document is returned.
## ---------------------------------------------------------------------------

## Alarm on Lambda invocation errors i.e. timeouts, out of memory, init failures
resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  count = local.observability_enabled

  alarm_name          = format("%s-lambda-errors", var.name)
  alarm_description   = "The SSO assignment Lambda failed to complete an invocation. This covers timeouts, out of memory kills and initialisation failures rather than handled errors."
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  statistic           = "Sum"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = 1
  period              = 300
  evaluation_periods  = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_sns_topic_arns
  ok_actions          = var.alarm_sns_topic_arns
  tags                = local.tags

  dimensions = {
    FunctionName = module.lambda.lambda_function_name
  }
}

## Alarm when the Lambda is throttled
resource "aws_cloudwatch_metric_alarm" "lambda_throttles" {
  count = local.observability_enabled

  alarm_name          = format("%s-lambda-throttles", var.name)
  alarm_description   = "The SSO assignment Lambda was throttled and one or more invocations were rejected."
  namespace           = "AWS/Lambda"
  metric_name         = "Throttles"
  statistic           = "Sum"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = 1
  period              = 300
  evaluation_periods  = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_sns_topic_arns
  ok_actions          = var.alarm_sns_topic_arns
  tags                = local.tags

  dimensions = {
    FunctionName = module.lambda.lambda_function_name
  }
}

## Alarm when the Lambda approaches its configured timeout. Assignments are created
## serially with a polling wait on each, so runtime grows with organisation size and
## a timeout is a realistic outcome as the estate expands.
resource "aws_cloudwatch_metric_alarm" "lambda_duration" {
  count = local.observability_enabled

  alarm_name          = format("%s-lambda-duration", var.name)
  alarm_description   = format("The SSO assignment Lambda ran for more than %d%% of its %ds timeout. Consider raising lambda_timeout before invocations start being killed.", var.alarm_duration_threshold_percent, var.lambda_timeout)
  namespace           = "AWS/Lambda"
  metric_name         = "Duration"
  statistic           = "Maximum"
  comparison_operator = "GreaterThanThreshold"
  threshold           = local.lambda_duration_threshold_ms
  period              = 300
  evaluation_periods  = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_sns_topic_arns
  ok_actions          = var.alarm_sns_topic_arns
  tags                = local.tags

  dimensions = {
    FunctionName = module.lambda.lambda_function_name
  }
}

## ---------------------------------------------------------------------------
## Triggers
## ---------------------------------------------------------------------------

## Alarm when an EventBridge rule cannot deliver to the Step Function
resource "aws_cloudwatch_metric_alarm" "eventbridge_failed_invocations" {
  for_each = local.monitored_event_rules

  alarm_name          = format("%s-eventbridge-%s-failed-invocations", var.name, replace(each.key, "_", "-"))
  alarm_description   = format("The %s EventBridge rule failed to invoke the SSO assignment Step Function. Reconciliation is not being triggered.", each.key)
  namespace           = "AWS/Events"
  metric_name         = "FailedInvocations"
  statistic           = "Sum"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = 1
  period              = 300
  evaluation_periods  = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_sns_topic_arns
  ok_actions          = var.alarm_sns_topic_arns
  tags                = local.tags

  dimensions = {
    RuleName = each.value
  }
}

## Alarm when the EventBridge Pipe carrying config table changes fails
resource "aws_cloudwatch_metric_alarm" "pipes_execution_failed" {
  count = var.enable_observability && var.enable_config_triggers ? 1 : 0

  alarm_name          = format("%s-pipes-execution-failed", var.name)
  alarm_description   = "The EventBridge Pipe watching the config table failed to start a Step Function execution. Configuration changes are not being applied on write."
  namespace           = "AWS/Pipes"
  metric_name         = "ExecutionFailed"
  statistic           = "Sum"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = 1
  period              = 300
  evaluation_periods  = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_sns_topic_arns
  ok_actions          = var.alarm_sns_topic_arns
  tags                = local.tags

  dimensions = {
    PipeName = aws_pipes_pipe.config_update[0].name
  }
}

## ---------------------------------------------------------------------------
## DynamoDB
## ---------------------------------------------------------------------------

## Alarm when either table throttles requests
resource "aws_cloudwatch_metric_alarm" "dynamodb_throttled_requests" {
  for_each = local.monitored_dynamodb_tables

  alarm_name          = format("%s-dynamodb-%s-throttled-requests", var.name, each.key)
  alarm_description   = format("The %s DynamoDB table throttled one or more requests, which will leave reconciliation working from an incomplete view.", each.key)
  namespace           = "AWS/DynamoDB"
  metric_name         = "ThrottledRequests"
  statistic           = "Sum"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = 1
  period              = 300
  evaluation_periods  = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_sns_topic_arns
  ok_actions          = var.alarm_sns_topic_arns
  tags                = local.tags

  dimensions = {
    TableName = each.value
  }
}
