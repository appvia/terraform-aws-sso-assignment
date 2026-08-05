output "cloudwatch_alarm_arns" {
  description = "Map of alarm name to ARN for the CloudWatch alarms provisioned when enable_observability is true, empty otherwise"
  value = merge(
    { for v in aws_cloudwatch_metric_alarm.handler_errors : "handler_errors" => v.arn },
    { for v in aws_cloudwatch_metric_alarm.identity_resolution_failures : "identity_resolution_failures" => v.arn },
    { for v in aws_cloudwatch_metric_alarm.step_function_executions_failed : "step_function_executions_failed" => v.arn },
    { for v in aws_cloudwatch_metric_alarm.step_function_executions_timed_out : "step_function_executions_timed_out" => v.arn },
    { for v in aws_cloudwatch_metric_alarm.step_function_no_executions : "step_function_no_executions" => v.arn },
    { for v in aws_cloudwatch_metric_alarm.lambda_errors : "lambda_errors" => v.arn },
    { for v in aws_cloudwatch_metric_alarm.lambda_throttles : "lambda_throttles" => v.arn },
    { for v in aws_cloudwatch_metric_alarm.lambda_duration : "lambda_duration" => v.arn },
    { for v in aws_cloudwatch_metric_alarm.pipes_execution_failed : "pipes_execution_failed" => v.arn },
    { for k, v in aws_cloudwatch_metric_alarm.eventbridge_failed_invocations : "eventbridge_${k}_failed_invocations" => v.arn },
    { for k, v in aws_cloudwatch_metric_alarm.dynamodb_throttled_requests : "dynamodb_${k}_throttled_requests" => v.arn },
  )
}

output "config_dynamodb_table_arn" {
  description = "ARN of the DynamoDB table storing group configurations"
  value       = aws_dynamodb_table.config.arn
}

output "config_dynamodb_table_name" {
  description = "Name of the DynamoDB table storing group configurations"
  value       = aws_dynamodb_table.config.name
}

output "eventbridge_invoke_role_arn" {
  description = "ARN of EventBridge roles for account creation and cron schedule"
  value       = aws_iam_role.eventbridge_invoke.arn
}

output "eventbridge_rule_arns" {
  description = "ARNs of EventBridge rules for account creation and cron schedule"
  value = {
    account_creation = try(aws_cloudwatch_event_rule.account_creation[0].arn, null)
    cron_schedule    = aws_cloudwatch_event_rule.cron_schedule.arn
    config_update    = try(aws_pipes_pipe.config_update[0].arn, null)
  }
}

output "eventbridge_rule_names" {
  description = "Names of EventBridge rules for account creation and cron schedule"
  value = {
    account_creation = try(aws_cloudwatch_event_rule.account_creation[0].name, null)
    cron_schedule    = aws_cloudwatch_event_rule.cron_schedule.name
    config_update    = try(aws_pipes_pipe.config_update[0].name, null)
  }
}

output "lambda_cloudwatch_log_group_arn" {
  description = "ARN of the CloudWatch log group receiving the Lambda function logs"
  value       = module.lambda.lambda_cloudwatch_log_group_arn
}

output "lambda_cloudwatch_log_group_name" {
  description = "Name of the CloudWatch log group receiving the Lambda function logs"
  value       = module.lambda.lambda_cloudwatch_log_group_name
}

output "lambda_function_arn" {
  description = "ARN of the Lambda function for SSO group assignment"
  value       = module.lambda.lambda_function_arn
}

output "lambda_function_name" {
  description = "Name of the Lambda function for SSO group assignment"
  value       = module.lambda.lambda_function_name
}

output "lambda_policy_json" {
  description = "IAM policy document (JSON) attached to the Lambda role via policy_json"
  value       = data.aws_iam_policy_document.lambda_policy.json
}

output "step_function_arn" {
  description = "ARN of the Step Function state machine orchestrating SSO assignments"
  value       = aws_sfn_state_machine.main.arn
}

output "tracking_dynamodb_table_arn" {
  description = "ARN of the DynamoDB table tracking managed SSO assignments"
  value       = aws_dynamodb_table.assignments_tracking.arn
}

output "tracking_dynamodb_table_name" {
  description = "Name of the DynamoDB table tracking managed SSO assignments"
  value       = aws_dynamodb_table.assignments_tracking.name
}
