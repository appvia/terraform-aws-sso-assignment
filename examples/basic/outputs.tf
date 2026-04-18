output "dynamodb_table_arn" {
  description = "ARN of the DynamoDB configuration table"
  value       = module.sso_assignment.dynamodb_table_arn
}

output "dynamodb_table_name" {
  description = "Name of the DynamoDB configuration table"
  value       = module.sso_assignment.dynamodb_table_name
}

output "eventbridge_rule_arns" {
  description = "ARNs of EventBridge rules"
  value       = module.sso_assignment.eventbridge_rule_arns
}

output "lambda_function_arn" {
  description = "ARN of the Lambda function"
  value       = module.sso_assignment.lambda_function_arn
}

output "lambda_function_name" {
  description = "Name of the Lambda function"
  value       = module.sso_assignment.lambda_function_name
}

output "step_function_arn" {
  description = "ARN of the Step Function state machine"
  value       = module.sso_assignment.step_function_arn
}
