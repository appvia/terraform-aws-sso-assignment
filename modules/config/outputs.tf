output "items_created" {
  description = "Count of DynamoDB items created for group configurations"
  value       = length(aws_dynamodb_table_item.group_configurations)
}

output "table_arn" {
  description = "ARN of the DynamoDB table"
  value       = data.aws_dynamodb_table.config.arn
}

output "table_name" {
  description = "Name of the DynamoDB table"
  value       = var.dynamodb_table_name
}
