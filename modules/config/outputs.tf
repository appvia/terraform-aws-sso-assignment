output "configuration" {
  description = "Count of DynamoDB items created for group configurations"
  value = {
    account_templates = try(var.configuration.account_templates, null)
    templates         = try(var.configuration.templates, null)
  }
}

output "table_arn" {
  description = "ARN of the DynamoDB table"
  value       = data.aws_dynamodb_table.config.arn
}

output "table_name" {
  description = "Name of the DynamoDB table"
  value       = var.dynamodb_table_name
}
