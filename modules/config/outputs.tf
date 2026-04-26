output "configuration" {
  description = "Count of DynamoDB items created for group configurations"
  value = {
    account_templates = try(var.configuration.account_templates, null)
    templates         = try(var.configuration.templates, null)
  }
}

output "dynamodb_table_name" {
  description = "Name of the DynamoDB table where configurations are stored"
  value       = local.table_name
}

output "dynamodb_table_arn" {
  description = "ARN of the DynamoDB table where configurations are stored"
  value       = var.dynamodb_table_arn
}
