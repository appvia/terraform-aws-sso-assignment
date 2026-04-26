locals {
  ## Name of the table from the supplied ARN (format: arn:aws:dynamodb:{region}:{account_id}:table/{table_name})
  table_name = element(split("/", var.dynamodb_table_arn), 1)
}
