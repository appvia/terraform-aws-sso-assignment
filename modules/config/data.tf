## Retrieve the DynamoDB table details
data "aws_dynamodb_table" "config" {
  name = var.dynamodb_table_name
}