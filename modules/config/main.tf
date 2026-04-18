## Create DynamoDB items for each group in the configuration
## Each item stores the group name, associated permission sets, and metadata
resource "aws_dynamodb_table_item" "group_configurations" {
  for_each = var.groups_configuration

  hash_key = "group_name"
  item = jsonencode({
    group_name = {
      S = each.key
    }
    description = {
      S = each.value.description
    }
    enabled = {
      BOOL = each.value.enabled
    }
    permission_sets = {
      SS = each.value.permission_sets
    }
    updated_at = {
      S = timestamp()
    }
  })
  table_name = var.dynamodb_table_name
}
