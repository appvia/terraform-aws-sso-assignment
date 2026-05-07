## Create DynamoDB items for each template in the configuration
## Each item stores the template name, associated permission sets, and metadata
resource "aws_dynamodb_table_item" "templates" {
  for_each = var.configuration.templates

  hash_key   = "group_name"
  range_key  = "type"
  table_name = local.table_name

  item = jsonencode({
    group_name = {
      S = each.key
    }
    type = {
      S = "template"
    }
    description = {
      S = each.value.description
    }
    permission_sets = {
      SS = each.value.permission_sets
    }
  })
}

## Create DynamoDB items for each account template matcher
## These enable auto-provisioning of accounts matching specific conditions
resource "aws_dynamodb_table_item" "account_templates" {
  for_each = var.configuration.account_templates

  hash_key   = "group_name"
  range_key  = "type"
  table_name = local.table_name

  item = jsonencode(merge(
    {
      group_name = {
        S = each.key
      }
      type = {
        S = "account_template"
      }
      description = {
        S = each.value.description
      }
      matcher = {
        M = merge(
          each.value.matcher.organizational_units != null ? {
            organizational_units = {
              L = [for ou in each.value.matcher.organizational_units : { S = ou }]
            }
          } : {},
          each.value.matcher.name_patterns != null ? {
            name_patterns = {
              L = [for p in each.value.matcher.name_patterns : { S = p }]
            }
          } : {},
          each.value.matcher.account_tags != null ? {
            account_tags = {
              M = { for k, v in each.value.matcher.account_tags : k => { S = v } }
            }
          } : {}
        )
      }
      template_names = {
        L = [for template_name in each.value.template_names : { S = template_name }]
      }
      groups = {
        L = [for group in each.value.groups : { S = group }]
      }
    },
    length(try(each.value.users, [])) > 0 ? {
      users = {
        L = [for user in each.value.users : { S = user }]
      }
    } : {},
    each.value.excluded != null ? {
      excluded = {
        L = [for r in each.value.excluded : { S = r }]
      }
    } : {}
  ))
}
