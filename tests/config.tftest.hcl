mock_provider "aws" {
  mock_data "aws_dynamodb_table" {
    defaults = {
      arn  = "arn:aws:dynamodb:eu-west-2:123456789012:table/lz-sso-config"
      name = "lz-sso-config"
    }
  }
}

run "creates_items_for_templates_only" {
  command = plan

  module {
    source = "./modules/config"
  }

  variables {
    dynamodb_table_arn = "arn:aws:dynamodb:eu-west-2:123456789012:table/lz-sso-config"
    configuration = {
      templates = {
        default = {
          permission_sets = ["ReadOnlyAccess"]
          description     = "Default template"
        }
        finance = {
          permission_sets = ["FinanceAdmin", "FinanceReadOnly"]
          description     = "Finance template"
        }
      }
      account_templates = {}
    }
  }

  assert {
    condition     = length(aws_dynamodb_table_item.templates) == 2
    error_message = "Expected one DynamoDB item per configuration.templates entry."
  }

  assert {
    condition     = length(aws_dynamodb_table_item.account_templates) == 0
    error_message = "Expected no account template items when configuration.account_templates is empty."
  }

  assert {
    condition     = output.dynamodb_table_name == "lz-sso-config"
    error_message = "Expected output.table_name to match the provided dynamodb_table_name."
  }

  assert {
    condition     = output.dynamodb_table_arn == "arn:aws:dynamodb:eu-west-2:123456789012:table/lz-sso-config"
    error_message = "Expected output.table_arn to be read from the DynamoDB table data source."
  }
}

run "creates_items_for_account_templates" {
  command = plan

  module {
    source = "./modules/config"
  }

  variables {
    dynamodb_table_arn = "arn:aws:dynamodb:eu-west-2:123456789012:table/lz-sso-config"
    configuration = {
      templates = {
        prod = {
          permission_sets = ["PowerUserAccess"]
          description     = "Prod baseline"
        }
      }
      account_templates = {
        prod_workloads = {
          description    = "Match prod OU workloads"
          template_names = ["prod"]
          groups         = ["ProdEngineers", "ProdAdmins"]
          matcher = {
            organizational_units = ["/prod/workloads/*"]
          }
        }
      }
    }
  }

  assert {
    condition     = length(aws_dynamodb_table_item.templates) == 1
    error_message = "Expected one DynamoDB item for the single template."
  }

  assert {
    condition     = length(aws_dynamodb_table_item.account_templates) == 1
    error_message = "Expected one DynamoDB item for the single account template matcher."
  }
}

