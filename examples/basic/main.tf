## Example deployment of the terraform-aws-sso-assignment module

locals {
  tags = {
    Environment = "Testing"
    GitRepo     = "https://github.com/appvia/terraform-aws-sso-assignment"
    Owner       = "Engineering"
    Product     = "Identity"
  }

  ## SSO Configuration with templates and account-level template matchers
  ##
  ## Templates: Define permission sets grouped by template name
  ## Each template can be referenced by:
  ## 1. Account tags: {prefix}/{template_name} (e.g., sso/administrators)
  ## 2. Account templates: Auto-apply based on account conditions
  ##
  ## Account Templates: Auto-provision accounts matching specific conditions
  ## Conditions use logical AND (all specified conditions must match):
  ## - organizational_units: Match by OU trailing path with glob patterns
  ## - name_pattern: Match by account name with glob pattern
  ## - account_tags: Match by account tags (all must exist and match)
  configuration = {
    templates = {
      administrators = {
        description = "Provides full administrative permissions to the account"
        permission_sets = [
          "Administrator",
        ]
      }
      billing = {
        description = "Provides billing permissions to the account"
        permission_sets = [
          "BillingViewer",
        ]
      }
      data = {
        description = "Provides data engineering permissions to the account"
        permission_sets = [
          "DataEngineer",
        ]
      }
      finops = {
        description = "Provides finops permissions to the account"
        permission_sets = [
          "FinOpsEngineer",
        ]
      }
      lz-support = {
        description = "Provides support and landing zone permissions to the account"
        permission_sets = [
          "LandingZoneSupport",
        ]
      }
      platform = {
        description = "Provides support and platform engineering permissions to the account"
        permission_sets = [
          "Support",
        ]
      }
      security = {
        description = "Provides security permissions to the account"
        permission_sets = [
          "SecurityAuditor",
        ]
      }
    }

    account_templates = {
      ## Example 1: Auto-provision production accounts by OU path
      "baseline" = {
        description    = "Every account receives the platform template"
        template_names = ["platform"]
        groups         = ["Cloud Solutions"]
        excluded       = ["Management", "Audit", "LogArchive"]

        matcher = {
          name_patterns = [".*"]
        }
      }

      "user-provisioning" = {
        description    = "Used to provision one-off accounts"
        template_names = ["platform"]
        users          = ["alice@example.com"]

        matcher = {
          account_tags = {
            Environment = "Development"
          }
        }
      }

      "data-platform" = {
        description    = "Used to provision data engineering roles"
        template_names = ["data"]
        groups         = ["Cloud Data Engineers"]

        matcher = {
          # OU patterns match against a normalized OU path that includes a
          # leading "/". Prefer patterns that also include a leading "/".
          # Use fnmatch glob syntax (* matches any characters within a path
          # segment or across segments).
          organizational_units = ["/data/*"]
        }
      }

      "finops" = {
        description    = "Used to provision the permission for finops engineering roles"
        template_names = ["finops"]
        groups         = ["Cloud Billing"]

        matcher = {
          name_patterns = ["FinOps"]
        }
      }

      ## Example 3: Auto-provision accounts by tags (logical AND)
      # This would match any account that has BOTH tags:
      # - Environment = "Development"
      # - ManagedBySSO = "true"
      # (Uncomment to enable)
      # "dev-by-tags" = {
      #   description = "Auto-provision development accounts by tags"
      #   matcher = {
      #     account_tags = {
      #       Environment = "Development"
      #       ManagedBySSO = "true"
      #     }
      #   }
      #   template_names = ["developers"]
      #   groups         = ["DevEngineers"]
      # }
    }
  }

  # Replace with your actual SSO instance ARN
  # Find this in AWS SSO console or via: aws sso-admin list-instances
  sso_instance_arn = "arn:aws:sso:::instance/ssoins-75351008b92ccaec"

  # Optional: Set to SNS topic ARN to receive notifications (null = disabled)
  # Example: "arn:aws:sns:us-east-1:123456789012:my-notifications"
  # Leave null to disable notifications
  sns_topic_arn = null
}

## Provision the SSO assignment module
module "sso_assignment" {
  source = "../.."

  # Indicate dry run mode
  enable_dry_run = true
  # CloudWatch Logs retention in days
  cloudwatch_logs_retention_in_days = 7
  # Enable server-side encryption for DynamoDB tables (default is false)
  dynamodb_encryption_enabled = true
  # Enable EventBridge Pipes to trigger Lambda when config table is updated (defaults to true)
  enable_config_triggers = true
  # Optional: Set to SNS topic ARN to receive notifications (null = disabled)
  sns_topic_arn = local.sns_topic_arn
  # ARN of the Identity Center instance
  sso_instance_arn = local.sso_instance_arn
  # EventBridge cron/rate schedule for Lambda execution (defaults to rate(180 minutes))
  step_function_schedule = "rate(180 minutes)"
  # Tags for the resources
  tags = local.tags
}

## Configure the dynamoDB table for the SSO group assignments
module "config" {
  source = "../../modules/config"

  # Configuration for the SSO assignment module
  configuration = local.configuration
  # ARN of the DynamoDB table for storing configuration
  dynamodb_table_arn = module.sso_assignment.config_dynamodb_table_arn

  depends_on = [
    module.sso_assignment
  ]
}
