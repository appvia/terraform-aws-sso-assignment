## Example deployment of the terraform-aws-sso-assignment module

locals {
  tags = {
    Environment = "Testing"
    GitRepo     = "https://github.com/appvia/terraform-aws-sso-assignment"
    Owner       = "Engineering"
    Product     = "Identity"
  }

  ## Permission-set templates. Each top-level key becomes `sso/<key>` on a member
  ## account; the tag value is a comma-separated list of Identity Center **group
  ## display names** that receive that template’s permission sets on that account.
  groups_configuration = {
    default = {
      permission_sets = ["Developer", "Developer-ReadOnly"]
      description     = "e.g. tag sso/default = App-Developers,App-ReadOnly-Users"
    }
    finance = {
      permission_sets = ["FinanceReadOnly", "FinanceApprover"]
      description     = "e.g. tag sso/finance = Finance-Team"
    }
    security = {
      permission_sets = ["SecurityAdmin", "SecurityAudit"]
      description     = "e.g. tag sso/security = SecOps"
    }
    operations = {
      permission_sets = ["Operator"]
      description     = "e.g. tag sso/operations = SRE-Team"
    }
  }
  # Replace with your actual SSO instance ARN
  # Find this in AWS SSO console or via: aws sso-admin list-instances
  sso_instance_arn = "arn:aws:sso:::instance/sso-instance-id"

  # Optional: Set to SNS topic ARN to receive notifications (null = disabled)
  # Example: "arn:aws:sns:us-east-1:123456789012:my-notifications"
  # Leave null to disable notifications
  sns_topic_arn = null
}

## Provision the SSO assignment module
module "sso_assignment" {
  source = "../.."

  sns_topic_arn    = local.sns_topic_arn
  sso_instance_arn = local.sso_instance_arn
  tags             = local.tags
}

## Configure the dynamoDB table for the SSO group assignments
module "config" {
  source = "../../modules/config"

  dynamodb_table_name  = module.sso_assignment.dynamodb_table_name
  groups_configuration = local.groups_configuration
}
