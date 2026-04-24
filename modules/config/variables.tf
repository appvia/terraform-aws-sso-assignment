variable "dynamodb_table_name" {
  description = "Name of the DynamoDB table for storing group configurations"
  type        = string
}

variable "configuration" {
  description = "SSO configuration containing templates and account-level template matchers"
  type = object({
    # Permission-set templates keyed by template name
    # Each key can be referenced by account tags ({prefix}/{key}) or account templates
    templates = map(object({
      # List of permission sets to assign to the group
      permission_sets = list(string)
      # Description of the template
      description = string
    }))

    # Account-level templates: auto-provision accounts matching conditions
    # Optional - default empty (no account-level matching)
    account_templates = optional(map(object({
      # Description of this account template matcher
      description = string
      # Exclude accounts that match this pattern - supports python re syntax
      excluded = optional(list(string))
      # List of template names to apply to matching accounts
      template_names = list(string)
      # List of groups from those templates to assign
      # These groups will receive the permission sets defined in the templates
      groups = list(string)
      # Matcher conditions (logical AND: all specified conditions must match)
      matcher = object({
        # Match by organizational unit trailing path with glob patterns
        # e.g., ["production/accounts/*", "prod"]
        organizational_units = optional(list(string))
        # Match by account name with glob pattern
        # e.g., "prod-*"
        name_patterns = optional(list(string))
        # Match by account tags (all specified tags must exist and match)
        # e.g., { Environment = "Production", CostCenter = "Engineering" }
        account_tags = optional(map(string))
      })
    })), {})
  })

  validation {
    condition     = length(var.configuration.templates) > 0
    error_message = "configuration.templates must contain at least one template"
  }

  validation {
    condition = alltrue([
      for acct_tmpl in var.configuration.account_templates :
      (acct_tmpl.matcher.organizational_units != null && length(acct_tmpl.matcher.organizational_units) > 0) ||
      (acct_tmpl.matcher.name_patterns != null && length(acct_tmpl.matcher.name_patterns) > 0) ||
      (acct_tmpl.matcher.account_tags != null && length(acct_tmpl.matcher.account_tags) > 0)
    ])
    error_message = "Each account_templates matcher must specify at least one condition (organizational_units, name_pattern, or account_tags)"
  }

  validation {
    condition = alltrue([
      for acct_tmpl in var.configuration.account_templates :
      alltrue([
        for template_name in acct_tmpl.template_names :
        contains(keys(var.configuration.templates), template_name)
      ])
    ])
    error_message = "All template_names in account_templates must reference existing templates"
  }
}

