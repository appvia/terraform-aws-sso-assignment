variable "dynamodb_table_name" {
  description = "Name of the DynamoDB table for storing group configurations"
  type        = string
}

variable "groups_configuration" {
  description = "Permission-set templates keyed by template name; each key must match the account tag suffix {prefix}/{key} (e.g. sso/<key> when prefix is sso)"
  type = map(object({
    # List of permission sets to assign to the group
    permission_sets = list(string)
    # Description of the group
    description = string
    # Whether the group is enabled
    enabled = optional(bool, true)
  }))
}