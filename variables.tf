variable "cloudwatch_logs_kms_key_id" {
  description = "KMS key ID for CloudWatch logs"
  type        = string
  default     = null

  validation {
    condition     = var.cloudwatch_logs_kms_key_id == null || length(var.cloudwatch_logs_kms_key_id) > 0
    error_message = "cloudwatch_logs_kms_key_id must be a non-empty string."
  }
}

variable "cloudwatch_logs_log_group_class" {
  description = "The class of the CloudWatch log group"
  type        = string
  default     = "STANDARD"
}

variable "cloudwatch_logs_retention_in_days" {
  description = "The number of days to retain the CloudWatch logs"
  type        = number
  default     = 30

  validation {
    condition     = var.cloudwatch_logs_retention_in_days >= 1 && var.cloudwatch_logs_retention_in_days <= 3650
    error_message = "cloudwatch_logs_retention_in_days must be between 1 and 3650 days."
  }
}

variable "dynamodb_billing_mode" {
  description = "DynamoDB billing mode (PAY_PER_REQUEST or PROVISIONED)"
  type        = string
  default     = "PAY_PER_REQUEST"

  validation {
    condition     = contains(["PAY_PER_REQUEST", "PROVISIONED"], var.dynamodb_billing_mode)
    error_message = "dynamodb_billing_mode must be either PAY_PER_REQUEST or PROVISIONED."
  }
}

variable "dynamodb_encryption_enabled" {
  description = "Enable server-side encryption for DynamoDB tables (will use AWS managed KMS key by default)"
  type        = bool
  default     = false
}

variable "dynamodb_kms_key" {
  description = "Optional KMS key ID for DynamoDB encryption"
  type        = string
  default     = null
}

variable "enable_config_triggers" {
  description = "Enable EventBridge Pipes to trigger Lambda when config table is updated"
  type        = bool
  default     = true
}

variable "enable_dry_run" {
  description = "When true, triggers run the Lambda in dry-run (noop) mode"
  type        = bool
  default     = false
}

variable "lambda_memory" {
  description = "Lambda function memory allocation in MB"
  type        = number
  default     = 512

  validation {
    condition     = var.lambda_memory >= 128 && var.lambda_memory <= 10240
    error_message = "lambda_memory must be between 128 and 10240."
  }
}

variable "lambda_runtime" {
  description = "Lambda function runtime"
  type        = string
  default     = "python3.14"

  validation {
    condition     = contains(["python3.14", "python3.13", "python3.12"], var.lambda_runtime)
    error_message = "lambda_runtime must be either python3.14, python3.13, or python3.12."
  }
}

variable "lambda_schedule" {
  description = "EventBridge cron/rate schedule for Lambda execution"
  type        = string
  default     = "rate(180 minutes)"
}

variable "lambda_timeout" {
  description = "Lambda function timeout in seconds"
  type        = number
  default     = 300

  validation {
    condition     = var.lambda_timeout >= 3 && var.lambda_timeout <= 900
    error_message = "lambda_timeout must be between 3 and 900 seconds."
  }
}

variable "name" {
  description = "Name for all resources i.e. handler, lambda, step function, event bridge, etc."
  type        = string
  default     = "lz-sso"

  validation {
    condition     = length(var.name) > 0
    error_message = "name must be a non-empty string."
  }
}

variable "sns_topic_arn" {
  description = "ARN of SNS topic for Step Function notifications (if null, notifications disabled)"
  type        = string
  default     = null
}

variable "events_sns_topic_arn" {
  description = "Optional ARN of an existing SNS topic to publish assignment creation/deletion events from the Lambda (if null, event publishing disabled). This topic is NOT created by this module."
  type        = string
  default     = null
}

variable "sso_account_tag_prefix" {
  description = "Account tag key prefix for permission-set templates. Keys are {prefix}/{template_name} (e.g. sso/default) — see module README"
  type        = string
  default     = "Grant"

  validation {
    condition     = length(var.sso_account_tag_prefix) > 0 && !strcontains(var.sso_account_tag_prefix, "/")
    error_message = "sso_account_tag_prefix must be non-empty and must not contain '/'."
  }
}

variable "sso_instance_arn" {
  description = "ARN of the AWS SSO instance"
  type        = string
}

variable "tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default     = {}
}
