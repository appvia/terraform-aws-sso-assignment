variable "alarm_duration_threshold_percent" {
  description = "Percentage of lambda_timeout at which the Lambda duration alarm triggers"
  type        = number
  default     = 80

  validation {
    condition     = var.alarm_duration_threshold_percent > 0 && var.alarm_duration_threshold_percent <= 100
    error_message = "alarm_duration_threshold_percent must be between 1 and 100."
  }
}

variable "alarm_execution_staleness_period_seconds" {
  description = "Period in seconds over which an absence of Step Function executions raises the liveness alarm. Must be comfortably longer than step_function_schedule"
  type        = number
  default     = 21600

  validation {
    condition     = var.alarm_execution_staleness_period_seconds >= 60 && var.alarm_execution_staleness_period_seconds <= 86400
    error_message = "alarm_execution_staleness_period_seconds must be between 60 and 86400 seconds."
  }

  validation {
    condition     = var.alarm_execution_staleness_period_seconds % 60 == 0
    error_message = "alarm_execution_staleness_period_seconds must be a multiple of 60."
  }
}

variable "alarm_identity_resolution_threshold" {
  description = "Number of identity resolution warnings (groups, users or permission sets missing from Identity Center) tolerated before alarming"
  type        = number
  default     = 0

  validation {
    condition     = var.alarm_identity_resolution_threshold >= 0
    error_message = "alarm_identity_resolution_threshold must be zero or greater."
  }
}

variable "alarm_metric_namespace" {
  description = "CloudWatch namespace for the custom metrics extracted from the Lambda logs"
  type        = string
  default     = "Appvia/SSOAssignment"

  validation {
    condition     = length(var.alarm_metric_namespace) > 0
    error_message = "alarm_metric_namespace must be a non-empty string."
  }
}

variable "alarm_sns_topic_arns" {
  description = "ARNs of existing SNS topics to notify on alarm and recovery. These topics are NOT created by this module"
  type        = list(string)
  default     = []
}

variable "cloudwatch_logs_kms_key_id" {
  description = "KMS key ID for CloudWatch logs"
  type        = string
  default     = null

  validation {
    condition     = var.cloudwatch_logs_kms_key_id == null || length(var.cloudwatch_logs_kms_key_id) > 0
    error_message = "cloudwatch_logs_kms_key_id must be a non-empty string."
  }
}

variable "trigger_on_package_timestamp" {
  description = "When true, the Lambda is triggered to redeploy when the package timestamp changes (useful for CI/CD pipelines)"
  type        = bool
  default     = false
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

variable "dynamodb_point_in_time_recovery_enabled" {
  description = "Enable point-in-time recovery for DynamoDB tables (for both tables)"
  type        = bool
  default     = false
}

variable "dynamodb_point_in_time_recovery_retention_period" {
  description = "The number of days to retain the DynamoDB point-in-time recovery"
  type        = number
  default     = 7

  validation {
    condition     = var.dynamodb_point_in_time_recovery_retention_period >= 1 && var.dynamodb_point_in_time_recovery_retention_period <= 35
    error_message = "dynamodb_point_in_time_recovery_retention_period must be between 1 and 35 days."
  }
}

variable "enable_account_triggers" {
  description = "Enable EventBridge rules to trigger Lambda when AWS Organizations account creation events are detected (Only available in the us-east-1 region)"
  type        = bool
  default     = false
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

variable "enable_observability" {
  description = "Enable CloudWatch alarms and log metric filters covering the Lambda, Step Function, triggers and DynamoDB tables"
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

variable "lambda_timeout" {
  description = "Lambda function timeout in seconds"
  type        = number
  default     = 300

  validation {
    condition     = var.lambda_timeout >= 3 && var.lambda_timeout <= 900
    error_message = "lambda_timeout must be between 3 and 900 seconds."
  }
}

variable "log_level" {
  description = "Log level for the Lambda function. Callers can still override this per invocation via the event's logging_level field"
  type        = string
  default     = "INFO"

  validation {
    condition     = contains(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], var.log_level)
    error_message = "log_level must be one of DEBUG, INFO, WARNING, ERROR or CRITICAL."
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

variable "step_function_schedule" {
  description = "EventBridge cron/rate schedule for Lambda execution"
  type        = string
  default     = "rate(180 minutes)"
}

variable "events_sns_topic_arn" {
  description = "Optional ARN of an existing SNS topic to publish assignment creation/deletion events from the Lambda (if null, event publishing disabled). This topic is NOT created by this module."
  type        = string
  default     = null
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
