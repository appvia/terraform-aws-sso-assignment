mock_provider "aws" {
  override_resource {
    target = aws_iam_role.eventbridge_invoke
    values = {
      arn = "arn:aws:iam::123456789012:role/eventbridge-invoke"
    }
  }

  override_resource {
    target = aws_iam_role.eventbridge_pipes[0]
    values = {
      arn = "arn:aws:iam::123456789012:role/eventbridge-pipes"
    }
  }

  override_resource {
    target = aws_iam_role.step_function
    values = {
      arn = "arn:aws:iam::123456789012:role/step-function"
    }
  }

  override_resource {
    target = module.lambda.aws_iam_role.lambda[0]
    values = {
      arn = "arn:aws:iam::123456789012:role/lambda"
    }
  }

  override_resource {
    target = aws_dynamodb_table.config
    values = {
      stream_arn = "arn:aws:dynamodb:eu-west-2:123456789012:table/lz-sso-config/stream/2020-01-01T00:00:00.000"
    }
  }

  override_resource {
    target = module.lambda.aws_lambda_function.this[0]
    values = {
      arn = "arn:aws:lambda:eu-west-2:123456789012:function:lz-sso"
    }
  }

  override_resource {
    target = module.lambda.aws_cloudwatch_log_group.lambda[0]
    values = {
      arn  = "arn:aws:logs:eu-west-2:123456789012:log-group:/aws/lambda/lz-sso"
      name = "/aws/lambda/lz-sso"
    }
  }

  override_resource {
    target = aws_sfn_state_machine.main
    values = {
      arn = "arn:aws:states:eu-west-2:123456789012:stateMachine:lz-sso"
    }
  }

  mock_data "aws_iam_policy" {
    defaults = {
      arn    = "arn:aws:iam::aws:policy/ReadOnlyAccess"
      policy = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }

  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }

  mock_data "aws_availability_zones" {
    defaults = {
      names = [
        "eu-west-2a",
        "eu-west-2b",
        "eu-west-2c"
      ]
    }
  }

  mock_data "aws_region" {
    defaults = {
      region = "eu-west-2"
    }
  }

  mock_data "aws_partition" {
    defaults = {
      partition = "aws"
    }
  }

  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "1234567890"
    }
  }
}


run "config_triggers_disabled_removes_pipe_and_stream" {
  command = plan

  variables {
    enable_config_triggers = false
    sso_instance_arn       = "arn:aws:sso:::instance/ssoins-1234567890abcdef"
  }

  assert {
    condition     = aws_dynamodb_table.config.name != "" && aws_dynamodb_table.assignments_tracking.name != ""
    error_message = "Expected both DynamoDB tables (config and assignments_tracking) to be planned."
  }

  assert {
    condition     = length(aws_pipes_pipe.config_update) == 0
    error_message = "Expected no EventBridge Pipe when enable_config_triggers=false."
  }

  assert {
    condition     = length(aws_cloudwatch_log_group.eventbridge_pipes_config_update) == 0
    error_message = "Expected no EventBridge Pipes log group when enable_config_triggers=false."
  }

  assert {
    condition     = length(aws_iam_role.eventbridge_pipes) == 0
    error_message = "Expected no EventBridge Pipes IAM role when enable_config_triggers=false."
  }

  assert {
    condition     = aws_dynamodb_table.config.stream_enabled == false
    error_message = "Expected DynamoDB streams disabled on config table when enable_config_triggers=false."
  }
}

run "config_triggers_enabled_creates_pipe_and_stream" {
  command = plan

  variables {
    enable_config_triggers = true
    sso_instance_arn       = "arn:aws:sso:::instance/ssoins-1234567890abcdef"
  }

  assert {
    condition     = aws_dynamodb_table.config.name != "" && aws_dynamodb_table.assignments_tracking.name != ""
    error_message = "Expected both DynamoDB tables (config and assignments_tracking) to be planned."
  }

  assert {
    condition     = length(aws_pipes_pipe.config_update) == 1
    error_message = "Expected EventBridge Pipe created when enable_config_triggers=true."
  }

  assert {
    condition     = length(aws_cloudwatch_log_group.eventbridge_pipes_config_update) == 1
    error_message = "Expected EventBridge Pipes log group created when enable_config_triggers=true."
  }

  assert {
    condition     = length(aws_iam_role.eventbridge_pipes) == 1
    error_message = "Expected EventBridge Pipes IAM role created when enable_config_triggers=true."
  }

  assert {
    condition     = aws_dynamodb_table.config.stream_enabled == true
    error_message = "Expected DynamoDB streams enabled on config table when enable_config_triggers=true."
  }
}

run "account_triggers_disabled_removes_account_creation_rule_and_target" {
  command = plan

  variables {
    enable_account_triggers = false
    sso_instance_arn        = "arn:aws:sso:::instance/ssoins-1234567890abcdef"
  }

  assert {
    condition     = length(aws_cloudwatch_event_rule.account_creation) == 0
    error_message = "Expected no account creation EventBridge rule when enable_account_triggers=false."
  }

  assert {
    condition     = length(aws_cloudwatch_event_target.account_creation_target) == 0
    error_message = "Expected no account creation EventBridge target when enable_account_triggers=false."
  }
}

run "account_triggers_enabled_creates_account_creation_rule_and_target" {
  command = plan

  variables {
    enable_account_triggers = true
    sso_instance_arn        = "arn:aws:sso:::instance/ssoins-1234567890abcdef"
  }

  assert {
    condition     = length(aws_cloudwatch_event_rule.account_creation) == 1
    error_message = "Expected account creation EventBridge rule when enable_account_triggers=true."
  }

  assert {
    condition     = length(aws_cloudwatch_event_target.account_creation_target) == 1
    error_message = "Expected account creation EventBridge target when enable_account_triggers=true."
  }
}

run "step_function_policy_includes_sns_publish_when_topic_provided" {
  command = apply

  override_data {
    target = data.aws_iam_policy_document.step_function_lambda
    values = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"lambda:InvokeFunction\"],\"Resource\":[\"arn:aws:lambda:eu-west-2:123456789012:function:lz-sso\"]},{\"Effect\":\"Allow\",\"Action\":[\"sns:Publish\"],\"Resource\":[\"arn:aws:sns:eu-west-2:123456789012:topic\"]}]}"
    }
  }

  variables {
    enable_config_triggers = true
    sns_topic_arn          = "arn:aws:sns:eu-west-2:123456789012:topic"
    sso_instance_arn       = "arn:aws:sso:::instance/ssoins-1234567890abcdef"
  }

  assert {
    condition     = strcontains(aws_iam_role_policy.step_function_lambda.policy, "sns:Publish")
    error_message = "Expected Step Function role policy to include sns:Publish when sns_topic_arn is provided."
  }

  assert {
    condition     = strcontains(aws_iam_role_policy.step_function_lambda.policy, "arn:aws:sns:eu-west-2:123456789012:topic")
    error_message = "Expected Step Function role policy to include the provided sns_topic_arn when set."
  }
}

run "observability_disabled_creates_no_alarms" {
  command = plan

  variables {
    enable_observability = false
    sso_instance_arn     = "arn:aws:sso:::instance/ssoins-1234567890abcdef"
  }

  assert {
    condition     = length(aws_cloudwatch_log_metric_filter.handler_errors) == 0 && length(aws_cloudwatch_log_metric_filter.identity_resolution_failures) == 0
    error_message = "Expected no log metric filters when enable_observability=false."
  }

  assert {
    condition     = length(aws_cloudwatch_metric_alarm.lambda_errors) == 0 && length(aws_cloudwatch_metric_alarm.step_function_executions_failed) == 0
    error_message = "Expected no Lambda or Step Function alarms when enable_observability=false."
  }

  assert {
    condition     = length(aws_cloudwatch_metric_alarm.eventbridge_failed_invocations) == 0 && length(aws_cloudwatch_metric_alarm.dynamodb_throttled_requests) == 0
    error_message = "Expected no EventBridge or DynamoDB alarms when enable_observability=false."
  }

  assert {
    condition     = length(aws_cloudwatch_metric_alarm.pipes_execution_failed) == 0
    error_message = "Expected no Pipes alarm when enable_observability=false, even with config triggers enabled."
  }
}

run "observability_enabled_creates_alarms_and_metric_filters" {
  command = plan

  variables {
    enable_account_triggers = false
    enable_config_triggers  = true
    enable_observability    = true
    sso_instance_arn        = "arn:aws:sso:::instance/ssoins-1234567890abcdef"
  }

  assert {
    condition     = length(aws_cloudwatch_log_metric_filter.handler_errors) == 1 && length(aws_cloudwatch_log_metric_filter.identity_resolution_failures) == 1
    error_message = "Expected both log metric filters when enable_observability=true."
  }

  assert {
    condition     = aws_cloudwatch_log_metric_filter.handler_errors[0].metric_transformation[0].default_value == "0"
    error_message = "Expected the handler errors metric filter to report a zero default so the alarm has continuous data."
  }

  assert {
    condition     = length(aws_cloudwatch_metric_alarm.lambda_errors) == 1 && length(aws_cloudwatch_metric_alarm.lambda_throttles) == 1 && length(aws_cloudwatch_metric_alarm.lambda_duration) == 1
    error_message = "Expected the three Lambda alarms when enable_observability=true."
  }

  assert {
    condition     = length(aws_cloudwatch_metric_alarm.step_function_executions_failed) == 1 && length(aws_cloudwatch_metric_alarm.step_function_executions_timed_out) == 1 && length(aws_cloudwatch_metric_alarm.step_function_no_executions) == 1
    error_message = "Expected the three Step Function alarms when enable_observability=true."
  }

  assert {
    condition     = aws_cloudwatch_metric_alarm.step_function_no_executions[0].treat_missing_data == "breaching"
    error_message = "Expected the liveness alarm to treat missing data as breaching, otherwise a stopped schedule never alarms."
  }

  assert {
    condition     = aws_cloudwatch_metric_alarm.step_function_no_executions[0].comparison_operator == "LessThanThreshold"
    error_message = "Expected the liveness alarm to fire when executions drop below the threshold."
  }

  assert {
    condition     = aws_cloudwatch_metric_alarm.lambda_duration[0].threshold == 240000
    error_message = "Expected the duration alarm threshold to be 80% of the default 300s timeout, expressed in milliseconds."
  }

  assert {
    condition     = length(aws_cloudwatch_metric_alarm.dynamodb_throttled_requests) == 2
    error_message = "Expected a DynamoDB throttling alarm for each of the config and tracking tables."
  }

  assert {
    condition     = length(aws_cloudwatch_metric_alarm.eventbridge_failed_invocations) == 1
    error_message = "Expected a single EventBridge alarm for the cron schedule rule when account triggers are disabled."
  }

  assert {
    condition     = length(aws_cloudwatch_metric_alarm.pipes_execution_failed) == 1
    error_message = "Expected the Pipes alarm when both observability and config triggers are enabled."
  }
}

run "observability_enabled_without_config_triggers_skips_pipes_alarm" {
  command = plan

  variables {
    enable_config_triggers = false
    enable_observability   = true
    sso_instance_arn       = "arn:aws:sso:::instance/ssoins-1234567890abcdef"
  }

  assert {
    condition     = length(aws_cloudwatch_metric_alarm.pipes_execution_failed) == 0
    error_message = "Expected no Pipes alarm when enable_config_triggers=false."
  }

  assert {
    condition     = length(aws_cloudwatch_metric_alarm.lambda_errors) == 1
    error_message = "Expected the remaining alarms to still be created when only config triggers are disabled."
  }
}

run "observability_enabled_with_account_triggers_alarms_both_rules" {
  command = plan

  variables {
    enable_account_triggers = true
    enable_observability    = true
    sso_instance_arn        = "arn:aws:sso:::instance/ssoins-1234567890abcdef"
  }

  assert {
    condition     = length(aws_cloudwatch_metric_alarm.eventbridge_failed_invocations) == 2
    error_message = "Expected an EventBridge alarm for both the cron schedule and account creation rules."
  }
}

run "observability_alarm_arns_output_is_populated" {
  command = apply

  variables {
    enable_account_triggers = true
    enable_config_triggers  = true
    enable_observability    = true
    sso_instance_arn        = "arn:aws:sso:::instance/ssoins-1234567890abcdef"
  }

  # 9 count-based alarms, plus one per EventBridge rule (2) and DynamoDB table (2)
  assert {
    condition     = length(keys(output.cloudwatch_alarm_arns)) == 13
    error_message = "Expected cloudwatch_alarm_arns to contain an entry for every provisioned alarm."
  }

  assert {
    condition     = contains(keys(output.cloudwatch_alarm_arns), "step_function_no_executions")
    error_message = "Expected cloudwatch_alarm_arns to include the liveness alarm."
  }

  assert {
    condition     = contains(keys(output.cloudwatch_alarm_arns), "eventbridge_account_creation_failed_invocations") && contains(keys(output.cloudwatch_alarm_arns), "eventbridge_cron_schedule_failed_invocations")
    error_message = "Expected cloudwatch_alarm_arns to key EventBridge alarms by rule."
  }

  assert {
    condition     = contains(keys(output.cloudwatch_alarm_arns), "dynamodb_config_throttled_requests") && contains(keys(output.cloudwatch_alarm_arns), "dynamodb_tracking_throttled_requests")
    error_message = "Expected cloudwatch_alarm_arns to key DynamoDB alarms by table."
  }
}

run "observability_alarm_arns_output_is_empty_when_disabled" {
  command = apply

  variables {
    enable_observability = false
    sso_instance_arn     = "arn:aws:sso:::instance/ssoins-1234567890abcdef"
  }

  assert {
    condition     = length(keys(output.cloudwatch_alarm_arns)) == 0
    error_message = "Expected cloudwatch_alarm_arns to be empty when enable_observability=false."
  }
}

run "observability_alarm_actions_use_provided_topics" {
  command = plan

  variables {
    alarm_sns_topic_arns = ["arn:aws:sns:eu-west-2:123456789012:sso-alarms"]
    enable_observability = true
    sso_instance_arn     = "arn:aws:sso:::instance/ssoins-1234567890abcdef"
  }

  assert {
    condition     = contains(aws_cloudwatch_metric_alarm.lambda_errors[0].alarm_actions, "arn:aws:sns:eu-west-2:123456789012:sso-alarms")
    error_message = "Expected alarm_actions to contain the provided SNS topic ARN."
  }

  assert {
    condition     = contains(aws_cloudwatch_metric_alarm.lambda_errors[0].ok_actions, "arn:aws:sns:eu-west-2:123456789012:sso-alarms")
    error_message = "Expected ok_actions to contain the provided SNS topic ARN so alarms notify on recovery."
  }
}

run "lambda_policy_includes_sns_publish_when_events_topic_provided" {
  command = apply

  override_data {
    target = data.aws_iam_policy_document.lambda_policy
    values = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"AllowDynamoDB\",\"Effect\":\"Allow\",\"Action\":[\"dynamodb:GetItem\",\"dynamodb:Query\",\"dynamodb:Scan\"],\"Resource\":[\"arn:aws:dynamodb:eu-west-2:123456789012:table/lz-sso-config\",\"arn:aws:dynamodb:eu-west-2:123456789012:table/lz-sso-tracking\"]},{\"Sid\":\"AllowPublishAssignmentEventsToSNS\",\"Effect\":\"Allow\",\"Action\":[\"sns:Publish\"],\"Resource\":[\"arn:aws:sns:eu-west-2:123456789012:assignment-events\"]}]}"
    }
  }

  variables {
    enable_config_triggers = true
    events_sns_topic_arn   = "arn:aws:sns:eu-west-2:123456789012:assignment-events"
    sso_instance_arn       = "arn:aws:sso:::instance/ssoins-1234567890abcdef"
  }

  assert {
    condition     = strcontains(output.lambda_policy_json, "sns:Publish")
    error_message = "Expected Lambda role policy to include sns:Publish when events_sns_topic_arn is provided."
  }

  assert {
    condition     = strcontains(output.lambda_policy_json, "arn:aws:sns:eu-west-2:123456789012:assignment-events")
    error_message = "Expected Lambda role policy to include the provided events_sns_topic_arn when set."
  }
}

