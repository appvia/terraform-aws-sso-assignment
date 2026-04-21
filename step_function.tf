locals {
  ## Base states for the Step Function state machine
  base_states = {
    DetermineExecutionMode = {
      Type = "Choice"
      Choices = [
        {
          Variable     = "$.source"
          StringEquals = "eventbridge.account_creation"
          Next         = "InvokeLambdaSingleAccount"
        }
      ]
      Default = "InvokeLambdaAllAccounts"
    }

    InvokeLambdaSingleAccount = {
      Type     = "Task"
      Resource = module.lambda.lambda_function_arn
      Parameters = {
        "account_id.$" = "$.detail.account_id"
        "source"       = "account_creation"
      }
      Next = "EvaluateResponse"
      Retry = [
        {
          ErrorEquals     = ["States.ALL"]
          IntervalSeconds = 2
          MaxAttempts     = 3
          BackoffRate     = 2.0
        }
      ]
    }

    InvokeLambdaAllAccounts = {
      Type     = "Task"
      Resource = module.lambda.lambda_function_arn
      Parameters = {
        "source" = "cron_schedule"
      }
      Next = "EvaluateResponse"
      Retry = [
        {
          ErrorEquals     = ["States.ALL"]
          IntervalSeconds = 2
          MaxAttempts     = 3
          BackoffRate     = 2.0
        }
      ]
    }

    EvaluateResponse = {
      Type = "Choice"
      Choices = [
        {
          Variable = "$.errors"
          IsNull   = false
          Next     = var.sns_topic_arn != null ? "SendNotification" : "Failure"
        }
      ]
      Default = "Success"
    }

    Success = {
      Type = "Succeed"
    }

    Failure = {
      Type = "Fail"
    }
  }

  ## Optional SendNotification state (only included if SNS topic is provided)
  optional_states = var.sns_topic_arn != null ? {
    SendNotification = {
      Type     = "Task"
      Resource = "arn:aws:states:::sns:publish"
      Parameters = {
        TopicArn = var.sns_topic_arn
        Message = {
          "error_details.$" = "$"
        }
      }
      Next = "Failure"
    }
  } : {}

  ## Merge all states
  all_states = merge(local.base_states, local.optional_states)

  ## The definition of the Step Function state machine
  step_function_definition = jsonencode({
    Comment = "Used to orchestrate the SSO group assignment workflow"
    StartAt = "DetermineExecutionMode"
    States  = local.all_states
  })
}

## Craft assume role policy for the Step Function
data "aws_iam_policy_document" "step_function_assume_role" {
  statement {
    sid    = "AllowStepFunctionToAssumeRole"
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
    actions = [
      "sts:AssumeRole"
    ]
  }
}

## Craft a IAM policy document for the Step Function
data "aws_iam_policy_document" "step_function_lambda" {
  statement {
    sid    = "AllowStepFunctionToInvokeLambda"
    effect = "Allow"
    actions = [
      "lambda:InvokeFunction"
    ]
    resources = [module.lambda.lambda_function_arn]
  }

  dynamic "statement" {
    for_each = var.sns_topic_arn != null ? [1] : []
    content {
      sid    = "AllowStepFunctionToPublishToSNS"
      effect = "Allow"
      actions = [
        "sns:Publish"
      ]
      resources = [var.sns_topic_arn]
    }
  }
}

## Step Function IAM role
resource "aws_iam_role" "step_function" {
  name               = format("%s-step-function", var.name)
  tags               = local.tags
  assume_role_policy = data.aws_iam_policy_document.step_function_assume_role.json
}

## Lambda invocation policy for Step Function
resource "aws_iam_role_policy" "step_function_lambda" {
  name   = format("%s-step-function-lambda", var.name)
  role   = aws_iam_role.step_function.id
  policy = data.aws_iam_policy_document.step_function_lambda.json
}

## Step Function state machine
resource "aws_sfn_state_machine" "main" {
  name       = var.name
  role_arn   = aws_iam_role.step_function.arn
  definition = local.step_function_definition
  tags       = local.tags

  depends_on = [
    aws_iam_role_policy.step_function_lambda,
  ]
}

