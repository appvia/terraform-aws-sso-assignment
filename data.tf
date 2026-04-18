## Find the current AWS region
data "aws_region" "current" {}

## Craft a IAM policy document for the Lambda function
data "aws_iam_policy_document" "lambda_policy" {
  statement {
    sid    = "AllowLambdaToReadDynamoDB"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:Query",
      "dynamodb:Scan"
    ]
    resources = [aws_dynamodb_table.config.arn]
  }

  statement {
    sid    = "AllowLambdaToInvokeSSO"
    effect = "Allow"
    actions = [
      "identitystore:ListGroups",
      "sso:CreateAccountAssignment",
      "sso:DescribeAccountAssignment",
      "sso:ListInstances",
      "sso:ListPermissionSets",
      "sso:DescribePermissionSet",
      "sso:DescribeAccountAssignmentCreationStatus"
    ]
  }

  statement {
    sid    = "AllowLambdaToInvokeOrganizations"
    effect = "Allow"
    actions = [
      "organizations:DescribeAccount",
      "organizations:ListAccounts",
      "organizations:ListTagsForResource"
    ]
    resources = ["*"]
  }
}

## Craft a IAM policy document for the EventBridge
data "aws_iam_policy_document" "eventbridge_invoke_step_function" {
  statement {
    sid    = "AllowEventBridgeToInvokeStepFunction"
    effect = "Allow"
    actions = [
      "states:StartExecution"
    ]
    resources = [aws_sfn_state_machine.main.arn]
  }
}

## Craft the assume role policy for the EventBridge
data "aws_iam_policy_document" "eventbridge_assume_role" {
  statement {
    sid    = "AllowEventBridgeToAssumeRole"
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}