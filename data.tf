## Craft a IAM policy document for the Lambda function
data "aws_iam_policy_document" "lambda_policy" {
  statement {
    sid    = "AllowDynamoDB"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:Query",
      "dynamodb:Scan"
    ]
    resources = [
      aws_dynamodb_table.config.arn,
      aws_dynamodb_table.assignments_tracking.arn,
    ]
  }

  statement {
    sid    = "AllowIdentityStore"
    effect = "Allow"
    actions = [
      "identitystore:DescribeGroup",
      "identitystore:ListGroups",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "AllowIAM"
    effect = "Allow"
    actions = [
      "iam:GetSAMLProvider"
    ]
    resources = ["*"]
  }

  statement {
    sid    = "AllowTrackingTable"
    effect = "Allow"
    actions = [
      "dynamodb:DeleteItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
    ]
    resources = [
      aws_dynamodb_table.assignments_tracking.arn
    ]
  }

  statement {
    sid    = "AllowSSO"
    effect = "Allow"
    actions = [
      "sso:CreateAccountAssignment",
      "sso:DeleteAccountAssignment",
      "sso:DescribeAccountAssignment",
      "sso:DescribeAccountAssignmentCreationStatus",
      "sso:DescribeAccountAssignmentDeletionStatus",
      "sso:DescribePermissionSet",
      "sso:ListAccountAssignments",
      "sso:ListInstances",
      "sso:ListPermissionSets",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "AllowOrganizations"
    effect = "Allow"
    actions = [
      "organizations:DescribeAccount",
      "organizations:DescribeOrganizationalUnit",
      "organizations:ListAccounts",
      "organizations:ListParents",
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
    actions = [
      "sts:AssumeRole"
    ]
  }
}

## Craft assume role policy for EventBridge Pipes
data "aws_iam_policy_document" "eventbridge_pipes_assume_role" {
  statement {
    sid    = "AllowPipesToAssumeRole"
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["pipes.amazonaws.com"]
    }
    actions = [
      "sts:AssumeRole"
    ]
  }
}

## Craft IAM policy for EventBridge Pipes to read from DynamoDB Streams and invoke targets
data "aws_iam_policy_document" "eventbridge_pipes_policy" {
  statement {
    sid    = "AllowReadFromDynamoDBStream"
    effect = "Allow"
    actions = [
      "dynamodb:GetRecords",
      "dynamodb:GetShardIterator",
      "dynamodb:DescribeStream",
      "dynamodb:ListStreams",
      "dynamodb:ListShards"
    ]
    resources = [
      "${aws_dynamodb_table.config.arn}/stream/*"
    ]
  }

  statement {
    sid    = "AllowInvokeStepFunction"
    effect = "Allow"
    actions = [
      "states:StartExecution"
    ]
    resources = [aws_sfn_state_machine.main.arn]
  }

  dynamic "statement" {
    for_each = var.enable_config_triggers ? [1] : []
    content {
      sid    = "AllowWritePipeLogs"
      effect = "Allow"
      actions = [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
      ]
      resources = [
        aws_cloudwatch_log_group.eventbridge_pipes_config_update[0].arn,
        "${aws_cloudwatch_log_group.eventbridge_pipes_config_update[0].arn}:*",
      ]
    }
  }
}


