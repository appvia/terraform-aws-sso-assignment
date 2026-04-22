# AWS SSO Assignment Automation - AI Agent Guide

## Project Overview

This is a **Terraform module** that automates **AWS IAM Identity Center (SSO) account assignments** across AWS Organizations using a sophisticated event-driven architecture. The module enables infrastructure-as-code driven SSO management without manual console intervention.

### Core Concept

- **Permission-set templates** are defined in **DynamoDB** (template name → list of permission set names)
- **Member accounts** are tagged with `{prefix}/{template_name}` (default: `sso/default`)
- Tag **values** contain **comma-separated Identity Center group display names**
- **Lambda function** automatically applies template's permission sets to the listed groups for that account
- **EventBridge + Step Functions** orchestrate scheduled reconciliation and handle new account creation

## Architecture Components

### 1. Core Infrastructure (Root Module)

```
EventBridge (schedule/account creation) → Step Functions (retry logic) → Lambda (assignment logic) ↔ DynamoDB (config/tracking) + Organizations (account tags) + Identity Center (assignments)
```

**Key Resources:**

- **DynamoDB Tables**: Configuration storage + assignment tracking
- **Lambda Function**: Python-based assignment processor (1,590 lines)
- **Step Functions**: Orchestration with retry logic and SNS notifications
- **EventBridge**: Scheduled runs (`rate(10 minutes)` default) + account creation triggers
- **IAM Roles**: Comprehensive permissions for all components

### 2. Configuration Module (`modules/config/`)

**Purpose**: Populates DynamoDB with permission-set templates

**Input Format**:

```hcl
groups_configuration = {
  default = {
    permission_sets = ["ReadOnly", "PowerUser"]
    description     = "Baseline access for all accounts"
    enabled         = true  # optional, default: true
  }
  finance = {
    permission_sets = ["FinanceAdmin", "FinanceReadOnly"]
    description     = "Financial systems access"
  }
}
```

### 3. Lambda Function (`assets/functions/handler.py`)

**Comprehensive Python implementation** with:

- **Event-driven processing**: Handles scheduled runs and account creation events
- **Data classes**: Typed models for Groups, Bindings, Permissions, Configurations
- **Account discovery**: Lists all active Organization accounts
- **Tag processing**: Reads `sso/*` tags and matches to templates
- **Identity Store integration**: Maps group display names to IDs
- **Assignment management**: Creates/deletes with completion polling
- **Reconciliation**: Removes assignments that no longer match desired state
- **Tracking**: Records managed assignments in DynamoDB

### 4. Step Functions Workflow

**Sophisticated orchestration**:

- **DetermineExecutionMode**: Routes single-account vs. bulk processing
- **Retry Logic**: 3 attempts with exponential backoff
- **Error Handling**: Intelligent failure routing with optional SNS notifications
- **State Tracking**: Comprehensive execution logging

## Usage Patterns

### Account Tagging Model

```bash
aws organizations tag-resource \
  --resource-id 123456789012 \
  --tags \
    Key=sso/default,Value="App-Developers,App-ReadOnly-Users" \
    Key=sso/finance,Value="Finance-Approvers"
```

### Deployment Pattern

1. **Deploy root module** (DynamoDB, Lambda, Step Functions, EventBridge)
2. **Deploy config module** (populate DynamoDB with templates)
3. **Tag member accounts** with `sso/{template}` keys
4. **EventBridge triggers** automatic reconciliation

### Template Workflow

1. **Define templates** in `groups_configuration`
2. **Apply tags** to accounts with group names
3. **Lambda processes** tags → matches templates → creates assignments
4. **Step Functions** provides retry logic and failure notifications

## Configuration Parameters

### Root Module Inputs

| Parameter                | Purpose                  | Default              |
| ------------------------ | ------------------------ | -------------------- |
| `sso_instance_arn`       | Identity Center instance | Required             |
| `name`                   | Resource name prefix     | `"lz-sso"`           |
| `lambda_schedule`        | EventBridge schedule     | `"rate(10 minutes)"` |
| `lambda_timeout`         | Function timeout         | `60s`                |
| `lambda_memory`          | Memory allocation        | `512MB`              |
| `sso_account_tag_prefix` | Tag key prefix           | `"Grant"`            |
| `sns_topic_arn`          | Failure notifications    | `null` (disabled)    |

### Config Module Inputs

| Parameter              | Purpose                  |
| ---------------------- | ------------------------ |
| `dynamodb_table_name`  | From root module output  |
| `groups_configuration` | Template definitions map |

## Lambda Environment Variables

| Variable                  | Purpose                       |
| ------------------------- | ----------------------------- |
| `DYNAMODB_CONFIG_TABLE`   | Configuration table name      |
| `DYNAMODB_TRACKING_TABLE` | Assignment tracking table     |
| `SSO_INSTANCE_ARN`        | Identity Center instance      |
| `SSO_ACCOUNT_TAG_PREFIX`  | Tag prefix (default: "Grant") |

## Data Models

### DynamoDB Configuration Table

- **Hash Key**: `group_name` (template name)
- **Attributes**: `permission_sets` (string set), `description`, `enabled`

### DynamoDB Tracking Table

- **Hash Key**: `assignment_id`
- **Purpose**: Track managed assignments for reconciliation

### Lambda Data Classes

- **Groups**: Identity Center group mapping
- **Bindings**: Group-to-permission-set associations
- **Permissions**: Permission set definitions
- **Configurations**: Template definitions

## Event Flow

### Scheduled Processing

1. **EventBridge** triggers on schedule (`rate(10 minutes)`)
2. **Step Functions** starts with all-accounts mode
3. **Lambda** processes all Organization accounts
4. **Assignment reconciliation** across all accounts

### Account Creation Processing

1. **CloudTrail** detects Organizations `CreateAccount`
2. **EventBridge** rule matches event
3. **Step Functions** starts with single-account mode
4. **Lambda** processes new account only

## IAM Permissions Required

### Lambda Function

- **DynamoDB**: Read config table, read/write tracking table
- **SSO Admin**: List/create/delete permission set assignments
- **Identity Store**: List groups and users
- **Organizations**: Read account tags and list accounts
- **CloudWatch**: Write logs

### Step Functions

- **Lambda**: Invoke function
- **SNS**: Publish to notification topic (optional)

### EventBridge

- **Step Functions**: Start execution

## Troubleshooting Guide

### Common Issues

- **No assignments**: Check DynamoDB config table has data, verify account tags exist
- **Group not found**: Ensure tag values match Identity Center group **DisplayName** exactly
- **Timeouts**: Increase `lambda_timeout` and `lambda_memory` for large organizations
- **Permissions**: Verify IAM roles have required SSO Admin and Organizations permissions

### Debugging Commands

```bash
# Check configuration table
TABLE_NAME=$(terraform output -raw dynamodb_table_name)
aws dynamodb scan --table-name "$TABLE_NAME"

# Verify account tags
aws organizations list-tags-for-resource --resource-id 123456789012

# Check function logs
FUNCTION_NAME=$(terraform output -raw lambda_function_name)
aws logs describe-log-groups --log-group-name-prefix "/aws/lambda/$FUNCTION_NAME"

# Test Step Function
STEP_ARN=$(terraform output -raw step_function_arn)
aws stepfunctions start-execution --state-machine-arn "$STEP_ARN" --input '{"source":"cron_schedule"}'
```

## Development Guidelines

### Code Organization

- **Root module**: Infrastructure provisioning only
- **Config module**: Data population only
- **Lambda**: Business logic with comprehensive error handling
- **Examples**: Real-world usage patterns

### Best Practices

- **Pin module versions** in production (`?ref=v1.0.0`)
- **Use existing SNS topics** for notifications
- **Start with basic example** then customize
- **Monitor CloudWatch logs** for assignment details
- **Test with small account sets** first

### Extension Points

- **Custom tag prefixes**: Modify `sso_account_tag_prefix`
- **Additional templates**: Extend `groups_configuration`
- **Custom schedules**: Adjust `lambda_schedule`
- **Memory optimization**: Tune `lambda_memory` and `lambda_timeout`
- **Notification integration**: Connect `sns_topic_arn`

## Prerequisites

1. **AWS Organizations** enabled with management account access
2. **IAM Identity Center** enabled with existing permission sets and groups
3. **Terraform >= 1.0** and **AWS provider >= 6.0**
4. **Permission sets and groups** pre-created in Identity Center
5. **SNS topic** (optional) for failure notifications

## Security Considerations

- **Least privilege IAM**: All roles follow minimal permissions
- **Resource-based policies**: DynamoDB tables are module-private
- **CloudWatch encryption**: Optional KMS key support
- **ARM64 architecture**: Lambda runs on Graviton for performance
- **Account isolation**: Each account processes independently

## Lambda Function Development Guide

### Code Structure (`assets/functions/handler.py`)

The Lambda function is a **1,590-line Python implementation** with sophisticated architecture:

#### **Core Components**

**Data Models** (Dataclasses with JSON serialization):
- `Group`: Identity Center group mapping (name, id)
- `Binding`: Permission set to group associations for accounts
- `Permission`: Template processing (name, groups list)
- `GroupConfiguration`: Template definition (permission_sets, enabled, description)
- `Configuration`: Complete template collection
- `TrackedAssignment`: Assignment tracking record (for reconciliation)

**Main Functions**:
```python
# Entry point - handles both cron and single-account events
lambda_handler(event, context) -> dict

# Core workflow functions
load_configuration(table_name) -> Configuration
get_identity_store_groups(identity_store_id) -> dict[str, str]
get_permission_sets(instance_arn) -> dict[str, str]
get_bindings(account_id, groups, permission_sets, request, template) -> tuple
assign_permissions(bindings, instance_arn, tracking_table) -> tuple
reconcile_assignments(instance_arn, desired_bindings, tracking_table) -> tuple

# Assignment management (with polling)
create_account_assignment(..., poll_timeout_seconds=60) -> None
delete_permission(..., poll_timeout_seconds=60) -> None

# Account and tag processing
list_active_accounts() -> list[str]
get_account_tags(account_id) -> dict[str, str]
get_account_permission_tags(tags, prefix) -> list[Permission]

# Assignment tracking for reconciliation
record_tracking_assignment(tracking_table, assignment_id, ...)
get_tracking_assignments(tracking_table) -> list[TrackedAssignment]
delete_tracking_assignment(account_id, assignment_id, ...)
```

#### **Key Design Patterns**

**Event-Driven Processing**:
- **Scheduled Mode**: `{"source": "cron_schedule"}` → processes all Organization accounts
- **Single Account**: `{"source": "account_creation", "account_id": "123456789012"}` → processes one account

**Polling Pattern** (for async SSO operations):
```python
deadline = time.time() + poll_timeout_seconds
while True:
    status = describe_operation_status(request_id)
    if status == "SUCCEEDED": return
    if status == "FAILED": raise HandlerError(failure_reason)
    if time.time() >= deadline: raise HandlerError("Timed out")
    time.sleep(poll_interval_seconds)
```

**Error Handling**:
- `HandlerError` for expected failures (marks workflow as failed)
- Comprehensive logging with structured JSON output
- Graceful degradation (continues processing other accounts on individual failures)

**Assignment Reconciliation**:
1. Get all tracked assignments from DynamoDB
2. Compare against desired bindings from current account tags
3. Delete assignments that no longer match
4. Remove from tracking table

#### **Environment Variables**

```python
# Required
DYNAMODB_CONFIG_TABLE      # Configuration templates table
DYNAMODB_TRACKING_TABLE    # Assignment tracking table  
SSO_INSTANCE_ARN          # Identity Center instance ARN

# Optional
SSO_ACCOUNT_TAG_PREFIX    # Tag prefix (default: "sso")
LOG_LEVEL                 # Logging level (default: "INFO")
AWS_REGION               # AWS region (default: "us-east-1")
```

### Unit Testing Guide (`assets/functions/test_handler.py`)

#### **Test Structure & Patterns**

The test suite uses **pytest** with **comprehensive mocking** of AWS services:

**Test Organization**:
```python
class TestConfigurationModel:       # Data model validation
class TestPermissionAndBindingModels: # Core data structures
class TestGetIdentityStoreId:       # SSO Admin API interactions
class TestGetPermissionSets:        # Permission set enumeration
class TestGetIdentityStoreGroups:   # Group discovery
class TestListActiveAccounts:      # Organizations API
class TestGetAccountTags:          # Account tag processing
class TestLoadConfiguration:       # DynamoDB configuration loading
class TestGetBindings:             # Binding generation logic
class TestCreateAccountAssignment: # Assignment creation with polling
class TestAssignPermissions:       # Batch assignment processing
class TestLambdaHandler:           # End-to-end handler testing
class TestReconcileAssignments:    # Assignment reconciliation
```

#### **Testing Patterns & Techniques**

**AWS Service Mocking**:
```python
# Mock boto3 clients with patch.object
@patch.object(handler, "sso_admin", mock_sso)
@patch.object(handler, "organizations", mock_org)
@patch.object(handler, "dynamodb", mock_dynamodb)

# Mock paginated API responses
paginator = MagicMock()
mock_sso.get_paginator.return_value = paginator
paginator.paginate.return_value = [{"PermissionSets": ["arn:ps:1"]}]
```

**Environment Variable Testing**:
```python
@pytest.fixture(autouse=True)
def restore_env():
    """Snapshot and restore env vars after each test."""
    keys = ("DYNAMODB_CONFIG_TABLE", "SSO_INSTANCE_ARN", ...)
    before = {k: os.environ.get(k) for k in keys}
    yield
    # Restore original values
```

**Error Simulation**:
```python
# Test ClientError handling
mock_org.list_tags_for_resource.side_effect = ClientError(
    {"Error": {"Code": "AccessDenied", "Message": "nope"}},
    "ListTagsForResource"
)
with pytest.raises(handler.HandlerError, match="Could not list tags"):
    handler.get_account_tags("123456789012")
```

**Polling Logic Testing**:
```python
# Mock time progression for timeout testing
with patch("handler.time.time", side_effect=[0, 100]):
    with patch("handler.time.sleep"):
        with pytest.raises(handler.HandlerError, match="Timed out"):
            handler.create_account_assignment(...)
```

#### **Test Data Patterns**

**Mock AWS Responses**:
```python
# Organizations account list
mock_org.list_accounts.return_value = {
    "Accounts": [
        {"Id": "111111111111", "Status": "ACTIVE"},
        {"Id": "222222222222", "Status": "SUSPENDED"}
    ]
}

# Identity Store groups
mock_ids.list_groups.return_value = {
    "Groups": [
        {"DisplayName": "TeamA", "GroupId": "g-1"},
        {"DisplayName": "TeamB", "GroupId": "g-2"}
    ]
}

# Account tags
mock_org.list_tags_for_resource.return_value = {
    "Tags": [
        {"Key": "sso/default", "Value": "g1,g2"},
        {"Key": "other", "Value": "x"}
    ]
}
```

**Configuration Objects**:
```python
cfg = handler.Configuration(
    groups={
        "default": handler.GroupConfiguration(
            permission_sets=["Admin", "ReadOnly"],
            enabled=True,
            description="Default template"
        )
    }
)
```

#### **Running Tests**

**Local Testing**:
```bash
cd assets/functions/

# Install dependencies
pip install pytest boto3 botocore

# Run all tests
pytest test_handler.py -v

# Run specific test class
pytest test_handler.py::TestLambdaHandler -v

# Run with coverage
pytest test_handler.py --cov=handler --cov-report=html

# Debug specific test
pytest test_handler.py::TestLambdaHandler::test_single_account_success_path -s -v
```

**Test Coverage Areas**:
- ✅ **Data model serialization** (JSON roundtrip)
- ✅ **AWS API pagination** (Organizations, SSO Admin, Identity Store)
- ✅ **Error handling** (ClientError → HandlerError conversion)
- ✅ **Environment validation** (required variables)
- ✅ **Tag processing** (prefix filtering, group parsing)
- ✅ **Assignment logic** (creation, polling, reconciliation)
- ✅ **End-to-end workflows** (single account, cron schedule)
- ✅ **Failure scenarios** (timeouts, missing groups, disabled templates)

#### **Testing Best Practices for Extensions**

**New Function Testing**:
1. **Mock all AWS calls** with realistic response structures
2. **Test error paths** using `ClientError` side effects
3. **Validate logging** with `caplog` fixture for important operations
4. **Test edge cases** (empty results, missing fields, pagination)

**Mock Pattern for New AWS APIs**:
```python
def test_new_aws_function():
    mock_client = MagicMock()
    mock_client.new_operation.return_value = {"Result": "value"}
    
    with patch.object(handler, "new_aws_client", mock_client):
        result = handler.new_function("param")
    
    assert result == "expected"
    mock_client.new_operation.assert_called_once_with(Param="param")
```

**Data Structure Testing**:
```python
def test_new_dataclass():
    obj = handler.NewDataClass(field1="value1", field2=["list"])
    
    # Test JSON serialization
    data = json.loads(obj.to_json())
    assert data["field1"] == "value1"
    assert data["field2"] == ["list"]
    
    # Test field access
    assert obj.field1 == "value1"
```

This comprehensive testing framework ensures **high code quality**, **AWS service reliability**, and **easier debugging** when issues arise in production environments.

---

This module represents a **production-ready, enterprise-scale solution** for automating AWS SSO assignments across large multi-account environments using infrastructure-as-code principles.

