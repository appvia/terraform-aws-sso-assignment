"""
AWS SSO Group Assignment Lambda Handler

This Lambda function orchestrates the assignment of AWS SSO groups to accounts.
It reads group configurations from DynamoDB and manages account assignments
across the organization.
"""

import os
import time
from datetime import datetime, timezone
from typing import Any, Tuple

from libs.errors import HandlerError
from libs.logging import logger
from libs.organizations import Organizations
from libs.events import Publisher
from libs.identity_center import IdentityCenter
from libs.tracking import Tracking
from libs.types import Assignment, Binding, Account, Configuration, Permission, Group

# Initialize the events publisher
events_publisher: Publisher | None = None  # pylint: disable=invalid-name


def has_matching_binding(
    assignment: Assignment,
    bindings: list[Binding],
) -> bool:
    """
    Check if the assignment has a matching binding.

    Args:
        assignment: The assignment to check
        binding: The list of bindings to check against

    Returns:
        True if the assignment has a matching binding, False otherwise
    """

    # Iterate over the bindings and check if the assignment has a matching binding
    for binding in bindings:
        logger.debug(
            "Checking if binding has a matching assignment",
            extra={
                "action": "has_matching_binding",
                "assignment.account_id": assignment.account_id,
                "assignment.group_name": assignment.group_name,
                "assignment.permission_set_name": assignment.permission_set_name,
                "binding.account_id": binding.account_id,
                "binding.groups": binding.groups,
                "binding.permission_set_name": binding.permission_set_name,
            },
        )
        # Ensure the assignment is for the correct account
        if assignment.account_id != binding.account_id:
            continue
        # Ensure the assignment is for the correct permission set
        if assignment.permission_set_name != binding.permission_set_name:
            continue
        # Ensure the assignment is for a group that is in the binding
        for group in binding.groups:
            if assignment.principal_id == group.id:
                logger.debug(
                    "Found matching binding",
                    extra={
                        "action": "has_matching_binding",
                        "assignment.account_id": assignment.account_id,
                        "assignment.permission_set_name": assignment.permission_set_name,
                        "assignment.group_name": assignment.group_name,
                    },
                )
                return True

    return False


def reconcile_creations(
    bindings: list[Binding],
    identity_center: IdentityCenter,
    tracking: Tracking,
    dry_run: bool = False,
) -> Tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Assign each binding's permission set to every listed Identity Center group.

    Args:
        bindings: Per-permission-set bindings for one account
        identity_center: The Identity Center client used to create the assignments
        tracking: The tracking client used to manipulate the tracking table

    Returns:
        ``(successes, failures)`` for each attempted assignment
    """

    if len(bindings) == 0:
        logger.warning(
            "No bindings to assign",
            extra={
                "action": "assign_permissions",
                "bindings": bindings,
            },
        )
        return [], []

    logger.info(
        "Assigning permissions to the accounts",
        extra={
            "action": "assign_permissions",
            "bindings": bindings,
        },
    )

    # Initialize the lists to store the successes and failures
    successes: list[dict[str, Any]] = []
    # Initialize the list to store the failures
    failures: list[dict[str, Any]] = []

    # Assign the permission set to the groups
    for binding in bindings:
        # Iterate over the groups in the binding
        for group in binding.groups:
            try:
                identity_center.create_assignment(
                    account_id=binding.account_id,
                    permission_set_arn=binding.permission_set_arn,
                    permission_set_name=binding.permission_set_name,
                    principal_id=group.id,
                    principal_type="GROUP",
                    dry_run=dry_run,
                )
                # Create the item in the tracking table
                tracking.create(
                    account_id=binding.account_id,
                    dry_run=dry_run,
                    group_name=group.name,
                    permission_set_arn=binding.permission_set_arn,
                    permission_set_name=binding.permission_set_name,
                    principal_id=group.id,
                    principal_type="GROUP",
                    template_name=binding.template_name,
                )
                events_publisher.publish(
                    dry_run=dry_run,
                    event_type="AccountAssignmentCreated",
                    detail={
                        "account_id": binding.account_id,
                        "assignment_id": tracking.get_assignment_id(
                            binding.account_id, group.id, binding.permission_set_arn
                        ),
                        "group": {"id": group.id, "name": group.name},
                        "permission_set": {
                            "arn": binding.permission_set_arn,
                            "name": binding.permission_set_name,
                        },
                        "principal_type": "GROUP",
                        "template_name": binding.template_name,
                    },
                )
                # Add the success to the list
                successes.append(
                    {
                        "account_id": binding.account_id,
                        "group_name": group.name,
                        "permission_set_arn": binding.permission_set_arn,
                        "permission_set_name": binding.permission_set_name,
                    }
                )
            except HandlerError as e:
                failures.append(
                    {
                        "account_id": binding.account_id,
                        "group_name": group.name,
                        "permission_set_arn": binding.permission_set_arn,
                        "permission_set_name": binding.permission_set_name,
                        "error": str(e),
                    }
                )

    logger.debug(
        "Completed assigning permission set to groups",
        extra={
            "action": "assign_permissions",
            "successes": successes,
            "failures": failures,
        },
    )

    return successes, failures


def reconcile_deletions(
    desired_bindings: list[Binding],
    tracking: Tracking,
    identity_center: IdentityCenter,
    dry_run: bool = False,
) -> Tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Reconcile provisioned assignments against the desired configuration.

    For each account, compare what is currently provisioned in AWS IAM Identity Center
    against what *should* exist based on the provided bindings. If an active assignment
    exists in AWS but is not present in the desired bindings, delete it.

    Args:
        instance_arn: The ARN of the SSO Instance
        desired_bindings: Full set of desired bindings (typically across all target accounts)
        tracking_table_name: Optional name of DynamoDB tracking table for marking deletions

    Returns:
        (deleted_assignments, deletion_failures) - lists of dictionaries documenting deletions
    """

    logger.info(
        "Starting deletion reconciliation",
        extra={
            "action": "reconcile_deletions",
            "dry_run": dry_run,
            "desired_bindings_count": len(desired_bindings),
            "instance_arn": identity_center.instance_arn,
        },
    )

    # Initialize the lists to store the successful and failed deletions
    successful_deletions: list[dict[str, Any]] = []
    failed_deletions: list[dict[str, Any]] = []

    try:
        # Retrieve all active assignments from the tracking table
        assignments = tracking.list()

        logger.debug(
            "Retrieved all tracking deletions from tracking table",
            extra={
                "action": "reconcile_deletions",
                "assignments_count": len(assignments),
            },
        )
        # If there are no assignments, we can return an empty list
        if len(assignments) == 0:
            logger.debug(
                "No tracking deletions to reconcile",
                extra={
                    "action": "reconcile_deletions",
                },
            )
            return [], []

        # For each of the assignments, we need to find a corresponding binding in t
        # he desired_bindings list. If no matching binding is found, we need to delete
        # the assignment.
        for assignment in assignments:
            if not has_matching_binding(assignment, desired_bindings):
                logger.debug(
                    "Deleting tracking assignment, no matching binding found for account",
                    extra={
                        "action": "reconcile_assignments",
                        "account_id": assignment.account_id,
                        "assignment_id": assignment.assignment_id,
                        "permission_set_name": assignment.permission_set_name,
                    },
                )
                try:
                    # Attempt to delete the assignment from the account
                    identity_center.delete_assignment(
                        account_id=assignment.account_id,
                        dry_run=dry_run,
                        permission_set_arn=assignment.permission_set_arn,
                        permission_set_name=assignment.permission_set_name,
                        principal_id=assignment.principal_id,
                        principal_type=assignment.principal_type,
                    )
                    events_publisher.publish(
                        event_type="AccountAssignmentDeleted",
                        dry_run=dry_run,
                        detail={
                            "account_id": assignment.account_id,
                            "assignment_id": assignment.assignment_id,
                            "group": {
                                "id": assignment.principal_id,
                                "name": assignment.group_name,
                            },
                            "permission_set": {
                                "arn": assignment.permission_set_arn,
                                "name": assignment.permission_set_name,
                            },
                            "principal_type": assignment.principal_type,
                            "template_name": assignment.template_name,
                        },
                    )
                    successful_deletions.append(
                        {
                            "assignment_id": assignment.assignment_id,
                            "account_id": assignment.account_id,
                            "permission_set_name": assignment.permission_set_name,
                        }
                    )
                except Exception as e:
                    # If the deletion failed because the assignment does not exist, we can ignore it
                    if "Assignment does not exist" in str(e):
                        logger.info(
                            "Assignment does not exist, skipping",
                            extra={
                                "action": "reconcile_assignments",
                            },
                        )
                        # Treat "does not exist" as a successful reconciliation outcome:
                        # the desired end state (no assignment) is already met.
                        successful_deletions.append(
                            {
                                "assignment_id": assignment.assignment_id,
                                "account_id": assignment.account_id,
                                "permission_set_name": assignment.permission_set_name,
                            }
                        )
                    else:
                        logger.error(
                            "Error trying to delete assignment",
                            extra={
                                "action": "reconcile_assignments",
                                "assignment_id": assignment.assignment_id,
                                "account_id": assignment.account_id,
                                "permission_set_name": assignment.permission_set_name,
                                "error": str(e),
                            },
                        )
                        failed_deletions.append(
                            {
                                "assignment_id": assignment.assignment_id,
                                "account_id": assignment.account_id,
                                "permission_set_name": assignment.permission_set_name,
                                "error": str(e),
                            }
                        )

                # We need to delete the assignment from the tracking table
                tracking.delete(assignment.assignment_id)
                logger.debug(
                    "Deleted tracking assignment",
                    extra={
                        "action": "reconcile_assignments",
                        "failed_deletions_count": len(failed_deletions),
                        "successful_deletions_count": len(successful_deletions),
                    },
                )

        return successful_deletions, failed_deletions

    except Exception as e:
        logger.error(
            "Unhandled error during tracking assignment reconciliation",
            extra={
                "action": "reconcile_assignments",
                "error": str(e),
            },
        )
        raise


def build_permission_bindings(
    account: Account,
    configuration: Configuration,
    identity_center: IdentityCenter,
    permission: Permission,
) -> Tuple[list[Binding], list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Build a list of bindings from a given permission request.

    Args:
        account: The account to build the bindings for
        configuration: The configuration to use
        identity_center: The identity center to use
        permission: The permission to build the bindings for
    Returns:
        A tuple of (bindings, successes, failures)
    """

    # Initialize the list to store the bindings
    bindings: list[Binding] = []
    # Initialize the lists to store the successes and failures
    successes: list[dict[str, Any]] = []
    # Initialize the list to store the failures
    failures: list[dict[str, Any]] = []

    logger.info(
        "Building bindings from account permissions",
        extra={
            "action": "build_bindings",
            "account_id": account.id,
            "permission": permission,
        },
    )

    template = configuration.templates.get(permission.name)
    if not template:
        failures.append(
            {
                "account_id": account.id,
                "permission": permission.name,
                "error": "Permission template not found in configuration",
            }
        )
        return [], successes, failures

    logger.debug(
        "Found permission template",
        extra={
            "action": "build_bindings",
            "account_id": account.id,
            "permission": permission.name,
            "template": template,
        },
    )

    # Used to hold the groups that are available in the identity store
    available_groups: list[Group] = []

    # Check all the groups exist in the identity store
    for group in permission.groups:
        logger.debug(
            "Checking if group exists in Identity Center",
            extra={
                "action": "build_bindings",
                "account_id": account.id,
                "group": group,
                "permission": permission.name,
            },
        )

        if not identity_center.has_group(group):
            logger.warning(
                "Group not found in Identity Center, skipping",
                extra={
                    "action": "build_bindings",
                    "account_id": account.id,
                    "group": group,
                    "permission": permission.name,
                },
            )
            failures.append(
                {
                    "account_id": account.id,
                    "error": "Group not found in identity store",
                    "group": group,
                    "permission": permission.name,
                }
            )
            continue

        logger.debug(
            "Group found in Identity Center",
            extra={
                "action": "build_permission_bindings",
                "account_id": account.id,
                "permission": permission.name,
                "group": group,
            },
        )

        # Add the group to the list of available groups
        available_groups.append(identity_center.get_group(group))

    # Check all the permission sets exist in the identity store
    for permission_set_name in template.permission_sets:
        # Get the ARN of the permission set
        permission_set = identity_center.get_permission_set(permission_set_name)
        # If the permission set is not found, add a failure to the list
        if not permission_set:
            logger.warning(
                "Permission set not found in identity store, skipping",
                extra={
                    "action": "build_permission_bindings",
                    "account_id": account.id,
                    "permission": permission_set_name,
                },
            )
            failures.append(
                {
                    "account_id": account.id,
                    "permission": permission_set_name,
                    "error": "Permission set not found in identity store",
                }
            )
            continue

        # Build a binding for the permission set
        binding: Binding = Binding(
            account_id=account.id,
            groups=available_groups,
            permission_set_arn=permission_set.arn,
            permission_set_name=permission_set_name,
            template_name=permission.name,
        )
        bindings.append(binding)

    logger.debug(
        "Built the following permission bindings",
        extra={
            "action": "build_permission_bindings",
            "account_id": account.id,
            "bindings": len(bindings),
            "template_name": permission.name,
        },
    )

    return bindings, successes, failures


def build_account_bindings(
    account: Account,
    configuration: Configuration,
    identity_center: IdentityCenter,
) -> Tuple[list[Binding], list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Evaluate which account templates match the given account and return Permission objects.

    Uses AND logic: all specified conditions in a matcher must match for the account template to apply.

    Args:
        account: The account to evaluate
        configuration: The configuration to use

    Returns:
        A tuple of (bindings, successes, failures)
    """

    logger.info(
        "Building account bindings from account templates",
        extra={
            "action": "build_account_bindings",
            "account.name": account.name,
        },
    )

    # Initialize the lists to store the bindings, successes, and failures
    all_bindings: list[Binding] = []
    all_successes: list[dict[str, Any]] = []
    all_failures: list[dict[str, Any]] = []

    for name, template in configuration.account_templates.items():
        # Check if account matches this template's matcher
        if not template.matcher.matches(account):
            logger.debug(
                "Account did not match account template, skipping",
                extra={
                    "action": "build_account_bindings",
                    "account.name": account.name,
                    "account_template_name": name,
                },
            )
            continue

        # If excluded patterns are configured and the account matches any of them,
        # do not apply this account template.
        try:
            if template.is_excluded(account):
                continue
        except HandlerError as e:
            all_failures.append(
                {
                    "account.name": account.name,
                    "account_template_name": name,
                    "error": str(e),
                }
            )
            continue

        logger.debug(
            "Found a matching account template for account",
            extra={
                "action": "build_account_bindings",
                "account.name": account.name,
                "template.groups": template.groups,
                "template.matcher": template.matcher,
                "template.name": name,
                "template.template_names": template.template_names,
            },
        )

        # Create a permission for each of the template's groups
        for template_ref in template.template_names:
            permission = Permission(
                name=template_ref,
                groups=template.groups,
            )
            logger.debug(
                "Building Permission object from account template",
                extra={
                    "action": "build_account_bindings",
                    "account.name": account.name,
                    "template.name": name,
                    "template.template_names": template.template_names,
                    "permission.name": permission.name,
                    "permission.groups": permission.groups,
                },
            )
            # Build the permission bindings
            bindings, successes, failures = build_permission_bindings(
                account=account,
                configuration=configuration,
                identity_center=identity_center,
                permission=permission,
            )

            logger.debug(
                "Built the following permission bindings",
                extra={
                    "action": "build_account_bindings",
                    "account.name": account.name,
                    "bindings": len(bindings),
                    "successes": len(successes),
                    "failures": len(failures),
                },
            )

            all_bindings.extend(bindings)
            all_successes.extend(successes)
            all_failures.extend(failures)

    return all_bindings, all_successes, all_failures


def validate_environment() -> None:
    """
    Validate the environment variables.

    Raises:
        HandlerError: If a required environment variable is missing
    """

    required_variables = [
        "DYNAMODB_CONFIG_TABLE",
        "DYNAMODB_TRACKING_TABLE",
        "SSO_INSTANCE_ARN",
    ]
    for var in required_variables:
        if not os.environ.get(var):
            raise HandlerError(f"Missing required environment variable: {var}")


def lambda_handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    """
    Main Lambda handler for SSO group assignment.

    Args:
        event: EventBridge or Step Function event containing:
        - source: "account_creation" or "cron_schedule"
        - account_id: (optional) specific account ID for single-account mode
        _context: Lambda context object (unused, required by Lambda signature)

    Returns:
        Dictionary with status and optional error details
    """

    logger.info(
        "Starting SSO group assignment run",
        extra={
            "action": "lambda_handler",
            "event": event,
        },
    )
    # Get the current UTC timestamp
    started_at = time.time()

    try:
        global events_publisher  # pylint: disable=global-statement
        # Get the AWS region
        aws_region = event.get("aws_region", os.environ.get("AWS_REGION", "eu-west-2"))
        # Get the logging level
        logging_level = event.get("logging_level", "INFO")
        # Set the logging level
        logger.setLevel(logging_level.upper())
        # Ensure we have a valid environment
        validate_environment()
        # If the events topic ARN is set, initialize the events publisher
        events_publisher = Publisher(
            topic_arn=os.environ.get("EVENTS_SNS_TOPIC_ARN", None),
            region_name=aws_region,
        )
        # Check if running in dry run mode
        dry_run = event.get("dry_run", os.environ.get("ENABLE_DRY_RUN", False))
        if dry_run:
            logger.debug(
                "Dry run mode, all actions will be skipped",
                extra={
                    "action": "lambda_handler",
                    "dry_run": dry_run,
                },
            )

        # Initialize the configuration
        configuration = Configuration(
            table_name=os.environ.get("DYNAMODB_CONFIG_TABLE"), region_name=aws_region
        )
        # Initialize the tracking client
        tracking = Tracking(
            table_name=os.environ.get("DYNAMODB_TRACKING_TABLE"),
            region_name=aws_region,
        )
        # Get the SSO Instance ARN
        identity_center = IdentityCenter(
            instance_arn=os.environ.get("SSO_INSTANCE_ARN"),
            region_name=aws_region,
        )
        # Get the Organizations client
        organizations = Organizations()
        # Get the tagging prefix (module doc default: ``sso``)
        tag_prefix = os.environ.get("SSO_ACCOUNT_TAG_PREFIX") or "sso"

        logger.info(
            "Using the following environment variables",
            extra={
                "action": "lambda_handler",
                "config_table_name": configuration.table_name,
                "instance_arn": identity_center.instance_arn,
                "tracking_table_name": tracking.table_name,
            },
        )

        # Supports the ability to assign to a single account - for debugging purposes
        account_id = event.get("account_id")
        if account_id is not None:
            account_id = str(account_id).strip()
        # Initialize the list of target accounts
        target_accounts: list[Account] = []
        # Get the target accounts, either a single account or all accounts
        if account_id:
            # Set the target accounts to the single account
            target_accounts = [organizations.get_account(account_id)]
        else:
            # List all active accounts
            target_accounts = organizations.list_accounts()

        logger.info(
            "Resolved execution targets",
            extra={
                "action": "lambda_handler",
                "accounts": [account.id for account in target_accounts],
            },
        )

        # Initialize the lists to store the successes and failures
        all_successes: list[dict[str, Any]] = []
        all_failures: list[dict[str, Any]] = []

        # Load the group configurations and account templates from DynamoDB
        configuration.load()
        ## Build a list of bindings for the account
        all_bindings: list[Binding] = []

        # Iterate over the target accounts and build a list of bindings based on
        # the accounts tags and account templates.
        for account in target_accounts:
            # Does the account have permission tags?
            permissions = account.get_permission_tags(tag_prefix)
            # If the account has permission tags, add them to the list of bindings
            if permissions or len(permissions) > 0:
                # Get all the bindings from each permission tags
                for permission in permissions:
                    bindings, successes, failures = build_permission_bindings(
                        account=account,
                        configuration=configuration,
                        identity_center=identity_center,
                        permission=permission,
                    )
                    # Add the bindings to the list
                    all_bindings.extend(bindings)
                    # Add the successes to the list
                    all_successes.extend(successes)
                    # Add the failures to the list
                    all_failures.extend(failures)
            else:
                logger.debug(
                    "Skipping account as it has no permission tags",
                    extra={
                        "action": "lambda_handler",
                        "account.name": account.name,
                    },
                )

            # Does the account conform to any account templates?
            bindings, successes, failures = build_account_bindings(
                account=account,
                configuration=configuration,
                identity_center=identity_center,
            )
            # Add the bindings to the list
            all_bindings.extend(bindings)
            # Add the successes to the list
            all_successes.extend(successes)
            # Add the failures to the list
            all_failures.extend(failures)

        # We should at this point have all the bindings for all the accounts - we should
        # iterate over the bindings and assign the permissions to the groups
        if len(all_bindings) > 0:
            successes, failures = reconcile_creations(
                bindings=all_bindings,
                identity_center=identity_center,
                tracking=tracking,
                dry_run=dry_run,
            )
            # Add the successes to the list
            all_successes.extend(successes)
            # Add the failures to the list
            all_failures.extend(failures)

        # Run reconciliation if tracking is enabled
        reconciliation_deleted: list[dict[str, Any]] = []
        reconciliation_failures: list[dict[str, Any]] = []

        try:
            reconciliation_deleted, reconciliation_failures = reconcile_deletions(
                desired_bindings=all_bindings,
                dry_run=dry_run,
                identity_center=identity_center,
                tracking=tracking,
            )

        except Exception as e:
            logger.error(
                "Reconciliation of deletions failed",
                extra={
                    "action": "lambda_handler",
                    "error": str(e),
                },
            )
            # Log reconciliation failures but don't fail the whole handler
            reconciliation_failures.append(
                {"error": f"Reconciliation failed: {str(e)}", "dry_run": dry_run}
            )

        # At the end of the loop, we should have all the bindings for the account
        status = (
            "success" if not all_failures and not reconciliation_failures else "failed"
        )

        logger.info(
            "Completed SSO group assignment run",
            extra={
                "action": "lambda_handler",
                "assignments_failed": len(all_failures),
                "assignments_succeeded": len(all_successes),
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "started_at": started_at,
                "status": status,
            },
        )

        return {
            "account_id": account_id,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "started_at": started_at,
            "status": status,
            "results": {
                "succeeded": all_successes,
                "failed": all_failures,
                "reconciliation_deleted": reconciliation_deleted,
                "reconciliation_failures": reconciliation_failures,
            },
            "errors": (
                None
                if not all_failures and not reconciliation_failures
                else {
                    "count": len(all_failures) + len(reconciliation_failures),
                    "items": all_failures + reconciliation_failures,
                }
            ),
        }

    except Exception as e:
        logger.error(
            "Unhandled error during SSO group assignment run",
            extra={
                "action": "lambda_handler",
                "error": str(e),
            },
        )
        return {
            "account_id": (event or {}).get("account_id"),
            "errors": {"message": str(e)},
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "results": None,
            "source": (event or {}).get("source", "unknown"),
            "started_at": started_at,
            "status": "error",
            "time_taken_seconds": time.time() - started_at,
        }
