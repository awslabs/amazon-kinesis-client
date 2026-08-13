"""
Copyright 2024 Amazon.com, Inc. or its affiliates.
Licensed under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import argparse
import builtins
import time
from datetime import datetime, timezone

from enum import Enum
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# DynamoDB attribute names and values
CLIENT_VERSION_ATTR = 'cv'
TIMESTAMP_ATTR = 'mts'
MODIFIED_BY_ATTR = 'mb'
HISTORY_ATTR = 'h'
MIGRATION_KEY = "Migration3.0"

# GSI constants
GSI_NAME = 'LeaseOwnerToLeaseKeyIndex'
GSI_DELETION_WAIT_TIME_SECONDS = 120

# Entity type constants for non-lease entity cleanup.
ENTITY_TYPE_ATTR = 'entityType'
ENTITY_TYPE_LEASE = 'LEASE'
BATCH_DELETE_SIZE = 25  # DynamoDB BatchWriteItem limit

# Confirmation phrase for rollback-to-v2
ROLLBACK_CONFIRMATION_PHRASE = "Code rollback to CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X_PHASE1 completed"

config = Config(
    retries = {
        'max_attempts': 10,
        'mode': 'standard'
    }
)


def _utc_timestamp():
    """Return the current UTC time formatted for log prefixes, e.g. '2026-09-07T22:54:02.874Z'."""
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'


def print(*args, **kwargs):
    """
    Drop-in replacement for the builtin print that prepends a UTC timestamp to
    every log line emitted by this tool. Blank or whitespace-only messages
    (used for spacing/banner separators) are passed through unmodified so the
    interactive UI stays readable.

    Multi-line messages are timestamped per line so each logged line is
    independently parseable.
    """
    sep = kwargs.get('sep', ' ')
    message = sep.join(str(arg) for arg in args)

    # Preserve intentional blank lines / separators without a timestamp.
    if message.strip() == '':
        builtins.print(*args, **kwargs)
        return

    prefix = f'[{_utc_timestamp()}] '
    timestamped = '\n'.join(
        prefix + line if line.strip() != '' else line
        for line in message.split('\n')
    )
    # Emit as a single positional arg so caller-provided end/file kwargs still apply,
    # while sep no longer affects the already-joined message.
    kwargs.pop('sep', None)
    builtins.print(timestamped, **kwargs)


class KclClientVersion(Enum):
    VERSION_2X = "CLIENT_VERSION_2X"
    UPGRADE_FROM_2X = "CLIENT_VERSION_UPGRADE_FROM_2X"
    VERSION_3X_WITH_ROLLBACK = "CLIENT_VERSION_3X_WITH_ROLLBACK"
    VERSION_3X = "CLIENT_VERSION_3X"

    def __str__(self):
        return self.value


def get_time_in_millis():
    return str(round(time.time() * 1000))


def is_valid_version(version, mode):
    """
    Validate if the given version is valid for the specified mode

    :param version: The KCL client version to validate
    :param mode: Either 'rollback' or 'rollforward'
    :return: True if the version is valid for the given mode, False otherwise
    """
    if mode == 'rollback':
        if version == KclClientVersion.VERSION_2X.value:
            print("Your KCL application already runs in a mode compatible with KCL 2.x."
                  " Please rollback to Phase 1 by deploying your KCL 3.5.x application with"
                  "  the Phase 1 configuration, if you still experience an issue.")
            return True
        if version in [KclClientVersion.UPGRADE_FROM_2X.value,
                       KclClientVersion.VERSION_3X_WITH_ROLLBACK.value]:
            return True
        if version == KclClientVersion.VERSION_3X.value:
            print("Cannot roll back the KCL application."
                  " It is not in a state that supports rollback.")
            return False
        print("Migration to KCL 3.0 not in progress or application_name / lease_table_name is incorrect."
              " Please double check and run again with correct arguments.")
        return False

    if mode == 'rollforward':
        if version == KclClientVersion.VERSION_2X.value:
            return True
        if version in [KclClientVersion.UPGRADE_FROM_2X.value,
                       KclClientVersion.VERSION_3X_WITH_ROLLBACK.value]:
            print("Cannot roll-forward application. It is not in a rolled back state.")
            return False
        if version == KclClientVersion.VERSION_3X.value:
            print("Cannot roll-forward the KCL application."
                  " Application has already migrated.")
            return False
        print("Cannot roll-forward because migration to KCL 3.0 is not in progress or application_name"
              " / lease_table_name is incorrect. Please double check and run again with correct arguments.")
        return False
    print(f"Invalid mode: {mode}. Mode must be either 'rollback' or 'rollforward'.")
    return False


def handle_get_item_client_error(e, operation, table_name):
    """
    Handle ClientError exceptions raised by get_item on given DynamoDB table

    :param e: The ClientError exception object
    :param operation: Rollback or Roll-forward for logging the errors
    :param table_name: The name of the DynamoDB table where the error occurred
    """
    error_code = e.response['Error']['Code']
    error_message = e.response['Error']['Message']
    print(f"{operation} could not be performed.")
    if error_code == 'ProvisionedThroughputExceededException':
        print(f"Throughput exceeded even after retries: {error_message}")
    else:
        print(f"Unexpected client error occurred: {error_code} - {error_message}")
    print("Please resolve the issue and run the KclMigrationTool again.")


def table_exists(dynamodb_client, table_name):
    """
    Check if a DynamoDB table exists.

    :param dynamodb_client: Boto3 DynamoDB client
    :param table_name: Name of the DynamoDB table to check
    :return: True if the table exists, False otherwise
    """
    try:
        dynamodb_client.describe_table(TableName=table_name)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print(f"Table '{table_name}' does not exist.")
            return False
        print(f"An error occurred while checking table '{table_name}': {e}.")
        return False


def validate_tables(dynamodb_client, operation, lease_table_name=None):
    """
    Validate the existence of DynamoDB tables required for KCL operations

    :param dynamodb_client: A boto3 DynamoDB client object
    :param operation: Rollback or Roll-forward for logging
    :param lease_table_name: Name of the DynamoDB KCL lease table (optional)
    :return: True if all required tables exist, False otherwise
    """
    if lease_table_name and not table_exists(dynamodb_client, lease_table_name):
        print(
            f"{operation} failed. Could not find a KCL Application DDB lease table "
            f"with name {lease_table_name}. Please pass in the correct application_name "
            "and/or lease_table_name that matches your KCL application configuration."
        )
        return False

    return True


def add_current_state_to_history(item, max_history=10):
    """
    Adds the current state of a DynamoDB item to its history attribute.
    Creates a new history entry from the current value and maintains a capped history list.

    :param item: DynamoDB item to add history to
    :param max_history: Maximum number of history entries to maintain (default: 10)
    :return: Updated history attribute as a DynamoDB-formatted dictionary
    """
    # Extract current values
    current_version = item.get(CLIENT_VERSION_ATTR, {}).get('S', 'Unknown')
    current_modified_by = item.get(MODIFIED_BY_ATTR, {}).get('S', 'Unknown')
    current_time_in_millis = (
        item.get(TIMESTAMP_ATTR, {}).get('N', get_time_in_millis())
    )

    # Create new history entry
    new_entry = {
        'M': {
            CLIENT_VERSION_ATTR: {'S': current_version},
            MODIFIED_BY_ATTR: {'S': current_modified_by},
            TIMESTAMP_ATTR: {'N': current_time_in_millis}
        }
    }

    # Get existing history or create new if doesn't exist
    history_dict = item.get(f'{HISTORY_ATTR}', {'L': []})
    history_list = history_dict['L']

    # Add new entry to the beginning of the list, capping at max_history
    history_list.insert(0, new_entry)
    history_list = history_list[:max_history]

    return history_dict


def get_current_state(dynamodb_client, table_name):
    """
    Retrieve the current state from the DynamoDB table and prepare history update.
    Fetches the current item from the specified DynamoDB table,
    extracts the initial client version, and creates a new history entry.

    :param dynamodb_client: Boto3 DynamoDB client
    :param table_name: Name of the DynamoDB table to query
    :return: A tuple containing:
             - initial_version (str): The current client version, or 'Unknown' if not found
             - new_history (dict): Updated history including the current state
    """
    response = dynamodb_client.get_item(
        TableName=table_name,
        Key={'leaseKey': {'S': MIGRATION_KEY}}
    )
    item = response.get('Item', {})
    initial_version = item.get(CLIENT_VERSION_ATTR, {}).get('S', 'Unknown')
    new_history = add_current_state_to_history(item)
    return initial_version, new_history


def rollback_client_version(dynamodb_client, table_name, history):
    """
    Update the client version in the lease table to initiate rollback.

    :param dynamodb_client: Boto3 DynamoDB client
    :param table_name: Name of the DynamoDB lease table
    :param history: Updated history attribute as a DynamoDB-formatted dictionary
    :return: A tuple containing:
             - success (bool): True if client version was successfully updated, False otherwise
             - previous_version (str): The version that was replaced, or None if update failed
    """
    try:
        print(f"Rolling back client version in table '{table_name}'...")
        update_response = dynamodb_client.update_item(
            TableName=table_name,
            Key={'leaseKey': {'S': MIGRATION_KEY}},
            UpdateExpression=(
                f"SET {CLIENT_VERSION_ATTR} = :rollback_client_version, "
                f"{TIMESTAMP_ATTR} = :updated_at, "
                f"{MODIFIED_BY_ATTR} = :modifier, "
                f"{HISTORY_ATTR} = :history"
            ),
            ConditionExpression=(
                f"{CLIENT_VERSION_ATTR} IN ("
                ":upgrade_from_2x_client_version, "
                ":3x_with_rollback_client_version)"
            ),
            ExpressionAttributeValues={
                ':rollback_client_version': {'S': KclClientVersion.VERSION_2X.value},
                ':updated_at': {'N': get_time_in_millis()},
                ':modifier': {'S': 'KclMigrationTool-rollback'},
                ':history': history,
                ':upgrade_from_2x_client_version': (
                    {'S': KclClientVersion.UPGRADE_FROM_2X.value}
                ),
                ':3x_with_rollback_client_version': (
                    {'S': KclClientVersion.VERSION_3X_WITH_ROLLBACK.value}
                ),
            },
            ReturnValues='UPDATED_OLD'
        )
        replaced_item = update_response.get('Attributes', {})
        replaced_version = replaced_item.get('cv', {}).get('S', '')
        return True, replaced_version
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            print("Unable to rollback, as application is not in a state that allows rollback. "
                  "Ensure that the given application_name or lease_table_name is correct and"
                  " you have followed all prior migration steps.")
        else:
            print(f"An unexpected error occurred while rolling back: {str(e)}"
                  "Please resolve and run this migration script again.")
        return False, None


def rollfoward_client_version(dynamodb_client, table_name, history):
    """
    Update the client version in the lease table to initiate roll-forward
    conditionally if application is currently in rolled back state.

    :param dynamodb_client: Boto3 DynamoDB client
    :param table_name: Name of the DynamoDB lease table
    :param history: Updated history attribute as a DynamoDB-formatted dictionary
    :return: True if client version was successfully updated, False otherwise
    """
    try:
        # Conditionally update client version
        dynamodb_client.update_item(
            TableName=table_name,
            Key={'leaseKey': {'S': MIGRATION_KEY}},
            UpdateExpression= (
                f"SET {CLIENT_VERSION_ATTR} = :rollforward_version, "
                f"{TIMESTAMP_ATTR} = :updated_at, "
                f"{MODIFIED_BY_ATTR} = :modifier, "
                f"{HISTORY_ATTR} = :new_history"
            ),
            ConditionExpression=f"{CLIENT_VERSION_ATTR} = :kcl_2x_version",
            ExpressionAttributeValues={
                ':rollforward_version': {'S': KclClientVersion.UPGRADE_FROM_2X.value},
                ':updated_at': {'N': get_time_in_millis()},
                ':modifier': {'S': 'KclMigrationTool-rollforward'},
                ':new_history': history,
                ':kcl_2x_version': {'S': KclClientVersion.VERSION_2X.value},
            }
        )
        print("Roll-forward has been initiated. KCL application will monitor for 3.0 readiness and"
              " automatically switch to 3.0 functionality when readiness criteria have been met.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            print("Unable to roll-forward because application is not in rolled back state."
                  " Ensure that the given application_name or lease_table_name is correct"
                  " and you have followed all prior migration steps.")
        else:
            print(f"Unable to roll-forward due to error: {str(e)}. "
                  "Please resolve and run this migration script again.")
    except Exception as e:
        print(f"Unable to roll-forward due to error: {str(e)}. "
              "Please resolve and run this migration script again.")


def delete_gsi_if_exists(dynamodb_client, table_name):
    """
    Deletes GSI on given lease table if it exists.

    :param dynamodb_client: Boto3 DynamoDB client
    :param table_name: Name of lease table to remove GSI from
    """
    try:
        gsi_present = False
        response = dynamodb_client.describe_table(TableName=table_name)
        if 'GlobalSecondaryIndexes' in response['Table']:
            gsi_list = response['Table']['GlobalSecondaryIndexes']
            for gsi in gsi_list:
                if gsi['IndexName'] == GSI_NAME:
                    gsi_present = True
                    break

        if not gsi_present:
            print(f"GSI {GSI_NAME} is not present on lease table {table_name}. It may already be successfully"
                  " deleted. Or if lease table name is incorrect, please re-run the KclMigrationTool with correct"
                  " application_name or lease_table_name.")
            return
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print(f"Lease table {table_name} does not exist, please check application_name or lease_table_name"
                  " configuration and try again.")
            return
        else:
            print(f"An unexpected error occurred while checking if GSI {GSI_NAME} exists"
                  f" on lease table {table_name}: {str(e)}. Please rectify the error and try again.")
            return

    print(f"Deleting GSI '{GSI_NAME}' from table '{table_name}'...")
    try:
        dynamodb_client.update_table(
            TableName=table_name,
            GlobalSecondaryIndexUpdates=[
                {
                    'Delete': {
                        'IndexName': GSI_NAME
                    }
                }
            ]
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print(f"{GSI_NAME} not found or table '{table_name}' not found.")
        elif e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"Unable to delete GSI: '{table_name}' is currently being modified.")
    except Exception as e:
        print(f"An unexpected error occurred while deleting GSI {GSI_NAME} on lease table {table_name}: {str(e)}."
              " Please manually confirm the GSI is removed from the lease table, or"
              " resolve the error and rerun the migration script.")

def scan_non_lease_entities(dynamodb_client, table_name):
    """
    Scan the lease table for all items where EntityType (et) exists,
    is not 'LEASE', and is not empty. These are non-lease entities
    written by KCL v3.5 Phase 2 that must be removed before rolling
    back to v2.

    :param dynamodb_client: Boto3 DynamoDB client
    :param table_name: Name of the DynamoDB lease table
    :return: List of (leaseKey, full_item) tuples to delete
    """
    non_lease_entries = []
    scan_kwargs = {
        'TableName': table_name,
        'FilterExpression': (
            'attribute_exists(#et) AND #et <> :lease_type AND #et <> :empty_string'
        ),
        'ExpressionAttributeNames': {'#et': ENTITY_TYPE_ATTR},
        'ExpressionAttributeValues': {
            ':lease_type': {'S': ENTITY_TYPE_LEASE},
            ':empty_string': {'S': ''}
        }
    }

    while True:
        response = dynamodb_client.scan(**scan_kwargs)
        for item in response.get('Items', []):
            lease_key = item['leaseKey']['S']
            non_lease_entries.append((lease_key, item))
        if 'LastEvaluatedKey' not in response:
            break
        scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']

    return non_lease_entries


def delete_non_lease_entities(dynamodb_client, table_name, keys_to_delete):
    """
    Batch-delete non-lease entities from the lease table.

    :param dynamodb_client: Boto3 DynamoDB client
    :param table_name: Name of the DynamoDB lease table
    :param keys_to_delete: List of leaseKey string values to delete
    :return: Number of items successfully deleted
    """
    deleted_count = 0
    for i in range(0, len(keys_to_delete), BATCH_DELETE_SIZE):
        batch = keys_to_delete[i:i + BATCH_DELETE_SIZE]
        request_items = {
            table_name: [
                {'DeleteRequest': {'Key': {'leaseKey': {'S': key}}}}
                for key in batch
            ]
        }
        try:
            response = dynamodb_client.batch_write_item(RequestItems=request_items)
            unprocessed = response.get('UnprocessedItems', {})
            retry_count = 0
            while unprocessed and retry_count < 3:
                time.sleep(2 ** retry_count)
                response = dynamodb_client.batch_write_item(RequestItems=unprocessed)
                unprocessed = response.get('UnprocessedItems', {})
                retry_count += 1
            if unprocessed:
                unprocessed_count = len(unprocessed.get(table_name, []))
                print(f"WARNING: {unprocessed_count} items not deleted after retries.")
                deleted_count += len(batch) - unprocessed_count
            else:
                deleted_count += len(batch)
        except ClientError as e:
            print(f"Error during batch delete: {e.response['Error']['Code']} - "
                  f"{e.response['Error']['Message']}")
            print(f"Deleted {deleted_count} items before failure. Re-run to continue.")
            return deleted_count
    return deleted_count


def perform_rollback_to_v2(dynamodb_client, lease_table_name):
    """
    Delete all non-lease entities from the lease table to enable rollback to v2.

    Prerequisites:
    1. Phase 2 -> Phase 1 rollback completed using this tool (MigrationState.ClientVersion == CLIENT_VERSION_2X)
    2. ALL workers have completed the Phase 1 code rollback (needs user confirmation)

    :param dynamodb_client: Boto3 DynamoDB client
    :param lease_table_name: Name of the DynamoDB lease table
    """
    if not validate_tables(dynamodb_client, "Rollback-to-v2", lease_table_name):
        return

    # Step 1: Verify MigrationState.ClientVersion == CLIENT_VERSION_2X
    try:
        response = dynamodb_client.get_item(
            TableName=lease_table_name,
            Key={'leaseKey': {'S': MIGRATION_KEY}}
        )
        item = response.get('Item', {})
        current_version = item.get(CLIENT_VERSION_ATTR, {}).get('S', 'Unknown')
    except ClientError as e:
        handle_get_item_client_error(e, "Rollback-to-v2", lease_table_name)
        return

    if current_version != KclClientVersion.VERSION_2X.value:
        print(f"ERROR: MigrationState.ClientVersion is '{current_version}'. "
              f"Expected '{KclClientVersion.VERSION_2X.value}'.")
        print("You must first complete the Phase 2 -> Phase 1 rollback:")
        print(f"  python3 ./KclMigrationTool.py --region <region> "
              f"--mode rollback-to-phase1 --lease_table_name {lease_table_name}")
        print("Then wait for ALL workers to complete the Phase 1 code rollback before re-running.")
        return

    # Step 2: Scan for non-lease entities first to check if cleanup is needed
    print(f"\nScanning '{lease_table_name}' for non-lease entities...")
    non_lease_entries = scan_non_lease_entities(dynamodb_client, lease_table_name)

    if not non_lease_entries:
        print("No non-lease entities found. The lease table is already clean for v2 rollback.")
        print("Next step: redeploy your application with the previous KCL v2 version.")
        return

    print(f"\nFound {len(non_lease_entries)} non-lease entities to delete:\n")
    for i, (lease_key, item) in enumerate(non_lease_entries, 1):
        print(f"--- Entry {i} ---")
        for attr_name, attr_value in sorted(item.items()):
            print(f"  {attr_name}: {attr_value}")
        print()
    print("-" * 80)

    # Step 3: Operator confirmation that the Phase 1 code rollback has actually
    # reached every worker. Note: we do NOT ask the operator to confirm that
    # rollback-to-phase1 was run -- Step 1 above already verified
    # MigrationState.ClientVersion == CLIENT_VERSION_2X, which is only reachable
    # via rollback-to-phase1. The remaining risk the tool cannot prove is that
    # all *workers* (not just the leader) have finished deploying the Phase 1
    # config, so that is what the operator must confirm here.
    print("\n" + "=" * 70)
    print("MigrationState.ClientVersion is already confirmed to be "
          f"{KclClientVersion.VERSION_2X.value},")
    print("so the Phase 2 -> Phase 1 rollback has run. Before proceeding, confirm:")
    print("")
    print("  ALL workers in your fleet have completed the Phase 1 code")
    print("  rollback deployment with:")
    print("     clientVersionConfig = CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X_PHASE1")
    print("")
    print("Verify using your CI/CD system that no Phase 2 workers remain.")
    print("=" * 70)
    print(f"\nType exactly: {ROLLBACK_CONFIRMATION_PHRASE}")
    user_input = input("> ").strip()
    if user_input != ROLLBACK_CONFIRMATION_PHRASE:
        print("Confirmation did not match. Aborting.")
        return

    # Step 4: Confirm deletion
    print(f"\nThe above {len(non_lease_entries)} entries will be permanently deleted "
          f"from '{lease_table_name}'.")
    print("After deletion, rollforward to Phase 2 will require a fresh migration from Phase 1.")
    print("\nType 'yes' to confirm deletion:")
    user_input = input("> ").strip()
    if user_input != 'yes':
        print("Aborting. No items deleted.")
        return

    # Step 5: Delete non-lease entities
    keys_to_delete = [entry[0] for entry in non_lease_entries]
    print(f"\nDeleting {len(keys_to_delete)} non-lease entities...")
    deleted = delete_non_lease_entities(dynamodb_client, lease_table_name, keys_to_delete)
    print(f"\nDone. Deleted {deleted}/{len(keys_to_delete)} non-lease entities.")

    if deleted == len(keys_to_delete):
        print("\nThe lease table is clean for v2 rollback.")
        print("Next step: redeploy your application with the previous KCL v2 version.")
        print("\nWhen ready to migrate to KCL v3.5 again:")
        print("  1. Deploy with Phase 1 config (no tool needed)")
        print("  2. Bake Phase 1 for your confidence interval")
        print("  3. Deploy with Phase 2 config (no tool needed to rollforward)")
    else:
        print(f"\n{len(keys_to_delete) - deleted} items were not deleted. Re-run the tool to retry.")


def perform_rollback(dynamodb_client, lease_table_name):
    """
    Perform KCL 3.0 migration rollback by updating MigrationState for the KCL application.
    Rolls client version back for 2X functionality and removes GSI from lease table.

    :param dynamodb_client: Boto3 DynamoDB client
    :param lease_table_name: Name of the DynamoDB lease table
    """
    if not validate_tables(dynamodb_client, "Rollback", lease_table_name):
        return

    try:
        initial_version, new_history = get_current_state(dynamodb_client, lease_table_name)
    except ClientError as e:
        handle_get_item_client_error(e, "Rollback", lease_table_name)
        return

    if not is_valid_version(version=initial_version, mode='rollback'):
        return

    # 1. Rollback client version
    if initial_version != KclClientVersion.VERSION_2X.value:
        rollback_succeeded, initial_version = rollback_client_version(
            dynamodb_client, lease_table_name, new_history
        )
        if not rollback_succeeded:
            return

    print(f"Waiting for {GSI_DELETION_WAIT_TIME_SECONDS} seconds before cleaning up KCL 3.0 resources after rollback...")
    time.sleep(GSI_DELETION_WAIT_TIME_SECONDS)

    # 2. Delete the GSI
    delete_gsi_if_exists(dynamodb_client, lease_table_name)

    # Log success
    if initial_version == KclClientVersion.UPGRADE_FROM_2X.value:
        print("\nRollback completed. Your application was running Phase 2 (2x compatible) functionality.")
        print("Please rollback to Phase 1 by deploying your KCL 3.5.x application with the Phase 1 configuration.")
    elif initial_version == KclClientVersion.VERSION_3X_WITH_ROLLBACK.value:
        print("\nRollback completed. Your KCL application was running Phase 2 (3x) functionality"
              " and has been rolled back to Phase 2 (2x compatible) mode.")
        print("If you don't see mitigation after a short period of time,"
              " please rollback to Phase 1 by deploying your KCL 3.5.x application with the Phase 1 configuration.")
    elif initial_version == KclClientVersion.VERSION_2X.value:
        print("\nApplication was already rolled back. Any KCLv3 resources that could be deleted were cleaned up"
              " to avoid charges until the application can be rolled forward with migration.")


def perform_rollforward(dynamodb_client, lease_table_name):
    """
    Perform KCL 3.0 migration roll-forward by updating MigrationState for the KCL application

    :param dynamodb_client: Boto3 DynamoDB client
    :param lease_table_name: Name of the DynamoDB lease table
    """
    if not validate_tables(dynamodb_client, "Roll-forward", lease_table_name):
        return

    try:
        initial_version, new_history = get_current_state(dynamodb_client, lease_table_name)
    except ClientError as e:
        handle_get_item_client_error(e, "Roll-forward", lease_table_name)
        return

    if not is_valid_version(version=initial_version, mode='rollforward'):
        return

    rollfoward_client_version(dynamodb_client, lease_table_name, new_history)


def run_kcl_migration(mode, lease_table_name):
    """
    Update the MigrationState in the DynamoDB lease table.

    :param mode: 'rollback-to-phase1', 'rollback', 'rollforward-to-phase2', 'rollforward', or 'rollback-to-v2'
    :param lease_table_name: Name of the DynamoDB KCL lease table
    """
    dynamodb_client = boto3.client('dynamodb', config=config)

    if mode in ("rollback", "rollback-to-phase1"):
        perform_rollback(dynamodb_client, lease_table_name)
    elif mode in ("rollforward", "rollforward-to-phase2"):
        perform_rollforward(dynamodb_client, lease_table_name)
    elif mode == "rollback-to-v2":
        perform_rollback_to_v2(dynamodb_client, lease_table_name)
    else:
        print(f"Invalid mode: {mode}. Please use 'rollback-to-phase1', 'rollforward-to-phase2', or 'rollback-to-v2'.")


def validate_args(args):
    if not (args.application_name or args.lease_table_name):
        raise ValueError(
            "Either application_name or lease_table_name must be provided."
        )

def process_table_names(args):
    """
    Process command line arguments to determine table names based on mode.
    Args:
        args: Parsed command line arguments
    Returns:
        tuple: (mode, lease_table_name)
    """
    mode_input = args.mode
    application_name_input = args.application_name

    lease_table_name_input = args.lease_table_name or application_name_input

    return (mode_input, lease_table_name_input)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=
        """
        KCL Migration Tool
        This tool facilitates the migration and rollback processes for Amazon KCLv3 applications.
    
        Before running this tool:
        1. Ensure you have the necessary AWS permissions configured to access and modify the following:
            - KCL application DynamoDB tables (lease table)
    
        2. Verify that your AWS credentials are properly set up in your environment or AWS config file.
    
        3. Confirm that you have the correct KCL application name and lease table name (if configured in KCL).
    
        Usage:
        This tool supports two main operations: rollforward (upgrade) and rollback.
        For detailed usage instructions, use the -h or --help option.
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=['rollback-to-phase1', 'rollback', 'rollforward-to-phase2', 'rollforward', 'rollback-to-v2'],
                        required=True, metavar='{rollback-to-phase1,rollforward-to-phase2,rollback-to-v2}',
                        help="Mode of operation: "
                             "'rollback-to-phase1': Client Version Migration Phase 2 -> Phase 1. "
                             "'rollforward-to-phase2': After a rollback to Phase1, rollforward Client Version Migration to Phase 2. "
                             "'rollback-to-v2': Delete non-lease entities to enable rollback to v2.")
    parser.add_argument("--application_name",
                        help="Name of the KCL application. This must match the application name "
                             "used in the KCL Library configurations.")
    parser.add_argument("--lease_table_name",
                        help="Name of the DynamoDB lease table (defaults to applicationName)."
                             " If LeaseTable name was specified for the application as part of "
                             "the KCL configurations, the same name must be passed here.")
    parser.add_argument("--region", required=True,
                        help="AWS Region where your KCL application exists")
    args = parser.parse_args()
    validate_args(args)
    config.region_name = args.region
    run_kcl_migration(*process_table_names(args))
