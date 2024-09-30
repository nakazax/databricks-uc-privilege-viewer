# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks User Privileges Viewer
# MAGIC This notebook views the privileges of a specific user in Databricks Unity Catalog. It performs the following tasks:
# MAGIC
# MAGIC 1. Retrieves user information based on their email address
# MAGIC 2. Identifies the groups the user belongs to (directly or indirectly)
# MAGIC
# MAGIC ## Prerequisites:
# MAGIC - Databricks account with appropriate access
# MAGIC - Account ID, client ID, and client secret for authentication
# MAGIC
# MAGIC ## Usage:
# MAGIC 1. Fill in the required parameters in the widgets
# MAGIC 2. Run the notebook cells sequentially
# MAGIC 3. View the results displayed after each major step
# MAGIC
# MAGIC Note: This notebook uses the Databricks SDK and requires authentication with a service principal.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-processing

# COMMAND ----------

# DBTITLE 1,Install Packages
# MAGIC %pip install databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import Libraries
import logging
logging.basicConfig(level=logging.INFO)

from typing import List, Dict, Any
from databricks.sdk import AccountClient
from databricks.sdk.service.iam import User, Group

# COMMAND ----------

# DBTITLE 1,Configurations
# Widget definitions
dbutils.widgets.text("email", "", "User Email")
dbutils.widgets.text("account_id", "", "Account ID")
dbutils.widgets.text("client_id", "", "Client ID")
dbutils.widgets.text("client_secret", "", "Client Secret")

# Get widget values
user_email = dbutils.widgets.get("email")
account_id = dbutils.widgets.get("account_id")
client_id = dbutils.widgets.get("client_id")
client_secret = dbutils.widgets.get("client_secret")

# COMMAND ----------

# DBTITLE 1,Define Helper Functions
def get_user_by_email(client: AccountClient, email: str) -> Dict[str, Any]:
    """
    Retrieves user information from Databricks based on the provided email address.

    Args:
        client (AccountClient): An initialized Databricks AccountClient object.
        email (str): The email address of the user to retrieve.

    Returns:
        Dict[str, Any]: A dictionary containing the user's information.

    Raises:
        ValueError: If no user is found with the provided email address.

    Example:
        client = AccountClient(...)
        user_info = get_user_by_email(client, "user@example.com")
    """
    users: List[User] = client.users.list(filter=f"emails.value eq '{email}'")
    for user in users:
        return user.as_dict()  # Return the first matching user as a dictionary
    raise ValueError(f"No user found with email: {email}")


def get_user_groups(client: AccountClient, user: Dict[str, Any]) -> List[str]:
    """
    Retrieves all groups (direct and indirect) that the specified user belongs to.

    This function performs a recursive search to find all groups the user is a member of,
    including nested group memberships.

    Args:
        client (AccountClient): An initialized Databricks AccountClient object.
        user (Dict[str, Any]): A dictionary containing the user's information, including their ID.

    Returns:
        List[str]: A list of group display names that the user belongs to.

    Example:
        client = AccountClient(...)
        user_info = get_user_by_email(client, "user@example.com")
        user_groups = get_user_groups(client, user_info)
    """
    user_id = user['id']
    groups: List[Group] = client.groups.list()
    group_dict: Dict[str, Dict[str, Any]] = {group.id: group.as_dict() for group in groups}
    user_groups: set = set()

    def check_group_membership(group_id: str) -> bool:
        """
        Recursively checks if the user is a member of the specified group or its nested groups.

        Args:
            group_id (str): The ID of the group to check.

        Returns:
            bool: True if the user is a member of the group or its nested groups, False otherwise.
        """
        group = group_dict[group_id]
        if 'members' in group:
            for member in group['members']:
                if member['value'] == user_id:
                    user_groups.add(group['displayName'])
                    return True
                elif member['$ref'].startswith('Groups/'):
                    nested_group_id = member['value']
                    if check_group_membership(nested_group_id):
                        user_groups.add(group['displayName'])
                        return True
        return False

    for group_id in group_dict:
        check_group_membership(group_id)

    return list(user_groups)

def print_fancy_header(text):
    """
    Prints a fancy header with the given text.
    """
    width = 60
    padding = (width - len(text) - 2) // 2
    print("\n" + "=" * width)
    print("|" + " " * padding + text + " " * padding + "|")
    print("=" * width)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get User Groups

# COMMAND ----------

# DBTITLE 1,Initialize Account Client
a = AccountClient(
    host="https://accounts.cloud.databricks.com",
    account_id=account_id,
    client_id=client_id,
    client_secret=client_secret
)

# COMMAND ----------

# DBTITLE 1,Get User by Email
user = get_user_by_email(a, user_email)
print("User details")
display(user)

# COMMAND ----------

# DBTITLE 1,Get User Groups
user_groups = get_user_groups(a, user)
print("List of account groups to which the user directly or indirectly belongs")
display(user_groups)

# COMMAND ----------

# MAGIC %md
# MAGIC ## List Privileges

# COMMAND ----------

# DBTITLE 1,Define Privilege Tables
privilege_tables = [
    "metastore_privileges",
    "catalog_privileges",
    "schema_privileges",
    "table_privileges",
    "volume_privileges",
    "routine_privileges",
    "storage_credential_privileges",
    "external_location_privileges",
    "connection_privileges",
]

# COMMAND ----------

# DBTITLE 1,Prepare Grantees
grantees = [user_email] + user_groups
grantees_str = ", ".join(f"'{grantee}'" for grantee in grantees)

# COMMAND ----------

# DBTITLE 1,List Privileges for User and User Groups
print_fancy_header(f"Grantees")
display(grantees)

for privilege_table in privilege_tables:
    query = f"""
    SELECT *
    FROM system.information_schema.{privilege_table}
    WHERE grantee IN ({grantees_str})
    """
    result_df = spark.sql(query)
    print_fancy_header(f"Privileges from {privilege_table}")
    display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## List Owners

# COMMAND ----------

# DBTITLE 1,Define Securable Object Tables
securable_object_tables = {
    "metastores": "metastore_owner",
    "catalogs": "catalog_owner",
    "schemata": "schema_owner",
    "tables": "table_owner",
    "volumes": "volume_owner",
    "routines": "routine_owner",
    "storage_credentials": "storage_credential_owner",
    "external_locations": "external_location_owner",
    "connections": "connection_owner"
}

# COMMAND ----------

for table, owner_column in securable_object_tables.items():
    query = f"""
    SELECT *
    FROM system.information_schema.{table}
    WHERE {owner_column} IN ({grantees_str})
    """
    result_df = spark.sql(query)
    print_fancy_header(f"{table.capitalize()} owned by user")
    display(result_df)
