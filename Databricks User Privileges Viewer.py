# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks User Privileges Viewer
# MAGIC This notebook views the privileges and ownership of a specific user in Databricks Unity Catalog. It performs the following tasks:
# MAGIC
# MAGIC 1. Retrieves user information based on their email address
# MAGIC 2. Identifies the groups the user belongs to (directly or indirectly)
# MAGIC 3. Lists the objects owned by the user or their groups
# MAGIC 4. Lists the privileges granted to the user or their groups
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
# MAGIC This section installs necessary packages, imports libraries, and sets up configurations.

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
# MAGIC This section retrieves user information and identifies the groups the user belongs to.

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
user_dict = get_user_by_email(a, user_email)
print("User details")
display(user_dict)

# COMMAND ----------

# DBTITLE 1,Get User Groups
user_groups_dict = get_user_groups(a, user_dict)
user_groups_df = spark.createDataFrame([(group,) for group in user_groups_dict], ["group_name"])
print("List of account groups to which the user directly or indirectly belongs")
display(user_groups_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## List User Privileges
# MAGIC This section queries and displays the objects owned by the user or their groups, as well as the privileges granted to them.

# COMMAND ----------

# DBTITLE 1,Define Securable Objects
securable_objects = [
    {"name": "Metastore", "owner_table": "metastores", "owner_column": "metastore_owner", "privilege_table": "metastore_privileges"},
    {"name": "Catalog", "owner_table": "catalogs", "owner_column": "catalog_owner", "privilege_table": "catalog_privileges"},
    {"name": "Schema", "owner_table": "schemata", "owner_column": "schema_owner", "privilege_table": "schema_privileges"},
    {"name": "Table", "owner_table": "tables", "owner_column": "table_owner", "privilege_table": "table_privileges"},
    {"name": "Volume", "owner_table": "volumes", "owner_column": "volume_owner", "privilege_table": "volume_privileges"},
    {"name": "Routine", "owner_table": "routines", "owner_column": "routine_owner", "privilege_table": "routine_privileges"},
    {"name": "Storage Credential", "owner_table": "storage_credentials", "owner_column": "storage_credential_owner", "privilege_table": "storage_credential_privileges"},
    {"name": "External Location", "owner_table": "external_locations", "owner_column": "external_location_owner", "privilege_table": "external_location_privileges"},
    {"name": "Connection", "owner_table": "connections", "owner_column": "connection_owner", "privilege_table": "connection_privileges"}
]

# COMMAND ----------

# DBTITLE 1,Query and Display Privileges
# Prepare grantees
grantees = [user_email] + user_groups_dict
grantees_str = ", ".join(f"'{grantee}'" for grantee in grantees)

print_fancy_header(f"Access Rights for {user_email}")
print("List of account groups to which the user directly or indirectly belongs:")
display(user_groups_df)

for obj in securable_objects:
    print_fancy_header(f"{obj['name']} Access Rights")
    
    # Query ownership
    owner_query = f"""
    SELECT *
    FROM system.information_schema.{obj['owner_table']}
    WHERE {obj['owner_column']} IN ({grantees_str})
    """
    owner_df = spark.sql(owner_query)
    
    # Query privileges
    privilege_query = f"""
    SELECT *
    FROM system.information_schema.{obj['privilege_table']}
    WHERE grantee IN ({grantees_str})
    """
    privilege_df = spark.sql(privilege_query)
    
    print(f"\n{obj['name']}s that the specified user or their groups own:")
    display(owner_df)
    
    print(f"\n{obj['name']}s that the specified user or their groups have privileges for:")
    display(privilege_df)
