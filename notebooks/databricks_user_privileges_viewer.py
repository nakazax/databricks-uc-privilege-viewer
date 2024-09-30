# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks User Privileges Viewer
# MAGIC This notebook views the privileges and ownership of a specific user in Databricks Unity Catalog.
# MAGIC
# MAGIC ## Purpose:
# MAGIC - Retrieve user information based on their email address
# MAGIC - Identify groups the user belongs to (directly or indirectly)
# MAGIC - List objects owned by the user or their groups
# MAGIC - List privileges granted to the user or their groups
# MAGIC
# MAGIC ## Prerequisites:
# MAGIC 1. **Secret scope with registered account ID, client ID, and client secret:**
# MAGIC    - Use the <a href="$./databricks_sp_secret_manager" target="_blank">databricks_sp_secret_manager</a> notebook to register these secrets.
# MAGIC
# MAGIC 2. **The user executing this notebook must have access to the secret scope:**
# MAGIC    - Ensure you have the necessary permissions to read from the secret scope.
# MAGIC
# MAGIC 3. **The user executing this notebook must have SELECT privilege on `system.information_schema` tables:**
# MAGIC    - This is required to query user privileges and ownerships.
# MAGIC
# MAGIC ## Usage:
# MAGIC 1. **Register secrets:**
# MAGIC    - Run the <a href="$./databricks_sp_secret_manager" target="_blank">databricks_sp_secret_manager</a> notebook to register account ID, client ID, and client secret in a secret scope.
# MAGIC
# MAGIC 2. **Run this notebook:**
# MAGIC    - Execute all cells up to and including the [**Define Widgets**] cell.
# MAGIC    - In the widgets, enter the `user_email` of the user whose privileges you want to view.
# MAGIC    - The `secret_scope_name` can be left as default unless you want to change it.
# MAGIC    - Run the remaining cells in order.

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

# DBTITLE 1,Import Constants
# MAGIC %run ./consts

# COMMAND ----------

# DBTITLE 1,Define Widgets
dbutils.widgets.text("user_email", "", "User Email")
dbutils.widgets.text("secret_scope_name", DEFAULT_SECRET_SCOPE_NAME, "Secret Scope Name")

# COMMAND ----------

# DBTITLE 1,Get Widget Values
user_email = dbutils.widgets.get("user_email")
secret_scope_name = dbutils.widgets.get("secret_scope_name")

if not user_email or not secret_scope_name:
    raise ValueError("Please set both the user email and secret scope name")

# COMMAND ----------

# DBTITLE 1,Get Account Info from Secret
try:
    account_id = dbutils.secrets.get(scope=secret_scope_name, key=ACCOUNT_ID_KEY)
    client_id = dbutils.secrets.get(scope=secret_scope_name, key=CLIENT_ID_KEY)
    client_secret = dbutils.secrets.get(scope=secret_scope_name, key=CLIENT_SECRET_KEY)
except Exception as e:
    raise ValueError(f"Failed to retrieve secrets: {str(e)}")

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
print("User details:")
display(user_dict)

# COMMAND ----------

# DBTITLE 1,Get User Groups
user_groups = get_user_groups(a, user_dict)
user_groups_df = spark.createDataFrame([(group,) for group in user_groups], ["group_name"])
print("List of account groups to which the user directly or indirectly belongs:")
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
grantees = [user_email] + user_groups
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
