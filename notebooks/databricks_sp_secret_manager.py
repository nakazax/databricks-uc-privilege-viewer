# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Account-Level Service Principal Secret Manager
# MAGIC This notebook creates a secret scope and sets up secrets for account-level service principal authentication with Databricks Account APIs.
# MAGIC
# MAGIC ## Purpose:
# MAGIC - Create a secret scope
# MAGIC - Store account ID, client ID, and client secret for an account-level service principal
# MAGIC
# MAGIC ## Prerequisites:
# MAGIC 1. **The user executing this notebook must have the Databricks Account Admin role:**
# MAGIC    - This role is necessary to access and manage account-level resources and settings.
# MAGIC
# MAGIC 2. **The service principal for which secrets are being registered must have the Account Admin role:**
# MAGIC    - While not strictly necessary for this notebook, the Account Admin role will be required for subsequent notebooks using these credentials.
# MAGIC
# MAGIC ## Usage:
# MAGIC ### 1. Obtain Account ID
# MAGIC - Access the account console. [[AWS](https://accounts.cloud.databricks.com) | [Azure](https://accounts.azuredatabricks.net/) | [Google Cloud](https://accounts.gcp.databricks.com/)]
# MAGIC - Click on the icon in the top right corner of the screen.
# MAGIC - Copy the Account ID and keep it handy.
# MAGIC
# MAGIC <img src="./images/account_console_1.png" alt="Account ID Location" width="1000"/>
# MAGIC
# MAGIC ### 2. Obtain Service Principal Client ID and Client Secret
# MAGIC If you already have a client ID and client secret, use those. If not, follow the steps below.
# MAGIC
# MAGIC - Go to [**User management**] in the account console.
# MAGIC - Access the service principal you want to use (or create a new one).
# MAGIC - Ensure the service principal has the Account Admin role.
# MAGIC - In the [**Principal information**] section, find the [**OAuth secrets**] area.
# MAGIC - Click [**Generate secret**] to get the client ID and client secret.
# MAGIC
# MAGIC <img src="./images/account_console_2.png" alt="Generate Secret" width="1000"/>
# MAGIC
# MAGIC ### 3. Run the Notebook
# MAGIC - Execute all cells up to and including the [**Define Widgets**] cell.
# MAGIC - In the widgets, enter the `account_id`, `client_id`, and `client_secret`.
# MAGIC - The `secret_scope_name` can be left as default unless you want to change it.
# MAGIC - Run the remaining cells in order.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Environment
# MAGIC Install required packages and import necessary libraries for secret management.

# COMMAND ----------

# DBTITLE 1,Install Packages
# MAGIC %pip install databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import Libraries
import logging
logging.basicConfig(level=logging.INFO)
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# DBTITLE 1,Import Constants
# MAGIC %run ./consts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Parameters
# MAGIC Define and retrieve parameters for secret scope and keys using widgets.

# COMMAND ----------

# DBTITLE 1,Define Widgets
dbutils.widgets.text("secret_scope_name", DEFAULT_SECRET_SCOPE_NAME, "Secret Scope Name")
dbutils.widgets.text("account_id", "", "Account ID")
dbutils.widgets.text("client_id", "", "Client ID")
dbutils.widgets.text("client_secret", "", "Client Secret")

# COMMAND ----------

# DBTITLE 1,Get Widget Values
secret_scope_name = dbutils.widgets.get("secret_scope_name")
account_id = dbutils.widgets.get("account_id")
client_id = dbutils.widgets.get("client_id")
client_secret = dbutils.widgets.get("client_secret")

if not all([secret_scope_name, account_id, client_id, client_secret]):
    raise ValueError("Please provide values for all widget parameters")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manage Secrets
# MAGIC Create a secret scope and set up secrets using the Databricks Workspace API.

# COMMAND ----------

# DBTITLE 1,Initialize Workspace Client
w = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,Create Secret Scope
try:
    w.secrets.create_scope(scope=secret_scope_name)
    print(f"Secret scope '{secret_scope_name}' created successfully.")
except Exception as e:
    print(f"Error creating secret scope: {str(e)}")
    print("Proceeding with existing scope or creation of secrets.")

# COMMAND ----------

# DBTITLE 1,Put Secrets
secrets = [
    (ACCOUNT_ID_KEY, account_id),
    (CLIENT_ID_KEY, client_id),
    (CLIENT_SECRET_KEY, client_secret)
]

for key, value in secrets:
    try:
        w.secrets.put_secret(scope=secret_scope_name, key=key, string_value=value)
        print(f"Secret '{key}' put successfully.")
    except Exception as e:
        print(f"Error putting secret '{key}': {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)
# MAGIC This cell is for cleaning up the created secret scope. It is commented out by default for safety reasons. Uncomment and run only when you want to remove the entire secret scope and all its associated secrets.

# COMMAND ----------

# DBTITLE 1,Delete Secret Scope
# from databricks.sdk import WorkspaceClient
# w = WorkspaceClient()
# try:
#     w.secrets.delete_scope(scope=secret_scope_name)
#     print(f"Secret scope '{secret_scope_name}' and all its secrets deleted successfully.")
# except Exception as e:
#     print(f"Error deleting secret scope: {str(e)}")
