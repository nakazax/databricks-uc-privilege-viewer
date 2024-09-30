# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Secret Management for User Privileges Viewer
# MAGIC
# MAGIC This notebook creates a secret scope and sets up the necessary secrets for the User Privileges Viewer notebook.
# MAGIC
# MAGIC **Note:** This notebook should be run by an administrator with the necessary permissions to create secret scopes and manage secrets.

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

# MAGIC %md
# MAGIC ## Configure Parameters
# MAGIC Define and retrieve parameters for secret scope and keys using widgets.

# COMMAND ----------

# DBTITLE 1,Define Widgets
# Define widgets for scope and secret names
dbutils.widgets.text("secret_scope_name", "user_privileges_viewer_scope", "Secret Scope Name")
dbutils.widgets.text("client_id_key", "client_id_key", "Client ID Secret Key")
dbutils.widgets.text("client_id_value", "", "Client ID Secret Value")
dbutils.widgets.text("client_secret_key", "client_secret_key", "Client Secret Key")
dbutils.widgets.text("client_secret_value", "", "Client Secret Value")

# COMMAND ----------

# DBTITLE 1,Get Widget Values
# Get widget values
secret_scope_name = dbutils.widgets.get("secret_scope_name")
client_id_key = dbutils.widgets.get("client_id_key")
client_id_value = dbutils.widgets.get("client_id_value")
client_secret_key = dbutils.widgets.get("client_secret_key")
client_secret_value = dbutils.widgets.get("client_secret_value")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manage Secrets
# MAGIC Create a secret scope and set up secrets using the Databricks Workspace API.

# COMMAND ----------

# DBTITLE 1,Initialize WorkspaceClient
w = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,Create Secret Scope
try:
    w.secrets.create_scope(scope=secret_scope_name)
    print(f"Secret scope '{secret_scope_name}' created successfully.")
except Exception as e:
    if "already exists" in str(e):
        print(f"Secret scope '{secret_scope_name}' already exists. Continuing with existing scope.")
    else:
        raise e

# COMMAND ----------

# DBTITLE 1,Set Secrets
# Set secrets
try:
    w.secrets.put_secret(scope=secret_scope_name, key=client_id_key, string_value=client_id_value)
    print(f"Secret '{client_id_key}' set successfully.")
except Exception as e:
    print(f"Error setting '{client_id_key}': {str(e)}")

try:
    w.secrets.put_secret(scope=secret_scope_name, key=client_secret_key, string_value=client_secret_value)
    print(f"Secret '{client_secret_key}' set successfully.")
except Exception as e:
    print(f"Error setting '{client_secret_key}': {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)
# MAGIC Instructions for removing the created secrets and scope if needed.

# COMMAND ----------

# DBTITLE 1,Delete Secrets and Scope (Optional)
# # Cleanup (uncomment to use)
# try:
#     w.secrets.delete_secret(scope=secret_scope_name, key=client_id_key)
#     w.secrets.delete_secret(scope=secret_scope_name, key=client_secret_key)
#     w.secrets.delete_scope(scope=secret_scope_name)
#     print(f"Secrets and scope '{secret_scope_name}' deleted successfully.")
# except Exception as e:
#     print(f"Error during cleanup: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. The secret scope and secrets have been set up successfully.
# MAGIC 2. You can now use these secrets in the User Privileges Viewer notebook.
# MAGIC 3. In the User Privileges Viewer notebook, use the following code to access the secrets:
# MAGIC
# MAGIC ```python
# MAGIC client_id = dbutils.secrets.get(scope="{secret_scope_name}", key="{client_id_key}")
# MAGIC client_secret = dbutils.secrets.get(scope="{secret_scope_name}", key="{client_secret_key}")
# MAGIC ```
# MAGIC
# MAGIC 4. Remember to remove the widgets for client_id and client_secret from the User Privileges Viewer notebook.

# COMMAND ----------

# DBTITLE 1,Clean Up Widgets
# Clean up widgets
dbutils.widgets.removeAll()
