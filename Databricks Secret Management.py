# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Secret Management for User Privileges Viewer
# MAGIC 
# MAGIC This notebook creates a secret scope and sets up the necessary secrets for the User Privileges Viewer notebook.
# MAGIC 
# MAGIC **Note:** This notebook should be run by an administrator with the necessary permissions to create secret scopes and manage secrets.

# COMMAND ----------

# MAGIC %pip install databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import time

# Initialize WorkspaceClient
w = WorkspaceClient()

# COMMAND ----------

# Define widgets for scope and secret names
dbutils.widgets.text("scope_name", "user_privileges_viewer_scope", "Secret Scope Name")
dbutils.widgets.text("client_id_key", "client_id", "Client ID Secret Key")
dbutils.widgets.text("client_secret_key", "client_secret", "Client Secret Key")

# Get widget values
SCOPE_NAME = dbutils.widgets.get("scope_name")
CLIENT_ID_KEY = dbutils.widgets.get("client_id_key")
CLIENT_SECRET_KEY = dbutils.widgets.get("client_secret_key")

# COMMAND ----------

# Create secret scope
try:
    w.secrets.create_scope(scope=SCOPE_NAME)
    print(f"Secret scope '{SCOPE_NAME}' created successfully.")
except Exception as e:
    if "already exists" in str(e):
        print(f"Secret scope '{SCOPE_NAME}' already exists. Continuing with existing scope.")
    else:
        raise e

# COMMAND ----------

# Create widgets for secret input
dbutils.widgets.text("client_id", "", "Enter Client ID")
dbutils.widgets.text("client_secret", "", "Enter Client Secret")

# Get secret values
client_id = dbutils.widgets.get("client_id")
client_secret = dbutils.widgets.get("client_secret")

# Set secrets
try:
    w.secrets.put_secret(scope=SCOPE_NAME, key=CLIENT_ID_KEY, string_value=client_id)
    print(f"Secret '{CLIENT_ID_KEY}' set successfully.")
except Exception as e:
    print(f"Error setting '{CLIENT_ID_KEY}': {str(e)}")

try:
    w.secrets.put_secret(scope=SCOPE_NAME, key=CLIENT_SECRET_KEY, string_value=client_secret)
    print(f"Secret '{CLIENT_SECRET_KEY}' set successfully.")
except Exception as e:
    print(f"Error setting '{CLIENT_SECRET_KEY}': {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC 
# MAGIC Uncomment and run the following cell if you need to delete the secrets and scope.
# MAGIC 
# MAGIC **Warning:** This will permanently delete the secrets and scope. Use with caution.

# COMMAND ----------

# # Cleanup (uncomment to use)
# try:
#     w.secrets.delete_secret(scope=SCOPE_NAME, key=CLIENT_ID_KEY)
#     w.secrets.delete_secret(scope=SCOPE_NAME, key=CLIENT_SECRET_KEY)
#     w.secrets.delete_scope(scope=SCOPE_NAME)
#     print(f"Secrets and scope '{SCOPE_NAME}' deleted successfully.")
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
# MAGIC client_id = dbutils.secrets.get(scope="{SCOPE_NAME}", key="{CLIENT_ID_KEY}")
# MAGIC client_secret = dbutils.secrets.get(scope="{SCOPE_NAME}", key="{CLIENT_SECRET_KEY}")
# MAGIC ```
# MAGIC 
# MAGIC 4. Remember to remove the widgets for client_id and client_secret from the User Privileges Viewer notebook.

# COMMAND ----------

# Clean up widgets
dbutils.widgets.removeAll()
