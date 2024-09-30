# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Account-Level Service Principal Secret Manager
# MAGIC
# MAGIC This notebook creates a secret scope and sets up the necessary secrets for account-level service principal authentication. It's designed to be used in conjunction with other notebooks or applications that require secure access to Databricks Account APIs.
# MAGIC
# MAGIC ## Purpose:
# MAGIC - Create a secret scope
# MAGIC - Store client ID and client secret for an account-level service principal
# MAGIC - Provide a secure way to manage and access these credentials
# MAGIC
# MAGIC ## Important Note:
# MAGIC This notebook should be run by an administrator with the necessary permissions to create secret scopes and manage secrets. Ensure that access to this notebook is strictly controlled.
# MAGIC
# MAGIC ## Usage:
# MAGIC 1. Fill in the required parameters using the provided widgets
# MAGIC 2. Run the notebook cells sequentially
# MAGIC 3. Use the created secrets in other notebooks or applications for secure authentication with Databricks Account APIs
# MAGIC
# MAGIC ## Security Warning:
# MAGIC Never share or expose the values of your client ID or client secret. Always use secure methods (like this notebook) to manage these credentials.

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
dbutils.widgets.text("secret_scope_name", "user_privileges_viewer_scope", "Secret Scope Name")
dbutils.widgets.text("client_id_key", "client_id_key", "Client ID Secret Key")
dbutils.widgets.text("client_id_value", "", "Client ID Secret Value")
dbutils.widgets.text("client_secret_key", "client_secret_key", "Client Secret Key")
dbutils.widgets.text("client_secret_value", "", "Client Secret Value")

# COMMAND ----------

# DBTITLE 1,Get Widget Values
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

# DBTITLE 1,Initialize Workspace Client
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
