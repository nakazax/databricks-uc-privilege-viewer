# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Secret Management for User Privileges Viewer
# MAGIC
# MAGIC This notebook creates a secret scope and sets up the necessary secrets for the User Privileges Viewer notebook.
# MAGIC
# MAGIC **Note:** This notebook should be run by an administrator with the necessary permissions to create secret scopes and manage secrets.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install and Import Liblaries

# COMMAND ----------

# MAGIC %pip install databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import logging
logging.basicConfig(level=logging.INFO)

from databricks.sdk import WorkspaceClient

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configurations

# COMMAND ----------

# Define widgets for scope and secret names
dbutils.widgets.text("secret_scope_name", "user_privileges_viewer_scope", "Secret Scope Name")
dbutils.widgets.text("client_id_key", "client_id_key", "Client ID Secret Key")
dbutils.widgets.text("client_id_value", "client_id_value", "Client ID Secret Value")
dbutils.widgets.text("client_secret_key", "client_secret_key", "Client Secret Key")
dbutils.widgets.text("client_secret_value", "client_secret_value", "Client Secret Value")

# COMMAND ----------

# Get widget values
secret_scope_name = dbutils.widgets.get("secret_scope_name")
client_id_key = dbutils.widgets.get("client_id_key")
client_id_value = dbutils.widgets.get("client_id_value")
client_secret_key = dbutils.widgets.get("client_secret_key")
client_secret_value = dbutils.widgets.get("client_secret_value")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main

# COMMAND ----------

w = WorkspaceClient()

# COMMAND ----------

try:
    w.secrets.create_scope(scope=secret_scope_name)
    print(f"Secret scope '{secret_scope_name}' created successfully.")
except Exception as e:
    if "already exists" in str(e):
        print(f"Secret scope '{secret_scope_name}' already exists. Continuing with existing scope.")
    else:
        raise e

# COMMAND ----------

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
