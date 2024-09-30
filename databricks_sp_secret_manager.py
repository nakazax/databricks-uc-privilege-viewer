# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Account-Level Service Principal Secret Manager
# MAGIC
# MAGIC This notebook creates a secret scope and sets up secrets for account-level service principal authentication with Databricks Account APIs.
# MAGIC
# MAGIC ## Purpose:
# MAGIC - Create a secret scope
# MAGIC - Store client ID and client secret for an account-level service principal
# MAGIC
# MAGIC ## Usage:
# MAGIC 1. Fill in the widget parameters
# MAGIC 2. Run all cells
# MAGIC 3. Use created secrets in other notebooks for Account API authentication
# MAGIC
# MAGIC **Note:** Run this notebook as an administrator. Keep all credentials secure.
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
