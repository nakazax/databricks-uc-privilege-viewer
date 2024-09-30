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
# MAGIC - Databricks workspace access with admin privileges
# MAGIC - Account ID, client ID, and client secret for the service principal
# MAGIC
# MAGIC ## Usage:
# MAGIC 1. Fill in the widget parameters (account ID, client ID, client secret, and scope name)
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
