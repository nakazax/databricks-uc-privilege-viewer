# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Account-Level Service Principal Secret Manager
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
dbutils.widgets.text("account_id", "", "Account ID")
dbutils.widgets.text("client_id", "", "Client ID")
dbutils.widgets.text("client_secret", "", "Client Secret")

# COMMAND ----------

# DBTITLE 1,Get Widget Values
secret_scope_name = dbutils.widgets.get("secret_scope_name")
account_id = dbutils.widgets.get("account_id")
client_id = dbutils.widgets.get("client_id")
client_secret = dbutils.widgets.get("client_secret")

# COMMAND ----------

# DBTITLE 1,Define Constants
ACCOUNT_ID_KEY = "account_id"
CLIENT_ID_KEY = "client_id"
CLIENT_SECRET_KEY = "client_secret"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manage Secrets
# MAGIC Create a secret scope and set up secrets using the Databricks Workspace API.

# COMMAND ----------

# DBTITLE 1,Initialize Workspace Client
w = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,Create Secret Scope
w.secrets.create_scope(scope=secret_scope_name)
print(f"Secret scope '{secret_scope_name}' created successfully.")

# COMMAND ----------

# DBTITLE 1,Put Secret for Account ID
w.secrets.put_secret(scope=secret_scope_name, key=ACCOUNT_ID_KEY, string_value=account_id)
print(f"Secret '{ACCOUNT_ID_KEY}' put successfully.")

# COMMAND ----------

# DBTITLE 1,Put Secret for Client ID
w.secrets.put_secret(scope=secret_scope_name, key=CLIENT_ID_KEY, string_value=client_id)
print(f"Secret '{CLIENT_ID_KEY}' put successfully.")

# COMMAND ----------

# DBTITLE 1,Put Secret for Client Secret
w.secrets.put_secret(scope=secret_scope_name, key=CLIENT_SECRET_KEY, string_value=client_secret)
print(f"Secret '{CLIENT_SECRET_KEY}' put successfully.")
