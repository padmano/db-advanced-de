# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Generate Tokens
# MAGIC In this lesson, you will generate a token which you will use throughout this module.
# MAGIC
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Generate credentials for working with the lessons in this module

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Introduction
# MAGIC In the next few lessons, we are going to use the Databricks API and the Databricks CLI to run code from a notebook. Since we are in a learning environment, we are going to save a credentials file right here in the workspace. In the "real world" we recommend that you follow your organization's security policies for storing credentials.
# MAGIC
# MAGIC Run the classroom setup script in the next cell to configure the classroom.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-06.1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify that Personal Access Token are enabled in the workspace
# MAGIC
# MAGIC You will need a personal access token to work through the next few lessons. In order to create them, personal access tokens must be permitted in your workspace.
# MAGIC
# MAGIC The procedure below verifies that personal access token creation is permitted in your workspace, but it requires workspace admin privileges. If you are not a workspace admin, skip this procedure, go to the next cell, and consult with your admin if you cannot create a token.
# MAGIC
# MAGIC 1. Click on your username in the top bar and select **Admin Settings** from the drop-down menu.
# MAGIC 1. Click **Workspace admin &gt; Advanced**.
# MAGIC 1. Enable **Personal Access Tokens** if it is not already enabled.
# MAGIC 1. Click on **Permission Settings**, next to the **Personal Access Tokens** switch. Add a new permission, so that *All Users* have *Can Use* permissions for token usage.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create a landing pad for the credentials
# MAGIC
# MAGIC A token is just like a username and password, so you should treat it with the same level of security as your own credentials. If you ever suspect a token has leaked, you should delete it immediately.
# MAGIC
# MAGIC For the purpose of this training, we are going to create a landing pad in this notebook to record and store the credentials within the workspace. When using credentials in production, you will want to follow the security practices of your organization. 
# MAGIC
# MAGIC Run the following cell. Notice that it creates two text fields which you will populate in the next section:
# MAGIC * **Host**: the URL of the target workspace, which will form the base for all REST API endpoints. The framework will populate this value automatically using the current workspace, but this value can be overridden if desired.
# MAGIC * **Token**: a bearer token to authenticate with the target workspace.

# COMMAND ----------

DA.get_credentials()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate credentials
# MAGIC
# MAGIC Create an authorization token for use with the Databricks CLI and API. If you are unable to create a token, please reach out to your workspace admin.
# MAGIC
# MAGIC 1. Click on your username in the top bar and select **User Settings** from the drop-down menu.
# MAGIC 1. Click **User &gt; Developer**, then click **Access tokens &gt; Manage**.
# MAGIC 1. Click **Generate new token**.
# MAGIC 1. Specify the following:
# MAGIC    * A comment describing the purpose of the token (for example, *CLI Demo*).
# MAGIC    * The lifetime of the token; estimate the number of days you anticipate needing to complete this module.
# MAGIC 1. Click **Generate**.
# MAGIC 1. Copy the displayed token to the clipboard. You will not be able to view the token again; if you lose it, you will need to delete it and create a new one.
# MAGIC 1. Paste the token into the **Token** field above.
# MAGIC 1. If you are targeting a workspace other than the current one, paste it into the **Host** field. Otherwise, leave this value as-is.
# MAGIC 1. Click **Done**.
# MAGIC
# MAGIC In response to these inputs, these values will be recorded as follows:
# MAGIC * In the environment variables **`DATABRICKS_HOST`** and **`DATABRICKS_TOKEN`** so that they can be used for [authentication](https://docs.databricks.com/en/dev-tools/auth/index.html) by the Databricks CLI, APIs, and SDK that we use in subsequent notebooks
# MAGIC * Since environment variables are limited in scope to the current execution context, the values are persisted to a [file in your workspace](https://docs.databricks.com/en/files/workspace.html#) for use by subsequent notebooks

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>