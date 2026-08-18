"""
One-time setup script: creates the Databricks secret scope and stores the
Kaggle API credentials used by bronze/01_bronze_ingest.py to download the
Resume Dataset. Run this once, either locally (with the Databricks CLI
configured) or by pasting it into a Databricks notebook cell - never commit
the resulting secret values anywhere.

Get your Kaggle credentials from https://www.kaggle.com/settings -> API ->
"Create New Token" (downloads kaggle.json with your username and key).

Usage:
    python setup_secrets.py
"""
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
import getpass

w = WorkspaceClient()

existing_scopes = {s.name for s in w.secrets.list_scopes()}
if "kaggle" not in existing_scopes:
    w.secrets.create_scope(scope="kaggle")

w.secrets.put_secret(
    scope="kaggle",
    key="username",
    string_value=getpass.getpass("Paste your Kaggle username: ")
)
w.secrets.put_secret(
    scope="kaggle",
    key="key",
    string_value=getpass.getpass("Paste your Kaggle API key: ")
)

w.secrets.put_acl(
    scope="kaggle",
    principal="users",
    permission=workspace.AclPermission.READ,
)

<<<<<<< Updated upstream
stored_keys = {s.key for s in w.secrets.list_secrets(scope="kaggle")}
assert {"username", "key"} <= stored_keys, f"Faltaron llaves: {stored_keys}"
print(f"OK - scope 'kaggle' listo con las llaves: {sorted(stored_keys)}")
=======

dbutils.secrets.list("kaggle")
>>>>>>> Stashed changes
