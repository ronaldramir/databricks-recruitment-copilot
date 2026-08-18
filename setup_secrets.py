"""
One-time setup script: creates the Databricks secret scopes and stores the
credentials used across this project - Kaggle (bronze/01_bronze_ingest.py)
and Lakebase (mcp_server/ and dashboard/, via lakebase.py). Run this once,
either locally (with the Databricks CLI configured) or by pasting it into a
Databricks notebook cell - never commit the resulting secret values anywhere.

Get your Kaggle credentials from https://www.kaggle.com/settings -> API ->
"Create New Token" (downloads kaggle.json with your username and key).

Get your Lakebase connection URL from the Lakebase instance's "Connection
details" tab in the Databricks UI (a postgresql://... URL). Leave that
prompt blank to skip it if you haven't provisioned a Lakebase instance yet -
run this script again once you have, it won't touch the Kaggle secret.

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

stored_keys = {s.key for s in w.secrets.list_secrets(scope="kaggle")}
assert {"username", "key"} <= stored_keys, f"Faltaron llaves: {stored_keys}"
print(f"OK - scope 'kaggle' listo con las llaves: {sorted(stored_keys)}")

lakebase_url = getpass.getpass(
    "Paste your Lakebase connection URL (blank to skip): "
)
if lakebase_url:
    if "database" not in existing_scopes:
        w.secrets.create_scope(scope="database")

    w.secrets.put_secret(scope="database", key="lakebase-url", string_value=lakebase_url)
    w.secrets.put_acl(
        scope="database",
        principal="users",
        permission=workspace.AclPermission.READ,
    )

    stored_db_keys = {s.key for s in w.secrets.list_secrets(scope="database")}
    assert "lakebase-url" in stored_db_keys, f"Faltó la llave: {stored_db_keys}"
    print(f"OK - scope 'database' listo con las llaves: {sorted(stored_db_keys)}")
else:
    print("Lakebase omitido - corré este script de nuevo cuando tengas la URL.")
