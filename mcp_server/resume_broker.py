"""
Gold-table reader for the recruiter MCP server and dashboard.

Queries Unity Catalog Gold tables (recruitment_copilot.gold.*) through a SQL
Warehouse using the Databricks SDK's Statement Execution API - the same
WorkspaceClient auth already used for secrets (lakebase.py), so a Databricks
App running this doesn't need any extra credentials beyond its own service
principal having CAN_USE on the warehouse and SELECT on the Gold schema.
"""

import os
import re
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

_w = WorkspaceClient()

WAREHOUSE_ID = os.environ["SQL_WAREHOUSE_ID"]
CATALOG = os.environ.get("GOLD_CATALOG", "recruitment_copilot")

_CATEGORY_STATS_TABLE = f"{CATALOG}.gold.gold_category_stats"
_TOP_CANDIDATES_TABLE = f"{CATALOG}.gold.gold_top_candidates"

# Category values only ever come from the dataset's fixed label set (upper
# case letters and hyphens) - validated here because they get interpolated
# straight into SQL below (the Statement Execution API used by _query has no
# built-in bind-parameter helper as simple as psycopg2's).
_SAFE_CATEGORY = re.compile(r"^[A-Za-z\-]+$")


def _query(sql: str) -> list[dict]:
    """Run a SQL statement against the configured warehouse, return rows as list[dict]."""
    response = _w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=sql,
        wait_timeout="30s",
    )
    while response.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(1)
        response = _w.statement_execution.get_statement(response.statement_id)

    if response.status.state != StatementState.SUCCEEDED:
        error = response.status.error
        message = error.message if error else response.status.state
        raise RuntimeError(f"Query failed ({response.status.state}): {message}")

    columns = [c.name for c in response.manifest.schema.columns]
    rows = response.result.data_array or []
    return [dict(zip(columns, row)) for row in rows]


def get_category_stats() -> list[dict]:
    """All rows from gold_category_stats, highest leadership signal first."""
    return _query(f"""
        SELECT category, resume_count, avg_word_count, avg_leadership_score
        FROM {_CATEGORY_STATS_TABLE}
        ORDER BY avg_leadership_score DESC
    """)


def search_resumes(category: str | None = None, limit: int = 20) -> list[dict]:
    """Top-ranked candidates from gold_top_candidates, optionally filtered by category."""
    limit = max(1, min(int(limit), 100))
    where_sql = ""
    if category:
        category = category.strip().upper()
        if not _SAFE_CATEGORY.match(category):
            raise ValueError(f"Invalid category: {category!r}")
        where_sql = f"WHERE category = '{category}'"
    return _query(f"""
        SELECT category, resume_id, leadership_score, rank
        FROM {_TOP_CANDIDATES_TABLE}
        {where_sql}
        ORDER BY category, rank
        LIMIT {limit}
    """)
