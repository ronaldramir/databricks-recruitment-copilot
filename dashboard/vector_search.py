"""
Semantic résumé <-> job-description matching via Databricks Vector Search.

Identical to mcp_server/vector_search.py - duplicated here for the same
reason as dashboard/lakebase.py and dashboard/resume_broker.py (each
Databricks App is its own independent bundle). See that file's docstring for
the one-time index setup steps.
"""

import os

from databricks.sdk import WorkspaceClient
from databricks.vector_search.client import VectorSearchClient

INDEX_NAME = os.environ.get(
    "VECTOR_SEARCH_INDEX", "recruitment_copilot.silver.silver_resumes_index"
)

_client: VectorSearchClient | None = None


def _get_client() -> VectorSearchClient:
    """VectorSearchClient only auto-detects credentials inside a Databricks
    notebook - a Databricks App needs the service principal creds Databricks
    injects into the app runtime (DATABRICKS_CLIENT_ID/_SECRET) passed in
    explicitly. Reuse WorkspaceClient's already-working host resolution
    instead of relying on a DATABRICKS_HOST env var that may not be set."""
    global _client
    if _client is None:
        host = WorkspaceClient().config.host
        _client = VectorSearchClient(
            workspace_url=host,
            service_principal_client_id=os.environ["DATABRICKS_CLIENT_ID"],
            service_principal_client_secret=os.environ["DATABRICKS_CLIENT_SECRET"],
            disable_notice=True,
        )
    return _client


def find_matching_resumes(job_description: str, limit: int = 10) -> list[dict]:
    """
    Semantic search: résumés whose clean_text is most similar to
    job_description, most similar first.
    """
    if not job_description or not job_description.strip():
        raise ValueError("job_description is required")

    limit = max(1, min(int(limit), 50))
    index = _get_client().get_index(index_name=INDEX_NAME)
    results = index.similarity_search(
        query_text=job_description,
        columns=["resume_id", "category", "clean_text"],
        num_results=limit,
    )

    rows = results.get("result", {}).get("data_array", [])
    matches = []
    for row in rows:
        resume_id, category, clean_text, score = row
        matches.append({
            "resume_id": resume_id,
            "category": category,
            "match_score": round(float(score), 3),
            "snippet": (clean_text or "")[:200],
        })
    return matches
