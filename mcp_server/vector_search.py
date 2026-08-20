"""
Semantic résumé <-> job-description matching via Databricks Vector Search.

Queries a Delta Sync Index built on top of silver_resumes.clean_text (see
PLAN.md section 5) - Databricks computes and keeps the embeddings in sync
automatically whenever Silver changes; this module only ever sends text
queries and reads back scored results, it never touches an embedding model
directly (unlike the day-3 reference project's pgvector + sentence-transformers
approach, which is why there is no local model to lazy-load here).

Setup (one-time, done in the Databricks UI/SDK, not in this file):
    1. Enable Change Data Feed on silver_resumes (required for a sync index):
       ALTER TABLE recruitment_copilot.silver.silver_resumes
       SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
    2. Create a Vector Search endpoint.
    3. Create a Delta Sync Index on recruitment_copilot.silver.silver_resumes,
       embedding source column `clean_text`, primary key `resume_id`,
       with `category` as an extra returned column.
"""

import os

from databricks.sdk import WorkspaceClient
from databricks.vector_search.client import VectorSearchClient

INDEX_NAME = os.environ.get(
    "VECTOR_SEARCH_INDEX", "recruitment_copilot.silver.silver_resumes_index"
)

_client: VectorSearchClient | None = None


def _get_client() -> VectorSearchClient:
    """Lazy-load the Vector Search client once per process - there's no
    embedding model to load locally (Databricks hosts it), just an
    authenticated client worth reusing across calls.

    VectorSearchClient only auto-detects credentials inside a Databricks
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

    Args:
        job_description: Free-text job description to match against.
        limit: Max number of results (default 10, max 50).

    Returns:
        A list of dicts: resume_id, category, match_score (0-1, higher is
        more similar), snippet (first 200 chars of clean_text).
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
        # Vector Search appends the similarity score as the last element of
        # each row, after the requested columns.
        resume_id, category, clean_text, score = row
        matches.append({
            "resume_id": resume_id,
            "category": category,
            "match_score": round(float(score), 3),
            "snippet": (clean_text or "")[:200],
        })
    return matches
