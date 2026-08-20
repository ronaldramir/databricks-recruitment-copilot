"""
Recruiter MCP server.

Exposes recruiting tools over MCP (Model Context Protocol) so a Databricks
Agent Bricks agent can call them like any other tool:
    - get_category_stats()
    - search_resumes(category, limit)
    - get_shortlist(limit)
    - shortlist_candidate(resume_id, category, note)

The two read tools query the Gold Delta tables in Unity Catalog through
resume_broker.py. The write tool, shortlist_candidate, is the direct parallel
of place_trade in databricks-lakebase-app-day-3: it never approves or rejects
anyone, it only records a suggestion for human review in Lakebase
(candidate_shortlist table, via lakebase.py). See PLAN.md's human-in-the-loop
note - that constraint belongs in this tool's docstring AND in the Agent
Bricks system prompt, since the docstring alone doesn't stop the agent from
misusing the tool if the prompt doesn't reinforce it.

Deploy this as its own Databricks App (same app.yaml + FastMCP entrypoint
pattern as databricks-lakebase-app-day-3/mcp_server), separate from the
dashboard app, so an Agent Bricks agent (or any MCP client) can register its
URL as an external MCP server.

Run locally:
    python recruiter_mcp_server.py
"""

import logging
import os
from contextvars import ContextVar

from fastmcp import FastMCP
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

import lakebase
import resume_broker
import vector_search

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("recruiter-mcp-server")

# Context variable to store request headers for accessing end-user identity
_request_context: ContextVar[dict] = ContextVar("request_context", default={})


def _get_end_user_email() -> str:
    """Get the actual end user's email from request headers, or fallback to service principal.

    Databricks Apps forwards two different headers: x-forwarded-user is an opaque
    user ID (a UUID), x-forwarded-email is the actual email - candidate_shortlist.email
    needs the latter. Check email first so a present-but-empty x-forwarded-email
    doesn't fall through to the ID by accident.
    """
    headers = _request_context.get()
    forwarded_email = headers.get("x-forwarded-email")
    if forwarded_email:
        return forwarded_email

    forwarded_user = headers.get("x-forwarded-user")
    if forwarded_user:
        return forwarded_user

    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    return w.current_user.me().user_name


mcp = FastMCP("recruiter-copilot")


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Middleware to capture HTTP headers containing end-user identity."""
    async def dispatch(self, request: Request, call_next):
        headers = {
            "x-forwarded-user": request.headers.get("x-forwarded-user"),
            "x-forwarded-email": request.headers.get("x-forwarded-email"),
        }
        _request_context.set(headers)
        return await call_next(request)


@mcp.tool
def get_category_stats() -> dict:
    """
    Get résumé counts and average leadership/word-count signals per job
    category, from the Gold layer (gold_category_stats).

    Returns:
        A dict with a `categories` list, each entry: category, resume_count,
        avg_word_count, avg_leadership_score. Sorted by avg_leadership_score
        descending - the categories showing the most leadership language
        come first.
    """
    try:
        return {"status": "success", "categories": resume_broker.get_category_stats()}
    except Exception as e:
        logger.exception("Failed to get category stats")
        return {"status": "error", "message": str(e)}


@mcp.tool
def search_resumes(category: str | None = None, limit: int = 20) -> dict:
    """
    Get top-ranked candidates by leadership_score, optionally filtered to one
    job category, from the Gold layer (gold_top_candidates).

    Args:
        category: Job category to filter by, e.g. "INFORMATION-TECHNOLOGY"
            (see get_category_stats for the full list of valid categories).
            Omit to search across all categories.
        limit: Max number of candidates to return (default 20, max 100).

    Returns:
        A dict with a `candidates` list, each entry: category, resume_id,
        leadership_score, rank (the candidate's rank within its category).
    """
    try:
        return {
            "status": "success",
            "candidates": resume_broker.search_resumes(category, limit),
        }
    except Exception as e:
        logger.exception("Failed to search resumes")
        return {"status": "error", "message": str(e)}


@mcp.tool
def find_matching_resumes(job_description: str, limit: int = 10) -> dict:
    """
    Semantic search: find résumés whose content best matches a job
    description, using Databricks Vector Search over the Gold/Silver layer
    (semantic similarity, not keyword matching).

    Args:
        job_description: Free-text description of the role to match
            candidates against.
        limit: Max number of results (default 10, max 50).

    Returns:
        A dict with a `matches` list, each entry: resume_id, category,
        match_score (0-1, higher is more similar), snippet.
    """
    try:
        return {
            "status": "success",
            "matches": vector_search.find_matching_resumes(job_description, limit),
        }
    except Exception as e:
        logger.exception("Failed to find matching resumes")
        return {"status": "error", "message": str(e)}


@mcp.tool
def get_shortlist(limit: int = 100) -> dict:
    """
    Get the current candidate shortlist - résumés already flagged for human
    review, most recently flagged first.

    Args:
        limit: Max number of entries to return (default 100).

    Returns:
        A dict with a `shortlist` list, each entry: resume_id, category,
        note, added_by (email of whoever/whatever flagged it), added_at.
    """
    try:
        rows = lakebase.run_query(
            """
            SELECT resume_id, category, note, email AS added_by, added_at
            FROM candidate_shortlist
            ORDER BY added_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        return {"status": "success", "count": len(rows), "shortlist": rows}
    except Exception as e:
        logger.exception("Failed to get shortlist")
        return {"status": "error", "message": str(e)}


@mcp.tool
def shortlist_candidate(resume_id: str, category: str, note: str) -> dict:
    """
    Flag a résumé for human review by adding it to the recruiter shortlist.

    This tool NEVER approves or rejects a candidate - it only records a
    suggestion for a human recruiter to look at. Always explain in `note`
    why this résumé deserves review (e.g. which leadership signals stood
    out, or how it compares to others in its category). The recruiter
    reviewing the shortlist makes the actual hiring decision, not this tool.

    Args:
        resume_id: The résumé's ID (from search_resumes or get_category_stats
            context).
        category: The résumé's job category.
        note: Why this résumé is being flagged for review - required, shown
            to the human recruiter reading the shortlist.

    Returns:
        A dict confirming the résumé was added (or updated, if it was
        already shortlisted) with who flagged it and when.
    """
    try:
        added_by = _get_end_user_email()
        lakebase.run_write(
            """
            INSERT INTO candidate_shortlist (email, resume_id, category, note, added_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (email, resume_id)
            DO UPDATE SET note = EXCLUDED.note, added_at = NOW()
            """,
            (added_by, resume_id, category, note),
        )
        return {
            "status": "success",
            "message": f"{resume_id} agregado al shortlist para revisión humana.",
            "resume_id": resume_id,
            "category": category,
            "note": note,
            "added_by": added_by,
        }
    except Exception as e:
        logger.exception(f"Failed to shortlist {resume_id}")
        return {"status": "error", "message": str(e)}


if __name__ == "__main__":
    # Add middleware to capture request headers for end-user identity
    # This must be done before mcp.run() is called
    if hasattr(mcp, "app") and mcp.app is not None:
        mcp.app.add_middleware(RequestContextMiddleware)

    # Databricks Apps route external HTTP traffic to this port via app.yaml;
    # streamable-http is the transport Databricks' MCP client/gateway expects.
    port = int(os.getenv("DATABRICKS_APP_PORT", os.getenv("PORT", 8000)))
    mcp.run(transport="http", host="0.0.0.0", port=port)
