"""
Recruiter dashboard: a small Flask app to WATCH the candidate shortlist the
Agent Bricks agent is building via the recruiter MCP server
(recruiter_mcp_server.py). This app never shortlists anyone itself - it only
reads candidate_shortlist from Lakebase (via lakebase.py) and
gold_category_stats from Unity Catalog (via resume_broker.py), so it can run
side-by-side with the MCP server and show, in near real time, what the agent
has flagged for human review. Also doubles as the red-of-safety-net view
mentioned in PLAN.md if the agent/MCP server doesn't get finished in time -
gold_category_stats alone already answers the business question.

Deploy this as its OWN Databricks App (separate from recruiter_mcp_server.py).

Run locally:
    python app.py
"""

import os

from flask import Flask, jsonify, render_template

import lakebase
import resume_broker

app = Flask(__name__)


@app.route("/healthz")
def healthz():
    return jsonify({"status": "ok"})


@app.errorhandler(Exception)
def handle_exception(err):
    """Ensure all unhandled errors return JSON (not an HTML error page)."""
    status_code = getattr(err, "code", 500)
    if not isinstance(status_code, int):
        status_code = 500
    return jsonify({"error": str(err)}), status_code


@app.route("/")
def index():
    """Dashboard UI: category stats + the live shortlist."""
    return render_template("index.html")


@app.route("/api/category_stats")
def api_category_stats():
    """gold_category_stats, highest leadership signal first."""
    return jsonify(resume_broker.get_category_stats())


@app.route("/api/shortlist")
def api_shortlist():
    """Current candidate_shortlist from Lakebase, most recently flagged first."""
    rows = lakebase.run_query(
        """
        SELECT resume_id, category, note, email AS added_by, added_at
        FROM candidate_shortlist
        ORDER BY added_at DESC
        """
    )
    return jsonify(rows)


if __name__ == "__main__":
    host = os.getenv("FLASK_RUN_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_RUN_PORT", 8001))
    app.run(debug=True, host=host, port=port)
