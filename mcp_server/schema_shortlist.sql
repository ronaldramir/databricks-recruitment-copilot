-- candidate_shortlist table: where shortlist_candidate (recruiter_mcp_server.py)
-- records résumés flagged for human review. Run this against your Lakebase
-- Postgres database (psql, or a SQL client pointed at LAKEBASE_URL) before
-- deploying the MCP server app.

CREATE TABLE IF NOT EXISTS candidate_shortlist (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    resume_id VARCHAR(50) NOT NULL,
    category VARCHAR(100) NOT NULL,
    note TEXT,
    added_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_recruiter_resume UNIQUE (email, resume_id)
);

CREATE INDEX IF NOT EXISTS idx_shortlist_category ON candidate_shortlist(category);
CREATE INDEX IF NOT EXISTS idx_shortlist_added_at ON candidate_shortlist(added_at DESC);
