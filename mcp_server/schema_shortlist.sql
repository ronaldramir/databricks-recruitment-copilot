-- candidate_shortlist table: where shortlist_candidate (recruiter_mcp_server.py)
-- records résumés flagged for human review. Run this against your Lakebase
-- Postgres database (psql, or a SQL client pointed at LAKEBASE_URL) before
-- deploying the MCP server app.
--
-- job_title/job_description tie each flag to the specific role search that
-- produced it, so the same résumé can be shortlisted more than once for
-- different roles without one flag overwriting the other's context.

CREATE TABLE IF NOT EXISTS candidate_shortlist (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    resume_id VARCHAR(50) NOT NULL,
    category VARCHAR(100) NOT NULL,
    job_title VARCHAR(255) NOT NULL,
    job_description TEXT,
    note TEXT,
    added_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_recruiter_resume_job UNIQUE (email, resume_id, job_title)
);

CREATE INDEX IF NOT EXISTS idx_shortlist_category ON candidate_shortlist(category);
CREATE INDEX IF NOT EXISTS idx_shortlist_job_title ON candidate_shortlist(job_title);
CREATE INDEX IF NOT EXISTS idx_shortlist_added_at ON candidate_shortlist(added_at DESC);

-- ---------------------------------------------------------------------------
-- Migration for an existing candidate_shortlist (created before job_title/
-- job_description existed) - run this instead of the CREATE TABLE above if
-- the table is already there. Backfills job_title from category so existing
-- rows stay valid under the new UNIQUE constraint.
-- ---------------------------------------------------------------------------
-- ALTER TABLE candidate_shortlist ADD COLUMN IF NOT EXISTS job_title VARCHAR(255);
-- ALTER TABLE candidate_shortlist ADD COLUMN IF NOT EXISTS job_description TEXT;
-- UPDATE candidate_shortlist SET job_title = category WHERE job_title IS NULL;
-- ALTER TABLE candidate_shortlist ALTER COLUMN job_title SET NOT NULL;
-- ALTER TABLE candidate_shortlist DROP CONSTRAINT IF EXISTS unique_recruiter_resume;
-- ALTER TABLE candidate_shortlist ADD CONSTRAINT unique_recruiter_resume_job UNIQUE (email, resume_id, job_title);
-- CREATE INDEX IF NOT EXISTS idx_shortlist_job_title ON candidate_shortlist(job_title);
