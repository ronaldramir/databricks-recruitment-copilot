-- candidate_shortlist table: where shortlist_candidate (recruiter_mcp_server.py)
-- records résumés flagged for human review. Run this against your Lakebase
-- Postgres database (psql, or a SQL client pointed at LAKEBASE_URL) before
-- deploying the MCP server app.
--
-- job_title/job_description tie each flag to the specific role search that
-- produced it, so the same résumé can be shortlisted more than once for
-- different roles without one flag overwriting the other's context.
-- recruiter_email (not just "email") makes explicit whose address this is -
-- the human recruiter who flagged the candidate, not the candidate's own.

CREATE TABLE IF NOT EXISTS candidate_shortlist (
    id SERIAL PRIMARY KEY,
    recruiter_email VARCHAR(255) NOT NULL,
    resume_id VARCHAR(50) NOT NULL,
    category VARCHAR(100) NOT NULL,
    job_title VARCHAR(255) NOT NULL,
    job_description TEXT,
    note TEXT,
    added_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_recruiter_resume_job UNIQUE (recruiter_email, resume_id, job_title)
);

CREATE INDEX IF NOT EXISTS idx_shortlist_category ON candidate_shortlist(category);
CREATE INDEX IF NOT EXISTS idx_shortlist_job_title ON candidate_shortlist(job_title);
CREATE INDEX IF NOT EXISTS idx_shortlist_added_at ON candidate_shortlist(added_at DESC);

-- ---------------------------------------------------------------------------
-- Migration for an existing candidate_shortlist - run this instead of the
-- CREATE TABLE above if the table is already there. Safe to run whether or
-- not you already added job_title/job_description in an earlier pass: every
-- ADD COLUMN is IF NOT EXISTS. The RENAME COLUMN line will error if the
-- table already has recruiter_email (i.e. you already ran this once) - just
-- skip that one line and run the rest.
-- ---------------------------------------------------------------------------
-- ALTER TABLE candidate_shortlist RENAME COLUMN email TO recruiter_email;
-- ALTER TABLE candidate_shortlist ADD COLUMN IF NOT EXISTS job_title VARCHAR(255);
-- ALTER TABLE candidate_shortlist ADD COLUMN IF NOT EXISTS job_description TEXT;
-- UPDATE candidate_shortlist SET job_title = category WHERE job_title IS NULL;
-- ALTER TABLE candidate_shortlist ALTER COLUMN job_title SET NOT NULL;
-- ALTER TABLE candidate_shortlist DROP CONSTRAINT IF EXISTS unique_recruiter_resume;
-- ALTER TABLE candidate_shortlist DROP CONSTRAINT IF EXISTS unique_recruiter_resume_job;
-- ALTER TABLE candidate_shortlist ADD CONSTRAINT unique_recruiter_resume_job UNIQUE (recruiter_email, resume_id, job_title);
-- CREATE INDEX IF NOT EXISTS idx_shortlist_job_title ON candidate_shortlist(job_title);
