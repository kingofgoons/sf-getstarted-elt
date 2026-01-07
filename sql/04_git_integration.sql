-- Snowflake Git integration for this repo (generic names)
-- Prereqs: create a SECRET with GitHub PAT (read-only) or adjust to OAUTH as needed.

USE ROLE ACCOUNTADMIN;

-- Optional: secret for PAT (replace placeholders)
-- CREATE OR REPLACE SECRET GIT_PAT_SECRET TYPE=PASSWORD USERNAME='<github-username>' PASSWORD='<github-pat>';

CREATE OR REPLACE GIT INTEGRATION DEMO_LAB_GIT_INT
  ENABLED = TRUE
  GIT_AUTH_TYPE = SECRET
  GIT_PROVIDER = GITHUB
  SECRET = GIT_PAT_SECRET
  ALLOWED_HOSTS = ('github.com');

CREATE OR REPLACE GIT REPOSITORY DEMO_LAB_REPO
  INTEGRATION = DEMO_LAB_GIT_INT
  ORIGIN = 'https://github.com/kingofgoons/sf-getstarted-elt.git';

-- Fetch latest main branch
ALTER GIT REPOSITORY DEMO_LAB_REPO FETCH;

-- Example execution:
-- EXECUTE IMMEDIATE FROM '@DEMO_LAB_REPO/branches/main/sql/00_setup.sql';
-- EXECUTE IMMEDIATE FROM '@DEMO_LAB_REPO/branches/main/sql/01_stages_formats.sql';
-- EXECUTE IMMEDIATE FROM '@DEMO_LAB_REPO/branches/main/sql/02_streams_tasks.sql';
-- EXECUTE IMMEDIATE FROM '@DEMO_LAB_REPO/branches/main/sql/03_cost_monitoring.sql';

