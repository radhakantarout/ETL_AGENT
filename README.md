# ETL_AGENT - Claude-driven Agentic ETL Starter

This starter repo contains a prototype agent that uses **Claude (Anthropic)** to generate, patch, and retry PySpark ETL jobs from a single `claude.md` instruction file.

**What you get**
- `claude.md` — plain English instructions for the ETL run.
- `dq_rules.json` — sample DQ rules for customers, orders, order_items.
- `business_mapping.xlsx` — sample mapping definitions.
- `run_agent.py` — supervisor script that:
  - Reads `claude.md`
  - Calls Claude to generate ETL job code
  - Commits & pushes generated code to your repo (git)
  - Uploads artifacts to S3
  - Submits Spark job to EMR (optional)
  - Monitors logs; on failure asks Claude to propose fixes; applies patches and retries (up to 3 times)
- `utils/` — helper modules for GitHub, AWS and LLM calls.

**Important**
- This is a PoC starter. Review IAM permissions and never commit secrets to the repo.
- Install dependencies: `pip install -r requirements.txt`
- Environment variables required:
  - `ANTHROPIC_API_KEY` — your Claude/Anthropic API key
  - `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (or use OIDC)
  - `GIT_AUTHOR_NAME`, `GIT_AUTHOR_EMAIL` (optional)
  - `EMR_CLUSTER_ID` (optional, to auto-submit)
  - `S3_BUCKET` — bucket used for upload (must exist)

**How to run locally**
1. Configure AWS CLI and ensure your IAM credentials have S3/EMR permissions.
2. Install python deps: `pip install -r requirements.txt`
3. Edit `claude.md` to point to your bucket (replace `your-bucket`).
4. Run: `python run_agent.py --no-push` to generate and test locally (no git push / emr submit).
5. To run fully: `python run_agent.py` (will push and submit if env vars are set).

See code comments inside `run_agent.py` for details about customization.