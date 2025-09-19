#!/usr/bin/env python3
"""run_agent.py

Supervisor script that:
- Reads claude.md
- Calls Claude (Anthropic) to generate ETL PySpark code
- Saves generated code to glue_jobs/generated_etl.py
- Commits & pushes generated code to a branch
- Uploads artifacts to S3 and optionally submits an EMR step
- Monitors job; on failure asks Claude to patch the code and retries (up to 3 attempts)

Configure environment:
- ANTHROPIC_API_KEY (required)
- AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY (if upload/EMR)
- S3_BUCKET (bucket to upload scripts and rules)
- EMR_CLUSTER_ID (optional to auto-submit)
- GIT_AUTHOR_NAME, GIT_AUTHOR_EMAIL (optional)

Usage:
  python run_agent.py [--no-push] [--no-upload] [--no-submit]

Note: This is a PoC. Review IAM permissions and IAM roles before running in production.
"""
import os, sys, json, argparse, tempfile, shutil, time
from pathlib import Path
from utils import llm_utils, github_utils, aws_utils

MAX_RETRIES = 3

def load_claude_md(path='claude.md'):
    if not os.path.exists(path):
        raise FileNotFoundError('claude.md not found')
    return open(path,'r',encoding='utf-8').read()

def ask_claude_generate(prompt):
    # ask Claude to generate a single Python file (only code)
    print('Calling Claude to generate ETL job...')
    code = llm_utils.generate_code_with_claude(prompt)
    return code

def save_generated_job1(code, out_path='glue_jobs/generated_etl.py'):
    if isinstance(code, list):
        code_str = "\n".join(code)
    else:
        code_str = str(code)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path,'w',encoding='utf-8') as f:
        f.write(code_str)
    print('Wrote generated job to', out_path)
    return out_path
def save_generated_job(code, filepath='glue_jobs/generated_etl.py'):
    """
    Save the generated code to a file.
    code: str, list[str], or list[TextBlock] returned by Claude SDK
    """
    # If code is a list of TextBlocks, extract text
    if isinstance(code, list):
        code_str = "\n".join([getattr(block, "text", str(block)) for block in code])
    else:
        code_str = str(code)

    # Ensure folder exists
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(code_str)

    return filepath
def save_rules_local(src='dq_rules.json', dst='config/dq_rules.json'):
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    shutil.copyfile(src, dst)
    return dst

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--no-push', action='store_true', help='Do not commit/push generated files to git')
    parser.add_argument('--no-upload', action='store_true', help='Do not upload artifacts to S3')
    parser.add_argument('--no-submit', action='store_true', help='Do not submit EMR step')
    args = parser.parse_args()

    claude_text = load_claude_md('claude.md')
    # construct a rich prompt for Claude
    prompt = f"""You are Claude, an expert PySpark developer and data engineer.
Given the following ETL instructions, generate a complete PySpark job file that:
- Reads CSV inputs from S3 (wildcard allowed).
- Applies data quality rules specified in dq_rules.json (which will be uploaded to S3).
- Writes clean parquet to s3://{{BUCKET}}/access/{{category}}/ingest_date={{LOAD_DATE}}/.
- Writes rejects to s3://{{BUCKET}}/access/{{category}}/rejects/ingest_date={{LOAD_DATE}}/.
- Creates or updates AWS Glue external tables for clean outputs.
- Accepts command-line arguments: --rules_s3, --s3_bucket, --aws_region, --ingest_date

Return only the raw Python file content (no explanation). Here are the ETL instructions:
{claude_text}
"""
    # 1. ask Claude to generate code
    code = ask_claude_generate(prompt)
    gen_path = save_generated_job(code, 'glue_jobs/generated_etl.py')
    # 2. copy rules locally into config
    rules_local = save_rules_local('dq_rules.json','config/dq_rules.json')

    # 3. commit/push if allowed
    if not args.no_push:
        branch = os.environ.get('GENERATED_BRANCH','generated/dq-auto')
        github_utils.git_commit_and_push([gen_path, rules_local], branch=branch)
        print('Committed generated code and rules to branch', branch)
    else:
        print('Skipping git push (--no-push)')

    # 4. upload to S3 if requested
    bucket = os.environ.get('S3_BUCKET')
    if not args.no_upload and bucket:
        job_s3_key = 'scripts/generated_etl.py'
        rules_s3_key = 'config/dq_rules.json'
        s3_job = aws_utils.upload_to_s3(gen_path, bucket, job_s3_key)
        s3_rules = aws_utils.upload_to_s3(rules_local, bucket, rules_s3_key)
        print('Uploaded job to', s3_job)
        print('Uploaded rules to', s3_rules)
    else:
        print('Skipping S3 upload (no S3_BUCKET set or --no-upload)')

    # 5. optionally submit to EMR and monitor with retries
    cluster_id = os.environ.get('EMR_CLUSTER_ID')
    if not args.no_submit and cluster_id and bucket:
        rules_s3_path = f's3://{bucket}/config/dq_rules.json'
        s3_script_path = f's3://{bucket}/scripts/generated_etl.py'
        attempt = 0
        while attempt < MAX_RETRIES:
            attempt += 1
            print(f'Submitting EMR step attempt {attempt}/{MAX_RETRIES}')
            step_id = aws_utils.submit_emr_step(cluster_id, s3_script_path, rules_s3_path, bucket, os.environ.get('AWS_REGION'))
            if not step_id:
                print('Failed to submit EMR step; aborting.')
                break
            state, resp = aws_utils.monitor_emr_step(cluster_id, step_id, timeout_minutes=60)
            if state == 'COMPLETED':
                print('EMR step completed successfully.')
                break
            else:
                print('EMR step failed or timed out. Asking Claude to propose a patch...')
                error_context = json.dumps(resp, default=str)[:8000]
                patch_prompt = f"""The following PySpark job failed when running on EMR. Here are the EMR step details and error info:
{error_context}

Please propose a minimal patch to the existing job that fixes the error. Return only the patched full Python file content. If you cannot determine an exact fix, suggest robust exception handling and diagnostic logging to help debug.
"""
                patched_code = llm_utils.generate_code_with_claude(patch_prompt)
                if patched_code and len(patched_code) > 50:
                    print('Received patched code from Claude; applying and retrying.')
                    save_generated_job(patched_code, gen_path)
                    if not args.no_push:
                        github_utils.git_commit_and_push([gen_path], branch=os.environ.get('GENERATED_BRANCH','generated/dq-auto'), commit_message=f'post-failure-patch-attempt-{attempt}')
                    if not args.no_upload and bucket:
                        aws_utils.upload_to_s3(gen_path, bucket, 'scripts/generated_etl.py')
                    continue
                else:
                    print('Claude could not produce a useful patch. Aborting retries.')
                    break
        else:
            print('Exceeded max retries.')
    else:
        print('Skipping EMR submit (--no-submit or missing EMR_CLUSTER_ID/S3_BUCKET).')

if __name__ == '__main__':
    main()