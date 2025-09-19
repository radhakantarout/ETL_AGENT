import os, time, json
import boto3

def upload_to_s3(local_path, bucket, key):
    s3 = boto3.client('s3', region_name=os.environ.get('AWS_REGION'))
    s3.upload_file(local_path, bucket, key)
    return f"s3://{bucket}/{key}"

def submit_emr_step(cluster_id, s3_script_path, rules_s3_path, bucket, aws_region=None):
    emr = boto3.client('emr', region_name=aws_region or os.environ.get('AWS_REGION'))
    step_args = [
        "spark-submit",
        "--deploy-mode","cluster",
        s3_script_path,
        "--rules_s3", rules_s3_path,
        "--s3_bucket", bucket,
        "--aws_region", aws_region or os.environ.get('AWS_REGION')
    ]
    step = {
        'Name': 'generated-dq-step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {'Jar': 'command-runner.jar', 'Args': step_args}
    }
    resp = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
    return resp.get('StepIds', [None])[0]

def monitor_emr_step(cluster_id, step_id, timeout_minutes=60):
    emr = boto3.client('emr', region_name=os.environ.get('AWS_REGION'))
    deadline = time.time() + timeout_minutes*60
    while time.time() < deadline:
        resp = emr.describe_step(ClusterId=cluster_id, StepId=step_id)
        state = resp['Step']['Status']['State']
        print("EMR step state:", state)
        if state in ('COMPLETED','FAILED','CANCELLED','INTERRUPTED'):
            return state, resp
        time.sleep(15)
    return 'TIMED_OUT', {}