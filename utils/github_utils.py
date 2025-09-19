import os, subprocess

def git_commit_and_push(files, branch='generated/dq-auto', commit_message='chore: generated ETL job'):
    # Configure git user if provided
    name = os.environ.get('GIT_AUTHOR_NAME','GitHub Action')
    email = os.environ.get('GIT_AUTHOR_EMAIL','actions@github.com')
    subprocess.run(['git','config','user.name',name], check=True)
    subprocess.run(['git','config','user.email',email], check=True)
    # create branch
    subprocess.run(['git','checkout','-b', branch], check=True)
    # add files
    for f in files:
        subprocess.run(['git','add', f], check=True)
    subprocess.run(['git','commit','-m', commit_message], check=True)
    subprocess.run(['git','push','--set-upstream','origin', branch], check=True)
    return branch