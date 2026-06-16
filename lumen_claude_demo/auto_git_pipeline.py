# Databricks notebook source
# MAGIC %md
# MAGIC # Automated Git Branch & PR Pipeline
# MAGIC 
# MAGIC This notebook automates the process of:
# MAGIC 1. Detecting changes in notebooks/aggregations
# MAGIC 2. Creating a feature branch automatically
# MAGIC 3. Committing changes to the feature branch
# MAGIC 4. A GitHub Action then auto-creates a PR for admin review

# COMMAND ----------

import requests
import json
from datetime import datetime

# Configuration
GITHUB_TOKEN = dbutils.secrets.get(scope="github", key="pat_token")
REPO_OWNER = "syedzyad-ld"
REPO_NAME = "lumen_demo"
BASE_BRANCH = "main"
GITHUB_API = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}"

headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# COMMAND ----------

def get_main_sha():
    """Get the latest commit SHA on main branch"""
    resp = requests.get(f"{GITHUB_API}/git/ref/heads/{BASE_BRANCH}", headers=headers)
    resp.raise_for_status()
    return resp.json()["object"]["sha"]

def create_feature_branch(branch_name):
    """Create a new feature branch from main"""
    main_sha = get_main_sha()
    data = {"ref": f"refs/heads/{branch_name}", "sha": main_sha}
    resp = requests.post(f"{GITHUB_API}/git/refs", headers=headers, json=data)
    if resp.status_code == 422:
        print(f"Branch {branch_name} already exists, using existing branch")
        return main_sha
    resp.raise_for_status()
    print(f"Created branch: {branch_name}")
    return main_sha

def commit_file_to_branch(branch_name, file_path, content, commit_message):
    """Commit a file to the specified branch"""
    import base64
    encoded = base64.b64encode(content.encode()).decode()
    
    # Check if file exists
    resp = requests.get(f"{GITHUB_API}/contents/{file_path}?ref={branch_name}", headers=headers)
    
    data = {
        "message": commit_message,
        "content": encoded,
        "branch": branch_name
    }
    
    if resp.status_code == 200:
        data["sha"] = resp.json()["sha"]
    
    resp = requests.put(f"{GITHUB_API}/contents/{file_path}", headers=headers, json=data)
    resp.raise_for_status()
    print(f"Committed: {file_path} to {branch_name}")
    return resp.json()

# COMMAND ----------

def auto_commit_changes(notebook_path, content, change_description="Notebook update"):
    """
    Main function to automate the branch-commit-PR flow.
    Call this whenever a notebook or aggregation changes.
    """
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
    user_short = user.split("@")[0].replace(".", "-")
    
    # Create branch name
    branch_name = f"feature/{user_short}/{timestamp}"
    
    print(f"=== Automated Git Pipeline ===")
    print(f"User: {user}")
    print(f"Branch: {branch_name}")
    print(f"Description: {change_description}")
    print(f"==============================")
    
    # Step 1: Create feature branch
    create_feature_branch(branch_name)
    
    # Step 2: Commit changes
    commit_message = f"[Auto] {change_description} by {user_short} at {timestamp}"
    commit_file_to_branch(branch_name, notebook_path, content, commit_message)
    
    # Step 3: GitHub Actions will automatically create the PR
    print(f"\nDone! GitHub Actions will create a PR from '{branch_name}' to 'main'.")
    print(f"An admin must approve the PR before changes are merged.")
    
    return branch_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Example
# MAGIC 
# MAGIC Call `auto_commit_changes()` from any notebook when changes are made:
# MAGIC ```python
# MAGIC branch = auto_commit_changes(
# MAGIC     notebook_path="lumen_claude_demo/my_notebook.py",
# MAGIC     content=notebook_content_string,
# MAGIC     change_description="Updated aggregation logic for monthly sales"
# MAGIC )
# MAGIC ```

# COMMAND ----------

# Example: Detect and push changes for the current notebook
# Uncomment to test:
# branch = auto_commit_changes(
#     notebook_path="lumen_claude_demo/test_change.json",
#     content=json.dumps({"test": "automated commit", "timestamp": str(datetime.now())}),
#     change_description="Test automated pipeline"
# )