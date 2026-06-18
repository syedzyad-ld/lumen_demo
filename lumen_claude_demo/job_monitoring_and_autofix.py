# Databricks notebook source
# MAGIC %md
# MAGIC # Job Monitoring, Root Cause Analysis & Auto-Fix
# MAGIC **Purpose:** Monitor all Databricks jobs, identify failures, diagnose root causes, generate fixes, and submit them via GitHub PR for approval.
# MAGIC
# MAGIC **Workflow:**
# MAGIC 1. Fetch all jobs and their latest run status
# MAGIC 2. Identify failed jobs and extract error details
# MAGIC 3. AI-powered root cause analysis and fix recommendation
# MAGIC 4. Generate monitoring report
# MAGIC 5. Auto-commit fix to GitHub feature branch → Auto-PR → Admin approves merge
# MAGIC 6. Summary with PR links

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: Configuration & Setup

# COMMAND ----------

import requests
import json
import base64
from datetime import datetime, timedelta
from pyspark.sql import Row
from pyspark.sql.functions import col, lit, current_timestamp

# Databricks workspace configuration (auto-detected)
WORKSPACE_URL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# GitHub configuration
GITHUB_TOKEN = "dbutils.secrets.get(scope='github', key='token')"
GITHUB_REPO = "syedzyad-ld/lumen_demo"
GITHUB_HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}
REPO_NOTEBOOK_FOLDER = "lumen_claude_demo"

def api_get(endpoint, params=None):
    url = f"{WORKSPACE_URL}{endpoint}"
    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()
    return response.json()

def api_post(endpoint, data=None):
    url = f"{WORKSPACE_URL}{endpoint}"
    response = requests.post(url, headers=HEADERS, json=data)
    response.raise_for_status()
    return response.json()

def github_get(endpoint):
    url = f"https://api.github.com{endpoint}"
    response = requests.get(url, headers=GITHUB_HEADERS)
    response.raise_for_status()
    return response.json()

def github_post(endpoint, data):
    url = f"https://api.github.com{endpoint}"
    response = requests.post(url, headers=GITHUB_HEADERS, json=data)
    response.raise_for_status()
    return response.json()

def github_put(endpoint, data):
    url = f"https://api.github.com{endpoint}"
    response = requests.put(url, headers=GITHUB_HEADERS, json=data)
    response.raise_for_status()
    return response.json()

print(f"Workspace: {WORKSPACE_URL}")
print(f"GitHub Repo: {GITHUB_REPO}")
print(f"Monitoring started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: Fetch All Jobs & Latest Run Status

# COMMAND ----------

jobs_response = api_get("/api/2.1/jobs/list", params={"limit": 100})
all_jobs = jobs_response.get("jobs", [])

print(f"Total jobs found: {len(all_jobs)}")
print("="*80)

job_status_list = []

for job in all_jobs:
    job_id = job["job_id"]
    job_name = job.get("settings", {}).get("name", "Unnamed")

    try:
        runs_response = api_get("/api/2.1/jobs/runs/list", params={"job_id": job_id, "limit": 1})
        runs = runs_response.get("runs", [])

        if runs:
            latest_run = runs[0]
            run_id = latest_run["run_id"]
            state = latest_run.get("state", {})
            result_state = state.get("result_state", state.get("life_cycle_state", "UNKNOWN"))
            start_time = datetime.fromtimestamp(latest_run.get("start_time", 0) / 1000).strftime('%Y-%m-%d %H:%M:%S') if latest_run.get("start_time") else "N/A"
            duration_ms = latest_run.get("run_duration", 0)
            duration_str = f"{duration_ms // 60000}m {(duration_ms % 60000) // 1000}s"
            error_msg = state.get("state_message", "")

            # Get notebook path from task config
            notebook_path = ""
            tasks = latest_run.get("tasks", [])
            if tasks:
                for task in tasks:
                    if task.get("notebook_task"):
                        notebook_path = task["notebook_task"].get("notebook_path", "")
                        break
            elif latest_run.get("task", {}).get("notebook_task"):
                notebook_path = latest_run["task"]["notebook_task"].get("notebook_path", "")

            job_status_list.append({
                "job_id": job_id,
                "job_name": job_name,
                "run_id": run_id,
                "status": result_state,
                "start_time": start_time,
                "duration": duration_str,
                "error_message": error_msg[:200] if error_msg else "",
                "notebook_path": notebook_path
            })
        else:
            job_status_list.append({
                "job_id": job_id,
                "job_name": job_name,
                "run_id": None,
                "status": "NO_RUNS",
                "start_time": "N/A",
                "duration": "N/A",
                "error_message": "",
                "notebook_path": ""
            })
    except Exception as e:
        job_status_list.append({
            "job_id": job_id,
            "job_name": job_name,
            "run_id": None,
            "status": "ERROR_FETCHING",
            "start_time": "N/A",
            "duration": "N/A",
            "error_message": str(e)[:200],
            "notebook_path": ""
        })

# Display
print(f"\n{'Status':<3} {'Job Name':<40} {'Result':<15} {'Start Time':<20} {'Duration':<12}")
print("-"*90)
for job in job_status_list:
    icon = "+" if job["status"] == "SUCCESS" else "X" if job["status"] == "FAILED" else "~"
    print(f"[{icon}] {job['job_name']:<38} {job['status']:<15} {job['start_time']:<20} {job['duration']:<12}")

if job_status_list:
    status_df = spark.createDataFrame([Row(**item) for item in job_status_list])
    display(status_df.select("job_id", "job_name", "status", "start_time", "duration", "error_message"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: Identify Failed Jobs & Extract Error Details

# COMMAND ----------

failed_jobs = [j for j in job_status_list if j["status"] == "FAILED"]

print(f"Failed jobs: {len(failed_jobs)} out of {len(job_status_list)} total")
print("="*80)

failure_details = []

for job in failed_jobs:
    print(f"\n--- Analyzing: {job['job_name']} (Run ID: {job['run_id']}) ---")

    detail = {
        "job_id": job["job_id"],
        "job_name": job["job_name"],
        "run_id": job["run_id"],
        "notebook_path": job["notebook_path"],
        "error_message": job["error_message"],
        "full_error": "",
        "error_type": "UNKNOWN",
        "notebook_content": ""
    }

    try:
        run_output = api_get("/api/2.1/jobs/runs/get-output", params={"run_id": job["run_id"]})
        full_error = run_output.get("error", "")
        error_trace = run_output.get("error_trace", "")
        detail["full_error"] = error_trace if error_trace else full_error

        # Classify error type
        error_text = (full_error + " " + error_trace).lower()
        if "table_or_view_not_found" in error_text or "table not found" in error_text:
            detail["error_type"] = "TABLE_NOT_FOUND"
        elif "permission" in error_text or "access denied" in error_text:
            detail["error_type"] = "PERMISSION_DENIED"
        elif "modulenotfounderror" in error_text or "no module named" in error_text:
            detail["error_type"] = "LIBRARY_NOT_FOUND"
        elif "schema" in error_text or "cannot resolve" in error_text:
            detail["error_type"] = "SCHEMA_MISMATCH"
        elif "timeout" in error_text or "timed out" in error_text:
            detail["error_type"] = "TIMEOUT"
        elif "outofmemoryerror" in error_text:
            detail["error_type"] = "OUT_OF_MEMORY"
        elif "filenotfounderror" in error_text or "path does not exist" in error_text:
            detail["error_type"] = "FILE_NOT_FOUND"
        else:
            detail["error_type"] = "PYTHON_ERROR"

        print(f"  Error Type: {detail['error_type']}")
        print(f"  Error: {detail['full_error'][:300]}")
    except Exception as e:
        detail["full_error"] = str(e)
        print(f"  Could not fetch error details: {e}")

    # Get notebook source
    if job["notebook_path"]:
        try:
            export_resp = api_get("/api/2.0/workspace/export", params={
                "path": job["notebook_path"],
                "format": "SOURCE"
            })
            detail["notebook_content"] = base64.b64decode(export_resp.get("content", "")).decode("utf-8")
            print(f"  Notebook loaded: {len(detail['notebook_content'])} chars")
        except Exception as e:
            print(f"  Could not export notebook: {e}")

    failure_details.append(detail)

if not failed_jobs:
    print("\nAll jobs are healthy! No failures detected.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: Root Cause Analysis & Fix Recommendation

# COMMAND ----------

import re

def analyze_and_recommend_fix(detail):
    error_type = detail["error_type"]
    error_text = detail["full_error"]
    notebook_code = detail["notebook_content"]

    analysis = {
        "root_cause": "",
        "explanation": "",
        "recommended_fix": "",
        "fix_code": "",
        "severity": "HIGH",
        "confidence": "MEDIUM"
    }

    if error_type == "TABLE_NOT_FOUND":
        table_match = re.search(r"table or view[`'\s]*([^\s`']+)", error_text, re.IGNORECASE)
        missing_table = table_match.group(1) if table_match else "unknown"
        analysis["root_cause"] = f"Missing table or view: {missing_table}"
        analysis["explanation"] = f"Table '{missing_table}' does not exist. Upstream pipeline may not have run or table was dropped/renamed."
        analysis["recommended_fix"] = "Add table existence check before reading"
        analysis["fix_code"] = f'# AUTO-FIX: Table existence check\nif spark.catalog.tableExists("{missing_table}"):\n    df = spark.table("{missing_table}")\nelse:\n    raise Exception(f"Required table \'{missing_table}\' not found. Run upstream pipeline first.")'
        analysis["confidence"] = "HIGH"

    elif error_type == "LIBRARY_NOT_FOUND":
        module_match = re.search(r"no module named ['\"]?([^\s'\"]+)", error_text, re.IGNORECASE)
        missing_module = module_match.group(1) if module_match else "unknown"
        analysis["root_cause"] = f"Missing Python library: {missing_module}"
        analysis["explanation"] = f"Package '{missing_module}' not installed on cluster."
        analysis["recommended_fix"] = f"Add %pip install {missing_module} at notebook start"
        analysis["fix_code"] = f"%pip install {missing_module}"
        analysis["confidence"] = "HIGH"

    elif error_type == "PERMISSION_DENIED":
        analysis["root_cause"] = "Insufficient permissions to access resource"
        analysis["explanation"] = "Job owner lacks required GRANT on catalog/schema/table."
        analysis["recommended_fix"] = "Grant appropriate permissions to the job service principal"
        analysis["fix_code"] = "# PERMISSION FIX: Run as admin\n# GRANT ALL PRIVILEGES ON CATALOG <catalog> TO `service_principal`;"
        analysis["severity"] = "CRITICAL"

    elif error_type == "SCHEMA_MISMATCH":
        analysis["root_cause"] = "Schema evolution conflict"
        analysis["explanation"] = "Column types or names changed between source and target."
        analysis["recommended_fix"] = "Enable schema merge on write"
        analysis["fix_code"] = '# AUTO-FIX: Enable schema evolution\nspark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")'
        analysis["confidence"] = "MEDIUM"

    elif error_type == "TIMEOUT":
        analysis["root_cause"] = "Operation timed out"
        analysis["explanation"] = "Query or cluster startup exceeded time limit."
        analysis["recommended_fix"] = "Optimize query or increase timeout"
        analysis["fix_code"] = '# AUTO-FIX: Increase timeout and enable AQE\nspark.conf.set("spark.sql.adaptive.enabled", "true")\nspark.conf.set("spark.network.timeout", "600s")'
        analysis["severity"] = "MEDIUM"

    elif error_type == "OUT_OF_MEMORY":
        analysis["root_cause"] = "Insufficient memory"
        analysis["explanation"] = "Data volume exceeds cluster memory capacity."
        analysis["recommended_fix"] = "Enable adaptive execution and increase partitions"
        analysis["fix_code"] = '# AUTO-FIX: Memory optimization\nspark.conf.set("spark.sql.adaptive.enabled", "true")\nspark.conf.set("spark.sql.shuffle.partitions", "400")'
        analysis["severity"] = "HIGH"

    elif error_type == "FILE_NOT_FOUND":
        path_match = re.search(r"['\"]([^'\"]*volumes[^'\"]*)['\"]", error_text, re.IGNORECASE)
        missing_path = path_match.group(1) if path_match else "unknown path"
        analysis["root_cause"] = f"File not found: {missing_path}"
        analysis["explanation"] = "Source file missing or path changed."
        analysis["recommended_fix"] = "Add file existence validation"
        analysis["fix_code"] = f'# AUTO-FIX: File check\nimport os\nif not os.path.exists("{missing_path}"):\n    raise FileNotFoundError(f"Source not found: {missing_path}")'
        analysis["confidence"] = "HIGH"

    else:
        analysis["root_cause"] = "Runtime error in notebook"
        analysis["explanation"] = f"Error: {error_text[:300]}"
        analysis["recommended_fix"] = "Add error handling"
        analysis["fix_code"] = "# AUTO-FIX: Error handling wrapper\ntry:\n    # Original code here\n    pass\nexcept Exception as e:\n    print(f'Error encountered: {e}')\n    raise"
        analysis["confidence"] = "LOW"

    return analysis

# Run analysis
fix_recommendations = []

for detail in failure_details:
    print(f"\n{'='*80}")
    print(f"ANALYSIS: {detail['job_name']}")
    print(f"{'='*80}")

    analysis = analyze_and_recommend_fix(detail)
    detail["analysis"] = analysis
    fix_recommendations.append(detail)

    print(f"  Root Cause: {analysis['root_cause']}")
    print(f"  Severity: {analysis['severity']}")
    print(f"  Fix: {analysis['recommended_fix']}")
    print(f"  Code:\n    {analysis['fix_code']}")

if not failure_details:
    print("No failures to analyze.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: Generate Monitoring Report

# COMMAND ----------

total_jobs = len(job_status_list)
success_count = len([j for j in job_status_list if j["status"] == "SUCCESS"])
failed_count = len([j for j in job_status_list if j["status"] == "FAILED"])
running_count = len([j for j in job_status_list if j["status"] in ("RUNNING", "PENDING")])

report_html = f"""
<style>
    .monitor-report {{ font-family: 'Segoe UI', sans-serif; max-width: 1200px; margin: 0 auto; }}
    .report-header {{ background: linear-gradient(135deg, #1e3a5f, #2d5a87); color: white; padding: 25px; border-radius: 8px; margin-bottom: 20px; }}
    .kpi-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin-bottom: 20px; }}
    .kpi-card {{ background: white; border-radius: 8px; padding: 20px; text-align: center; box-shadow: 0 2px 8px rgba(0,0,0,0.1); border-top: 4px solid #ccc; }}
    .kpi-card.success {{ border-top-color: #27ae60; }}
    .kpi-card.failed {{ border-top-color: #e74c3c; }}
    .kpi-card.running {{ border-top-color: #f39c12; }}
    .kpi-card.total {{ border-top-color: #3498db; }}
    .kpi-value {{ font-size: 36px; font-weight: bold; margin: 5px 0; }}
    .kpi-label {{ font-size: 13px; color: #666; text-transform: uppercase; }}
    .failure-card {{ background: #fff5f5; border: 1px solid #fed7d7; border-radius: 8px; padding: 20px; margin-bottom: 15px; }}
    .failure-title {{ font-size: 16px; font-weight: bold; color: #c53030; margin-bottom: 10px; }}
    .fix-code {{ background: #1e1e1e; color: #d4d4d4; padding: 12px; border-radius: 4px; font-family: monospace; font-size: 12px; white-space: pre-wrap; }}
    .success-table {{ width: 100%; border-collapse: collapse; }}
    .success-table th {{ background: #f8f9fa; padding: 10px; text-align: left; border-bottom: 2px solid #dee2e6; }}
    .success-table td {{ padding: 8px 10px; border-bottom: 1px solid #eee; }}
</style>
<div class="monitor-report">
    <div class="report-header">
        <h2 style="margin:0;">Job Monitoring Report</h2>
        <p style="margin:5px 0 0; opacity:0.8;">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>
    <div class="kpi-grid">
        <div class="kpi-card total"><div class="kpi-value">{total_jobs}</div><div class="kpi-label">Total Jobs</div></div>
        <div class="kpi-card success"><div class="kpi-value">{success_count}</div><div class="kpi-label">Succeeded</div></div>
        <div class="kpi-card failed"><div class="kpi-value">{failed_count}</div><div class="kpi-label">Failed</div></div>
        <div class="kpi-card running"><div class="kpi-value">{running_count}</div><div class="kpi-label">Running</div></div>
    </div>
"""

if fix_recommendations:
    report_html += "<h3>Failed Jobs - Root Cause & Auto-Fix</h3>"
    for rec in fix_recommendations:
        a = rec["analysis"]
        report_html += f"""
    <div class="failure-card">
        <div class="failure-title">X {rec['job_name']}</div>
        <p><strong>Error Type:</strong> {rec['error_type']} | <strong>Severity:</strong> {a['severity']}</p>
        <p><strong>Root Cause:</strong> {a['root_cause']}</p>
        <p><strong>Explanation:</strong> {a['explanation']}</p>
        <p><strong>Fix:</strong> {a['recommended_fix']}</p>
        <div class="fix-code">{a['fix_code']}</div>
        <p style="margin-top:10px;color:#2d5a87;"><strong>Action:</strong> Fix will be committed to GitHub feature branch for PR approval</p>
    </div>"""

success_jobs = [j for j in job_status_list if j["status"] == "SUCCESS"]
if success_jobs:
    report_html += '<h3>Healthy Jobs</h3><table class="success-table"><tr><th>Job</th><th>Last Run</th><th>Duration</th></tr>'
    for sj in success_jobs:
        report_html += f"<tr><td>{sj['job_name']}</td><td>{sj['start_time']}</td><td>{sj['duration']}</td></tr>"
    report_html += "</table>"

report_html += "</div>"
displayHTML(report_html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: Auto-Commit Fixes to GitHub Feature Branch

# COMMAND ----------

created_prs = []

if fix_recommendations:
    print("="*80)
    print("COMMITTING FIXES TO GITHUB")
    print("="*80)

    for rec in fix_recommendations:
        analysis = rec["analysis"]
        job_name = rec["job_name"]
        notebook_path = rec["notebook_path"]
        fix_code = analysis["fix_code"]

        if not notebook_path or not fix_code:
            print(f"\n[SKIP] {job_name} - No notebook path or fix code available")
            continue

        print(f"\n--- Processing: {job_name} ---")

        try:
            # Step 1: Get main branch SHA
            ref_data = github_get(f"/repos/{GITHUB_REPO}/git/ref/heads/main")
            main_sha = ref_data["object"]["sha"]

            # Step 2: Create feature branch
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            clean_name = re.sub(r'[^a-zA-Z0-9]', '-', job_name.lower())[:30]
            branch_name = f"fix/{clean_name}-{timestamp}"

            github_post(f"/repos/{GITHUB_REPO}/git/refs", {
                "ref": f"refs/heads/{branch_name}",
                "sha": main_sha
            })
            print(f"  Branch created: {branch_name}")

            # Step 3: Export current notebook and apply fix
            export_resp = api_get("/api/2.0/workspace/export", params={
                "path": notebook_path,
                "format": "SOURCE"
            })
            current_content = base64.b64decode(export_resp["content"]).decode("utf-8")

            # Apply fix: insert fix code after the first command separator
            fix_header = f"# --- AUTO-FIX by Job Monitor ({datetime.now().strftime('%Y-%m-%d %H:%M')}) ---"
            fix_footer = "# --- END AUTO-FIX ---"
            fix_block = f"\n{fix_header}\n# Root Cause: {analysis['root_cause']}\n{fix_code}\n{fix_footer}\n"

            if "# COMMAND ----------" in current_content:
                parts = current_content.split("# COMMAND ----------", 2)
                if len(parts) >= 3:
                    modified_content = parts[0] + "# COMMAND ----------" + parts[1] + "# COMMAND ----------\n" + fix_block + "\n# COMMAND ----------" + "# COMMAND ----------".join(parts[2:])
                else:
                    modified_content = current_content + "\n\n# COMMAND ----------\n" + fix_block
            else:
                modified_content = fix_block + "\n\n" + current_content

            # Step 4: Determine repo file path
            # Map workspace path to repo path
            notebook_filename = notebook_path.split("/")[-1]
            repo_file_path = f"{REPO_NOTEBOOK_FOLDER}/{notebook_filename}.py"

            # Step 5: Check if file exists on the branch (get SHA if it does)
            file_sha = None
            try:
                existing = github_get(f"/repos/{GITHUB_REPO}/contents/{repo_file_path}?ref={branch_name}")
                file_sha = existing.get("sha")
            except:
                pass

            # Step 6: Commit the fixed file to feature branch
            commit_data = {
                "message": f"fix: Auto-fix for {job_name} - {analysis['root_cause']}",
                "content": base64.b64encode(modified_content.encode("utf-8")).decode("utf-8"),
                "branch": branch_name
            }
            if file_sha:
                commit_data["sha"] = file_sha

            github_put(f"/repos/{GITHUB_REPO}/contents/{repo_file_path}", commit_data)
            print(f"  Fix committed: {repo_file_path}")

            # Step 7: The auto-PR workflow will trigger automatically
            pr_url = f"https://github.com/{GITHUB_REPO}/pull"
            print(f"  Auto-PR will be created by GitHub Actions workflow")
            print(f"  Branch: {branch_name} -> main")

            created_prs.append({
                "job_name": job_name,
                "branch": branch_name,
                "root_cause": analysis["root_cause"],
                "fix": analysis["recommended_fix"],
                "repo_file": repo_file_path,
                "pr_url": f"https://github.com/{GITHUB_REPO}/pulls",
                "status": "PR_PENDING"
            })

        except Exception as e:
            print(f"  ERROR: {str(e)}")
            created_prs.append({
                "job_name": job_name,
                "branch": "N/A",
                "root_cause": analysis["root_cause"],
                "fix": analysis["recommended_fix"],
                "repo_file": "N/A",
                "pr_url": "N/A",
                "status": f"FAILED: {str(e)}"
            })

else:
    print("No failed jobs detected. No fixes needed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: Summary & PR Status

# COMMAND ----------

print("="*80)
print("FIX SUBMISSION SUMMARY")
print("="*80)

if created_prs:
    print(f"\nTotal fixes submitted: {len([p for p in created_prs if p['status'] == 'PR_PENDING'])}")
    print(f"Failed submissions: {len([p for p in created_prs if 'FAILED' in p['status']])}")
    print()

    for i, pr in enumerate(created_prs, 1):
        status_icon = "[OK]" if pr["status"] == "PR_PENDING" else "[!!]"
        print(f"{status_icon} Fix #{i}: {pr['job_name']}")
        print(f"      Root Cause: {pr['root_cause']}")
        print(f"      Fix: {pr['fix']}")
        print(f"      Branch: {pr['branch']}")
        print(f"      File: {pr['repo_file']}")
        print(f"      Status: {pr['status']}")
        print(f"      PRs: {pr['pr_url']}")
        print()

    print("-"*80)
    print("NEXT STEPS:")
    print("  1. GitHub Actions auto-creates Pull Requests for each fix branch")
    print("  2. Admin reviews the PR and approves the merge")
    print("  3. Once merged to main, Databricks Repos syncs automatically")
    print("  4. Re-run the original failed job to verify the fix")
    print("-"*80)

    # Display as table
    pr_df = spark.createDataFrame([Row(**p) for p in created_prs])
    display(pr_df)
else:
    print("\nAll jobs are healthy - no fixes were needed!")
    print("Run this monitoring job periodically to catch failures early.")
