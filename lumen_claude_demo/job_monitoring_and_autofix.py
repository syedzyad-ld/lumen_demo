# Databricks notebook source
# MAGIC %md
# MAGIC # Job Monitoring, Root Cause Analysis & Auto-Fix
# MAGIC **Purpose:** Monitor all Databricks jobs, identify failures, diagnose root causes, generate precise fixes, and submit them via GitHub PR for approval.
# MAGIC
# MAGIC **Workflow:**
# MAGIC 1. Fetch all jobs and their latest run status
# MAGIC 2. Identify failed jobs and extract FULL error details from multiple API sources
# MAGIC 3. Intelligent error parsing — reads exact error text and generates a targeted fix
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
import re
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

def api_get_safe(endpoint, params=None):
    """GET that returns None on error instead of raising"""
    try:
        url = f"{WORKSPACE_URL}{endpoint}"
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code == 200:
            return response.json()
        return None
    except:
        return None

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

            # Get notebook path from job settings (runs/list may not include task details)
            notebook_path = ""
            job_tasks = job.get("settings", {}).get("tasks", [])
            if job_tasks:
                for jt in job_tasks:
                    if jt.get("notebook_task", {}).get("notebook_path"):
                        notebook_path = jt["notebook_task"]["notebook_path"]
                        break

            job_status_list.append({
                "job_id": job_id,
                "job_name": job_name,
                "run_id": run_id,
                "status": result_state,
                "start_time": start_time,
                "duration": duration_str,
                "error_message": error_msg[:500] if error_msg else "",
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
            "error_message": str(e)[:500],
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
# MAGIC ## Cell 3: Extract FULL Error Details (Deep Task-Level Extraction)
# MAGIC For multi-task jobs, the parent run only says "Workload failed, see run output".
# MAGIC The REAL error lives in the task-level run output. This cell digs into each failed task.

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
        "notebook_content": ""
    }

    collected_errors = []

    # STEP 1: Get full run details via runs/get (this returns task-level run_ids)
    run_detail = api_get_safe("/api/2.1/jobs/runs/get", params={"run_id": job["run_id"]})

    if run_detail and run_detail.get("tasks"):
        print(f"  Multi-task job detected: {len(run_detail['tasks'])} tasks")

        for task in run_detail["tasks"]:
            task_key = task.get("task_key", "unknown")
            task_state = task.get("state", {})
            task_result = task_state.get("result_state", "")
            task_run_id = task.get("run_id")

            # Only dig into FAILED tasks (skip UPSTREAM_FAILED, SKIPPED)
            if task_result != "FAILED":
                continue

            print(f"  Failed task: '{task_key}' (task run_id: {task_run_id})")

            # Get notebook path from this specific task
            if task.get("notebook_task", {}).get("notebook_path") and not detail["notebook_path"]:
                detail["notebook_path"] = task["notebook_task"]["notebook_path"]

            # STEP 2: Call get-output on the TASK's run_id — this is where the real error lives
            if task_run_id:
                task_output = api_get_safe("/api/2.1/jobs/runs/get-output", params={"run_id": task_run_id})
                if task_output:
                    if task_output.get("error"):
                        collected_errors.append(f"[Task: {task_key}] {task_output['error']}")
                        print(f"    Error: {task_output['error'][:200]}")
                    if task_output.get("error_trace"):
                        collected_errors.append(task_output["error_trace"])
                        print(f"    Trace: {task_output['error_trace'][:200]}...")
                else:
                    print(f"    get-output returned nothing for task run_id {task_run_id}")

            # Also capture task state_message if useful
            task_msg = task_state.get("state_message", "")
            if task_msg and "see run output" not in task_msg.lower():
                collected_errors.append(f"[Task: {task_key}] {task_msg}")

    else:
        # Single-task job or no tasks in response — try get-output on parent run_id directly
        print(f"  Single-task job, fetching output from run_id: {job['run_id']}")
        run_output = api_get_safe("/api/2.1/jobs/runs/get-output", params={"run_id": job["run_id"]})
        if run_output:
            if run_output.get("error"):
                collected_errors.append(run_output["error"])
                print(f"    Error: {run_output['error'][:200]}")
            if run_output.get("error_trace"):
                collected_errors.append(run_output["error_trace"])

    # STEP 3: If still no real error found, fall back to state_message
    if not collected_errors and job["error_message"]:
        collected_errors.append(job["error_message"])

    # Filter out generic "Workload failed, see run output" messages — they're useless
    real_errors = [e for e in collected_errors if "see run output for details" not in e.lower()]
    if not real_errors:
        real_errors = collected_errors  # Keep them if that's all we have

    # Combine all error sources, deduplicate while preserving order
    seen = set()
    unique_errors = []
    for e in real_errors:
        if e.strip() and e not in seen:
            seen.add(e)
            unique_errors.append(e)

    detail["full_error"] = "\n---\n".join(unique_errors) if unique_errors else "Error details could not be retrieved"

    print(f"\n  Total error sources: {len(unique_errors)}")
    print(f"  Full error ({len(detail['full_error'])} chars):")
    print(f"  {detail['full_error'][:600]}")

    # STEP 4: Get notebook source for context
    if detail["notebook_path"]:
        try:
            export_resp = api_get("/api/2.0/workspace/export", params={
                "path": detail["notebook_path"],
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
# MAGIC ## Cell 4: Intelligent Root Cause Analysis & Fix Generation
# MAGIC Reads the actual error text and generates a precise fix — not limited to predefined categories.

# COMMAND ----------

def intelligent_fix_generator(error_text, notebook_code):
    """
    Parse the actual error message and generate a specific fix.
    This reads the exact error and produces a targeted remedy.
    """
    error_lower = error_text.lower()
    analysis = {
        "root_cause": "",
        "explanation": "",
        "recommended_fix": "",
        "fix_code": "",
        "fix_location": "prepend",  # prepend = add at top, inline = modify existing code
        "severity": "HIGH",
        "confidence": "HIGH"
    }

    # --- Missing Python package / optional dependency ---
    # Matches: "Missing optional dependency 'xlrd'", "No module named 'xxx'", "ModuleNotFoundError"
    pkg_patterns = [
        r"missing optional dependency ['\"]([^'\"]+)['\"]",
        r"install ([a-zA-Z0-9_\-]+)\s*>=?\s*([0-9.]+)",
        r"no module named ['\"]?([a-zA-Z0-9_\.\-]+)['\"]?",
        r"modulenotfounderror.*?['\"]([a-zA-Z0-9_\.\-]+)['\"]",
        r"import error.*?['\"]([a-zA-Z0-9_\.\-]+)['\"]",
        r"pip or conda to install ([a-zA-Z0-9_\-]+)",
    ]
    for pattern in pkg_patterns:
        match = re.search(pattern, error_text, re.IGNORECASE)
        if match:
            pkg_name = match.group(1).split(".")[0]  # Get top-level package
            # Check for version requirement
            version_match = re.search(rf"{pkg_name}\s*>=?\s*([0-9.]+)", error_text)
            version_spec = f">={version_match.group(1)}" if version_match else ""
            install_target = f"{pkg_name}{version_spec}" if version_spec else pkg_name

            analysis["root_cause"] = f"Missing Python package: {pkg_name}"
            analysis["explanation"] = f"The notebook requires '{pkg_name}' which is not installed on the cluster. Error: {error_text[:200]}"
            analysis["recommended_fix"] = f"Install {install_target} at the beginning of the notebook"
            analysis["fix_code"] = f"# AUTO-FIX: Install missing dependency\n%pip install {install_target}\ndbutils.library.restartPython()"
            analysis["confidence"] = "HIGH"
            return analysis

    # --- Table or view not found ---
    table_patterns = [
        r"table_or_view_not_found.*?[`'\"]([^`'\"]+)[`'\"]",
        r"table or view ['\"`]?([^\s'\"`;]+)['\"`]? not found",
        r"table not found:?\s*[`'\"]?([^\s`'\"]+)",
        r"schema ['\"]?([^\s'\"]+)['\"]? not found",
        r"database ['\"]?([^\s'\"]+)['\"]? not found",
        r"catalog ['\"]?([^\s'\"]+)['\"]? does not exist",
    ]
    for pattern in table_patterns:
        match = re.search(pattern, error_text, re.IGNORECASE)
        if match:
            missing_obj = match.group(1)
            analysis["root_cause"] = f"Table/View/Schema not found: {missing_obj}"
            analysis["explanation"] = f"The query references '{missing_obj}' which does not exist. The upstream pipeline may not have run, or the object was renamed/dropped."
            analysis["recommended_fix"] = f"Add existence check for {missing_obj} before use"
            analysis["fix_code"] = f'# AUTO-FIX: Validate object exists before use\ntry:\n    spark.sql("DESCRIBE {missing_obj}")\nexcept Exception as e:\n    raise Exception(f"Required object \'{missing_obj}\' not found. Ensure upstream pipeline has run. Error: {{e}}")'
            analysis["confidence"] = "HIGH"
            return analysis

    # --- File/path not found ---
    path_patterns = [
        r"path does not exist:?\s*['\"]?([^\s'\"]+)",
        r"filenotfounderror.*?['\"]([^'\"]+)['\"]",
        r"no such file or directory:?\s*['\"]?([^\s'\"]+)",
        r"java\.io\.filenotfoundexception:?\s*([^\s]+)",
        r"input path ['\"]?([^'\"]+)['\"]? does not exist",
    ]
    for pattern in path_patterns:
        match = re.search(pattern, error_text, re.IGNORECASE)
        if match:
            missing_path = match.group(1)
            analysis["root_cause"] = f"File/Path not found: {missing_path}"
            analysis["explanation"] = f"The path '{missing_path}' does not exist or is not accessible."
            analysis["recommended_fix"] = f"Add path validation before reading"
            analysis["fix_code"] = f'# AUTO-FIX: Validate path exists\nimport os\nsource_path = "{missing_path}"\nif not os.path.exists(source_path) and not source_path.startswith("dbfs:"):\n    # Check DBFS\n    try:\n        dbutils.fs.ls(source_path)\n    except Exception:\n        raise FileNotFoundError(f"Source path not found: {{source_path}}")\nprint(f"Path verified: {{source_path}}")'
            analysis["confidence"] = "HIGH"
            return analysis

    # --- Permission / access denied ---
    if any(kw in error_lower for kw in ["permission denied", "access denied", "not authorized", "forbidden", "insufficient privileges"]):
        resource_match = re.search(r"(?:on|to|for|accessing)\s+[`'\"]?([^\s`'\"]+)", error_text, re.IGNORECASE)
        resource = resource_match.group(1) if resource_match else "the resource"
        analysis["root_cause"] = f"Permission denied on: {resource}"
        analysis["explanation"] = f"The service principal or user running this job lacks access to '{resource}'."
        analysis["recommended_fix"] = f"Grant required permissions"
        analysis["fix_code"] = f'# AUTO-FIX: Permission issue - requires admin action\n# Run the following as catalog admin:\n# GRANT USE CATALOG ON CATALOG <catalog_name> TO `<principal>`;\n# GRANT USE SCHEMA ON SCHEMA <schema_name> TO `<principal>`;\n# GRANT SELECT ON TABLE {resource} TO `<principal>`;\nraise PermissionError("This notebook requires elevated permissions. Contact admin to grant access to: {resource}")'
        analysis["severity"] = "CRITICAL"
        return analysis

    # --- Schema mismatch / column errors ---
    schema_patterns = [
        r"cannot resolve ['\"`]([^'\"]+)['\"`].*?given input columns",
        r"column ['\"]?([^\s'\"]+)['\"]? does not exist",
        r"analysisexception.*?cannot resolve.*?['\"`]([^'\"`]+)['\"`]",
        r"schema mismatch",
        r"cannot cast.*?from\s+(\w+)\s+to\s+(\w+)",
    ]
    for pattern in schema_patterns:
        match = re.search(pattern, error_text, re.IGNORECASE)
        if match:
            col_info = match.group(1) if match.lastindex >= 1 else "unknown column"
            analysis["root_cause"] = f"Schema mismatch: {col_info}"
            analysis["explanation"] = f"Column '{col_info}' reference is invalid. Source schema may have changed."
            analysis["recommended_fix"] = "Enable schema auto-merge and add column validation"
            analysis["fix_code"] = f'# AUTO-FIX: Handle schema evolution\nspark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")\n# Validate columns before transformation\n# df.printSchema()  # Uncomment to inspect actual schema'
            analysis["confidence"] = "MEDIUM"
            return analysis

    # --- Timeout errors ---
    if any(kw in error_lower for kw in ["timed out", "timeout", "deadline exceeded", "cancelled due to timeout"]):
        analysis["root_cause"] = "Operation timeout"
        analysis["explanation"] = "The job exceeded its time limit. Query may be too heavy or cluster was slow to start."
        analysis["recommended_fix"] = "Optimize query performance and increase timeout"
        analysis["fix_code"] = '# AUTO-FIX: Performance optimization\nspark.conf.set("spark.sql.adaptive.enabled", "true")\nspark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")\nspark.conf.set("spark.sql.shuffle.partitions", "auto")\nspark.conf.set("spark.network.timeout", "800s")\nspark.conf.set("spark.sql.broadcastTimeout", "600")'
        analysis["severity"] = "MEDIUM"
        return analysis

    # --- Out of memory ---
    if any(kw in error_lower for kw in ["outofmemoryerror", "out of memory", "java.lang.outofmemory", "gc overhead limit", "container killed by yarn"]):
        analysis["root_cause"] = "Out of memory"
        analysis["explanation"] = "Data volume exceeds available cluster memory."
        analysis["recommended_fix"] = "Increase partitions, enable AQE, reduce data in memory"
        analysis["fix_code"] = '# AUTO-FIX: Memory optimization\nspark.conf.set("spark.sql.adaptive.enabled", "true")\nspark.conf.set("spark.sql.shuffle.partitions", "800")\nspark.conf.set("spark.sql.files.maxPartitionBytes", "64mb")\nspark.conf.set("spark.memory.fraction", "0.8")\n# Consider: .repartition(200) before heavy joins'
        analysis["severity"] = "CRITICAL"
        return analysis

    # --- Connection / network errors ---
    if any(kw in error_lower for kw in ["connectionerror", "connection refused", "connection reset", "unreachable", "dns resolution failed"]):
        host_match = re.search(r"(?:host|url|endpoint|connecting to)\s*[=:]?\s*['\"]?([^\s'\"]+)", error_text, re.IGNORECASE)
        host = host_match.group(1) if host_match else "external service"
        analysis["root_cause"] = f"Connection failure to: {host}"
        analysis["explanation"] = f"Network request to '{host}' failed. Service may be down or network rules block access."
        analysis["recommended_fix"] = "Add retry logic with exponential backoff"
        analysis["fix_code"] = f'# AUTO-FIX: Retry logic for network calls\nimport time\ndef retry_request(func, max_retries=3, backoff=2):\n    for attempt in range(max_retries):\n        try:\n            return func()\n        except Exception as e:\n            if attempt == max_retries - 1:\n                raise\n            wait = backoff ** attempt\n            print(f"Attempt {{attempt+1}} failed: {{e}}. Retrying in {{wait}}s...")\n            time.sleep(wait)'
        analysis["confidence"] = "MEDIUM"
        return analysis

    # --- Syntax / NameError / TypeError / ValueError ---
    code_error_patterns = [
        (r"nameerror.*?name ['\"]([^'\"]+)['\"].*?is not defined", "NameError"),
        (r"typeerror.*?:(.+?)(?:\n|$)", "TypeError"),
        (r"valueerror.*?:(.+?)(?:\n|$)", "ValueError"),
        (r"syntaxerror.*?:(.+?)(?:\n|$)", "SyntaxError"),
        (r"keyerror.*?['\"]?([^'\"]+)['\"]?", "KeyError"),
        (r"indexerror.*?:(.+?)(?:\n|$)", "IndexError"),
        (r"attributeerror.*?['\"]?([^'\"]+)['\"]?.*?has no attribute.*?['\"]?([^'\"]+)", "AttributeError"),
    ]
    for pattern, err_type in code_error_patterns:
        match = re.search(pattern, error_text, re.IGNORECASE)
        if match:
            detail_info = match.group(1).strip() if match.lastindex >= 1 else ""
            # Try to find the exact line from traceback
            line_match = re.search(r"line (\d+)", error_text, re.IGNORECASE)
            line_info = f" at line {line_match.group(1)}" if line_match else ""

            analysis["root_cause"] = f"{err_type}: {detail_info}{line_info}"
            analysis["explanation"] = f"Python {err_type} occurred: {detail_info}. {error_text[:200]}"
            analysis["recommended_fix"] = f"Fix the {err_type} in the notebook code"

            if err_type == "NameError":
                analysis["fix_code"] = f'# AUTO-FIX: Define missing variable/import\n# The variable \'{detail_info}\' is used but not defined.\n# Check if this requires an import or prior cell execution.\ntry:\n    {detail_info}\nexcept NameError:\n    raise NameError("Variable \'{detail_info}\' not defined. Ensure all cells run in order or add missing import.")'
            elif err_type == "KeyError":
                analysis["fix_code"] = f'# AUTO-FIX: Safe key access\n# Use .get() instead of direct key access to handle missing keys gracefully\n# Replace: data["{detail_info}"]  -->  data.get("{detail_info}", None)'
            elif err_type == "TypeError":
                analysis["fix_code"] = f'# AUTO-FIX: Type validation\n# {err_type}: {detail_info}\n# Add type checking before the operation'
            else:
                analysis["fix_code"] = f'# AUTO-FIX: {err_type} resolution\n# Error: {detail_info}\n# Review the code{line_info} and fix the logic error'

            analysis["confidence"] = "MEDIUM"
            return analysis

    # --- Spark / Java exceptions ---
    spark_patterns = [
        (r"org\.apache\.spark\.SparkException:(.+?)(?:\n|$)", "SparkException"),
        (r"java\.lang\.(\w+Exception):(.+?)(?:\n|$)", "JavaException"),
        (r"delta\.exceptions\.(\w+):(.+?)(?:\n|$)", "DeltaException"),
    ]
    for pattern, err_type in spark_patterns:
        match = re.search(pattern, error_text, re.IGNORECASE)
        if match:
            exc_detail = match.group(1).strip() if match.lastindex >= 1 else error_text[:200]
            analysis["root_cause"] = f"{err_type}: {exc_detail[:100]}"
            analysis["explanation"] = f"Spark/Java error: {exc_detail[:300]}"
            analysis["recommended_fix"] = f"Address the {err_type}"
            analysis["fix_code"] = f'# AUTO-FIX: Handle {err_type}\n# Error: {exc_detail[:150]}\nspark.conf.set("spark.sql.adaptive.enabled", "true")\n# If this is a data issue, check source data quality'
            analysis["confidence"] = "MEDIUM"
            return analysis

    # --- FALLBACK: Parse whatever we can from the error ---
    # Even in fallback, try to extract something useful
    # Look for the most informative line in the error
    error_lines = [l.strip() for l in error_text.split("\n") if l.strip() and not l.strip().startswith("at ")]
    key_error_line = ""
    for line in error_lines:
        if any(kw in line.lower() for kw in ["error", "exception", "failed", "cannot", "invalid", "missing"]):
            key_error_line = line[:300]
            break
    if not key_error_line and error_lines:
        key_error_line = error_lines[-1][:300]

    analysis["root_cause"] = f"Runtime failure: {key_error_line[:150]}"
    analysis["explanation"] = f"The job failed with: {key_error_line}. Full error: {error_text[:400]}"
    analysis["recommended_fix"] = f"Fix based on error: {key_error_line[:100]}"
    analysis["fix_code"] = f'# AUTO-FIX: Address runtime error\n# Error: {key_error_line[:200]}\n# Review and fix the above error in the notebook code.\n# Adding defensive checks:\nimport traceback\ntry:\n    pass  # Replace with the failing operation\nexcept Exception as e:\n    print(f"ERROR: {{e}}")\n    traceback.print_exc()\n    raise'
    analysis["confidence"] = "LOW"
    analysis["severity"] = "HIGH"
    return analysis


# Run analysis on each failed job
fix_recommendations = []

for detail in failure_details:
    print(f"\n{'='*80}")
    print(f"ANALYSIS: {detail['job_name']}")
    print(f"{'='*80}")

    analysis = intelligent_fix_generator(detail["full_error"], detail["notebook_content"])
    detail["analysis"] = analysis
    fix_recommendations.append(detail)

    print(f"  Root Cause:  {analysis['root_cause']}")
    print(f"  Severity:    {analysis['severity']}")
    print(f"  Confidence:  {analysis['confidence']}")
    print(f"  Explanation: {analysis['explanation'][:200]}")
    print(f"  Fix:         {analysis['recommended_fix']}")
    print(f"  Code:")
    for line in analysis['fix_code'].split('\n'):
        print(f"    {line}")

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
    .badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; }}
    .badge-high {{ background: #fed7d7; color: #c53030; }}
    .badge-critical {{ background: #c53030; color: white; }}
    .badge-medium {{ background: #fefcbf; color: #975a16; }}
    .success-table {{ width: 100%; border-collapse: collapse; }}
    .success-table th {{ background: #f8f9fa; padding: 10px; text-align: left; border-bottom: 2px solid #dee2e6; }}
    .success-table td {{ padding: 8px 10px; border-bottom: 1px solid #eee; }}
</style>
<div class="monitor-report">
    <div class="report-header">
        <h2 style="margin:0;">Job Monitoring Report</h2>
        <p style="margin:5px 0 0; opacity:0.8;">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Scanned: {total_jobs} jobs</p>
    </div>
    <div class="kpi-grid">
        <div class="kpi-card total"><div class="kpi-value">{total_jobs}</div><div class="kpi-label">Total Jobs</div></div>
        <div class="kpi-card success"><div class="kpi-value">{success_count}</div><div class="kpi-label">Succeeded</div></div>
        <div class="kpi-card failed"><div class="kpi-value">{failed_count}</div><div class="kpi-label">Failed</div></div>
        <div class="kpi-card running"><div class="kpi-value">{running_count}</div><div class="kpi-label">Running</div></div>
    </div>
"""

if fix_recommendations:
    report_html += "<h3>Failed Jobs - Diagnosis & Auto-Fix</h3>"
    for rec in fix_recommendations:
        a = rec["analysis"]
        sev_class = "badge-critical" if a["severity"] == "CRITICAL" else "badge-high" if a["severity"] == "HIGH" else "badge-medium"
        report_html += f"""
    <div class="failure-card">
        <div class="failure-title">&#10060; {rec['job_name']}</div>
        <p><span class="badge {sev_class}">{a['severity']}</span> | Confidence: {a['confidence']}</p>
        <p><strong>Root Cause:</strong> {a['root_cause']}</p>
        <p><strong>Explanation:</strong> {a['explanation'][:300]}</p>
        <p><strong>Recommended Fix:</strong> {a['recommended_fix']}</p>
        <div class="fix-code">{a['fix_code']}</div>
        <p style="margin-top:10px;color:#2d5a87;"><strong>&#8594; Action:</strong> Fix auto-committed to GitHub feature branch for PR approval</p>
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

            # Apply fix: insert fix code after the first COMMAND separator (after initial setup)
            fix_header = f"# --- AUTO-FIX by Job Monitor ({datetime.now().strftime('%Y-%m-%d %H:%M')}) ---"
            fix_comment = f"# Root Cause: {analysis['root_cause']}"
            fix_footer = "# --- END AUTO-FIX ---"
            fix_block = f"\n{fix_header}\n{fix_comment}\n{fix_code}\n{fix_footer}\n"

            if "# COMMAND ----------" in current_content:
                parts = current_content.split("# COMMAND ----------", 2)
                if len(parts) >= 3:
                    modified_content = parts[0] + "# COMMAND ----------" + parts[1] + "# COMMAND ----------\n" + fix_block + "\n# COMMAND ----------" + "# COMMAND ----------".join(parts[2:])
                else:
                    modified_content = current_content + "\n\n# COMMAND ----------\n" + fix_block
            else:
                modified_content = fix_block + "\n\n" + current_content

            # Step 4: Determine repo file path
            notebook_filename = notebook_path.split("/")[-1]
            repo_file_path = f"{REPO_NOTEBOOK_FOLDER}/{notebook_filename}.py"

            # Step 5: Check if file exists on the branch
            file_sha = None
            try:
                existing = github_get(f"/repos/{GITHUB_REPO}/contents/{repo_file_path}?ref={branch_name}")
                file_sha = existing.get("sha")
            except:
                pass

            # Step 6: Commit the fixed file to feature branch
            commit_msg = f"fix({clean_name}): {analysis['root_cause'][:60]}\n\nDiagnosis: {analysis['explanation'][:200]}\nFix: {analysis['recommended_fix']}"
            commit_data = {
                "message": commit_msg,
                "content": base64.b64encode(modified_content.encode("utf-8")).decode("utf-8"),
                "branch": branch_name
            }
            if file_sha:
                commit_data["sha"] = file_sha

            github_put(f"/repos/{GITHUB_REPO}/contents/{repo_file_path}", commit_data)
            print(f"  Fix committed: {repo_file_path}")
            print(f"  Auto-PR will be created by GitHub Actions")

            created_prs.append({
                "job_name": job_name,
                "branch": branch_name,
                "root_cause": analysis["root_cause"],
                "fix": analysis["recommended_fix"],
                "severity": analysis["severity"],
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
                "severity": analysis["severity"],
                "repo_file": "N/A",
                "pr_url": "N/A",
                "status": f"COMMIT_FAILED: {str(e)[:100]}"
            })

else:
    print("No failed jobs detected. No fixes needed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: Summary & PR Status

# COMMAND ----------

print("="*80)
print("JOB MONITOR - FIX SUBMISSION SUMMARY")
print("="*80)

if created_prs:
    submitted = [p for p in created_prs if p["status"] == "PR_PENDING"]
    failed_submissions = [p for p in created_prs if "FAILED" in p["status"]]

    print(f"\nFixes submitted via PR: {len(submitted)}")
    print(f"Failed to submit:       {len(failed_submissions)}")
    print()

    for i, pr in enumerate(submitted, 1):
        print(f"[OK] Fix #{i}: {pr['job_name']}")
        print(f"      Root Cause: {pr['root_cause']}")
        print(f"      Severity:   {pr['severity']}")
        print(f"      Fix:        {pr['fix']}")
        print(f"      Branch:     {pr['branch']}")
        print(f"      File:       {pr['repo_file']}")
        print(f"      PRs:        {pr['pr_url']}")
        print()

    if failed_submissions:
        print("\n--- FAILED SUBMISSIONS ---")
        for pr in failed_submissions:
            print(f"[!!] {pr['job_name']}: {pr['status']}")
        print()

    print("-"*80)
    print("GOVERNANCE WORKFLOW:")
    print("  1. GitHub Actions creates Pull Request automatically for each fix branch")
    print("  2. Admin/Approver reviews the code change in the PR")
    print("  3. Admin approves and merges PR to main")
    print("  4. Databricks Repos syncs with updated main branch")
    print("  5. Re-run the failed job — fix is now deployed")
    print("-"*80)

    # Display as table
    pr_df = spark.createDataFrame([Row(**p) for p in created_prs])
    display(pr_df)
else:
    print("\nAll jobs are healthy — no fixes needed!")
    print("Schedule this monitoring job to run periodically for continuous oversight.")
