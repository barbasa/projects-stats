import configparser
import requests
import json
import subprocess
import csv
import os
import gzip
import re
from datetime import datetime, timezone

# === Load configuration ===
config = configparser.ConfigParser()
config.read("config.ini")

GERRIT_URL = config["general"]["gerrit_url"]
GERRIT_USER = config["general"]["gerrit_user"]
GERRIT_PASSWORD = config["general"]["gerrit_password"]
GIT_BASE_PATH = config["general"]["git_base_path"]
CSV_OUTPUT = config["general"]["csv_output"]
LOGS_PATH = config["general"].get("logs_path", "")

AUTH = (GERRIT_USER, GERRIT_PASSWORD)
CSV_HEADER_REPOSITORY = "Repository"
CSV_HEADER_CREATION_DATE = "Creation Date"
CSV_HEADER_LAST_UPDATE = "Last Update Date"
CSV_HEADER_LAST_READ = "Last Read Date"
PROJECTS_ENDPOINT = "/projects"


def load_existing_csv():
    """Load existing CSV data into a dictionary mapping repo -> {creation,last_update, last_read}."""
    existing = {}
    if os.path.exists(CSV_OUTPUT):
        with open(CSV_OUTPUT, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                repo = row.get(CSV_HEADER_REPOSITORY)
                if not repo:
                    continue
                existing[repo] = {
                    "creation": row.get(CSV_HEADER_CREATION_DATE) or None,
                    "last_update": row.get(CSV_HEADER_LAST_UPDATE) or None,
                    "last_read": row.get(CSV_HEADER_LAST_READ) or None,
                }
    return existing


def get_gerrit_projects():
    """Fetch list of repositories from Gerrit REST API."""
    response = requests.get(GERRIT_URL + PROJECTS_ENDPOINT, auth=AUTH)
    response.raise_for_status()
    content = response.text.lstrip(")]}'\n")  # Remove Gerrit XSSI prefix
    projects = json.loads(content)
    return list(projects.keys())


def get_project_last_updated(project_name):
    """Get the last updated timestamp for a project based on the latest change."""
    query = f"/changes/?S=0&n=1&q=project:{project_name}"
    url = f"{GERRIT_URL}{query}"
    response = requests.get(url, auth=AUTH)
    response.raise_for_status()
    content = response.text.lstrip(")]}'\n")
    changes = json.loads(content)
    if changes:
        return changes[0].get("updated")
    return None


def get_first_commit_date(project_name):
    """Return the date of the first commit on the 'master' branch."""
    repo_path = os.path.join(GIT_BASE_PATH, f"{project_name}.git")
    try:
        result = subprocess.run(
            ["git", "--git-dir", repo_path, "log", "--reverse", "--format=%aI", "master"],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip().splitlines()[0]
    except Exception:
        return None


# === HTTP logs parsing for last-read extraction ===
_TS_RE = re.compile(r"\[(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)\]")
# TODO: match REST API calls as well
_UPLOAD_PACK_RE = re.compile(
    r'"(?:GET|POST)\s+/(?:(?:a|p)/)?(?P<proj>[^\s/][^\s]*?)(?:\.git)?/(?:info/refs\?service=git-upload-pack|(?:git-)?upload-pack)\b',
    re.IGNORECASE,
)


def _iter_log_files(logs_dir: str):
    files = []
    for f in os.listdir(logs_dir):
        full = os.path.join(logs_dir, f)
        if os.path.isfile(full) and (f.endswith(".log") or f.endswith(".gz") or ".log" in f):
            files.append(full)
    # sort by modification time descending (most recent first) so we can continue as soon as we find
    # a read without parsing all the files
    for full in sorted(files, key=os.path.getmtime, reverse=True):
        print(f"Analyzing file: {full}")
        yield full


def _open_maybe_gz(path: str):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return open(path, "rt", encoding="utf-8", errors="replace")


def _extract_proj(line: str):
    m = _UPLOAD_PACK_RE.search(line)
    if m:
        return m.group("proj")
    return None


def _extract_ts(line: str):
    m = _TS_RE.search(line)
    if not m:
        return None
    s = m.group("ts")
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    except ValueError:
        print(f"Error while converting date {s}")
        return None


def get_last_reads_from_logs(logs_dir: str):
    """Scan Gerrit HTTP logs and return {last_read} for git-upload-pack reads.

    If multiple log files are present, the latest timestamp per repo wins.
    """
    last_reads = {}
    for path in _iter_log_files(logs_dir):
        try:
            with _open_maybe_gz(path) as fh:
                for line in fh:
                    ts = _extract_ts(line)
                    if not ts:
                        continue

                    proj = _extract_proj(line)
                    if not proj:
                        continue

                    prev = last_reads.get(proj)
                    if prev is None or ts > prev:
                        last_reads[proj] = ts
        except Exception as e:
            print(f"Warning: failed to process {path}: {e}")
    return last_reads


def write_to_csv(repo_data):
    """Write repository name, creation date, last update, and last read dates to CSV.

    repo_data is an iterable of tuples: (repo, creation_date, last_update, last_read)
    """
    with open(CSV_OUTPUT, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([CSV_HEADER_REPOSITORY, CSV_HEADER_CREATION_DATE, CSV_HEADER_LAST_UPDATE, CSV_HEADER_LAST_READ])
        for repo, creation_date, last_update, last_read in repo_data:
            writer.writerow([repo, creation_date or "N/A", last_update or "N/A", last_read or "N/A"])


def main():
    print("Fetching repository list...")
    repos = get_gerrit_projects()

    repos.remove("All-Projects")
    repos.remove("All-Users")

    existing_data = load_existing_csv()  # {repo: {creation, last_update, last_read}}

    # If a logs directory is provided, extract last-reads from logs once.
    last_reads_from_logs = {}
    if LOGS_PATH:
        print(f"Scanning logs in {LOGS_PATH} for last reads...")
        last_reads_from_logs = get_last_reads_from_logs(LOGS_PATH)

    print(f"Last read {last_reads_from_logs}")
    repo_rows = []
    for repo in repos:
        existing_creation = existing_data.get(repo, {}).get("creation")
        existing_last_update = existing_data.get(repo, {}).get("last_update")

        # Always compute/keep creation date: if already in CSV, reuse; otherwise, try to get it.
        if existing_creation:
            creation_date = existing_creation
            print(f"Creation date already collected for {repo}")
        else:
            print(f"Processing: {repo}")
            creation_date = get_first_commit_date(repo)
            if not creation_date:
                print(f"Error: could not extract creation date for {repo}")

        last_read = None
        if repo in last_reads_from_logs:
            last_read = last_reads_from_logs[repo]

        last_update = existing_last_update

        repo_rows.append((repo, creation_date, last_update, last_read))

    write_to_csv(repo_rows)
    print(f"Done. Output saved to: {CSV_OUTPUT}")


if __name__ == "__main__":
    main()
