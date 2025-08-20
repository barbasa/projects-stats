# Copyright (c) 2025 BMW. All rights reserved.

import configparser
import requests
import json
import subprocess
import csv
import os
import gzip
import re
from datetime import datetime, timezone
from urllib.parse import unquote, parse_qs, urlsplit

# === Load configuration ===
config = configparser.ConfigParser()
config.read("config.ini")

GERRIT_URL = config["general"]["gerrit_url"]
GERRIT_USER = config["general"]["gerrit_user"]
GERRIT_PASSWORD = config["general"]["gerrit_password"]
GIT_BASE_PATH = config["general"]["git_base_path"]
CSV_OUTPUT = config["general"]["csv_output"]
DISCARDED_URLS_OUTPUT = config["general"]["discarded_urls_output"]
LOGS_PATH = config["general"].get("logs_path", "")

AUTH = (GERRIT_USER, GERRIT_PASSWORD)
CSV_HEADER_REPOSITORY = "Repository"
CSV_HEADER_CREATION_DATE = "Creation Date"
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

_UPLOAD_PACK_RE = re.compile(
    r'"(?:GET|POST)\s+/(?:(?:a|p)/)?(?P<proj>[^\s/][^\s]*?)(?:\.git)?/(?:info/refs\?service=git-upload-pack|(?:git-)?upload-pack)\b',
    re.IGNORECASE,
)
_HTTP_URL_RE = re.compile(r'"(?:GET|HEAD)\s+([^\s\"]+)', re.IGNORECASE)


def _extract_proj(line: str):

    m = _UPLOAD_PACK_RE.search(line)
    if m:
        proj = m.group("proj")
        try:
            return unquote(proj), None
        except Exception:
            return proj, None

    um = _HTTP_URL_RE.search(line)
    if not um:
        return None, None

    # e.g., /a/projects/foo/..., /changes/foo~bar~I123
    raw_url = um.group(1)
    try:
        parts = urlsplit(raw_url)
        path = parts.path
    except Exception:
        # Fallback: naive split if urlsplit fails on malformed lines
        if '?' in raw_url:
            path, query = raw_url.split('?', 1)
        else:
            path, query = raw_url, ''

    # Normalize: strip optional leading /a/
    if path.startswith('/a/'):
        path_ = path[2:]
    else:
        path_ = path

    # 1) Path form: /projects/<proj>/...
    if path_.startswith('/projects/'):
        segs = path_.split('/')
        encodedProj = segs[2]
        try:
            return unquote(encodedProj), None
        except Exception:
            print(f"WARN: Cannot decode project {encodedProj}")
            return encodedProj, None

    # 2) Change triplet: /changes/<project>~<branch>~<id>
    if path_.startswith('/changes/'):
        rest = path_[len('/changes/'):]
        proj = rest.split('~', 1)[0]
        try:
            return unquote(proj), None
        except Exception:
            return proj, None

    return None, path_


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
    discarded_urls = set()
    for path in _iter_log_files(logs_dir):
        try:
            with _open_maybe_gz(path) as fh:
                for line in fh:
                    ts = _extract_ts(line)
                    if not ts:
                        continue

                    proj, discarded_url = _extract_proj(line)
                    if not proj:
                        if discarded_url:
                            discarded_urls.add(discarded_url)
                        continue

                    prev = last_reads.get(proj)
                    if prev is None or ts > prev:
                        last_reads[proj] = ts
        except Exception as e:
            print(f"Warning: failed to process {path}: {e}")
    return last_reads, discarded_urls


def write_to_csv(repo_data):
    """Write repository name, creation date, last update, and last read dates to CSV.

    repo_data is an iterable of tuples: (repo, creation_date, last_update, last_read)
    """
    with open(CSV_OUTPUT, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([CSV_HEADER_REPOSITORY, CSV_HEADER_CREATION_DATE, CSV_HEADER_LAST_READ])
        for repo, creation_ts, last_update_ts in repo_data:
            writer.writerow([repo, creation_ts or "N/A", last_update_ts or "N/A"])


def write_to_discarded_urls(discarded_url: set):
    """Write urls which are not considered read operations.
    This can be useful for debugging purposes to identify missing positive matches.

    discarded_url is a set
    """
    output_path = os.path.join(DISCARDED_URLS_OUTPUT, "discarded_urls.txt")
    with open(output_path, "w+", encoding="utf-8") as f:
        for url in sorted(discarded_url):
            f.write(url + "\n")


def main():
    print("Fetching repository list...")
    repos = get_gerrit_projects()

    repos.remove("All-Projects")
    repos.remove("All-Users")

    existing_data = load_existing_csv()  # {repo: {creation, last_update, last_read}}

    # If a logs directory is provided, extract last-reads from logs once.
    last_reads_from_logs = {}
    discarded_urls = set()
    if LOGS_PATH:
        print(f"Scanning logs in {LOGS_PATH} for last reads...")
        last_reads_from_logs, discarded_urls = get_last_reads_from_logs(LOGS_PATH)

    repo_rows = []
    for repo in repos:
        existing_creation = existing_data.get(repo, {}).get("creation")

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

        repo_rows.append((repo, creation_date, last_read))

    write_to_csv(repo_rows)
    write_to_discarded_urls(discarded_urls)
    print(f"Done. Output saved to: {CSV_OUTPUT}")


if __name__ == "__main__":
    main()
