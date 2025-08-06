# Copyright (c) 2025 BMW. All rights reserved.

import configparser
import requests
import json
import subprocess
import csv
import os

# === Load configuration ===
config = configparser.ConfigParser()
config.read("config.ini")

GERRIT_URL = config["general"]["gerrit_url"]
GERRIT_USER = config["general"]["gerrit_user"]
GERRIT_PASSWORD = config["general"]["gerrit_password"]
GIT_BASE_PATH = config["general"]["git_base_path"]
CSV_OUTPUT = config["general"]["csv_output"]

AUTH = (GERRIT_USER, GERRIT_PASSWORD)
CSV_HEADER_REPOSITORY = "Repository"
CSV_HEADER_CREATION_DATE = "Creation Date"
PROJECTS_ENDPOINT="/projects"


def load_existing_csv():
    """Load existing CSV data into a dictionary."""
    existing = {}
    if os.path.exists(CSV_OUTPUT):
        with open(CSV_OUTPUT, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing[row[CSV_HEADER_REPOSITORY]] = row[CSV_HEADER_CREATION_DATE]
    return existing


def get_gerrit_projects():
    """Fetch list of repositories from Gerrit REST API."""
    response = requests.get(GERRIT_URL + PROJECTS_ENDPOINT, auth=AUTH)
    response.raise_for_status()
    content = response.text.lstrip(")]}'\n")  # Remove Gerrit XSSI prefix
    projects = json.loads(content)
    return list(projects.keys())


def get_first_commit_date(project_name):
    """Return the ISO-8601 author date of the first commit on 'master',
    falling back to 'refs/meta/config' if needed. Returns None if none found.
    """
    # Full path to the bare repo
    repo_git_dir = os.path.join(GIT_BASE_PATH, f"{project_name}.git")

    candidates = [
        "master",
        "refs/meta/config",
    ]

    for ref in candidates:
        rc, out = _git_capture(
            repo_git_dir,
            "log", "--reverse", "--format=%aI", "--max-count=1", ref
        )
        if rc == 0 and out:
            return out.splitlines()[0]

    return None


def _git_capture(repo_git_dir: str, *args) -> tuple[int, str]:
    """Run a git command against a bare repo and capture (returncode, stdout).
    repo_git_dir should point to the .git directory (bare repo path).
    """
    try:
        result = subprocess.run(
            ["git", "--git-dir", repo_git_dir, *args],
            capture_output=True,
            text=True,
            check=False,
        )
        return result.returncode, result.stdout.strip()
    except Exception:
        return 1, ""

def write_to_csv(repo_dates):
    """Write the repository names and creation dates to a CSV file."""
    with open(CSV_OUTPUT, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([CSV_HEADER_REPOSITORY, CSV_HEADER_CREATION_DATE])
        for repo, date in repo_dates:
            writer.writerow([repo, date])


def main():
    print("Fetching repository list...")
    repos = get_gerrit_projects()

    repos.remove("All-Projects")
    repos.remove("All-Users")

    existing_data = load_existing_csv()
    repo_dates = []

    for repo in repos:
        if repo in existing_data:
            print(f"Skipping {repo}, already in CSV.")
            repo_dates.append((repo, existing_data[repo]))
            continue

        date = get_first_commit_date(repo)
        if date:
            repo_dates.append((repo, date))
        else:
            print(f"Error: could not extract date for {repo}")

    write_to_csv(repo_dates)
    print(f"Done. Output saved to: {CSV_OUTPUT}")


if __name__ == "__main__":
    main()
