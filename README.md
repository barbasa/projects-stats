# Extract Gerrit Repository Stats

This script connects to a Gerrit Code Review server, fetches the list of all repositories,
and determines the creation date of each repository based on the first commit in the master branch
or fallback to `refs/meta/config` is this doesn't exist.
The result is written to a CSV file.

## Requirements

* Python 3.7+
* git CLI installed and available in $PATH
* Access to the Gerrit server’s REST API with authentication
* Local access to the .git repositories (bare format)

## Setting up a Python Virtual Environment

* Create a Python virtual environment:

```bash
python3 -m venv venv
```

* Activate the virtual environment:

```bash
source venv/bin/activate
```

* Install required Python packages:

```bash
pip install -r requirements.txt
```

## Configuration

Create a config.ini file in the same directory as the script, with the following structure:

```bash
[general]
    gerrit_url = http://localhost:8080/a
    gerrit_user = admin
    gerrit_password = your_http_password
    git_base_path = /var/gerrit/git
    csv_output = repo_creation_dates.csv
    logs_path = /tmp/logs
    discarded_urls_output = /tmp
```

* gerrit_url: Gerrit REST API URL
* gerrit_user: Gerrit user (must have access to list projects)
* gerrit_password: HTTP password or API token
* git_base_path: Path on disk where the repositories are stored (bare format, .git)
* csv_output: Output file name (default is repo_creation_dates.csv)
* logs_path: Directory containing the HTTP log files
* discarded_urls_output: Directory to write the `discarded_urls.txt` file containing URLs not matched

## Run the script

Once the virtual environment is activated and config.ini is created, run the script:

```bash
python extract_repo_creation_dates.py
```

Output will be saved to the CSV file specified in config.ini.

# Cleanup

To deactivate the virtual environment:

```bash
deactivate
```
