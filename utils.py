from copy import deepcopy
from settings import BASE_DIR
import yaml


def update_workflow():
    workflow = {
        "name": "Download InsideAirbnb Data",
        "on": {
            "workflow_dispatch": {},
            "schedule": [
                {
                    "cron": "0 0 * * 1"
                }
            ]
        },
        "jobs": {}
    }
    job_template = {
        "runs-on": "ubuntu-latest",
        "env": {
            "GIT_LFS_SKIP_SMUDGE": 1
        },
        "steps": [
            {
                "uses": "actions/checkout@v2"
            },
            {
                "name": "Set up Python 3.9",
                "uses": "actions/setup-python@v2",
                "with": {
                    "python-version": "3.9"
                }
            },
            {
                "uses": "actions/cache@v2",
                "with": {
                    "path": "~/.cache/pip",
                    "key": "pip-${{ hashFiles('requirements.txt') }}",
                    "restore-keys": "pip-"
                }
            },
            {
                "name": "Install git lfs",
                "run": "sudo apt-get install git-lfs"
            },
            {
                "name": "Install dependencies",
                "run": "pip install scrapy"
            },
            {
                "name": "Clone existing data repo",
                "run": "git clone https://github.com/bz-dev/inside-airbnb-data.git"
            },
            {
                "name": "Download data",
                "run": ""
            },
            {
                "name": "Get date",
                "run": "echo \"DATE_UPDATED=$(date --rfc-3339=date)\" >> ${GITHUB_ENV}"
            },
            {
                "name": "Commit",
                "run": "cd inside-airbnb-data\ngit lfs track \"*.csv\" \"*.csv.gz\" \"*.geojson\"\ngit config --global user.email 3310824+bz-dev@users.noreply.github.com\ngit config --global user.name \"Bo Zhao\"\ngit add .\ngit commit -m \"Updated on ${DATE_UPDATED}\" || echo \"No changes to commit\""
            },
            {
                "name": "Push changes",
                "uses": "ad-m/github-push-action@master",
                "with": {
                    "directory": "inside-airbnb-data",
                    "github_token": "${{ secrets.PERSONAL_ACCESS_TOKEN }}",
                    "repository": "bz-dev/inside-airbnb-data",
                    "branch": "main"
                }
            }
        ]
    }
    dir_data = BASE_DIR.joinpath("data")
    file_workflow = BASE_DIR.joinpath(".github", "workflows", "manual.yml")
    if file_workflow.exists():
        cities = []
        for city_json in dir_data.glob("*.json"):
            job = deepcopy(job_template)
            if cities:
                job["needs"] = f"download-{cities[-1]}"
            job["steps"][6]["run"] = f"python download.py {city_json.stem}"
            workflow["jobs"][f"download-{city_json.stem}"] = job
            cities.append(city_json.stem)
        with open(file_workflow, "w") as f:
            yaml.dump(workflow, f)
