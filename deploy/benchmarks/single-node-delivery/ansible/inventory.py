#!/usr/bin/env python3
"""Expose the single-node benchmark Terraform outputs to Ansible."""

import json
import subprocess
import sys
from pathlib import Path


def main() -> None:
    terraform_dir = Path(__file__).resolve().parents[1] / "terraform"
    completed = subprocess.run(
        ["terraform", f"-chdir={terraform_dir}", "output", "-json"],
        check=True,
        capture_output=True,
        text=True,
    )
    values = json.loads(completed.stdout)
    pushkin = values["pushkin"]["value"]
    fake_fcm = values["fake_fcm"]["value"]
    print(json.dumps({
        "_meta": {"hostvars": {
            "pushkin": {"ansible_host": pushkin["public_ip"], "private_ip": pushkin["private_ip"]},
            "fake-fcm": {"ansible_host": fake_fcm["public_ip"], "private_ip": fake_fcm["private_ip"]},
        }},
        "all": {"vars": {"ansible_user": "ubuntu"}},
        "pushkin": {"hosts": ["pushkin"]},
        "fake_fcm": {"hosts": ["fake-fcm"]},
    }))


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == "--list":
        main()
    else:
        print(json.dumps({}))
