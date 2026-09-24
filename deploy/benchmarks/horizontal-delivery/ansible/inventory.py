#!/usr/bin/env python3
"""Expose this benchmark's Terraform outputs as an Ansible inventory."""

import json
import subprocess
import sys
from pathlib import Path


def output() -> dict:
    terraform_dir = Path(__file__).resolve().parents[1] / "terraform"
    completed = subprocess.run(
        ["terraform", f"-chdir={terraform_dir}", "output", "-json"],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


def host(name: str, addresses: dict, **variables: str) -> dict:
    return {name: {"ansible_host": addresses["public_ip"], "private_ip": addresses["private_ip"], **variables}}


def inventory() -> dict:
    values = output()
    infrastructure = values["infrastructure"]["value"]
    fake_fcm = values["fake_fcm"]["value"]
    pushkin = values["pushkin_instances"]["value"]
    workers = {}
    for index, addresses in enumerate(pushkin, start=1):
        workers.update(host(f"pushkin-{index}", addresses, instance_id=f"pushkin-benchmark-{index}"))

    return {
        "_meta": {"hostvars": {**host("infrastructure", infrastructure), **host("fake-fcm", fake_fcm), **workers}},
        "all": {"vars": {"ansible_user": "ubuntu"}},
        "infrastructure": {"hosts": ["infrastructure"]},
        "pushkin": {"hosts": list(workers)},
        "fake_fcm": {"hosts": ["fake-fcm"]},
    }


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == "--list":
        print(json.dumps(inventory()))
    else:
        print(json.dumps({}))
