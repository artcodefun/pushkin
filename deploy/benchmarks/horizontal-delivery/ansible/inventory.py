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


def public_host(name: str, addresses: dict, **variables: str) -> dict:
    return {
        name: {
            "ansible_host": addresses["public_ip"],
            "private_ip": addresses["private_ip"],
            **variables,
        }
    }


def private_host(name: str, addresses: dict, bastion_ip: str, **variables: str) -> dict:
    return {
        name: {
            "ansible_host": addresses["private_ip"],
            "private_ip": addresses["private_ip"],
            "ansible_ssh_common_args": (
                f"-o ProxyJump=ubuntu@{bastion_ip} "
                "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
            ),
            **variables,
        }
    }


def inventory() -> dict:
    values = output()
    bastion = values["bastion"]["value"]
    bastion_ip = bastion["public_ip"]
    kafka = values["kafka"]["value"]
    postgres = values["postgres"]["value"]
    cassandra = values["cassandra"]["value"]
    fake_fcm = values["fake_fcm"]["value"]
    pushkin = values["pushkin_instances"]["value"]
    workers = {}
    for index, addresses in enumerate(pushkin, start=1):
        if index == 1:
            workers.update(public_host(f"pushkin-{index}", bastion, instance_id=f"pushkin-benchmark-{index}"))
        else:
            workers.update(
                private_host(
                    f"pushkin-{index}", addresses, bastion_ip, instance_id=f"pushkin-benchmark-{index}"
                )
            )

    return {
        "_meta": {
            "hostvars": {
                **private_host("kafka-node", kafka, bastion_ip),
                **private_host("postgres-node", postgres, bastion_ip),
                **private_host("cassandra-node", cassandra, bastion_ip),
                **private_host("fake-fcm", fake_fcm, bastion_ip),
                **workers,
            }
        },
        "all": {
            "vars": {
                "ansible_user": "ubuntu",
                "ansible_python_interpreter": "/usr/bin/python3",
                "ansible_ssh_common_args": "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null",
            }
        },
        "kafka": {"hosts": ["kafka-node"]},
        "postgres": {"hosts": ["postgres-node"]},
        "cassandra": {"hosts": ["cassandra-node"]},
        "pushkin": {"hosts": list(workers)},
        "fake_fcm": {"hosts": ["fake-fcm"]},
    }


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == "--list":
        print(json.dumps(inventory()))
    else:
        print(json.dumps({}))
