#!/usr/bin/env bash

# The collector is generic benchmark infrastructure. Keep this wrapper inside
# the single-node benchmark so its runner does not need to know its location.
benchmark_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
exec bash "$benchmark_dir/../horizontal-delivery/scripts/monitor.sh" "$@"
