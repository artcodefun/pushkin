#!/usr/bin/env bash

benchmark_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
exec bash "$benchmark_dir/../horizontal-delivery/scripts/topic-lag.sh" "$@"
