#!/usr/bin/env bash

set -euo pipefail

target=1000000
while :; do
  stats="$(ssh -o StrictHostKeyChecking=accept-new "ubuntu@$FAKE_FCM_IP" \
    "curl --fail --silent http://$FAKE_FCM_PRIVATE_IP:8081/stats")"
  completed="$(jq -r '.completed_count' <<<"$stats")"
  active="$(jq -r '.active_count' <<<"$stats")"
  printf 'completed=%s/%s active=%s\n' "$completed" "$target" "$active"
  if (( completed >= target )); then
    printf '%s\n' "$stats" >"$BENCHMARK_RESULTS_DIR/fake-fcm.json"
    exit 0
  fi
  sleep 5
done
