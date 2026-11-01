#!/usr/bin/env bash

set -euo pipefail

usage() {
	cat >&2 <<'EOF'
usage: wait-for-kafka-drain.sh --bastion <public-ip> --host <private-ip> --compose-file <path> --output <path> [--timeout-seconds <seconds>] [--max-final-lag <records>]
EOF
}

bastion=""
host=""
compose_file=""
output=""
timeout_seconds=300
max_final_lag=4
while (($# > 0)); do
	case "$1" in
		--bastion)
			bastion="$2"
			shift 2
			;;
		--host)
			host="$2"
			shift 2
			;;
		--compose-file)
			compose_file="$2"
			shift 2
			;;
		--output)
			output="$2"
			shift 2
			;;
		--timeout-seconds)
			timeout_seconds="$2"
			shift 2
			;;
		--max-final-lag)
			max_final_lag="$2"
			shift 2
			;;
		--help|-h)
			usage
			exit 0
			;;
		*)
			usage
			exit 2
			;;
	esac
done

if [[ -z "$bastion" || -z "$host" || -z "$compose_file" || -z "$output" ]] || ! [[ "$timeout_seconds" =~ ^[1-9][0-9]*$ ]] || ! [[ "$max_final_lag" =~ ^[0-9]+$ ]]; then
	usage
	exit 2
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
deadline=$(( $(date +%s) + timeout_seconds ))

echo "==> Waiting for Kafka consumer groups to settle"
while :; do
	bash "$script_dir/topic-lag.sh" \
		--bastion "$bastion" \
		--host "$host" \
		--compose-file "$compose_file" \
		--output "$output"
	if jq -e 'has("error")' "$output" >/dev/null; then
		cat "$output" >&2
		exit 1
	fi
	progress_lag="$(jq -r '."pushkin.campaign.progress".total_lag' "$output")"
	notification_lag="$(jq -r '."pushkin.notification.accepted".total_lag' "$output")"
	if (( progress_lag <= max_final_lag && notification_lag <= max_final_lag )); then
		break
	fi
	if (( $(date +%s) >= deadline )); then
		echo "Kafka consumer groups did not settle within ${timeout_seconds} seconds" >&2
		exit 1
	fi
	printf 'campaign-progress lag=%s notification-projection lag=%s\n' "$progress_lag" "$notification_lag"
	sleep 5
done
