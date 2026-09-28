#!/usr/bin/env bash

set -euo pipefail

usage() {
	cat >&2 <<'EOF'
usage: topic-lag.sh --bastion <public-ip> --host <private-ip> --compose-file <path> --output <path>
EOF
}

host=""
bastion=""
compose_file=""
output=""
while (($# > 0)); do
	case "$1" in
		--host)
			host="$2"
			shift 2
			;;
		--bastion)
			bastion="$2"
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

if [[ -z "$bastion" || -z "$host" || -z "$compose_file" || -z "$output" ]]; then
	usage
	exit 2
fi

mkdir -p "$(dirname "$output")"

describe_groups() {
	ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o "ProxyJump=ubuntu@$bastion" "ubuntu@$host" \
		"sudo docker compose --file $compose_file exec --no-TTY kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --all-groups --describe --timeout 5000"
}

if ! groups="$(describe_groups 2>&1)"; then
	jq -n --arg error "$groups" '{error: $error}' >"$output"
	exit 0
fi

topic_lag() {
	local topic="$1"
	local group="$2"
	awk -v topic="$topic" -v group="$group" '
		$1 == group && $2 == topic && $6 ~ /^[0-9]+$/ {
			lag = $6 + 0
			total += lag
			if (lag > maximum) maximum = lag
			partitions++
		}
		END {
			printf "{\"consumer_group\":\"%s\",\"total_lag\":%d,\"maximum_partition_lag\":%d,\"partition_count\":%d}", group, total, maximum, partitions
		}
	' <<<"$groups"
}

progress="$(topic_lag pushkin.campaign.progress pushkin.campaign-progress)"
notifications="$(topic_lag pushkin.notification.accepted pushkin.notification-projection)"
jq -n --argjson progress "$progress" --argjson notifications "$notifications" '
	{
	  "pushkin.campaign.progress": $progress,
	  "pushkin.notification.accepted": $notifications
	}
' >"$output"
