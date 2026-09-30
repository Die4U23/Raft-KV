#!/bin/sh
# One Raft-KV process. Compose service names node0, node1 and node2 are the
# peer DNS names. Each container keeps its own /data volume.
# The server address parser accepts numeric IPv4 only, so names are resolved here.
set -eu
if [ -z "${NODE_ID:-}" ]; then
    echo "NODE_ID is required" >&2
    exit 1
fi

# Compose starts node0 first, and node0's script resolves all three peers before
# the server launches. node1 and node2 may not exist on the network yet, so keep
# retrying for a generous window (60s) instead of giving up after 5s.
resolve_v4() {
    host=$1
    attempt=0
    while [ "$attempt" -lt 600 ]; do
        ip=$(getent ahostsv4 "$host" 2>/dev/null | awk 'NR==1 { print $1; exit }')
        if [ -n "$ip" ]; then
            printf '%s\n' "$ip"
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 0.1
    done
    echo "cannot resolve peer host ${host}" >&2
    return 1
}

peers_in=${PEERS:-0:node0:9080,1:node1:9080,2:node2:9080}
peers_out=
old_ifs=$IFS
IFS=','
for item in $peers_in; do
    id=${item%%:*}
    rest=${item#*:}
    host=${rest%:*}
    port=${rest##*:}
    ip=$(resolve_v4 "$host")
    peers_out="${peers_out:+${peers_out},}${id}:${ip}:${port}"
done
IFS=$old_ifs

mkdir -p /data/kv /data/raft-log
exec /usr/local/bin/raft_kv_server \
    --node_id="${NODE_ID}" \
    --client_port=8080 \
    --raft_port=9080 \
    --metrics_port=9090 \
    --db_path=/data/kv \
    --raft_log_path=/data/raft-log \
    --peers="${peers_out}" \
    --linearizable_reads=true
