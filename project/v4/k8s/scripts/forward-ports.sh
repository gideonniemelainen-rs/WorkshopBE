#!/usr/bin/env bash
# Forwards the consumer (7000), producer (9091), Grafana (3000), Prometheus (9090) and Redpanda Console (8080) to localhost. Ctrl+C stops them all.
set -euo pipefail

NS=wikistream

if [[ "$(kubectl config current-context)" != "minikube" ]]; then
  echo "kubectl context is '$(kubectl config current-context)', expected 'minikube'" >&2
  exit 1
fi

kubectl -n "$NS" wait --for=condition=available deploy/consumer deploy/producer deploy/grafana deploy/prometheus deploy/redpanda-console --timeout=2m

# Retries on drop: a port-forward to a service pins one pod and dies when that pod is replaced by a rollout.
forward() {
  local svc="$1" port="$2"
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "port $port is already in use, cannot forward $svc" >&2
    exit 1
  fi
  while true; do
    kubectl -n "$NS" port-forward "svc/$svc" "$port:$port" || true
    sleep 1
  done
}

# Kill the forwarding loops and their kubectl children when the script exits.
trap 'trap - EXIT INT TERM; kill 0' EXIT INT TERM

forward consumer 7000 &
forward producer 9091 &
forward grafana 3000 &
forward prometheus 9090 &
forward redpanda-console 8080 &

echo "consumer http://localhost:7000  producer http://localhost:9091  grafana http://localhost:3000  prometheus http://localhost:9090  console http://localhost:8080"
wait
