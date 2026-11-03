#!/usr/bin/env bash
# Creates wikistream-secrets from .env and the file-based ConfigMaps. Run after the namespace exists.
set -euo pipefail

NS=wikistream
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
V4_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
ENV_FILE="$V4_DIR/.env"

env_value() {
  local line
  line="$(grep -E "^$1=" "$ENV_FILE" || true)"
  line="$(printf '%s\n' "$line" | tail -n 1 | tr -d '\r')"
  printf '%s\n' "${line#*=}"
}

require() {
  if [[ -z "$(env_value "$1")" ]]; then
    echo "$1 is missing or empty in .env" >&2
    exit 1
  fi
}

apply_configmap() {
  local name="$1"
  shift
  kubectl create configmap "$name" -n "$NS" "$@" --dry-run=client -o yaml | kubectl apply -f -
}

require JWT_SECRET
require DEFAULT_PASS

# Relative paths keep MSYS from rewriting /c/... arguments for the Windows kubectl binary.
cd "$V4_DIR"

grafana_admin_password="$(env_value GRAFANA_ADMIN_PASSWORD)"
grafana_admin_password="${grafana_admin_password:-admin}"

kubectl create secret generic wikistream-secrets -n "$NS" \
  --from-literal=JWT_SECRET="$(env_value JWT_SECRET)" \
  --from-literal=DEFAULT_PASS="$(env_value DEFAULT_PASS)" \
  --from-literal=SCYLLA_USERNAME="$(env_value SCYLLA_USERNAME)" \
  --from-literal=SCYLLA_PASSWORD="$(env_value SCYLLA_PASSWORD)" \
  --from-literal=GRAFANA_ADMIN_PASSWORD="$grafana_admin_password" \
  --dry-run=client -o yaml | kubectl apply -f -

apply_configmap redpanda-schema \
  --from-file=recentchange.proto=proto/recentchange/v1/recentchange.proto

apply_configmap prometheus-config \
  --from-file=prometheus.yml=observability/prometheus/prometheus.yml

apply_configmap grafana-datasources \
  --from-file=prometheus.yaml=observability/grafana/provisioning/datasources/prometheus.yaml

apply_configmap grafana-dashboard-provider \
  --from-file=prometheus.yaml=observability/grafana/provisioning/dashboards/prometheus.yaml

apply_configmap grafana-dashboards \
  --from-file=observability/grafana/dashboards/wikistream-pipeline.json

echo "secrets and file-based configmaps applied to namespace $NS"
