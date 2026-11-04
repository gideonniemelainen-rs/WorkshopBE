#!/usr/bin/env bash
# Wanted a script to re-sync kube configs and re-apply manifests. Pass --wipe to also delete the namespace and persistent storage for a full reset
set -euo pipefail

NS=wikistream
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
V4_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

WIPE=false
case "${1:-}" in
  "") ;;
  --wipe) WIPE=true ;;
  *)
    echo "usage: $0 [--wipe]" >&2
    exit 1
    ;;
esac

if [[ "$(kubectl config current-context)" != "minikube" ]]; then
  echo "kubectl context is '$(kubectl config current-context)', expected 'minikube'" >&2
  exit 1
fi

cd "$V4_DIR"

if $WIPE; then
    echo "This deletes namespace $NS, including Redpanda, Scylla, Prometheus, and Grafana data."
    read -r -p "Type 'wipe' to continue: " answer
    if [[ "$answer" != "wipe" ]]; then
        echo "aborted"
        exit 1
    fi
    kubectl delete namespace "$NS" --ignore-not-found --wait=true

    # minikube's hostpath provisioner keys volume directories by namespace/pvc name, so a recreated PVC can pick up
    # the previous run's data if the directory was not reclaimed. Redpanda crash-loops on recovering a large stale volume.
    if [[ -n "$(minikube ssh -- "sudo ls -A /tmp/hostpath-provisioner/$NS 2>/dev/null" | tr -d '\r')" ]]; then
        echo "removing leftover volume data from the minikube node"
        minikube ssh -- "sudo sh -c 'rm -rf /tmp/hostpath-provisioner/$NS/*'"
    fi
fi

# Initial config
kubectl apply -f k8s/00-namespace.yaml
bash "$SCRIPT_DIR/sync-config.sh"

kubectl apply -f k8s/10-config.yaml -f k8s/20-redpanda.yaml -f k8s/30-scylla.yaml -f k8s/40-prometheus.yaml -f k8s/45-redpanda-console.yaml -f k8s/50-grafana.yaml
kubectl -n "$NS" rollout status statefulset/redpanda --timeout=5m
kubectl -n "$NS" rollout status statefulset/scylla --timeout=10m

# Delete and re-apply jobs
kubectl -n "$NS" delete job redpanda-init-topic redpanda-init-schema scylla-init --ignore-not-found
kubectl apply -f k8s/21-redpanda-init.yaml -f k8s/22-scylla-init.yaml
kubectl -n "$NS" wait --for=condition=complete job/redpanda-init-topic job/redpanda-init-schema job/scylla-init --timeout=10m

kubectl apply -f k8s/60-producer.yaml -f k8s/70-consumer.yaml

# Restart with updated config values. After a wipe everything is created fresh, so only wait for the rollout.
if ! $WIPE; then
    for deploy in prometheus grafana producer consumer; do
        kubectl -n "$NS" rollout restart deploy/"$deploy"
    done
fi
for deploy in prometheus redpanda-console grafana producer consumer; do
    kubectl -n "$NS" rollout status deploy/"$deploy" --timeout=5m
done

echo "reset complete for namespace $NS"
