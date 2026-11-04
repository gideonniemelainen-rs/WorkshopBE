#!/usr/bin/env bash
# Starts minikube if needed, loads the producer image if missing, applies everything, then forwards ports.
# Arguments are passed to reset-cluster.sh, so ./start-cluster.sh --wipe starts from empty storage.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! docker info >/dev/null 2>&1; then
  echo "Docker is not running, start Docker Desktop first" >&2
  exit 1
fi

if ! minikube status >/dev/null 2>&1; then
  minikube start
fi

# The producer image only exists in minikube's cache, so rebuild it if the cluster was recreated.
if [[ "$(minikube image ls)" != *wikistream-producer:dev* ]]; then
  bash "$SCRIPT_DIR/build-images.sh"
fi

bash "$SCRIPT_DIR/reset-cluster.sh" "$@"
bash "$SCRIPT_DIR/forward-ports.sh"
