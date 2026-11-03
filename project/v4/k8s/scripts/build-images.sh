#!/usr/bin/env bash
# Builds the producer and consumer images and loads them into minikube.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
V4_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$V4_DIR"

for svc in producer consumer; do
  docker build --build-arg SERVICE="$svc" -t "wikistream-$svc:dev" .
  minikube image load "wikistream-$svc:dev"
done
