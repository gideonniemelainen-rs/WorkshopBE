#!/usr/bin/env bash
# Builds the producer image and loads it into minikube. The consumer runs the pinned Redpanda Connect image.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
V4_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$V4_DIR"

svc=producer
docker build --build-arg SERVICE="$svc" -t "wikistream-$svc:dev" .
minikube image load "wikistream-$svc:dev"
