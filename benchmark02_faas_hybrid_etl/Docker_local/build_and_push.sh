#!/usr/bin/env bash
set -euo pipefail

IMAGE_REPO="${IMAGE_REPO:?Set IMAGE_REPO, e.g. docker.io/dariusheereuni/faas-hybrid-etl-single-invocation}"
IMAGE_TAG="${IMAGE_TAG:-v1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

docker build -t "${IMAGE_REPO}:${IMAGE_TAG}" -f Dockerfile .
docker push "${IMAGE_REPO}:${IMAGE_TAG}"

echo "Pushed ${IMAGE_REPO}:${IMAGE_TAG}"
