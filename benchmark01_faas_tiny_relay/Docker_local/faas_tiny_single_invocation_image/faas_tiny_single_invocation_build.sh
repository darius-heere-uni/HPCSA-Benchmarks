#!/usr/bin/env bash
set -euo pipefail

IMAGE_REF="${IMAGE_REF:-docker.io/<yourrepo>/faas-tiny-single-invocation:v1}"
BUILD_DIR="${BUILD_DIR:-./faas_tiny_single_invocation_image}"

mkdir -p "$BUILD_DIR"
cp faas_tiny_single_invocation.Dockerfile "$BUILD_DIR/Dockerfile"
cp faas_tiny_single_invocation.py "$BUILD_DIR/faas_tiny_single_invocation.py"
cp faas_tiny_single_invocation_version.txt "$BUILD_DIR/version.txt"

(
  cd "$BUILD_DIR"
  docker build -t "$IMAGE_REF" .
  docker push "$IMAGE_REF"
)
