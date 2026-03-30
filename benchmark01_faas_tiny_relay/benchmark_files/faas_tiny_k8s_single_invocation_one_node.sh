#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

K8S_CONTEXT="${K8S_CONTEXT:-$(kubectl config current-context 2>/dev/null || true)}"
RUN_ID="${RUN_ID:?set RUN_ID}"
BENCH_IMAGE="${BENCH_IMAGE:?set BENCH_IMAGE}"
TOTAL_INVOCATIONS="${TOTAL_INVOCATIONS:?set TOTAL_INVOCATIONS}"

BENCH_ROOT="${BENCH_ROOT:-/nfs/bench/faas_tiny_relay_v1}"
NS="${NS:-faas-b1-k8s}"
STACK="k8s"
WORKERS=1
PARTITION_ID=0
NODE_0="${NODE_0:-k8s-w1}"
KEEP_NAMESPACE="${KEEP_NAMESPACE:-0}"

mkdir -p "${BENCH_ROOT}/results/${STACK}/${RUN_ID}"

export K8S_CONTEXT RUN_ID BENCH_IMAGE TOTAL_INVOCATIONS BENCH_ROOT NS STACK WORKERS PARTITION_ID KEEP_NAMESPACE
export NODE_NAME="${NODE_0}"

bash "${SCRIPT_DIR}/workload-job-faas-tiny-single-batch-k8s.sh"
