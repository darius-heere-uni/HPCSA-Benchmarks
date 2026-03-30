#!/usr/bin/env bash
set -euo pipefail

: "${RUN_ID:?Set RUN_ID}"
: "${BENCH_IMAGE:?Set BENCH_IMAGE}"

NODE="${NODE:-hpc-node1}"
SHARE_ROOT="${SHARE_ROOT:-/nfs/bench/faas_tiny_relay_v1}"
WORKLOAD_SCRIPT="${WORKLOAD_SCRIPT:-/nfs/ksi/pub-2025-ksi/example-workloads/faas-tiny-relay-v1/workload-job-faas-tiny-single-batch-ksi.sh}"
STACK="${STACK:-ksi}"
WORKERS="${WORKERS:-1}"
TASK_ID="${TASK_ID:-0}"
PRINT_RESULT_JSON="${PRINT_RESULT_JSON:-0}"
IMAGE_PULL_POLICY="${IMAGE_PULL_POLICY:-IfNotPresent}"
TOTAL_INVOCATIONS="${TOTAL_INVOCATIONS:-}"
TIMEOUT_PER_JOB="${TIMEOUT_PER_JOB:-5m}"
KEEP_NAMESPACE="${KEEP_NAMESPACE:-0}"
DELETE_JOBS_AFTER_EACH="${DELETE_JOBS_AFTER_EACH:-1}"

srun -N1 -w "$NODE" --chdir="$SHARE_ROOT" /bin/bash -lc '
source ~/bin/ksi-env.sh
pwd
export RUN_ID='"$RUN_ID"'
export BENCH_IMAGE='"$BENCH_IMAGE"'
export SHARE_ROOT='"$SHARE_ROOT"'
export TASK_ID='"$TASK_ID"'
export STACK='"$STACK"'
export WORKERS='"$WORKERS"'
export PRINT_RESULT_JSON='"$PRINT_RESULT_JSON"'
export IMAGE_PULL_POLICY='"$IMAGE_PULL_POLICY"'
export TOTAL_INVOCATIONS='"$TOTAL_INVOCATIONS"'
export TIMEOUT_PER_JOB='"$TIMEOUT_PER_JOB"'
export KEEP_NAMESPACE='"$KEEP_NAMESPACE"'
export DELETE_JOBS_AFTER_EACH='"$DELETE_JOBS_AFTER_EACH"'
export STARTUP_T0_WALL="$(python3 - <<'"'"'PY'"'"'
import time
print(repr(time.time()))
PY
)"
/bin/bash /nfs/ksi/pub-2025-ksi/run-workload-no-liqo.sh '"$WORKLOAD_SCRIPT"' "$SHARE_ROOT"
'

