#!/usr/bin/env bash
set -euo pipefail

: "${RUN_ID:?Set RUN_ID}"
: "${BENCH_IMAGE:?Set BENCH_IMAGE}"

NODELIST="${NODELIST:-hpc-node1,hpc-node2}"
SHARE_ROOT="${SHARE_ROOT:-/nfs/bench/faas_hybrid_etl_v1}"
WORKLOAD_SCRIPT="${WORKLOAD_SCRIPT:-/nfs/ksi/pub-2025-ksi/example-workloads/faas-hybrid-etl-v1/workload-job-faas-hybrid-etl-batch-ksi.sh}"
STACK="${STACK:-ksi}"
WORKERS="${WORKERS:-2}"
IMAGE_PULL_POLICY="${IMAGE_PULL_POLICY:-IfNotPresent}"
TOTAL_SHARDS="${TOTAL_SHARDS:-}"
TIMEOUT_PER_JOB="${TIMEOUT_PER_JOB:-20m}"
KEEP_NAMESPACE="${KEEP_NAMESPACE:-0}"
DELETE_JOBS_AFTER_EACH="${DELETE_JOBS_AFTER_EACH:-1}"

srun -N2 -w "$NODELIST" --ntasks=2 --ntasks-per-node=1 --chdir="$SHARE_ROOT" /bin/bash -lc '
source ~/bin/ksi-env.sh
pwd
export RUN_ID='"$RUN_ID"'
export BENCH_IMAGE='"$BENCH_IMAGE"'
export SHARE_ROOT='"$SHARE_ROOT"'
export TASK_ID="${SLURM_PROCID:-0}"
export STACK='"$STACK"'
export WORKERS='"$WORKERS"'
export IMAGE_PULL_POLICY='"$IMAGE_PULL_POLICY"'
export TOTAL_SHARDS='"$TOTAL_SHARDS"'
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
