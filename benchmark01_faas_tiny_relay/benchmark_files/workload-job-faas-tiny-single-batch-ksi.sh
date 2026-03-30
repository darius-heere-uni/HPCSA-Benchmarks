#!/usr/bin/env bash
set -euo pipefail

: "${K8S_CLUSTER_NAME:?K8S_CLUSTER_NAME not set}"
: "${RUN_ID:?RUN_ID not set}"
: "${BENCH_IMAGE:?BENCH_IMAGE not set}"
: "${SHARE_ROOT:?SHARE_ROOT not set}"
: "${STARTUP_T0_WALL:?STARTUP_T0_WALL not set}"

HOST_SHARED_ROOT="${SHARE_ROOT}"
STACK="${STACK:-ksi}"
TASK_ID="${TASK_ID:-0}"
WORKERS="${WORKERS:-1}"
IMAGE_PULL_POLICY="${IMAGE_PULL_POLICY:-IfNotPresent}"
KEEP_NAMESPACE="${KEEP_NAMESPACE:-0}"
PRINT_RESULT_JSON="${PRINT_RESULT_JSON:-0}"
TOTAL_INVOCATIONS="${TOTAL_INVOCATIONS:-}"
TIMEOUT_PER_JOB="${TIMEOUT_PER_JOB:-5m}"
DELETE_JOBS_AFTER_EACH="${DELETE_JOBS_AFTER_EACH:-1}"

BENCH_ROOT="/shared"
RESULT_DIR="${HOST_SHARED_ROOT}/results/${STACK}/${RUN_ID}/part_$(printf '%02d' "$TASK_ID")"
JOBLAT_FILE="${RESULT_DIR}/job_latency.jsonl"
SUMMARY_FILE="${RESULT_DIR}/part_summary.json"
mkdir -p "$RESULT_DIR"

STARTUP_T1_WALL="$(python3 - <<'PY'
import time
print(repr(time.time()))
PY
)"

sanitize_dns() {
  tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9-]+/-/g; s/^-+//; s/-+$//; s/-+/-/g'
}

NS_BASE="faas-b1-${RUN_ID}-w${TASK_ID}"
NS="$(printf '%s' "$NS_BASE" | sanitize_dns | cut -c1-63)"

kubectl create --context "$K8S_CLUSTER_NAME" namespace "$NS" >/dev/null 2>&1 || true

cleanup() {
  if [[ "$KEEP_NAMESPACE" == "1" ]]; then
    echo "KEEP_NAMESPACE=1 -> leaving namespace $NS for debugging" >&2
    return 0
  fi
  kubectl delete --context "$K8S_CLUSTER_NAME" namespace "$NS" --ignore-not-found=true >/dev/null 2>&1 || true
}
trap cleanup EXIT

if [[ -z "$TOTAL_INVOCATIONS" ]]; then
  TOTAL_INVOCATIONS="$(find "${HOST_SHARED_ROOT}/input" -maxdepth 1 -type f -name 'inv_*.json' | wc -l | tr -d ' ')"
fi

if [[ "$TOTAL_INVOCATIONS" -le 0 ]]; then
  echo "TOTAL_INVOCATIONS resolved to ${TOTAL_INVOCATIONS}; nothing to do" >&2
  exit 1
fi

warm_first_submit_wall=""
warm_last_done_wall=""
assigned_count=0
ok_count=0
failed_count=0

for (( inv_id=0; inv_id<TOTAL_INVOCATIONS; inv_id++ )); do
  if (( inv_id % WORKERS != TASK_ID )); then
    continue
  fi

  assigned_count=$((assigned_count + 1))

  job_base="faas-b1-${RUN_ID}-w${TASK_ID}-i${inv_id}"
  job_name="$(printf '%s' "$job_base" | sanitize_dns | cut -c1-63)"
  out_file="${RESULT_DIR}/out_$(printf '%06d' "$inv_id").json"

  t_submit="$(python3 - <<'PY'
import time
print(repr(time.time()))
PY
)"

  if [[ -z "$warm_first_submit_wall" ]]; then
    warm_first_submit_wall="$t_submit"
  fi

  kubectl create --context "$K8S_CLUSTER_NAME" -n "$NS" -f - <<EOFYAML
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job_name}
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      securityContext:
        runAsUser: 0
      containers:
      - name: task
        image: ${BENCH_IMAGE}
        imagePullPolicy: ${IMAGE_PULL_POLICY}
        env:
        - name: RUN_ID
          value: "${RUN_ID}"
        - name: STACK
          value: "${STACK}"
        - name: BENCH_ROOT
          value: "${BENCH_ROOT}"
        - name: TASK_ID
          value: "${TASK_ID}"
        - name: PARTITION_ID
          value: "${TASK_ID}"
        - name: INVOCATION_ID
          value: "${inv_id}"
        - name: PRINT_RESULT_JSON
          value: "${PRINT_RESULT_JSON}"
        volumeMounts:
        - name: appshare
          mountPath: /shared
      volumes:
      - name: appshare
        hostPath:
          path: /app
          type: Directory
EOFYAML

  if ! kubectl wait --context "$K8S_CLUSTER_NAME" -n "$NS" \
    --for=condition=complete --timeout="$TIMEOUT_PER_JOB" "job/${job_name}" >/dev/null; then
    t_done="$(python3 - <<'PY'
import time
print(repr(time.time()))
PY
)"
    warm_last_done_wall="$t_done"
    failed_count=$((failed_count + 1))
    python3 - <<PY >> "$JOBLAT_FILE"
import json
rec = {
  "benchmark": "faas_tiny_relay_v1",
  "run_id": ${RUN_ID@Q},
  "stack": ${STACK@Q},
  "partition_id": int(${TASK_ID@Q}),
  "invocation_id": int(${inv_id@Q}),
  "job_name": ${job_name@Q},
  "status": "error",
  "submit_wall": float(${t_submit@Q}),
  "done_wall": float(${t_done@Q}),
  "job_wall_ms": (float(${t_done@Q}) - float(${t_submit@Q})) * 1000.0,
}
print(json.dumps(rec, separators=(",", ":")))
PY
    echo "Job ${job_name} failed; dumping diagnostics" >&2
    kubectl get --context "$K8S_CLUSTER_NAME" -n "$NS" job "$job_name" -o wide || true
    kubectl get --context "$K8S_CLUSTER_NAME" -n "$NS" pods -l "job-name=${job_name}" -o wide || true
    kubectl describe --context "$K8S_CLUSTER_NAME" -n "$NS" job "$job_name" || true
    kubectl describe --context "$K8S_CLUSTER_NAME" -n "$NS" pods -l "job-name=${job_name}" || true
    kubectl logs --context "$K8S_CLUSTER_NAME" -n "$NS" "job/${job_name}" --all-containers=true || true
    break
  fi

  t_done="$(python3 - <<'PY'
import time
print(repr(time.time()))
PY
)"
  warm_last_done_wall="$t_done"
  ok_count=$((ok_count + 1))

  python3 - <<PY >> "$JOBLAT_FILE"
import json, os
rec = {
  "benchmark": "faas_tiny_relay_v1",
  "run_id": ${RUN_ID@Q},
  "stack": ${STACK@Q},
  "partition_id": int(${TASK_ID@Q}),
  "invocation_id": int(${inv_id@Q}),
  "job_name": ${job_name@Q},
  "status": "ok",
  "submit_wall": float(${t_submit@Q}),
  "done_wall": float(${t_done@Q}),
  "job_wall_ms": (float(${t_done@Q}) - float(${t_submit@Q})) * 1000.0,
  "result_file_exists": os.path.exists(${out_file@Q}),
}
print(json.dumps(rec, separators=(",", ":")))
PY

  if [[ "$DELETE_JOBS_AFTER_EACH" == "1" ]]; then
    kubectl delete --context "$K8S_CLUSTER_NAME" -n "$NS" job "$job_name" --ignore-not-found=true >/dev/null 2>&1 || true
  fi
done

python3 - <<PY > "$SUMMARY_FILE"
import glob, json
startup_t0 = float(${STARTUP_T0_WALL@Q})
startup_t1 = float(${STARTUP_T1_WALL@Q})
warm_first = ${warm_first_submit_wall@Q}
warm_last = ${warm_last_done_wall@Q}
files = sorted(glob.glob(${RESULT_DIR@Q} + '/out_*.json'))
summary = {
  "benchmark": "faas_tiny_relay_v1",
  "run_id": ${RUN_ID@Q},
  "stack": ${STACK@Q},
  "partition_id": int(${TASK_ID@Q}),
  "workers": int(${WORKERS@Q}),
  "assigned_invocations": int(${assigned_count@Q}),
  "ok": int(${ok_count@Q}),
  "failed": int(${failed_count@Q}),
  "assigned_results": len(files),
  "job_latency_file": ${JOBLAT_FILE@Q},
  "startup_t0_wall": startup_t0,
  "startup_t1_wall": startup_t1,
  "startup_setup_ms": (startup_t1 - startup_t0) * 1000.0,
  "warm_first_submit_wall": None if warm_first == "" else float(warm_first),
  "warm_last_done_wall": None if warm_last == "" else float(warm_last),
  "warm_makespan_ms": None if warm_first == "" or warm_last == "" else (float(warm_last) - float(warm_first)) * 1000.0,
}
print(json.dumps(summary, separators=(",", ":")))
PY

