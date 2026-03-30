#!/bin/bash
set -euo pipefail

K8S_CONTEXT="${K8S_CONTEXT:-$(kubectl config current-context 2>/dev/null || true)}"
RUN_ID="${RUN_ID:?set RUN_ID}"
BENCH_IMAGE="${BENCH_IMAGE:?set BENCH_IMAGE}"
BENCH_ROOT="${BENCH_ROOT:-/nfs/bench/faas_tiny_relay_v1}"
TOTAL_INVOCATIONS="${TOTAL_INVOCATIONS:?set TOTAL_INVOCATIONS}"
PARTITION_ID="${PARTITION_ID:?set PARTITION_ID}"
WORKERS="${WORKERS:?set WORKERS}"
NODE_NAME="${NODE_NAME:?set NODE_NAME}"
NS="${NS:-faas-b1-k8s}"
STACK="${STACK:-k8s}"
IMAGE_PULL_POLICY="${IMAGE_PULL_POLICY:-IfNotPresent}"
JOB_TIMEOUT="${JOB_TIMEOUT:-10m}"
KEEP_NAMESPACE="${KEEP_NAMESPACE:-0}"

PART_DIR="${BENCH_ROOT}/results/${STACK}/${RUN_ID}/part_$(printf '%02d' "${PARTITION_ID}")"
JOB_LAT_FILE="${PART_DIR}/job_latency.jsonl"
SUMMARY_FILE="${PART_DIR}/part_summary.json"

mkdir -p "${PART_DIR}"
: > "${JOB_LAT_FILE}"

kubectl --context "${K8S_CONTEXT}" get namespace "${NS}" >/dev/null 2>&1 || \
  kubectl --context "${K8S_CONTEXT}" create namespace "${NS}" >/dev/null 2>&1 || true

# Clean up stale jobs from the same run/partition, if any.
kubectl --context "${K8S_CONTEXT}" -n "${NS}" delete job \
  -l "bench=faas-tiny-relay,run_id=${RUN_ID},partition_id=${PARTITION_ID}" \
  --ignore-not-found=true >/dev/null 2>&1 || true

assigned=0
ok=0
failed=0
warm_first_submit_wall=""
warm_last_done_wall=""

for ((i=0; i<${TOTAL_INVOCATIONS}; i++)); do
  if (( i % WORKERS != PARTITION_ID )); then
    continue
  fi

  assigned=$((assigned + 1))

  inv_file=$(printf "inv_%06d.json" "${i}")
  job_name=$(printf "faas-b1-%s-w%d-i%d" "${RUN_ID}" "${PARTITION_ID}" "${i}" | tr '[:upper:]' '[:lower:]')

  t_submit=$(python3 - <<'PY'
import time
print(repr(time.time()))
PY
)

  if [[ -z "${warm_first_submit_wall}" ]]; then
    warm_first_submit_wall="${t_submit}"
  fi

  kubectl --context "${K8S_CONTEXT}" -n "${NS}" create -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job_name}
  labels:
    bench: faas-tiny-relay
    run_id: "${RUN_ID}"
    partition_id: "${PARTITION_ID}"
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        bench: faas-tiny-relay
        run_id: "${RUN_ID}"
        partition_id: "${PARTITION_ID}"
    spec:
      restartPolicy: Never
      nodeName: ${NODE_NAME}
      containers:
      - name: task
        image: ${BENCH_IMAGE}
        imagePullPolicy: ${IMAGE_PULL_POLICY}
        command: ["python", "/app/faas_tiny_single_invocation.py"]
        env:
        - name: RUN_ID
          value: "${RUN_ID}"
        - name: STACK
          value: "${STACK}"
        - name: BENCH_ROOT
          value: "${BENCH_ROOT}"
        - name: INVOCATION_ID
          value: "${i}"
        - name: PARTITION_ID
          value: "${PARTITION_ID}"
        - name: INPUT_FILE
          value: "${BENCH_ROOT}/input/${inv_file}"
        volumeMounts:
        - name: nfs
          mountPath: /nfs
      volumes:
      - name: nfs
        hostPath:
          path: /nfs
          type: Directory
EOF

  status="ok"
  if ! kubectl --context "${K8S_CONTEXT}" -n "${NS}" wait \
      --for=condition=complete \
      --timeout="${JOB_TIMEOUT}" \
      "job/${job_name}" >/dev/null; then
    status="failed"
  fi

  t_done=$(python3 - <<'PY'
import time
print(repr(time.time()))
PY
)

  warm_last_done_wall="${t_done}"

  job_wall_ms=$(python3 - <<PY
a=float("${t_submit}")
b=float("${t_done}")
print(repr((b-a)*1000.0))
PY
)

  if [[ "${status}" == "ok" ]]; then
    ok=$((ok + 1))
  else
    failed=$((failed + 1))
  fi

  cat >> "${JOB_LAT_FILE}" <<EOF
{"benchmark":"faas_tiny_relay_v1","run_id":"${RUN_ID}","stack":"${STACK}","partition_id":${PARTITION_ID},"invocation_id":${i},"job_name":"${job_name}","node_name":"${NODE_NAME}","t_submit_wall":${t_submit},"t_done_wall":${t_done},"job_wall_ms":${job_wall_ms},"status":"${status}"}
EOF

  if [[ "${KEEP_NAMESPACE}" != "1" ]]; then
    kubectl --context "${K8S_CONTEXT}" -n "${NS}" delete job "${job_name}" \
      --ignore-not-found=true >/dev/null 2>&1 || true
  fi
done

if [[ -n "${warm_first_submit_wall}" && -n "${warm_last_done_wall}" ]]; then
  warm_makespan_ms=$(python3 - <<PY
a=float("${warm_first_submit_wall}")
b=float("${warm_last_done_wall}")
print(repr((b-a)*1000.0))
PY
)
else
  warm_makespan_ms="null"
  warm_first_submit_wall="null"
  warm_last_done_wall="null"
fi

cat > "${SUMMARY_FILE}" <<EOF
{
  "benchmark": "faas_tiny_relay_v1",
  "run_id": "${RUN_ID}",
  "stack": "${STACK}",
  "partition_id": ${PARTITION_ID},
  "workers": ${WORKERS},
  "node_name": "${NODE_NAME}",
  "assigned_invocations": ${assigned},
  "ok": ${ok},
  "failed": ${failed},
  "assigned_results": ${ok},
  "job_latency_file": "${JOB_LAT_FILE}",
  "startup_t0_wall": null,
  "startup_t1_wall": null,
  "startup_setup_ms": null,
  "warm_first_submit_wall": ${warm_first_submit_wall},
  "warm_last_done_wall": ${warm_last_done_wall},
  "warm_makespan_ms": ${warm_makespan_ms}
}
EOF

if [[ "${KEEP_NAMESPACE}" == "1" ]]; then
  echo "KEEP_NAMESPACE=1 -> leaving jobs in namespace ${NS} for debugging"
fi
