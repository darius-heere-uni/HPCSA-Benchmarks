Benchmark 2 hybrid ETL worker image.

One container run = one invocation = one main shard plus shared lookup/config files.

Required env vars:
- RUN_ID
- SHARD_ID

Common env vars:
- BENCH_ROOT   (default: /shared)
- STACK        (e.g. k8s or ksi)
- PARTITION_ID (or TASK_ID fallback)
- IMAGE_VERSION

Expected dataset layout:
- BENCH_ROOT/input/shards/shard_<SHARD_ID>.jsonl
- BENCH_ROOT/input/lookups/*.json

Behavior:
- reads one main shard
- loads shared lookups/config files
- normalizes and enriches category/region
- tokenizes payload and computes text features
- derives weighted_value
- aggregates counts/sums/min/max/means and grouped counters
- writes one result JSON to:
  BENCH_ROOT/results/<stack>/<run_id>/part_<partition_id>/out_<shard_id>.json
