#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import socket
import sys
import time
import zlib
from collections import Counter
from pathlib import Path


TOKEN_RE = re.compile(r"[a-z0-9]+")


def getenv_required(name: str) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def value_bucket(value: int, thresholds: dict) -> str:
    if value < thresholds["value_low_max"]:
        return "low"
    if value < thresholds["value_mid_max"]:
        return "mid"
    return "high"


def score_bucket(score: float, thresholds: dict) -> str:
    if score < thresholds["score_s0_max"]:
        return "s0"
    if score < thresholds["score_s1_max"]:
        return "s1"
    if score < thresholds["score_s2_max"]:
        return "s2"
    return "s3"


def keep_record(flag: bool, value: int) -> bool:
    return flag or value >= 500


def payload_features(payload: str, stopword_set: set[str], keyword_tags: dict[str, str]):
    normalized = payload.lower()
    tokens = TOKEN_RE.findall(normalized)
    kept = [t for t in tokens if t not in stopword_set]

    token_count = len(kept)
    unique_token_count = len(set(kept))
    char_count = len(normalized)

    checksum = 1
    tag_counts: Counter[str] = Counter()

    if kept:
        joined = " ".join(kept).encode("utf-8")
        checksum = zlib.adler32(joined) & 0xFFFFFFFF
        for tok in kept:
            tag = keyword_tags.get(tok)
            if tag is not None:
                tag_counts[tag] += 1

    bigram_count = max(0, token_count - 1)
    return token_count, unique_token_count, char_count, checksum, tag_counts, bigram_count


def main() -> int:
    run_id = getenv_required("RUN_ID")
    shard_id = int(getenv_required("SHARD_ID"))
    bench_root = os.getenv("BENCH_ROOT", "/shared")
    stack = os.getenv("STACK", "unknown")
    partition_id = int(os.getenv("PARTITION_ID", os.getenv("TASK_ID", "0")))
    benchmark = os.getenv("BENCHMARK_NAME", "faas_hybrid_etl_v1")
    dataset_version = os.getenv("DATASET_VERSION", "v1")

    bench_root_path = Path(bench_root)
    shard_path = bench_root_path / "input" / "shards" / f"shard_{shard_id:06d}.jsonl"
    lookups_root = bench_root_path / "input" / "lookups"

    if not shard_path.is_file():
        raise SystemExit(f"Shard file not found: {shard_path}")
    if not lookups_root.is_dir():
        raise SystemExit(f"Lookups directory not found: {lookups_root}")

    category_normalization = load_json(lookups_root / "category_normalization.json")
    category_groups = load_json(lookups_root / "category_groups.json")
    region_normalization = load_json(lookups_root / "region_normalization.json")
    region_groups = load_json(lookups_root / "region_groups.json")
    region_weights = load_json(lookups_root / "region_weights.json")
    stopwords = set(load_json(lookups_root / "stopwords.json"))
    keyword_tags = load_json(lookups_root / "keyword_tags.json")
    bucket_thresholds = load_json(lookups_root / "bucket_thresholds.json")

    result_dir = bench_root_path / "results" / stack / run_id / f"part_{partition_id:02d}"
    result_dir.mkdir(parents=True, exist_ok=True)
    result_path = result_dir / f"out_{shard_id:06d}.json"

    host = socket.gethostname()
    pid = os.getpid()

    t0_wall = time.time()
    t0_perf = time.perf_counter()

    input_bytes = shard_path.stat().st_size
    input_record_count = 0
    kept_record_count = 0

    sum_value = 0
    sum_weighted_value = 0.0
    score_min = None
    score_max = None
    score_sum = 0.0

    total_token_count = 0
    total_unique_token_count = 0
    total_bigram_count = 0
    total_char_count = 0
    payload_checksum_xor = 0

    category_counts: Counter[str] = Counter()
    category_group_counts: Counter[str] = Counter()
    region_counts: Counter[str] = Counter()
    region_group_counts: Counter[str] = Counter()
    value_bucket_counts: Counter[str] = Counter()
    score_bucket_counts: Counter[str] = Counter()
    tag_counts: Counter[str] = Counter()
    category_region_counts: Counter[str] = Counter()

    with shard_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            rec = json.loads(line)

            input_record_count += 1

            score = float(rec["score"])
            score_sum += score
            score_min = score if score_min is None else min(score_min, score)
            score_max = score if score_max is None else max(score_max, score)

            value = int(rec["value"])
            flag = bool(rec["flag"])

            category_raw = str(rec["category"])
            region_raw = str(rec["region"])
            category = category_normalization.get(category_raw, category_raw.lower())
            region = region_normalization.get(region_raw, region_raw.lower().replace("_", "-"))
            category_group = category_groups[category]
            region_group = region_groups[region]
            region_weight = float(region_weights[region])

            payload = str(rec["payload"])
            token_count, unique_token_count, char_count, checksum, this_tag_counts, bigram_count = payload_features(
                payload, stopwords, keyword_tags
            )

            weighted_value = (
                value * region_weight * (1.0 + score)
                + 0.010 * token_count
                + 0.005 * unique_token_count
                + 0.002 * bigram_count
            )

            if keep_record(flag, value):
                kept_record_count += 1
                sum_value += value
                sum_weighted_value += weighted_value
                total_token_count += token_count
                total_unique_token_count += unique_token_count
                total_bigram_count += bigram_count
                total_char_count += char_count
                payload_checksum_xor ^= checksum

                category_counts[category] += 1
                category_group_counts[category_group] += 1
                region_counts[region] += 1
                region_group_counts[region_group] += 1
                value_bucket_counts[value_bucket(value, bucket_thresholds)] += 1
                score_bucket_counts[score_bucket(score, bucket_thresholds)] += 1
                tag_counts.update(this_tag_counts)
                category_region_counts[f"{category}|{region}"] += 1

    t1_perf = time.perf_counter()
    t1_wall = time.time()
    duration_ms = (t1_perf - t0_perf) * 1000.0

    result = {
        "benchmark": benchmark,
        "dataset_version": dataset_version,
        "run_id": run_id,
        "stack": stack,
        "shard_id": shard_id,
        "input_file": str(shard_path),
        "input_bytes": input_bytes,
        "partition_id": partition_id,
        "host": host,
        "pid": pid,
        "image_version": os.getenv("IMAGE_VERSION", "faas-hybrid-etl-single-invocation-v1"),
        "status": "ok",
        "input_record_count": input_record_count,
        "kept_record_count": kept_record_count,
        "sum_value": sum_value,
        "sum_weighted_value": round(sum_weighted_value, 6),
        "score_min": score_min,
        "score_max": score_max,
        "score_mean": round(score_sum / input_record_count, 6) if input_record_count else None,
        "total_token_count": total_token_count,
        "total_unique_token_count": total_unique_token_count,
        "total_bigram_count": total_bigram_count,
        "total_char_count": total_char_count,
        "payload_checksum_xor": payload_checksum_xor,
        "category_counts": dict(sorted(category_counts.items())),
        "category_group_counts": dict(sorted(category_group_counts.items())),
        "region_counts": dict(sorted(region_counts.items())),
        "region_group_counts": dict(sorted(region_group_counts.items())),
        "value_bucket_counts": dict(sorted(value_bucket_counts.items())),
        "score_bucket_counts": dict(sorted(score_bucket_counts.items())),
        "tag_counts": dict(sorted(tag_counts.items())),
        "category_region_counts": dict(sorted(category_region_counts.items())),
        "t_start_wall": t0_wall,
        "t_end_wall": t1_wall,
        "duration_ms": duration_ms,
        "output_file": str(result_path),
    }

    with result_path.open("w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, sort_keys=True)
        f.write("\n")

    if os.getenv("LOG_RESULT_STDOUT", "0") == "1":
        print(json.dumps(result, sort_keys=True))

    return 0


if __name__ == "__main__":
    sys.exit(main())
