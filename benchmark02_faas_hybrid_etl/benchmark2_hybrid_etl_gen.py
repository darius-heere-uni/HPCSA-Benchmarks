#!/usr/bin/env python3
"""
Generate revised Benchmark 2 dataset:
ETL-style micro-batch FaaS benchmark with
- one main shard per invocation
- shared lookup/config files for enrichment/transforms

Layout:
<root>/
  input/
    shards/
      shard_000000.jsonl
      shard_000001.jsonl
      ...
    lookups/
      category_normalization.json
      category_groups.json
      region_normalization.json
      region_groups.json
      region_weights.json
      stopwords.json
      keyword_tags.json
      bucket_thresholds.json
  manifest/
    dataset.json
    lookups.json
    shards.jsonl
  results/
    k8s/
    ksi/

Suggested usage:
python3 benchmark2_hybrid_etl_gen.py \
  --root /nfs/bench/faas_hybrid_etl_v1 \
  --shards 64 \
  --target-bytes 67108864 \
  --seed 12345

This generates:
- 64 shard files
- about 64 MiB per shard
- 8 shared lookup files
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
import shutil
import string
from collections import Counter
from pathlib import Path


TOKEN_RE = re.compile(r"[a-z0-9]+")


CATEGORY_VARIANTS = {
    "alpha": ["Alpha", "ALPHA", "alpha"],
    "beta": ["Beta", "BETA", "beta"],
    "gamma": ["Gamma", "GAMMA", "gamma"],
    "delta": ["Delta", "DELTA", "delta"],
    "epsilon": ["Epsilon", "EPSILON", "epsilon"],
    "zeta": ["Zeta", "ZETA", "zeta"],
}

CATEGORY_GROUPS = {
    "alpha": "g1",
    "beta": "g1",
    "gamma": "g2",
    "delta": "g2",
    "epsilon": "g3",
    "zeta": "g3",
}

REGION_VARIANTS = {
    "eu-central": ["eu-central", "EU-CENTRAL", "eu_central"],
    "eu-west": ["eu-west", "EU-WEST", "eu_west"],
    "us-east": ["us-east", "US-EAST", "us_east"],
    "us-west": ["us-west", "US-WEST", "us_west"],
    "ap-south": ["ap-south", "AP-SOUTH", "ap_south"],
    "ap-northeast": ["ap-northeast", "AP-NORTHEAST", "ap_northeast"],
}

REGION_GROUPS = {
    "eu-central": "eu",
    "eu-west": "eu",
    "us-east": "us",
    "us-west": "us",
    "ap-south": "ap",
    "ap-northeast": "ap",
}

REGION_WEIGHTS = {
    "eu-central": 1.05,
    "eu-west": 1.02,
    "us-east": 1.08,
    "us-west": 1.04,
    "ap-south": 0.98,
    "ap-northeast": 1.01,
}

STOPWORDS = [
    "the", "a", "an", "and", "or", "but", "with", "to", "for", "from",
    "in", "on", "at", "by", "of", "is", "are", "was", "were", "be",
    "this", "that", "these", "those", "it", "as", "into", "over", "under", "via",
]

KEYWORD_TAGS = {
    "error": "ops",
    "latency": "perf",
    "throughput": "perf",
    "network": "infra",
    "storage": "infra",
    "cache": "infra",
    "billing": "biz",
    "payment": "biz",
    "order": "biz",
    "customer": "biz",
    "fraud": "risk",
    "alert": "ops",
    "retry": "ops",
    "timeout": "ops",
    "gpu": "hpc",
    "slurm": "hpc",
    "kubernetes": "platform",
    "serverless": "platform",
    "function": "platform",
    "queue": "platform",
    "index": "data",
    "schema": "data",
    "token": "nlp",
    "embedding": "nlp",
}

DOMAIN_WORDS = list(KEYWORD_TAGS.keys()) + [
    "pipeline", "dataset", "record", "transform", "aggregate", "window",
    "session", "event", "request", "response", "feature", "vector",
    "bucket", "region", "category", "worker", "controller", "container",
    "runtime", "cluster", "node", "checksum", "summary", "checkpoint",
    "lookup", "join", "normalization", "ranking", "signal", "forecast",
    "profile", "debug", "metric", "payload", "batch", "shard",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate hybrid ETL shard dataset for Benchmark 2")
    p.add_argument("--root", required=True, help="Benchmark root directory")
    p.add_argument("--shards", type=int, default=64, help="Number of shard files")
    p.add_argument(
        "--target-bytes",
        type=int,
        default=64 * 1024 * 1024,
        help="Approximate target bytes per shard file",
    )
    p.add_argument("--seed", type=int, default=12345, help="Deterministic seed")
    p.add_argument("--force", action="store_true", help="Overwrite existing root")
    return p.parse_args()


def ensure_empty_dir(path: Path, force: bool) -> None:
    if path.exists():
        if not force:
            raise SystemExit(f"Target already exists: {path}\nUse --force to overwrite.")
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def normalize_category(category: str) -> str:
    lower = category.lower()
    for canonical, variants in CATEGORY_VARIANTS.items():
        if category in variants or lower == canonical:
            return canonical
    return lower


def normalize_region(region: str) -> str:
    for canonical, variants in REGION_VARIANTS.items():
        if region in variants:
            return canonical
    return region.lower().replace("_", "-")


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


def make_payload(rng: random.Random) -> str:
    # Build a semi-realistic ETL payload with a mix of keywords, domain words, and stopwords.
    n_tokens = rng.randint(20, 55)
    tokens = []
    for _ in range(n_tokens):
        roll = rng.random()
        if roll < 0.18:
            tok = rng.choice(STOPWORDS)
        elif roll < 0.78:
            tok = rng.choice(DOMAIN_WORDS)
        else:
            tok = "".join(rng.choice(string.ascii_lowercase) for _ in range(rng.randint(4, 10)))
        tokens.append(tok)

    # Add a little punctuation / structure so tokenization still matters.
    text = []
    for i, tok in enumerate(tokens):
        text.append(tok)
        if i < len(tokens) - 1:
            if rng.random() < 0.08:
                text.append(",")
            elif rng.random() < 0.04:
                text.append(".")
    return " ".join(text)


def payload_features(payload: str, stopword_set: set[str]) -> tuple[int, int, int, int, Counter]:
    normalized = payload.lower()
    tokens = TOKEN_RE.findall(normalized)
    kept = [t for t in tokens if t not in stopword_set]
    token_count = len(kept)
    unique_token_count = len(set(kept))
    char_count = len(normalized)

    # Deterministic checksum-like feature
    checksum = 0
    for tok in kept:
        checksum ^= (zlib_adler(tok) & 0xFFFFFFFF)

    tag_counts: Counter = Counter()
    for tok in kept:
        tag = KEYWORD_TAGS.get(tok)
        if tag is not None:
            tag_counts[tag] += 1

    return token_count, unique_token_count, char_count, checksum, tag_counts


def zlib_adler(text: str) -> int:
    import zlib
    return zlib.adler32(text.encode("utf-8"))


def build_record(rng: random.Random, shard_id: int, record_id: int) -> dict:
    canonical_category = rng.choice(list(CATEGORY_VARIANTS.keys()))
    category = rng.choice(CATEGORY_VARIANTS[canonical_category])

    canonical_region = rng.choice(list(REGION_VARIANTS.keys()))
    region = rng.choice(REGION_VARIANTS[canonical_region])

    value = rng.randint(1, 1000)
    score = round(rng.random(), 6)
    flag = rng.random() < 0.35
    payload = make_payload(rng)

    return {
        "record_id": record_id,
        "shard_id": shard_id,
        "category": category,
        "region": region,
        "value": value,
        "score": score,
        "flag": flag,
        "payload": payload,
    }


def write_lookup(path: Path, obj) -> str:
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True)
        f.write("\n")
    return sha256_file(path)


def main() -> None:
    args = parse_args()

    root = Path(args.root)
    input_root = root / "input"
    shards_root = input_root / "shards"
    lookups_root = input_root / "lookups"
    manifest_root = root / "manifest"
    results_root = root / "results"

    ensure_empty_dir(root, args.force)
    shards_root.mkdir(parents=True, exist_ok=True)
    lookups_root.mkdir(parents=True, exist_ok=True)
    manifest_root.mkdir(parents=True, exist_ok=True)
    (results_root / "k8s").mkdir(parents=True, exist_ok=True)
    (results_root / "ksi").mkdir(parents=True, exist_ok=True)

    thresholds = {
        "value_low_max": 250,
        "value_mid_max": 750,
        "score_s0_max": 0.25,
        "score_s1_max": 0.50,
        "score_s2_max": 0.75,
    }

    category_normalization = {}
    for canonical, variants in CATEGORY_VARIANTS.items():
        for v in variants:
            category_normalization[v] = canonical

    region_normalization = {}
    for canonical, variants in REGION_VARIANTS.items():
        for v in variants:
            region_normalization[v] = canonical

    lookup_rows = []
    lookup_rows.append({
        "file_name": "category_normalization.json",
        "sha256": write_lookup(lookups_root / "category_normalization.json", category_normalization),
    })
    lookup_rows.append({
        "file_name": "category_groups.json",
        "sha256": write_lookup(lookups_root / "category_groups.json", CATEGORY_GROUPS),
    })
    lookup_rows.append({
        "file_name": "region_normalization.json",
        "sha256": write_lookup(lookups_root / "region_normalization.json", region_normalization),
    })
    lookup_rows.append({
        "file_name": "region_groups.json",
        "sha256": write_lookup(lookups_root / "region_groups.json", REGION_GROUPS),
    })
    lookup_rows.append({
        "file_name": "region_weights.json",
        "sha256": write_lookup(lookups_root / "region_weights.json", REGION_WEIGHTS),
    })
    lookup_rows.append({
        "file_name": "stopwords.json",
        "sha256": write_lookup(lookups_root / "stopwords.json", sorted(STOPWORDS)),
    })
    lookup_rows.append({
        "file_name": "keyword_tags.json",
        "sha256": write_lookup(lookups_root / "keyword_tags.json", KEYWORD_TAGS),
    })
    lookup_rows.append({
        "file_name": "bucket_thresholds.json",
        "sha256": write_lookup(lookups_root / "bucket_thresholds.json", thresholds),
    })

    with (manifest_root / "lookups.json").open("w", encoding="utf-8") as f:
        json.dump(lookup_rows, f, indent=2, sort_keys=True)
        f.write("\n")

    rng = random.Random(args.seed)
    stopword_set = set(STOPWORDS)

    total_input_bytes = 0
    total_records = 0

    shards_manifest_path = manifest_root / "shards.jsonl"
    with shards_manifest_path.open("w", encoding="utf-8") as shards_manifest:
        for shard_id in range(args.shards):
            shard_path = shards_root / f"shard_{shard_id:06d}.jsonl"

            current_bytes = 0
            record_id = 0

            input_record_count = 0
            kept_record_count = 0
            sum_value = 0
            sum_weighted_value = 0.0
            score_min = None
            score_max = None
            score_sum = 0.0
            total_token_count = 0
            total_unique_token_count = 0
            total_char_count = 0
            payload_checksum_xor = 0

            category_counts: Counter = Counter()
            category_group_counts: Counter = Counter()
            region_counts: Counter = Counter()
            region_group_counts: Counter = Counter()
            value_bucket_counts: Counter = Counter()
            score_bucket_counts: Counter = Counter()
            tag_counts: Counter = Counter()

            with shard_path.open("w", encoding="utf-8") as f:
                while current_bytes < args.target_bytes:
                    rec = build_record(rng, shard_id, record_id)
                    line = json.dumps(rec, separators=(",", ":"), sort_keys=True) + "\n"
                    f.write(line)
                    current_bytes += len(line.encode("utf-8"))

                    input_record_count += 1

                    score = float(rec["score"])
                    score_sum += score
                    score_min = score if score_min is None else min(score_min, score)
                    score_max = score if score_max is None else max(score_max, score)

                    value = int(rec["value"])
                    flag = bool(rec["flag"])

                    category = normalize_category(str(rec["category"]))
                    region = normalize_region(str(rec["region"]))
                    category_group = CATEGORY_GROUPS[category]
                    region_group = REGION_GROUPS[region]
                    region_weight = float(REGION_WEIGHTS[region])

                    token_count, unique_token_count, char_count, checksum, this_tag_counts = payload_features(
                        str(rec["payload"]), stopword_set
                    )

                    # Heavier ETL-like derived metric.
                    weighted_value = value * region_weight * (1.0 + score) \
                        + 0.01 * token_count + 0.005 * unique_token_count

                    if keep_record(flag, value):
                        kept_record_count += 1
                        sum_value += value
                        sum_weighted_value += weighted_value
                        total_token_count += token_count
                        total_unique_token_count += unique_token_count
                        total_char_count += char_count
                        payload_checksum_xor ^= checksum

                        category_counts[category] += 1
                        category_group_counts[category_group] += 1
                        region_counts[region] += 1
                        region_group_counts[region_group] += 1
                        value_bucket_counts[value_bucket(value, thresholds)] += 1
                        score_bucket_counts[score_bucket(score, thresholds)] += 1
                        tag_counts.update(this_tag_counts)

                    record_id += 1

            shard_bytes = shard_path.stat().st_size
            total_input_bytes += shard_bytes
            total_records += input_record_count
            shard_sha = sha256_file(shard_path)

            shard_meta = {
                "shard_id": shard_id,
                "file_name": shard_path.name,
                "input_bytes": shard_bytes,
                "input_record_count": input_record_count,
                "kept_record_count": kept_record_count,
                "sum_value": sum_value,
                "sum_weighted_value": round(sum_weighted_value, 6),
                "score_min": score_min,
                "score_max": score_max,
                "score_mean": round(score_sum / input_record_count, 6) if input_record_count else None,
                "total_token_count": total_token_count,
                "total_unique_token_count": total_unique_token_count,
                "total_char_count": total_char_count,
                "payload_checksum_xor": payload_checksum_xor,
                "category_counts": dict(sorted(category_counts.items())),
                "category_group_counts": dict(sorted(category_group_counts.items())),
                "region_counts": dict(sorted(region_counts.items())),
                "region_group_counts": dict(sorted(region_group_counts.items())),
                "value_bucket_counts": dict(sorted(value_bucket_counts.items())),
                "score_bucket_counts": dict(sorted(score_bucket_counts.items())),
                "tag_counts": dict(sorted(tag_counts.items())),
                "sha256": shard_sha,
            }

            shards_manifest.write(
                json.dumps(shard_meta, separators=(",", ":"), sort_keys=True) + "\n"
            )

    dataset_meta = {
        "benchmark": "faas_hybrid_etl_v1",
        "dataset_version": "v1",
        "seed": args.seed,
        "shards": args.shards,
        "target_bytes_per_shard": args.target_bytes,
        "invocation_model": "one invocation processes exactly one main shard plus shared lookup/config files",
        "lookup_files": len(lookup_rows),
        "partition_rule": "shard_id % 2 -> partition 0/1 for two-way benchmark split",
        "total_input_bytes": total_input_bytes,
        "total_record_count": total_records,
        "results_dirs": ["results/k8s", "results/ksi"],
    }

    with (manifest_root / "dataset.json").open("w", encoding="utf-8") as f:
        json.dump(dataset_meta, f, indent=2, sort_keys=True)
        f.write("\n")

    print(f"Generated dataset under: {root}")
    print(f"Shards: {args.shards}")
    print(f"Approx target bytes per shard: {args.target_bytes}")
    print(f"Lookup files: {len(lookup_rows)}")
    print(f"Total input bytes: {total_input_bytes}")
    print(f"Total records: {total_records}")


if __name__ == "__main__":
    main()
