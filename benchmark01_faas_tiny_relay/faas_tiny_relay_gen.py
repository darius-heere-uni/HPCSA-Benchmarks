#!/usr/bin/env python3
"""Generate deterministic tiny JSON inputs for Benchmark 1 (faas_tiny_relay_v1).

Default layout:
  /nfs/bench/faas_tiny_relay_v1/
    input/
    manifest/
    results/k8s/
    results/slurm/

Each input file is one logical invocation:
  input/inv_000000.json
  input/inv_000001.json
  ...

The payload is deterministic given (seed, invocation_id), so the dataset is reproducible.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
from typing import Iterable

BENCHMARK = "faas_tiny_relay_v1"
DATASET_VERSION = "v1"
DEFAULT_ROOT = "/nfs/bench/faas_tiny_relay_v1"


def sha256_stream(seed: int, invocation_id: int) -> Iterable[bytes]:
    ctr = 0
    while True:
        blob = f"{seed}:{invocation_id}:{ctr}".encode("utf-8")
        yield hashlib.sha256(blob).digest()
        ctr += 1


def deterministic_hex_payload(seed: int, invocation_id: int, payload_chars: int) -> str:
    if payload_chars <= 0:
        raise ValueError("payload_chars must be > 0")

    need_bytes = (payload_chars + 1) // 2
    out = bytearray()
    for chunk in sha256_stream(seed, invocation_id):
        out.extend(chunk)
        if len(out) >= need_bytes:
            break
    return bytes(out[:need_bytes]).hex()[:payload_chars]


def write_json(path: Path, obj: dict) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
        f.write("\n")
    os.replace(tmp, path)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--root", default=DEFAULT_ROOT, help="Benchmark root directory on shared NFS")
    ap.add_argument("--files", type=int, default=2048, help="Number of input files to generate")
    ap.add_argument(
        "--payload-chars",
        type=int,
        default=3584,
        help="ASCII payload length stored in the JSON field 'payload'",
    )
    ap.add_argument("--seed", type=int, default=12345, help="Deterministic dataset seed")
    ap.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing input/manifest directory under --root",
    )
    args = ap.parse_args()

    if args.files <= 0:
        raise SystemExit("--files must be > 0")

    root = Path(args.root)
    input_dir = root / "input"
    manifest_dir = root / "manifest"
    results_dir = root / "results"

    if (input_dir.exists() or manifest_dir.exists()) and not args.force:
        raise SystemExit(
            f"Refusing to overwrite existing dataset under {root}. "
            f"Use --force if you want to regenerate it."
        )

    input_dir.mkdir(parents=True, exist_ok=True)
    manifest_dir.mkdir(parents=True, exist_ok=True)
    (results_dir / "k8s").mkdir(parents=True, exist_ok=True)
    (results_dir / "slurm").mkdir(parents=True, exist_ok=True)

    file_manifest_path = manifest_dir / "files.jsonl"
    if file_manifest_path.exists():
        file_manifest_path.unlink()

    total_input_bytes = 0
    created_utc = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()

    with file_manifest_path.open("w", encoding="utf-8") as mf:
        for invocation_id in range(args.files):
            payload = deterministic_hex_payload(args.seed, invocation_id, args.payload_chars)
            record = {
                "benchmark": BENCHMARK,
                "dataset_version": DATASET_VERSION,
                "invocation_id": invocation_id,
                "payload": payload,
                "payload_chars": len(payload),
                "generator_seed": args.seed,
            }
            out_path = input_dir / f"inv_{invocation_id:06d}.json"
            write_json(out_path, record)
            input_bytes = out_path.stat().st_size
            total_input_bytes += input_bytes

            mf.write(
                json.dumps(
                    {
                        "invocation_id": invocation_id,
                        "file": out_path.name,
                        "input_bytes": input_bytes,
                        "partition": invocation_id % 2,
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                + "\n"
            )

    dataset_manifest = {
        "benchmark": BENCHMARK,
        "dataset_version": DATASET_VERSION,
        "created_utc": created_utc,
        "root": str(root),
        "input_dir": str(input_dir),
        "manifest_dir": str(manifest_dir),
        "results_dir": str(results_dir),
        "file_count": args.files,
        "generator_seed": args.seed,
        "payload_chars": args.payload_chars,
        "partition_rule": "worker 0 processes even invocation_id; worker 1 processes odd invocation_id",
        "input_naming": "inv_000000.json",
        "file_manifest": "manifest/files.jsonl",
        "estimated_total_input_bytes": total_input_bytes,
        "mean_input_bytes": total_input_bytes / args.files,
    }
    write_json(manifest_dir / "dataset.json", dataset_manifest)

    print("Dataset created successfully")
    print(f"  root:               {root}")
    print(f"  input files:        {args.files}")
    print(f"  payload chars:      {args.payload_chars}")
    print(f"  generator seed:     {args.seed}")
    print(f"  avg input size:     {dataset_manifest['mean_input_bytes']:.1f} bytes")
    print(f"  file manifest:      {file_manifest_path}")
    print(f"  dataset manifest:   {manifest_dir / 'dataset.json'}")


if __name__ == "__main__":
    main()

