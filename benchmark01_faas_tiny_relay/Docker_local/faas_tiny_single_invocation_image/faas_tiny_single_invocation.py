#!/usr/bin/env python3
import hashlib
import json
import os
import socket
import sys
import time
from pathlib import Path
from typing import Optional

IMAGE_VERSION_PATH = Path('/app/version.txt')


def getenv_required(name: str) -> str:
    value = os.getenv(name)
    if value is None or value == '':
        raise RuntimeError(f"Required environment variable missing: {name}")
    return value


def env_int(name: str, default: Optional[int] = None) -> Optional[int]:
    value = os.getenv(name)
    if value is None or value == '':
        return default
    return int(value)


def image_version() -> str:
    try:
        return IMAGE_VERSION_PATH.read_text(encoding='utf-8').strip()
    except Exception:
        return 'unknown'


def derive_partition_id() -> Optional[int]:
    for name in ('PARTITION_ID', 'TASK_ID', 'JOB_COMPLETION_INDEX'):
        value = os.getenv(name)
        if value not in (None, ''):
            try:
                return int(value)
            except ValueError:
                return None
    return None


def resolve_input_path(bench_root: Path, invocation_id: int) -> Path:
    explicit = os.getenv('INPUT_FILE', '').strip()
    if explicit:
        p = Path(explicit)
        if not p.is_absolute():
            p = bench_root / explicit
        return p
    return bench_root / 'input' / f'inv_{invocation_id:06d}.json'


def atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    with tmp.open('w', encoding='utf-8') as f:
        json.dump(payload, f, separators=(',', ':'), sort_keys=True)
        f.write('\n')
    tmp.replace(path)


def main() -> int:
    bench_root = Path(os.getenv('BENCH_ROOT', '/shared')).resolve()
    run_id = getenv_required('RUN_ID')
    stack = os.getenv('STACK', 'unknown')
    invocation_id = env_int('INVOCATION_ID')
    if invocation_id is None:
        raise RuntimeError('Required environment variable missing: INVOCATION_ID')

    partition_id = derive_partition_id()
    part_dir_name = f'part_{partition_id:02d}' if partition_id is not None else 'part_unknown'
    out_dir = bench_root / 'results' / stack / run_id / part_dir_name
    input_path = resolve_input_path(bench_root, invocation_id)
    result_path = out_dir / f'out_{invocation_id:06d}.json'

    t_start_wall = time.time()
    t_start_mono = time.monotonic_ns()

    try:
        raw = input_path.read_bytes()
        doc = json.loads(raw)
        payload = doc['payload']
        digest = hashlib.sha256(payload.encode('utf-8')).hexdigest()
        t_end_mono = time.monotonic_ns()
        t_end_wall = time.time()
        result = {
            'benchmark': doc.get('benchmark', 'faas_tiny_relay_v1'),
            'dataset_version': doc.get('dataset_version', 'unknown'),
            'run_id': run_id,
            'stack': stack,
            'image_version': image_version(),
            'partition_id': partition_id,
            'host': socket.gethostname(),
            'pid': os.getpid(),
            'invocation_id': invocation_id,
            'input_file': str(input_path),
            'input_bytes': len(raw),
            'status': 'ok',
            'result_sha256': digest,
            't_start_wall': t_start_wall,
            't_end_wall': t_end_wall,
            'duration_ms': (t_end_mono - t_start_mono) / 1_000_000.0,
        }
        atomic_write_json(result_path, result)
        if os.getenv('PRINT_RESULT_JSON', '0') == '1':
            print(json.dumps(result, separators=(',', ':')))
        return 0
    except Exception as exc:
        t_end_mono = time.monotonic_ns()
        t_end_wall = time.time()
        error_result = {
            'benchmark': 'faas_tiny_relay_v1',
            'run_id': run_id,
            'stack': stack,
            'image_version': image_version(),
            'partition_id': partition_id,
            'host': socket.gethostname(),
            'pid': os.getpid(),
            'invocation_id': invocation_id,
            'input_file': str(input_path),
            'status': 'error',
            'error_type': type(exc).__name__,
            'error_message': str(exc),
            't_start_wall': t_start_wall,
            't_end_wall': t_end_wall,
            'duration_ms': (t_end_mono - t_start_mono) / 1_000_000.0,
        }
        try:
            atomic_write_json(result_path, error_result)
        except Exception:
            pass
        print(json.dumps(error_result, separators=(',', ':')), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
