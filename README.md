# HPCSA-Benchmarks


This repository contains the benchmark artifacts for my report about Function as a Service (FaaS) in HPC, related to the Practical Course: High-Performance Computing System Administration.

The project compares two execution environments for short-lived containerized function workloads:

- Cloud-like stack: a persistent k3s cluster
- HPC-like stack: Slurm + KSI, where each Slurm task starts its own temporary Kubernetes cluster

The same workload images are executed on both platforms, and results are collected in a comparable format.

## Benchmarks

### Benchmark 1 — Tiny Relay Latency
A minimal FaaS-style benchmark.

One invocation:
- reads one tiny input object
- performs a trivial operation
- writes one tiny output object
- exits

This benchmark is intentionally orchestration-sensitive and highlights warm invocation latency, warm makespan and KSI startup/setup overhead.

### Benchmark 2 — Hybrid ETL Micro-Batch FaaS
A more realistic short-lived data-function benchmark.

One invocation:
- reads one main shard
- loads small shared lookup/config files
- deserializes records
- applies lightweight enrichment and transformation
- aggregates results
- writes one summary
- exits

This benchmark was designed to keep clear FaaS semantics while making real function work large enough to matter.

## Repository contents

This repository includes:

- dataset generation scripts
- worker/container image definitions
- benchmark runner scripts for:
  - k3s
  - Slurm + KSI
- aggregation scripts
- generated benchmark outputs
- aggregated CSV result files

## Methodology

The benchmarks follow a shared design:

- one Kubernetes Job = one invocation
- both platforms execute the same workload image
- work is split deterministically across execution partitions
- KSI startup/setup overhead is treated separately
- the main comparison is warm cluster vs warm cluster

This keeps the evaluation focused on short-lived function execution while still documenting the cluster bootstrap overhead of KSI.

## Goal

The purpose of this work is to study how well an HPC-oriented environment can support FaaS-style workloads compared with a cloud-like Kubernetes setup, and how the balance between orchestration cost and actual function work changes across different workload types.

