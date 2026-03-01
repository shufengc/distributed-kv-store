# Distributed Key-Value Store

A horizontally scalable, fault-tolerant key-value storage system built in Go. This implementation uses the Multi-Raft consensus algorithm for strong consistency across sharded regions, with a global scheduler that monitors cluster health and automatically rebalances replicas under load.

## Core Architecture & Features

- **Raft Consensus** — Full implementation of leader election, log replication, and state machine transitions. Handles membership changes (`AddNode`/`RemoveNode`) and leader transfer without downtime.

- **Fault Tolerance** — Snapshot-based log compaction and recovery from unreliable network partitions. Survives concurrent node failures and network splits while preserving linearizability.

- **Multi-Raft & Sharding** — Dynamic region splitting for horizontal scalability. Each region runs an independent Raft group; configuration changes are applied atomically across the cluster.

- **Global Scheduler** — Heartbeat-driven cluster metadata management. Processes region heartbeats, tracks store capacity, and generates balance-region scheduling decisions to move replicas from hot stores to cold stores.

## Tech Stack & Skills

- **Go (Golang)** — Concurrency primitives (goroutines, channels, mutexes), interfaces, and idiomatic error handling
- **gRPC / Protocol Buffers** — Inter-node and client-server RPC
- **BadgerDB** — Embedded LSM-tree storage engine
- **Concurrent Distributed Testing** — Raft store tests with simulated network partitions, unreliable links, and multi-client workloads

## Running the Test Suite

Prerequisites: Go 1.13+ and a configured `GOPATH`.

```bash
# Clone the repository
git clone https://github.com/shufengc/distributed-kv-store.git
cd distributed-kv-store

# Run all Project 3 tests (Raft conf change, split, scheduler)
make project3

# Or run individual components:
go test ./raft -run 3A -v                    # Raft: conf change & leader transfer
go test ./kv/test_raftstore -run 3B -v       # Raftstore: conf change & region split
go test ./scheduler/server ./scheduler/server/schedulers -check.f="3C" -v  # Scheduler
```

## Acknowledgements

This project is built on the [PingCAP Talent Plan](https://github.com/pingcap/talent-plan) (TinyKV) framework. The network simulation, test harness, and protocol definitions are from the Talent Plan; the Raft consensus logic, raftstore implementation, and scheduler are my own.
