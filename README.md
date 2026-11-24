# tinyq

A minimal, durable FIFO task queue written in Go using only the standard library. A single broker coordinates remote workers over HTTP and provides at-least-once delivery.

## System design

- **Storage:** Append-only write-ahead log (WAL) containing job payloads and enqueue/ack records.
- **Memory:** FIFO of pending job references (ID, disk offset, size) and a map of active leases. Payloads are read from disk on demand.
- **Enqueue:** Persist the job to the WAL before appending it to the FIFO and confirming submission.
- **Lease:** Remove the first pending job and assign it a unique token and expiration time.
- **Ack:** Validate the lease, persist the ACK, and remove the job from memory.
- **Expiration:** Return expired jobs to the tail of the FIFO.
- **Recovery:** Replay the WAL on startup to reconstruct unfinished jobs.
- **Compaction:** Rewrite the WAL with unfinished jobs, atomically replace it, and update in-memory offsets.
- **Concurrency:** A single mutex serializes state transitions.

## HTTP API

| Method | Endpoint | Description |
|---|---|---|
| POST | `/enqueue` | Submit a job |
| POST | `/lease` | Acquire the next available job |
| POST | `/ack` | Acknowledge job completion |
| GET | `/stats` | Queue metrics |

## Project structure

```text
tinyq/
├── cmd/tinyq/
│   └── main.go
├── internal/
│   ├── wal/
│   │   ├── wal.go
│   │   ├── record.go
│   │   ├── replay.go
│   │   └── compact.go
│   ├── queue/
│   │   ├── queue.go
│   │   ├── job.go
│   │   ├── lease.go
│   │   └── expire.go
│   └── server/
│       ├── server.go
│       └── handlers.go
├── client/
│   └── client.go
├── go.mod
└── README.md
```

## Guarantees and limitations

- At-least-once delivery.
- Durable enqueue and ACK through `fsync`.
- FIFO leasing, not FIFO completion.
- Duplicate execution is possible.
- Single broker, single partition.
- No replication or automatic failover.
- Zero external dependencies.
