# raft-kv

A distributed key-value store built on the [Raft consensus algorithm](https://raft.github.io/raft.pdf), written from scratch in Go. It has no Raft libraries and no `net/rpc` magic. The goal is to understand, line by line, the replication model that etcd (and so every Kubernetes control plane) is built on.

## Status

| Phase | What | State |
|-------|------|-------|
| 1 | Leader election | ✅ Done |
| 2 | Log replication + commit | 🚧 Next |
| 3 | Persistence (write-ahead log) + crash recovery | ⏳ |
| 4 | Real TCP transport, multi-process cluster | ⏳ |
| 5 | KV state machine + clients (exactly-once, linearizable reads) | ⏳ |
| 6 | Snapshots / log compaction | ⏳ |
| 7 | etcd-style extras: Watch, PreVote, membership changes | ⏳ |

## Design

The Raft core is a **deterministic state machine with no goroutines, timers or I/O**, the same design as [etcd-io/raft](https://github.com/etcd-io/raft).

```
          Tick()                 Step(msg)
   (one unit of time)      (a message arrived)
            │                      │
            ▼                      ▼
        ┌──────────────────────────────┐
        │             Node             │
        │ role, term, votedFor, votes, │
        │ timers, log (phase 2+)       │
        └──────────────────────────────┘
                       │
                       ▼
               ReadMessages()
          (messages to send, drained)
```

- **`Tick()`**: time passes. Followers and candidates count toward a randomized election timeout (10–19 ticks); leaders count toward the heartbeat interval (1 tick).
- **`Step(m Message)`**: a message arrived. It does the term check first (a higher term means become follower; a lower term means ignore), then routes by type to `handleVoteRequest`, `handleVoteResponse` or `handleHeartbeat`.
- **`ReadMessages()`**: returns everything the node wants to send and empties its outbox.

The node never touches a network. Whatever drives it decides how messages travel: a test simulator today, TCP in phase 4. Because time and delivery are injected, every test is fully reproducible from a seed.

### Election rules implemented (Raft paper §5.1, §5.2, §5.4.1, Figure 2)
- One vote per term; a duplicate request from the same candidate is granted again.
- A vote is granted only if the candidate's log is at least as up to date (last term first, then length).
- The election timer resets only when a vote is **granted** or a current leader is heard from.
- Votes are tracked in a map, so duplicate replies can't be double-counted.
- A higher term in any message forces a step-down; a lower term is ignored.
- A new leader sends heartbeats immediately.

## Layout

```
raft/
  raft.go          Node, Tick/Step/ReadMessages, election logic
  message.go       Message and message types
  network_test.go  in-memory cluster simulator (clock, delivery, partitions, drops)
  raft_test.go     unit tests + cluster scenarios + chaos test
```

## Testing

```
go test -v ./raft/
go test -race -count=50 ./raft/
```

The simulator (`network_test.go`) holds every node, ticks them, delivers messages, and can isolate nodes or drop a percentage of messages. After every tick it checks **Election Safety**: never two leaders in the same term.

Scenarios covered:
- A single node elects itself; the election timeout stays within range and is random
- Voting rules: grant, one vote per term, duplicate request, log up-to-date check
- Stale messages are ignored; higher terms force a step-down; a candidate yields to a same-term leader
- Rejected votes don't count toward a majority
- 3 nodes elect exactly one leader, who stays leader under heartbeats
- An isolated leader is replaced at a higher term, then steps down after the partition heals
- A minority partition (2 of 5) can never elect a leader
- **Chaos**: 200 seeds × 5 nodes, 10% message loss, a random node partitioned every 50 ticks, with no Election Safety violation and exactly one leader after healing

## References
- Ongaro & Ousterhout, *In Search of an Understandable Consensus Algorithm* (extended version)
- Jon Gjengset, *Students' Guide to Raft*
- MIT 6.5840 (Distributed Systems) Raft labs
- `etcd-io/raft`: `raft.go`, `raft_paper_test.go`
