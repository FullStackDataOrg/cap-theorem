# CAP Theorem

---

## What It Studies

The CAP theorem states a distributed system can only guarantee two of three properties simultaneously: **Consistency**, **Availability**, and **Partition Tolerance**. This project makes that observable — you watch inconsistency happen in your own terminal against two live PostgreSQL nodes.

---

## The Core Question

What does "eventual consistency" actually look like in data? What breaks first when you partition a network? What does it cost to repair the divergence — and who pays that cost?

---

## Infrastructure

| Component | Version | Role |
|---|---|---|
| PostgreSQL Node 1 | 15 | Primary write target — stays available during partition |
| PostgreSQL Node 2 | 15 | Secondary node — isolated via `docker pause` |
| Docker Compose | — | Container orchestration |
| psycopg2-binary | 2.9.9 | Python → PostgreSQL driver |
| Python | 3.12 | Experiment runner |

No replication is configured between the nodes. This is intentional — the absence of replication is what forces the divergence to be visible and manual.

---

## Directory Structure

```
03-cap-theorem/
├── docker-compose.yml        # two independent PostgreSQL containers
├── requirements.txt
├── generate.py               # seeded synthetic payment events (shared schema)
├── setup.py                  # creates tables on both nodes, pre-flight check
├── simulate_partition.py     # 4-phase partition experiment — the centrepiece
├── observe_divergence.py     # deep inspection of the diverged state
├── repair.py                 # LWW and FWW reconciliation strategies
└── data/                     # gitignored — simulation_log.json, repair_log.json
```

---

## Run Order

```bash
# 1. Start containers
docker compose up postgres_node1 postgres_node2 -d

# 2. Environment
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. Experiment
python setup.py                 # create tables, verify both nodes healthy
python simulate_partition.py    # run the 4-phase partition experiment
python observe_divergence.py    # inspect the diverged state in depth
python repair.py                # apply LWW and FWW strategies

# 4. Teardown
docker compose down             # stop containers, preserve volumes
docker compose down -v          # stop containers, destroy volumes (full reset)
```

---

## Experiment Phases

### Phase 1 — Baseline
50 events written to **both** nodes simultaneously. Both nodes are in sync. This is the healthy pre-partition state.

### Phase 2 — Network Partition
`docker pause postgres_node2` isolates Node 2. 30 events are written to **Node 1 only**. This is the AP choice — the system stays available at the cost of divergence.

### Phase 3 — Restore
`docker unpause postgres_node2` brings Node 2 back online. No automatic sync occurs. The 30-row gap persists exactly as it was at the moment of partition.

### Phase 4 — Divergence Report
Side-by-side row counts. The gap is the CAP theorem made visible.

---

## Results

### Divergence State (post-partition, pre-repair)

| Node | Total Rows | Rows Written to Both | Rows Node1 Only |
|---|---|---|---|
| Node 1 | 80 | 50 | 30 |
| Node 2 | 50 | 50 | 0 |
| **Gap** | **30** | — | — |

### Repair Strategy Comparison

| Strategy | Rows Synced | Data Lost | Mechanism | In Sync After |
|---|---|---|---|---|
| Last-Write-Wins (LWW) | 30 | 0 | Copy node1_only rows → node2 | ✓ |
| First-Write-Wins (FWW) | 0 | 30 | Delete node1_only rows from node1 | ✓ |

---

## Key Findings

- **The gap does not close on its own.** Restoring the network connection does not trigger any sync. PostgreSQL is not a distributed database — it does not know it has a peer.
- **Both repair strategies achieve consistency** but through opposite mechanisms. LWW preserves all data and brings the stale node forward. FWW discards the partition writes and rolls the advanced node back.
- **The `written_to` column is the observability hook.** Without it, you cannot distinguish partition writes from baseline writes in the data — you would only see a row count gap with no way to identify which rows caused it.
- **`docker pause` is an honest partition simulation.** The container is not crashed — it is suspended at the OS level. The node is alive but unreachable, which is exactly what a real network partition looks like.

---

## Observability

Each script prints timestamped section headers, per-node row counts after every write batch, and a summary table at exit. Two JSON logs are written to `data/`:

- `simulation_log.json` — records each phase, row counts, gap size, and partition duration
- `repair_log.json` — records both strategy outcomes, rows synced, and data lost

---

## Dataset

Synthetic payment events using the shared schema across all 8 projects:

```
event_id     UUID v4
user_id      pool of 10k users   (usr-XXXXX)
merchant_id  pool of 500 merchants (mer-XXXXX)
amount       uniform 0.01 – 9999.99
currency     USD / CAD / GBP / EUR / NGN
status       approved 85% / declined 10% / pending 5%
event_ts     Unix millis, last 90 days
is_flagged   3% true
written_to   which node accepted this write (observability column)
```

Seed 42 for baseline events. Seed 99 for partition events — ensures the two batches are distinct and non-overlapping.

---

*Fullstack Data — Kenneth | Project 03 of 8*