"""
simulate_partition.py — Project 03: CAP Theorem
Fullstack Data — Kenneth

Responsibility:
  This is the centrepiece of the experiment. It runs in four phases:

  PHASE 1 — BASELINE
    Write 50 events to BOTH nodes simultaneously.
    Both nodes are in sync. This is the healthy state.

  PHASE 2 — PARTITION
    Pause node2 via `docker pause postgres_node2`.
    Node2 is now unreachable — simulating a network partition.
    Write 30 events to node1 ONLY. Node2 cannot receive them.

  PHASE 3 — RESTORE
    Unpause node2 via `docker unpause postgres_node2`.
    Node2 comes back online — but it missed 30 writes.
    No automatic sync happens. The divergence is permanent until repair.

  PHASE 4 — DIVERGENCE REPORT
    Show a live side-by-side row count from both nodes.
    The gap IS the CAP theorem made visible.

Observability:
  - Every phase is timestamped and printed with clear section headers
  - Each write batch prints a per-node row count after insertion
  - Partition window duration is measured and printed
  - A JSON event log is written to data/simulation_log.json for observe_divergence.py

Run after: setup.py
Run before: observe_divergence.py
"""

import psycopg2
import subprocess
import time
import json
from datetime import datetime, timezone
from tabulate import tabulate
from generate import generate_events

# ── Config ────────────────────────────────────────────────────────────────────

NODE1 = {"host": "localhost", "port": 5432, "dbname": "payments",
         "user": "engineer", "password": "engineer", "connect_timeout": 5}
NODE2 = {"host": "localhost", "port": 5433, "dbname": "payments",
         "user": "engineer", "password": "engineer", "connect_timeout": 5}

BASELINE_ROWS    = 50    # written to both nodes — the shared starting state
PARTITION_ROWS   = 30    # written to node1 only during the partition window
LOG_PATH         = "data/simulation_log.json"

INSERT_SQL = """
INSERT INTO payment_events
  (event_id, user_id, merchant_id, amount, currency,
   status, event_ts, is_flagged, written_to)
VALUES
  (%(event_id)s, %(user_id)s, %(merchant_id)s, %(amount)s, %(currency)s,
   %(status)s, %(event_ts)s, %(is_flagged)s, %(written_to)s)
ON CONFLICT (event_id) DO NOTHING;
"""

# ── Helpers ───────────────────────────────────────────────────────────────────

def ts() -> str:
    """Current UTC time string for log output."""
    return datetime.now(timezone.utc).strftime("%H:%M:%S UTC")


def get_conn(config: dict):
    conn = psycopg2.connect(**config)
    conn.autocommit = True
    return conn


def row_count(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM payment_events;")
        return cur.fetchone()[0]


def insert_batch(conn, events: list[dict], label: str) -> int:
    """Insert a batch of events, return inserted count."""
    with conn.cursor() as cur:
        cur.executemany(INSERT_SQL, events)
    count = row_count(conn)
    print(f"    {label:<12} → {len(events)} inserted  |  total rows: {count}")
    return count


def docker_pause(container: str):
    subprocess.run(["docker", "pause", container], check=True, capture_output=True)


def docker_unpause(container: str):
    subprocess.run(["docker", "unpause", container], check=True, capture_output=True)


def section(title: str):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}\n")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "═" * 60)
    print("  PROJECT 03 — CAP THEOREM  |  simulate_partition.py")
    print("  Fullstack Data — Kenneth")
    print("═" * 60)

    log = {
        "experiment_start": ts(),
        "phases": {}
    }

    # ── Connect ───────────────────────────────────────────────────────────────
    print(f"\n[{ts()}] Connecting …")
    conn1 = get_conn(NODE1)
    conn2 = get_conn(NODE2)
    print("  ✓  Node 1 connected")
    print("  ✓  Node 2 connected")

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 1 — BASELINE
    # Both nodes receive identical writes. This is the healthy pre-partition state.
    # ─────────────────────────────────────────────────────────────────────────
    section("PHASE 1 — BASELINE  (both nodes active)")
    print(f"  Writing {BASELINE_ROWS} events to BOTH nodes …\n")

    baseline_events = generate_events(BASELINE_ROWS, written_to="both", seed=42)

    t_phase1_start = time.time()
    n1_after_baseline = insert_batch(conn1, baseline_events, "Node 1")
    n2_after_baseline = insert_batch(conn2, baseline_events, "Node 2")
    t_phase1_end = time.time()

    print(f"\n  ✓  Both nodes in sync — {n1_after_baseline} rows each")
    print(f"  ✓  Phase 1 duration: {t_phase1_end - t_phase1_start:.2f}s")

    log["phases"]["baseline"] = {
        "rows_written": BASELINE_ROWS,
        "node1_total": n1_after_baseline,
        "node2_total": n2_after_baseline,
        "in_sync": True,
        "timestamp": ts(),
    }

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 2 — NETWORK PARTITION
    # Node2 is paused. It cannot receive writes. Node1 continues alone.
    # This forces the AP choice: keep writing (available) knowing divergence will occur.
    # ─────────────────────────────────────────────────────────────────────────
    section("PHASE 2 — NETWORK PARTITION  (node2 paused)")
    print(f"  [{ts()}] Pausing postgres_node2 …")

    t_partition_start = time.time()
    docker_pause("postgres_node2")

    print(f"  ✓  postgres_node2 PAUSED — network partition active")
    print(f"\n  CAP CHOICE: We are taking the AP path.")
    print(f"  Node1 will continue accepting writes.")
    print(f"  Node2 cannot receive them.")
    print(f"\n  Writing {PARTITION_ROWS} events to Node 1 ONLY …\n")

    # Use a different seed so partition events are distinct from baseline events
    partition_events = generate_events(PARTITION_ROWS, written_to="node1_only", seed=99)

    n1_after_partition = insert_batch(conn1, partition_events, "Node 1")

    # Node2 is paused — attempting to query it would hang. Log its last known state.
    print(f"    Node 2    → UNREACHABLE (paused)")

    t_partition_end = time.time()
    partition_duration = t_partition_end - t_partition_start

    print(f"\n  ✓  Partition window duration: {partition_duration:.2f}s")
    print(f"  ✓  Node 1 now has {n1_after_partition} rows")
    print(f"  ✓  Node 2 still has {n2_after_baseline} rows (frozen at partition start)")
    print(f"\n  GAP = {n1_after_partition - n2_after_baseline} rows — this is the divergence")

    log["phases"]["partition"] = {
        "rows_written_to_node1": PARTITION_ROWS,
        "node1_total": n1_after_partition,
        "node2_total_frozen": n2_after_baseline,
        "divergence_gap": n1_after_partition - n2_after_baseline,
        "partition_duration_s": round(partition_duration, 2),
        "timestamp": ts(),
    }

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 3 — RESTORE
    # Node2 comes back online. It was not crashed — it was isolated.
    # Nothing syncs automatically. The gap persists.
    # ─────────────────────────────────────────────────────────────────────────
    section("PHASE 3 — PARTITION HEALED  (node2 restored)")
    print(f"  [{ts()}] Unpausing postgres_node2 …")

    docker_unpause("postgres_node2")
    time.sleep(1)   # brief settle — let postgres accept connections again

    n2_after_restore = row_count(conn2)
    t_restore = time.time()

    print(f"  ✓  postgres_node2 RESTORED")
    print(f"\n  Node 2 row count after restore: {n2_after_restore}")
    print(f"  Expected if synced:              {n1_after_partition}")
    print(f"  Actual gap:                      {n1_after_partition - n2_after_restore} rows")
    print(f"\n  → No automatic sync. Node2 does not know it missed anything.")
    print(f"    This is what eventual consistency WITHOUT a repair mechanism looks like.")

    log["phases"]["restore"] = {
        "node1_total": n1_after_partition,
        "node2_total_after_restore": n2_after_restore,
        "gap_persists": n1_after_partition - n2_after_restore,
        "auto_sync": False,
        "timestamp": ts(),
    }

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 4 — DIVERGENCE REPORT
    # Side-by-side state of both nodes. The gap is the CAP theorem.
    # ─────────────────────────────────────────────────────────────────────────
    section("PHASE 4 — DIVERGENCE REPORT")

    # Pull written_to distribution from both nodes
    def written_to_counts(conn) -> dict:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT written_to, COUNT(*) as count
                FROM payment_events
                GROUP BY written_to
                ORDER BY written_to;
            """)
            return {row[0]: row[1] for row in cur.fetchall()}

    n1_dist = written_to_counts(conn1)
    n2_dist = written_to_counts(conn2)

    summary_rows = [
        {
            "metric":                    "Total rows",
            "node1":                     n1_after_partition,
            "node2":                     n2_after_restore,
            "in_sync":                   "✗" if n1_after_partition != n2_after_restore else "✓",
        },
        {
            "metric":                    "Rows written to both",
            "node1":                     n1_dist.get("both", 0),
            "node2":                     n2_dist.get("both", 0),
            "in_sync":                   "✓",
        },
        {
            "metric":                    "Rows written to node1 only",
            "node1":                     n1_dist.get("node1_only", 0),
            "node2":                     n2_dist.get("node1_only", 0),
            "in_sync":                   "✗",
        },
    ]

    print(tabulate(summary_rows, headers="keys", tablefmt="rounded_outline"))

    print(f"""
  WHAT YOU ARE SEEING:
  ┌─────────────────────────────────────────────────────┐
  │  Node 1 accepted {BASELINE_ROWS} baseline + {PARTITION_ROWS} partition writes = {BASELINE_ROWS + PARTITION_ROWS} total  │
  │  Node 2 accepted {BASELINE_ROWS} baseline only. Partition writes = 0  │
  │                                                     │
  │  The {PARTITION_ROWS}-row gap is the cost of choosing Availability   │
  │  over Consistency during the partition window.      │
  │                                                     │
  │  This is AP behaviour. Node 1 stayed available.     │
  │  The price is Node 2 is now stale.                  │
  └─────────────────────────────────────────────────────┘
    """)

    # ── Write simulation log ──────────────────────────────────────────────────
    log["experiment_end"] = ts()
    log["final_state"] = {
        "node1_rows": n1_after_partition,
        "node2_rows": n2_after_restore,
        "divergence_gap": n1_after_partition - n2_after_restore,
    }

    import os
    os.makedirs("data", exist_ok=True)
    with open(LOG_PATH, "w") as f:
        json.dump(log, f, indent=2)

    print(f"  Simulation log written → {LOG_PATH}")
    print(f"  Next step: python observe_divergence.py\n")

    conn1.close()
    conn2.close()


if __name__ == "__main__":
    main()