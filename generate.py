"""
generate.py — Project 03: CAP Theorem
Fullstack Data — Kenneth

Responsibility:
  - Generate synthetic payment events using the shared project schema
  - Returns a list of dicts ready for psycopg2 insertion
  - Seeded with random.Random(42) — deterministic across all runs
  - Imported by simulate_partition.py, not run directly

Schema (shared across all 8 projects):
  event_id     UUID v4
  user_id      pool of 10k users  (usr-XXXXX)
  merchant_id  pool of 500 merchants (mer-XXXXX)
  amount       uniform 0.01 – 9999.99
  currency     USD/CAD/GBP/EUR/NGN
  status       approved 85% / declined 10% / pending 5%
  event_ts     Unix millis, last 90 days
  is_flagged   3% true
"""

import uuid
import random
import time

# ── Constants ─────────────────────────────────────────────────────────────────

SEED          = 42
CURRENCIES    = ["USD", "CAD", "GBP", "EUR", "NGN"]
STATUSES      = ["approved"] * 85 + ["declined"] * 10 + ["pending"] * 5
NOW_MS        = int(time.time() * 1000)
NINETY_DAYS   = 90 * 24 * 60 * 60 * 1000   # ms


# ── Generator ─────────────────────────────────────────────────────────────────

def generate_events(n: int, written_to: str, seed: int = SEED) -> list[dict]:
    """
    Generate n payment events.

    written_to: label injected into each row so we can trace which
                node accepted a given write during the partition.
                Values: "both", "node1_only"
    """
    rng = random.Random(seed)

    events = []
    for _ in range(n):
        events.append({
            "event_id":   str(uuid.UUID(int=rng.getrandbits(128), version=4)),
            "user_id":    f"usr-{rng.randint(1, 10_000):05d}",
            "merchant_id": f"mer-{rng.randint(1, 500):05d}",
            "amount":     round(rng.uniform(0.01, 9999.99), 2),
            "currency":   rng.choice(CURRENCIES),
            "status":     rng.choice(STATUSES),
            "event_ts":   NOW_MS - rng.randint(0, NINETY_DAYS),
            "is_flagged": rng.random() < 0.03,
            "written_to": written_to,
        })
    return events


if __name__ == "__main__":
    # Smoke test — print 3 sample rows
    from tabulate import tabulate
    samples = generate_events(3, written_to="both")
    print(tabulate(samples, headers="keys", tablefmt="rounded_outline"))