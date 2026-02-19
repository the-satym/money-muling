"""
scorer.py
Takes all detected rings and computes a final suspicion score for every
flagged account.

Score breakdown (max 100):
  Cycle membership      : up to 40 pts  (+ speed bonus)
  Smurfing pattern      : up to 25 pts
  Velocity bonus        : up to 20 pts  (transaction frequency spike)
  Shell chain           : up to 15 pts
  Multi-pattern bonus   : up to 10 pts  (flagged by 2+ detectors)
"""

import networkx as nx
import pandas as pd
from datetime import timedelta

# ── Weights ───────────────────────────────────────────────────────────────────
WEIGHT_CYCLE        = 40
WEIGHT_SMURF        = 25
WEIGHT_VELOCITY     = 20
WEIGHT_SHELL        = 15
WEIGHT_MULTI_DETECT = 10   # bonus if account appears in multiple ring types


def score_accounts(
    G            : nx.MultiDiGraph,
    df           : pd.DataFrame,
    cycle_rings  : list[dict],
    smurf_rings  : list[dict],
    shell_rings  : list[dict],
) -> dict:
    """
    Returns a dict of all flagged accounts with their suspicion data:

    {
        "ACC_001": {
            "account_id"       : "ACC_001",
            "suspicion_score"  : 87.5,
            "detected_patterns": ["cycle_length_3", "high_velocity"],
            "ring_id"          : "RING_C_001",   ← highest-risk ring this account is in
            "ring_ids"         : ["RING_C_001"],  ← all rings
        },
        ...
    }
    """

    # ── Step 1: Map each account to the rings it appears in ──────────────────
    account_data = {}   # {acc_id: {ring_ids, patterns, raw_scores}}

    def _ensure(acc_id):
        if acc_id not in account_data:
            account_data[acc_id] = {
                "ring_ids"    : [],
                "patterns"    : [],
                "cycle_score" : 0,
                "smurf_score" : 0,
                "shell_score" : 0,
            }

    # Process cycle rings
    for ring in cycle_rings:
        cycle_len = ring.get("cycle_length", 3)
        speed     = ring.get("completed_hours", 168)
        pattern   = f"cycle_length_{cycle_len}"

        # Speed bonus
        if speed <= 24:
            pattern_extra = "high_velocity"
        elif speed <= 72:
            pattern_extra = "moderate_velocity"
        else:
            pattern_extra = None

        for acc in ring["members"]:
            _ensure(acc)
            account_data[acc]["ring_ids"].append(ring["ring_id"])
            account_data[acc]["patterns"].append(pattern)
            if pattern_extra:
                account_data[acc]["patterns"].append(pattern_extra)

            # Cycle score — use the ring's risk_score as basis, normalised to weight
            raw = ring.get("risk_score", 70)
            normalized = (raw / 100) * WEIGHT_CYCLE
            account_data[acc]["cycle_score"] = max(account_data[acc]["cycle_score"], normalized)

    # Process smurfing rings
    for ring in smurf_rings:
        pattern = ring.get("pattern_type", "smurf")   # "fan_in" or "fan_out"

        for acc in ring["members"]:
            _ensure(acc)
            account_data[acc]["ring_ids"].append(ring["ring_id"])
            account_data[acc]["patterns"].append(pattern)

            raw = ring.get("risk_score", 60)
            normalized = (raw / 100) * WEIGHT_SMURF
            account_data[acc]["smurf_score"] = max(account_data[acc]["smurf_score"], normalized)

    # Process shell chains
    for ring in shell_rings:
        for acc in ring["members"]:
            _ensure(acc)
            account_data[acc]["ring_ids"].append(ring["ring_id"])
            account_data[acc]["patterns"].append("shell_chain")

            raw = ring.get("risk_score", 50)
            normalized = (raw / 100) * WEIGHT_SHELL
            account_data[acc]["shell_score"] = max(account_data[acc]["shell_score"], normalized)

    # ── Step 2: Velocity score (transaction frequency spike) ─────────────────
    velocity_scores = _compute_velocity_scores(df)

    # ── Step 3: Compute final score per account ───────────────────────────────
    result = {}

    for acc_id, data in account_data.items():

        cycle_pts    = data["cycle_score"]
        smurf_pts    = data["smurf_score"]
        shell_pts    = data["shell_score"]
        velocity_pts = velocity_scores.get(acc_id, 0) * WEIGHT_VELOCITY

        # Multi-detector bonus: extra points if flagged by 2+ different pattern types
        unique_types = _count_unique_pattern_types(data["patterns"])
        multi_bonus  = WEIGHT_MULTI_DETECT if unique_types >= 2 else 0

        total_score = cycle_pts + smurf_pts + shell_pts + velocity_pts + multi_bonus
        total_score = min(100.0, round(total_score, 2))   # cap at 100

        # Deduplicate patterns list
        unique_patterns = list(dict.fromkeys(data["patterns"]))   # preserves order

        # Primary ring = highest-risk ring this account is in
        primary_ring = _pick_primary_ring(
            data["ring_ids"],
            cycle_rings + smurf_rings + shell_rings,
        )

        result[acc_id] = {
            "account_id"        : acc_id,
            "suspicion_score"   : total_score,
            "detected_patterns" : unique_patterns,
            "ring_id"           : primary_ring,
            "ring_ids"          : list(dict.fromkeys(data["ring_ids"])),
        }

    print(f"[scorer] Total accounts scored: {len(result)}")
    return result


# ─────────────────────────────────────────────────────────────────────────────
#  INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _compute_velocity_scores(df: pd.DataFrame) -> dict:
    """
    Detects accounts with sudden transaction frequency spikes.

    Approach:
      - Compute average daily transaction rate per account
      - Find the peak 24-hour window
      - If peak > 3× average → velocity spike → score 0–1
    """
    velocity = {}

    all_accounts = set(df["sender_id"].unique()) | set(df["receiver_id"].unique())

    for acc in all_accounts:
        acc_txns = df[(df["sender_id"] == acc) | (df["receiver_id"] == acc)].copy()
        acc_txns = acc_txns.sort_values("timestamp")

        if len(acc_txns) < 3:
            continue

        # Average daily rate
        total_days = max(
            (acc_txns["timestamp"].max() - acc_txns["timestamp"].min()).total_seconds() / 86400,
            1
        )
        avg_daily = len(acc_txns) / total_days

        # Peak 24-hour window
        peak_count = 0
        window = timedelta(hours=24)

        for _, row in acc_txns.iterrows():
            t_start = row["timestamp"]
            t_end   = t_start + window
            count   = ((acc_txns["timestamp"] >= t_start) & (acc_txns["timestamp"] <= t_end)).sum()
            peak_count = max(peak_count, count)

        # Spike ratio
        if avg_daily > 0 and peak_count > avg_daily * 3:
            # Normalize to 0–1
            spike_ratio = min(1.0, (peak_count / avg_daily) / 20)
            velocity[acc] = spike_ratio

    return velocity


def _count_unique_pattern_types(patterns: list[str]) -> int:
    """Counts how many distinct detector types are in the patterns list."""
    types = set()
    for p in patterns:
        if "cycle" in p:
            types.add("cycle")
        elif "fan" in p:
            types.add("smurf")
        elif "shell" in p:
            types.add("shell")
        elif "velocity" in p:
            types.add("velocity")
    return len(types)


def _pick_primary_ring(ring_ids: list[str], all_rings: list[dict]) -> str:
    """Returns the ring_id of the highest-risk ring from the list."""
    if not ring_ids:
        return "UNKNOWN"

    ring_map = {r["ring_id"]: r.get("risk_score", 0) for r in all_rings}
    best = max(ring_ids, key=lambda rid: ring_map.get(rid, 0))
    return best