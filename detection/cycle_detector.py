"""
cycle_detector.py
Detects circular fund routing — money that flows in a loop:
  Example:  A → B → C → A

Rules:
  - Cycles of length 3 to 5 only
  - Cycle must complete within MAX_CYCLE_DAYS days (timestamp check)
  - Total amount moved must exceed MIN_CYCLE_AMOUNT (filters micro-test txns)
"""

import networkx as nx
import pandas as pd
from itertools import combinations

# ── Tuneable thresholds ───────────────────────────────────────────────────────
MIN_CYCLE_LENGTH  = 3
MAX_CYCLE_LENGTH  = 5
MAX_CYCLE_DAYS    = 7      # cycle must complete within 7 days
MIN_CYCLE_AMOUNT  = 500    # total amount across all edges in the cycle


def detect_cycles(G: nx.MultiDiGraph, df: pd.DataFrame) -> list[dict]:
    """
    Returns a list of detected cycle rings.

    Each ring dict:
    {
        "ring_id"         : "RING_C_001",
        "pattern_type"    : "cycle",
        "members"         : ["ACC_A", "ACC_B", "ACC_C"],
        "cycle_length"    : 3,
        "completed_hours" : 14.5,     # how fast the cycle closed
        "total_amount"    : 1200.0,
        "risk_score"      : 0–100
    }
    """

    rings = []
    ring_counter = 1

    # ── Step 1: Find all elementary cycles in the graph ──────────────────────
    # nx.simple_cycles returns every cycle as a list of nodes (no repeat of start)
    all_cycles = list(nx.simple_cycles(G))

    for cycle in all_cycles:

        # ── Step 2: Length filter ─────────────────────────────────────────────
        if not (MIN_CYCLE_LENGTH <= len(cycle) <= MAX_CYCLE_LENGTH):
            continue

        # ── Step 3: Collect all transactions that belong to this cycle ────────
        # A cycle [A, B, C] means edges: A→B, B→C, C→A
        cycle_edges = []
        valid = True

        for i in range(len(cycle)):
            src = cycle[i]
            dst = cycle[(i + 1) % len(cycle)]   # wrap around to close the loop

            # Get all edges between src → dst
            edge_data = G.get_edge_data(src, dst)
            if not edge_data:
                valid = False   # edge doesn't exist — not a real cycle in our data
                break

            # edge_data is a dict of {0: {...}, 1: {...}} for MultiDiGraph
            for key, attrs in edge_data.items():
                cycle_edges.append({
                    "src"       : src,
                    "dst"       : dst,
                    "amount"    : attrs.get("amount", 0),
                    "timestamp" : attrs.get("timestamp"),
                })

        if not valid or not cycle_edges:
            continue

        # ── Step 4: Timestamp check — did the cycle close fast enough? ────────
        timestamps = [e["timestamp"] for e in cycle_edges if e["timestamp"] is not None]
        if len(timestamps) < len(cycle):
            continue   # missing timestamps — skip

        earliest = min(timestamps)
        latest   = max(timestamps)
        duration_hours = (latest - earliest).total_seconds() / 3600
        duration_days  = duration_hours / 24

        if duration_days > MAX_CYCLE_DAYS:
            continue   # cycle took too long — not a tight muling loop

        # ── Step 5: Amount check — is enough money involved? ──────────────────
        total_amount = sum(e["amount"] for e in cycle_edges)
        if total_amount < MIN_CYCLE_AMOUNT:
            continue   # micro-transactions — likely test payments, skip

        # ── Step 6: Calculate risk score for this cycle ───────────────────────
        risk_score = _cycle_risk_score(len(cycle), duration_hours, total_amount)

        rings.append({
            "ring_id"         : f"RING_C_{ring_counter:03d}",
            "pattern_type"    : "cycle",
            "members"         : list(cycle),
            "cycle_length"    : len(cycle),
            "completed_hours" : round(duration_hours, 2),
            "total_amount"    : round(total_amount, 2),
            "risk_score"      : round(risk_score, 2),
        })

        ring_counter += 1

    print(f"[cycle_detector] Rings found: {len(rings)}")
    return rings


def _cycle_risk_score(length: int, duration_hours: float, total_amount: float) -> float:
    """
    Scores a cycle 0–100 based on:
      - Length:   shorter cycles are more suspicious (tight loop = deliberate)
      - Speed:    faster completion = more suspicious
      - Amount:   larger amounts = more suspicious (capped)
    """

    # Length score: length 3 = max suspicious, length 5 = less
    length_score = {3: 40, 4: 30, 5: 20}.get(length, 20)

    # Speed score: under 24hrs = very suspicious, up to 7 days = less
    if duration_hours <= 24:
        speed_score = 40
    elif duration_hours <= 72:
        speed_score = 30
    else:
        speed_score = 15

    # Amount score: log scale, capped at 20 points
    import math
    amount_score = min(20, math.log10(max(total_amount, 1)) * 4)

    return min(100, length_score + speed_score + amount_score)