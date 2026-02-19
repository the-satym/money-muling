"""
smurfing_detector.py
Detects smurfing patterns — unusual aggregation or dispersal of funds.

  Fan-in  : 10+ different senders → 1 receiver within 72 hours
  Fan-out : 1 sender → 10+ different receivers within 72 hours

Key insight: We use a SLIDING 72-HOUR WINDOW per node, not total lifetime counts.
This prevents flagging legitimate merchants or payroll systems.
"""

import networkx as nx
import pandas as pd
from datetime import timedelta

# ── Tuneable thresholds ───────────────────────────────────────────────────────
SMURF_THRESHOLD     = 10      # min unique accounts within the window
TIME_WINDOW_HOURS   = 72      # the sliding window size


def detect_smurfing(G: nx.MultiDiGraph, df: pd.DataFrame) -> list[dict]:
    """
    Returns a list of smurfing rings (fan-in or fan-out hubs).

    Each ring dict:
    {
        "ring_id"      : "RING_S_001",
        "pattern_type" : "fan_in" or "fan_out",
        "members"      : ["ACC_HUB", "ACC_1", "ACC_2", ...],
        "hub_account"  : "ACC_HUB",
        "peak_count"   : 14,          # max unique accounts in any 72hr window
        "peak_window_start": <Timestamp>,
        "peak_window_end"  : <Timestamp>,
        "total_amount" : 45000.0,
        "risk_score"   : 0–100
    }
    """

    rings = []
    ring_counter = 1

    for node in G.nodes():

        # ── Fan-in check: who sends TO this node? ─────────────────────────────
        fan_in_result = _check_fan_in(node, G, df)
        if fan_in_result:
            fan_in_result["ring_id"] = f"RING_S_{ring_counter:03d}"
            rings.append(fan_in_result)
            ring_counter += 1

        # ── Fan-out check: who does this node send TO? ────────────────────────
        fan_out_result = _check_fan_out(node, G, df)
        if fan_out_result:
            fan_out_result["ring_id"] = f"RING_S_{ring_counter:03d}"
            rings.append(fan_out_result)
            ring_counter += 1

    print(f"[smurfing_detector] Rings found: {len(rings)}")
    return rings


# ─────────────────────────────────────────────────────────────────────────────
#  INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _check_fan_in(node: str, G: nx.MultiDiGraph, df: pd.DataFrame) -> dict | None:
    """
    Checks if `node` receives from 10+ unique senders within any 72-hour window.
    Returns a ring dict if suspicious, else None.
    """

    # Collect all incoming transactions to this node
    incoming = []
    for sender in G.predecessors(node):
        edge_data = G.get_edge_data(sender, node)
        for key, attrs in edge_data.items():
            incoming.append({
                "counterparty" : sender,
                "amount"       : attrs.get("amount", 0),
                "timestamp"    : attrs.get("timestamp"),
            })

    if not incoming:
        return None

    return _sliding_window_check(
        node       = node,
        events     = incoming,
        pattern    = "fan_in",
    )


def _check_fan_out(node: str, G: nx.MultiDiGraph, df: pd.DataFrame) -> dict | None:
    """
    Checks if `node` sends to 10+ unique receivers within any 72-hour window.
    Returns a ring dict if suspicious, else None.
    """

    # Collect all outgoing transactions from this node
    outgoing = []
    for receiver in G.successors(node):
        edge_data = G.get_edge_data(node, receiver)
        for key, attrs in edge_data.items():
            outgoing.append({
                "counterparty" : receiver,
                "amount"       : attrs.get("amount", 0),
                "timestamp"    : attrs.get("timestamp"),
            })

    if not outgoing:
        return None

    return _sliding_window_check(
        node       = node,
        events     = outgoing,
        pattern    = "fan_out",
    )


def _sliding_window_check(node: str, events: list[dict], pattern: str) -> dict | None:
    """
    Core logic: Slides a 72-hour window across all events for a node.
    Finds the window with the maximum number of UNIQUE counterparties.

    If that max >= SMURF_THRESHOLD → suspicious.

    Returns ring dict or None.
    """

    # Filter out events with missing timestamps
    events = [e for e in events if e["timestamp"] is not None]
    if not events:
        return None

    # Sort by time
    events.sort(key=lambda e: e["timestamp"])

    window_size = timedelta(hours=TIME_WINDOW_HOURS)
    best_window = None
    best_unique_count = 0
    best_window_events = []

    # Slide the window: for each event as the START point, find all events within 72hrs
    for i, start_event in enumerate(events):
        window_start = start_event["timestamp"]
        window_end   = window_start + window_size

        # Collect all events within this window
        window_events = [
            e for e in events
            if window_start <= e["timestamp"] <= window_end
        ]

        # Count unique counterparties in this window
        unique_counterparties = set(e["counterparty"] for e in window_events)

        if len(unique_counterparties) > best_unique_count:
            best_unique_count    = len(unique_counterparties)
            best_window          = (window_start, window_end)
            best_window_events   = window_events

    # ── Decision: is this suspicious? ─────────────────────────────────────────
    if best_unique_count < SMURF_THRESHOLD:
        return None   # not enough unique accounts in any window

    # Build member list: hub + all counterparties in the best window
    counterparties = list(set(e["counterparty"] for e in best_window_events))
    total_amount   = sum(e["amount"] for e in best_window_events)

    risk_score = _smurfing_risk_score(best_unique_count, total_amount)

    return {
        "ring_id"            : None,    # assigned by caller
        "pattern_type"       : pattern,
        "hub_account"        : node,
        "members"            : [node] + counterparties,
        "peak_count"         : best_unique_count,
        "peak_window_start"  : best_window[0],
        "peak_window_end"    : best_window[1],
        "total_amount"       : round(total_amount, 2),
        "risk_score"         : round(risk_score, 2),
    }


def _smurfing_risk_score(unique_count: int, total_amount: float) -> float:
    """
    Scores smurfing 0–100.
      - More unique accounts = higher score
      - Higher total amount  = higher score
    """
    import math

    # Count score: 10 accounts = 40pts, scales up, capped at 60
    count_score  = min(60, (unique_count - SMURF_THRESHOLD) * 4 + 40)

    # Amount score: log scale, max 40pts
    amount_score = min(40, math.log10(max(total_amount, 1)) * 5)

    return min(100, count_score + amount_score)