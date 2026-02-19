"""
engine.py
──────────────────────────────────────────────────────────────────────────────
The ONLY file the web backend needs to import.

Usage:
    from detection.engine import analyze
    result = analyze("path/to/transactions.csv")

Returns the exact JSON structure required by the hackathon spec.
"""

import time
import networkx as nx
import pandas as pd

from detection.graph_builder       import build_graph
from detection.cycle_detector      import detect_cycles
from detection.smurfing_detector   import detect_smurfing
from detection.shell_detector      import detect_shell_chains
from detection.false_positive_filter import filter_false_positives
from detection.scorer              import score_accounts


def analyze(csv_path: str) -> dict:
    """
    Full pipeline:
      CSV → Graph → Detect → Score → Filter → JSON output

    Returns:
    {
        "suspicious_accounts": [...],   ← sorted by suspicion_score descending
        "fraud_rings":         [...],
        "summary": {
            "total_accounts_analyzed":   int,
            "suspicious_accounts_flagged": int,
            "fraud_rings_detected":      int,
            "processing_time_seconds":   float
        },
        "_graph_data": {                 ← for visualization (not in download JSON)
            "nodes": [...],
            "edges": [...]
        }
    }
    """

    start_time = time.time()

    # ── Stage 1: Build graph ──────────────────────────────────────────────────
    print("\n══ Stage 1: Building graph ══")
    G, df = build_graph(csv_path)
    total_accounts = G.number_of_nodes()

    # ── Stage 2: Run all three detectors ─────────────────────────────────────
    print("\n══ Stage 2: Running detectors ══")
    cycle_rings = detect_cycles(G, df)
    smurf_rings = detect_smurfing(G, df)
    shell_rings = detect_shell_chains(G, df)

    all_rings_raw = cycle_rings + smurf_rings + shell_rings
    print(f"[engine] Raw rings before filtering: {len(all_rings_raw)}")

    # ── Stage 3: Score all flagged accounts ───────────────────────────────────
    print("\n══ Stage 3: Scoring accounts ══")
    raw_scores = score_accounts(G, df, cycle_rings, smurf_rings, shell_rings)

    # ── Stage 4: False positive filtering ────────────────────────────────────
    print("\n══ Stage 4: Filtering false positives ══")
    clean_rings, clean_accounts = filter_false_positives(
        G            = G,
        df           = df,
        all_rings    = all_rings_raw,
        flagged_accs = raw_scores,
    )

    # ── Stage 5: Build the final output structure ─────────────────────────────
    print("\n══ Stage 5: Building output ══")

    # suspicious_accounts — sorted descending by score
    suspicious_accounts = sorted(
        [
            {
                "account_id"       : acc["account_id"],
                "suspicion_score"  : acc["suspicion_score"],
                "detected_patterns": acc["detected_patterns"],
                "ring_id"          : acc["ring_id"],
            }
            for acc in clean_accounts.values()
        ],
        key=lambda x: x["suspicion_score"],
        reverse=True,
    )

    # fraud_rings — build from clean rings, deduplicated
    seen_ring_ids = set()
    fraud_rings   = []

    for ring in clean_rings:
        rid = ring["ring_id"]
        if rid in seen_ring_ids:
            continue
        seen_ring_ids.add(rid)

        fraud_rings.append({
            "ring_id"        : rid,
            "member_accounts": ring["members"],
            "pattern_type"   : ring["pattern_type"],
            "risk_score"     : ring.get("risk_score", 0.0),
        })

    # Sort rings by risk score descending
    fraud_rings.sort(key=lambda r: r["risk_score"], reverse=True)

    processing_time = round(time.time() - start_time, 2)

    summary = {
        "total_accounts_analyzed"   : total_accounts,
        "suspicious_accounts_flagged": len(suspicious_accounts),
        "fraud_rings_detected"      : len(fraud_rings),
        "processing_time_seconds"   : processing_time,
    }

    # ── Graph data for visualization ──────────────────────────────────────────
    suspicious_ids = {a["account_id"] for a in suspicious_accounts}
    ring_membership = {}
    for ring in fraud_rings:
        for member in ring["member_accounts"]:
            ring_membership[member] = ring["ring_id"]

    graph_nodes = [
        {
            "id"              : node,
            "suspicious"      : node in suspicious_ids,
            "suspicion_score" : clean_accounts.get(node, {}).get("suspicion_score", 0),
            "ring_id"         : ring_membership.get(node),
            "in_degree"       : G.in_degree(node),
            "out_degree"      : G.out_degree(node),
        }
        for node in G.nodes()
    ]

    graph_edges = [
        {
            "source"    : u,
            "target"    : v,
            "amount"    : attrs.get("amount", 0),
            "timestamp" : str(attrs.get("timestamp", "")),
            "txn_id"    : attrs.get("transaction_id", ""),
        }
        for u, v, attrs in G.edges(data=True)
    ]

    result = {
        "suspicious_accounts" : suspicious_accounts,
        "fraud_rings"         : fraud_rings,
        "summary"             : summary,
        "_graph_data"         : {          # underscore = internal, excluded from download
            "nodes" : graph_nodes,
            "edges" : graph_edges,
        },
    }

    print(f"\n══ DONE in {processing_time}s ══")
    print(f"   Accounts analyzed : {total_accounts}")
    print(f"   Suspicious flagged: {len(suspicious_accounts)}")
    print(f"   Fraud rings found : {len(fraud_rings)}")

    return result


def get_download_json(result: dict) -> dict:
    """
    Returns the result dict WITHOUT the _graph_data key.
    This is what gets written to the downloadable JSON file.
    """
    return {k: v for k, v in result.items() if not k.startswith("_")}