"""
graph_builder.py
Reads the CSV and constructs a directed multigraph.
Each account = node, each transaction = directed edge with metadata.
"""

import pandas as pd
import networkx as nx
from datetime import datetime


def build_graph(csv_path: str) -> tuple[nx.MultiDiGraph, pd.DataFrame]:
    """
    Reads CSV and returns:
      - G       : NetworkX MultiDiGraph
      - df      : cleaned DataFrame (for timestamp lookups later)
    """

    # ── 1. Load CSV ──────────────────────────────────────────────────────────
    df = pd.read_csv(csv_path)

    required_columns = {"transaction_id", "sender_id", "receiver_id", "amount", "timestamp"}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"CSV is missing columns: {missing}")

    # ── 2. Clean & type-cast ─────────────────────────────────────────────────
    df["timestamp"] = pd.to_datetime(df["timestamp"])          # parse timestamps
    df["amount"]    = pd.to_numeric(df["amount"], errors="coerce")  # ensure float
    df.dropna(subset=["sender_id", "receiver_id", "amount", "timestamp"], inplace=True)
    df.reset_index(drop=True, inplace=True)

    # ── 3. Build graph ────────────────────────────────────────────────────────
    G = nx.MultiDiGraph()

    for _, row in df.iterrows():
        G.add_edge(
            row["sender_id"],           # source node
            row["receiver_id"],         # target node
            transaction_id = row["transaction_id"],
            amount         = row["amount"],
            timestamp      = row["timestamp"],   # pandas Timestamp
        )

    print(f"[graph_builder] Nodes: {G.number_of_nodes()} | Edges: {G.number_of_edges()}")
    return G, df
