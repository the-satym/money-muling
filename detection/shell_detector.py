"""
shell_detector.py
Detects layered shell networks — chains of low-activity "pass-through" accounts.

Pattern:
  ORIGIN → SHELL_1 → SHELL_2 → DESTINATION
             (≤3 txns)  (≤3 txns)

A shell account:
  - Has very few total transactions (in + out ≤ MAX_SHELL_DEGREE)
  - Exists seemingly only to relay money from one place to another
  - Is NOT the first or last account in the chain

Chains must be at least MIN_CHAIN_LENGTH hops long.
"""

import networkx as nx
import pandas as pd

# ── Tuneable thresholds ───────────────────────────────────────────────────────
MAX_SHELL_DEGREE   = 3    # total edges (in+out) for an account to be considered a shell
MIN_CHAIN_LENGTH   = 3    # minimum number of hops (edges) to flag a chain
MAX_CHAIN_LENGTH   = 8    # DFS depth limit — prevents infinite walks
MAX_CHAIN_AMOUNT   = None # set to a value to only flag chains below a certain amount


def detect_shell_chains(G: nx.MultiDiGraph, df: pd.DataFrame) -> list[dict]:
    """
    Returns a list of detected shell chain rings.

    Each ring dict:
    {
        "ring_id"       : "RING_H_001",
        "pattern_type"  : "shell_chain",
        "members"       : ["ORIGIN", "SHELL_1", "SHELL_2", "DESTINATION"],
        "chain_length"  : 3,            # number of hops
        "shell_nodes"   : ["SHELL_1", "SHELL_2"],
        "total_amount"  : 3500.0,
        "risk_score"    : 0–100
    }
    """

    rings = []
    ring_counter = 1

    # Pre-compute degree for every node (in + out, counting multi-edges)
    node_degree = {
        node: G.in_degree(node) + G.out_degree(node)
        for node in G.nodes()
    }

    visited_chains = set()   # avoid duplicate chain reports

    # Try starting a DFS from every node
    for start_node in G.nodes():
        chains = _dfs_find_chains(
            G           = G,
            node_degree = node_degree,
            start       = start_node,
            path        = [start_node],
            depth       = 0,
        )

        for chain in chains:
            # Deduplicate: use frozenset of nodes as key
            chain_key = tuple(chain)   # keep order — A→B→C ≠ C→B→A
            if chain_key in visited_chains:
                continue
            visited_chains.add(chain_key)

            # Identify shell nodes (all intermediates, not first or last)
            shell_nodes = [
                node for node in chain[1:-1]
                if node_degree[node] <= MAX_SHELL_DEGREE
            ]

            # Calculate total amount along the chain
            total_amount = _chain_amount(G, chain)

            risk_score = _shell_risk_score(len(chain) - 1, len(shell_nodes), total_amount)

            rings.append({
                "ring_id"       : f"RING_H_{ring_counter:03d}",
                "pattern_type"  : "shell_chain",
                "members"       : chain,
                "chain_length"  : len(chain) - 1,   # hops = nodes - 1
                "shell_nodes"   : shell_nodes,
                "total_amount"  : round(total_amount, 2),
                "risk_score"    : round(risk_score, 2),
            })
            ring_counter += 1

    print(f"[shell_detector] Chains found: {len(rings)}")
    return rings


# ─────────────────────────────────────────────────────────────────────────────
#  INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _dfs_find_chains(
    G           : nx.MultiDiGraph,
    node_degree : dict,
    start       : str,
    path        : list,
    depth       : int,
) -> list[list[str]]:
    """
    Recursive DFS from `start` that builds chains.
    A valid chain is returned when:
      - It has >= MIN_CHAIN_LENGTH hops
      - At least one intermediate node is a shell (degree <= MAX_SHELL_DEGREE)

    DFS stops when:
      - Depth exceeds MAX_CHAIN_LENGTH
      - A node is revisited (prevents cycles being re-reported as chains)
      - Current intermediate node is NOT a shell (chain broken — no point continuing)
    """

    found_chains = []

    if depth >= MAX_CHAIN_LENGTH:
        return found_chains

    current_node = path[-1]

    for neighbor in G.successors(current_node):

        # Prevent cycles within the chain path
        if neighbor in path:
            continue

        new_path = path + [neighbor]
        hops     = len(new_path) - 1

        # Check if intermediates (everything except first and last) are shells
        intermediate_nodes = new_path[1:-1]
        all_intermediates_are_shells = all(
            node_degree.get(n, 0) <= MAX_SHELL_DEGREE
            for n in intermediate_nodes
        ) if intermediate_nodes else True

        if not all_intermediates_are_shells:
            # Chain broken — intermediate is a busy account, not a shell
            # Still check if current path (before this node) is valid
            if hops - 1 >= MIN_CHAIN_LENGTH:
                found_chains.append(path)   # save the chain up to current_node
            continue   # don't extend further down this broken path

        # If chain is long enough → record it
        if hops >= MIN_CHAIN_LENGTH:
            found_chains.append(list(new_path))

        # Keep extending deeper
        deeper = _dfs_find_chains(
            G           = G,
            node_degree = node_degree,
            start       = start,
            path        = new_path,
            depth       = depth + 1,
        )
        found_chains.extend(deeper)

    return found_chains


def _chain_amount(G: nx.MultiDiGraph, chain: list[str]) -> float:
    """
    Sums up the maximum single-transaction amount between each consecutive
    pair of nodes in the chain (proxy for the "main" transfer amount).
    """
    total = 0.0
    for i in range(len(chain) - 1):
        src = chain[i]
        dst = chain[i + 1]
        edge_data = G.get_edge_data(src, dst)
        if edge_data:
            amounts = [attrs.get("amount", 0) for attrs in edge_data.values()]
            total += max(amounts)   # take the largest transaction on each hop
    return total


def _shell_risk_score(hops: int, shell_count: int, total_amount: float) -> float:
    """
    Scores a shell chain 0–100:
      - More hops       = more layering = higher score
      - More shell nodes = more deliberate obfuscation
      - Higher amount   = higher score
    """
    import math

    hop_score    = min(40, hops * 8)                                # 3 hops=24, 5 hops=40
    shell_score  = min(30, shell_count * 10)                        # each shell = 10pts
    amount_score = min(30, math.log10(max(total_amount, 1)) * 5)    # log scale

    return min(100, hop_score + shell_score + amount_score)