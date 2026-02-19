"""
false_positive_filter.py
Removes or downweights legitimate accounts that triggered detection
but are NOT actually money mules.

Three checks:
  1. MERCHANT CHECK   — high volume spread across 30+ days
  2. PAYROLL CHECK    — fan-out to many accounts with similar amounts on same day
  3. MICRO-TXN CHECK  — cycles with tiny total amounts (test payments)
"""

import networkx as nx
import pandas as pd
from datetime import timedelta

# ── Tuneable thresholds ───────────────────────────────────────────────────────
MERCHANT_MIN_TRANSACTIONS = 50      # must have at least this many txns
MERCHANT_MIN_DAYS         = 30      # spread across at least this many days
MERCHANT_SCORE_PENALTY    = 0.30    # keep only 30% of original score

PAYROLL_AMOUNT_VARIANCE   = 0.20    # amounts within 20% of each other = payroll
PAYROLL_MIN_RECEIVERS     = 10      # minimum receivers on same day to check

MICRO_TXN_CYCLE_MAX       = 500     # cycles under this total amount = micro/test


def filter_false_positives(
    G            : nx.MultiDiGraph,
    df           : pd.DataFrame,
    all_rings    : list[dict],
    flagged_accs : dict,             # {account_id: {...score data...}}
) -> tuple[list[dict], dict]:
    """
    Takes the raw detection output and cleans it up.

    Returns:
      - cleaned_rings    : rings list with false positives removed
      - cleaned_accounts : account dict with scores adjusted
    """

    # ── Step 1: Identify legitimate accounts ─────────────────────────────────
    merchants = _find_merchants(G, df)
    payroll   = _find_payroll_accounts(G, df)

    print(f"[fp_filter] Merchants identified: {len(merchants)}")
    print(f"[fp_filter] Payroll accounts identified: {len(payroll)}")

    # ── Step 2: Filter rings ──────────────────────────────────────────────────
    cleaned_rings = []
    for ring in all_rings:

        # Remove micro-transaction cycles
        if ring["pattern_type"] == "cycle" and ring.get("total_amount", 0) < MICRO_TXN_CYCLE_MAX:
            print(f"[fp_filter] Removing micro-cycle {ring['ring_id']} (amount={ring['total_amount']})")
            continue

        # Remove rings where ALL members are known merchants or payroll
        legit_members = merchants | payroll
        suspicious_members = [m for m in ring["members"] if m not in legit_members]
        if not suspicious_members:
            print(f"[fp_filter] Removing ring {ring['ring_id']} — all members are legitimate accounts")
            continue

        cleaned_rings.append(ring)

    # ── Step 3: Adjust account scores ────────────────────────────────────────
    cleaned_accounts = {}
    for acc_id, acc_data in flagged_accs.items():

        score = acc_data["suspicion_score"]

        if acc_id in merchants:
            # Slash score heavily — it's probably a real business
            score = score * MERCHANT_SCORE_PENALTY
            acc_data["detected_patterns"].append("fp_merchant_downweight")

        elif acc_id in payroll:
            # Remove fan-out pattern, cut score
            acc_data["detected_patterns"] = [
                p for p in acc_data["detected_patterns"]
                if "fan_out" not in p
            ]
            score = score * 0.40
            acc_data["detected_patterns"].append("fp_payroll_downweight")

        acc_data["suspicion_score"] = round(score, 2)

        # Only keep accounts that are still above threshold after filtering
        if acc_data["suspicion_score"] >= 10:
            cleaned_accounts[acc_id] = acc_data

    print(f"[fp_filter] Accounts after filtering: {len(cleaned_accounts)}")
    return cleaned_rings, cleaned_accounts


# ─────────────────────────────────────────────────────────────────────────────
#  INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _find_merchants(G: nx.MultiDiGraph, df: pd.DataFrame) -> set:
    """
    Identifies likely merchant accounts:
      - Have MERCHANT_MIN_TRANSACTIONS or more total transactions
      - Those transactions span at least MERCHANT_MIN_DAYS days
    """
    merchants = set()

    all_accounts = set(df["sender_id"].unique()) | set(df["receiver_id"].unique())

    for acc in all_accounts:
        # Get all transactions involving this account
        acc_txns = df[(df["sender_id"] == acc) | (df["receiver_id"] == acc)]

        if len(acc_txns) < MERCHANT_MIN_TRANSACTIONS:
            continue

        # Check time spread
        time_range = acc_txns["timestamp"].max() - acc_txns["timestamp"].min()
        if time_range.days >= MERCHANT_MIN_DAYS:
            merchants.add(acc)

    return merchants


def _find_payroll_accounts(G: nx.MultiDiGraph, df: pd.DataFrame) -> set:
    """
    Identifies payroll-like accounts:
      - Single sender → many receivers (10+) on the same calendar day
      - All amounts within 20% of each other (consistent salary-like amounts)
    """
    payroll = set()

    # Group outgoing transactions by sender and day
    df_out = df.copy()
    df_out["date"] = df_out["timestamp"].dt.date

    grouped = df_out.groupby(["sender_id", "date"])

    for (sender, date), group in grouped:

        if len(group) < PAYROLL_MIN_RECEIVERS:
            continue

        unique_receivers = group["receiver_id"].nunique()
        if unique_receivers < PAYROLL_MIN_RECEIVERS:
            continue

        # Check if amounts are similar (payroll-like)
        amounts = group["amount"].values
        mean_amount = amounts.mean()
        if mean_amount == 0:
            continue

        # All amounts within PAYROLL_AMOUNT_VARIANCE of mean
        variance_ok = all(
            abs(a - mean_amount) / mean_amount <= PAYROLL_AMOUNT_VARIANCE
            for a in amounts
        )

        if variance_ok:
            payroll.add(sender)

    return payroll