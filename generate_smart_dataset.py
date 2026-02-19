import pandas as pd
import random
from datetime import datetime, timedelta
import json

# --- Configuration ---
NUM_TRANSACTIONS = 10000
START_DATE = datetime(2026, 2, 1)

# 1. Generate a pool of realistic, anonymous Account IDs (e.g., ACC_54921)
all_ids = [f"ACC_{random.randint(10000, 99999)}" for _ in range(5000)]
random.shuffle(all_ids)

# 2. Allocate IDs for different behaviors secretly
regular_users = all_ids[:4000]
payroll_accounts = all_ids[4000:4020]  # False Positives (Legitimate Fan-out)
merchant_accounts = all_ids[4020:4050]  # False Positives (Legitimate Fan-in)
mule_pool = all_ids[4050:]  # Reserved for complex fraud patterns

transactions = []
tx_counter = 1

# This dictionary keeps track of the "answers" so you can grade your algorithm
ground_truth = {
    "fraud_rings": [],
    "false_positives_to_ignore": {
        "payroll_accounts": payroll_accounts,
        "merchant_accounts": merchant_accounts
    }
}


def add_tx(sender, receiver, amount, timestamp):
    global tx_counter
    transactions.append([
        f"TX_{tx_counter:07d}", sender, receiver,
        round(amount, 2), timestamp.strftime("%Y-%m-%d %H:%M:%S")
    ])
    tx_counter += 1


def random_date():
    return START_DATE + timedelta(days=random.randint(0, 20), minutes=random.randint(0, 1440))


# --- GENERATION PHASE ---

# A. Generate Innocent Noise (Regular P2P Transfers)
for _ in range(7000):
    sender, receiver = random.sample(regular_users, 2)
    add_tx(sender, receiver, random.uniform(10, 5000), random_date())

# B. Trap: Payroll Accounts (Legitimate Fan-Out)
# They send large amounts ($3k-$8k) to 20-50 people exactly on the 1st or 15th
for payroll_acc in payroll_accounts:
    pay_day = START_DATE + timedelta(days=random.choice([0, 14]))
    for _ in range(random.randint(20, 50)):
        employee = random.choice(regular_users)
        add_tx(payroll_acc, employee, random.uniform(3000, 8000), pay_day + timedelta(minutes=random.randint(0, 120)))

# C. Trap: Merchant Accounts (Legitimate Fan-In)
# They receive many small payments ($15-$300) scattered randomly over time
for merchant in merchant_accounts:
    for _ in range(random.randint(30, 80)):
        customer = random.choice(regular_users)
        add_tx(customer, merchant, random.uniform(15, 300), random_date())

# D. Smart Fraud: High-Value Cycles (3 to 5 Hops)
mule_idx = 0
for i in range(25):
    cycle_length = random.randint(3, 5)
    cycle_nodes = [mule_pool[mule_idx + j] for j in range(cycle_length)]
    mule_idx += cycle_length

    amount = random.uniform(25000, 150000)  # Big criminal money
    base_time = random_date()

    for j in range(cycle_length):
        sender = cycle_nodes[j]
        receiver = cycle_nodes[(j + 1) % cycle_length]
        # Transactions happen in rapid succession (every 30 mins)
        add_tx(sender, receiver, amount, base_time + timedelta(minutes=j * 30))
        amount *= random.uniform(0.95, 0.99)  # 1-5% fee dropped at each hop

    ground_truth["fraud_rings"].append({
        "ring_id": f"RING_CYCLE_{i + 1}",
        "pattern": "cycle",
        "nodes": cycle_nodes
    })

# E. Smart Fraud: Rapid Smurfing (Fan-Out)
for i in range(15):
    distributor = mule_pool[mule_idx]
    mule_idx += 1
    base_time = random_date()
    receivers = []

    # Sends $8k-$9.5k to 15-25 accounts in a very tight time window (minutes apart)
    for j in range(random.randint(15, 25)):
        receiver = mule_pool[mule_idx]
        receivers.append(receiver)
        mule_idx += 1
        add_tx(distributor, receiver, random.uniform(8000, 9500), base_time + timedelta(minutes=j * 5))

    ground_truth["fraud_rings"].append({
        "ring_id": f"RING_FANOUT_{i + 1}",
        "pattern": "fan_out",
        "distributor_node": distributor,
        "receiver_nodes": receivers
    })

# F. Fill remainder with noise to hit exactly 10,000 transactions
while len(transactions) < NUM_TRANSACTIONS:
    sender, receiver = random.sample(regular_users, 2)
    add_tx(sender, receiver, random.uniform(10, 5000), random_date())

# --- EXPORT PHASE ---

# 1. Shuffle heavily so patterns aren't just sequential blocks in the CSV
random.shuffle(transactions)
transactions = transactions[:NUM_TRANSACTIONS]

# 2. Save the blinded CSV for your engine
df = pd.DataFrame(transactions, columns=["transaction_id", "sender_id", "receiver_id", "amount", "timestamp"])
df.to_csv("smart_dataset_10k.csv", index=False)

# 3. Save the Answer Key so you can grade your engine
with open("ground_truth_key.json", "w") as f:
    json.dump(ground_truth, f, indent=4)

print("✅ Created smart_dataset_10k.csv (Feed this to your app)")
print("✅ Created ground_truth_key.json (Use this to check your accuracy)")