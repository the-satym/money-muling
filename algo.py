# run.py
import json
from detection.engine import analyze, get_download_json
import pandas as pd
import time

# point this to your actual CSV file
start=time.time()
df = pd.read_csv("smart_dataset_10k.csv")
df.iloc[:1000,:].to_csv("sample.csv", index=False)
result = analyze("sample.csv")

# print summary
print(result["summary"])

# print top 20 suspicious accounts
for acc in result["suspicious_accounts"][:20]:
    print(acc)

# print all fraud rings
for ring in result["fraud_rings"]:
    print(ring)

# save full output to a JSON file to inspect
download = get_download_json(result)
with open("output.json", "w") as f:
    json.dump(download, f, indent=2, default=str)

print("\nDone. Check output.json for full results.")
end=time.time()
print(end-start)