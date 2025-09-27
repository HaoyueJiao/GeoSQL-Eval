import json
from collections import defaultdict
import os
import shutil

model_name = os.environ.get("MODEL_NAME", "Qwen3-32B")
base_dir = os.environ.get("BASE_DIR", r"./GeoSQL-Eval/GeoSQL_Table_Schema_Level_results")
INPUT_PATH = os.path.join(base_dir, model_name, "predictions.jsonl")
OUTPUT_PATH = os.path.join(base_dir, model_name, "predictions_reorder.jsonl")

os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

with open(INPUT_PATH, 'r', encoding='utf-8') as f:
    data = [json.loads(line) for line in f if line.strip()]

grouped = defaultdict(list)
for item in data:
    key = (item['id'], item['function'], item['question'])
    grouped[key].append(item)

all_sorted_already = True
for group_items in grouped.values():
    rounds = [item['round'] for item in group_items]
    if rounds != sorted(rounds):
        all_sorted_already = False
        break

if all_sorted_already:
    shutil.copyfile(INPUT_PATH, OUTPUT_PATH)
    print(f"All groups have been sorted by round, original file copied to: {OUTPUT_PATH}")

else:
    all_sorted = []
    for group_key in sorted(grouped.keys()):
        group_items = grouped[group_key]
        group_items_sorted = sorted(group_items, key=lambda x: x['round'])
        all_sorted.extend(group_items_sorted)

    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        for entry in all_sorted:
            f.write(json.dumps(entry, ensure_ascii=False) + '\n')

    print(f"Sorting completed, results saved to: {OUTPUT_PATH}")

