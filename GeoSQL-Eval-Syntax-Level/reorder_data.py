import json
from collections import defaultdict
import os

model_name = os.environ.get("MODEL_NAME", "default-model")
base_dir = os.environ.get("BASE_DIR", r"./GeoSQL-Eval/GeoSQL_Syntax_Level_results")
INPUT_PATH = os.path.join(base_dir, model_name, "predictions.jsonl")
OUTPUT_PATH = os.path.join(base_dir, model_name, "predictions_reorder.jsonl")

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
    print(f"All groups are already sorted by round, no changes needed: {OUTPUT_PATH}")

else:
    all_sorted = []
    for group_key in sorted(grouped.keys()):
        group_items = grouped[group_key]
        group_items_sorted = sorted(group_items, key=lambda x: x['round'])
        all_sorted.extend(group_items_sorted)

    # ===== 写回新文件 =====
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        for entry in all_sorted:
            f.write(json.dumps(entry, ensure_ascii=False) + '\n')
    print(f"Sorting completed, results saved to: {OUTPUT_PATH}")

