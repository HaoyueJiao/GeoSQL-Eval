import json
import os

model_name = os.environ.get("MODEL_NAME", "default-model")

base_dir = os.environ.get("BASE_DIR", r"./GeoSQL-Eval/GeoSQL_Table_Schema_Level_results")
input_file = os.path.join(base_dir, model_name, "predictions_cleaned.jsonl")
output_file = os.path.join(base_dir, model_name, "predictions_deduplicated.jsonl")

# Store already seen unique_key
seen_keys = set()

# Start processing
with open(input_file, 'r', encoding='utf-8') as fin, open(output_file, 'w', encoding='utf-8') as fout:
    for line in fin:
        try:
            data = json.loads(line)
            key = data.get("unique_key")
            if key and key not in seen_keys:
                seen_keys.add(key)
                fout.write(json.dumps(data, ensure_ascii=False) + '\n')
        except json.JSONDecodeError as e:
            print(f"[Skipped] Failed to parse line: {line.strip()}\nError: {e}")

print(f"Deduplication completed, {len(seen_keys)} records retained. Output file: {output_file}")
