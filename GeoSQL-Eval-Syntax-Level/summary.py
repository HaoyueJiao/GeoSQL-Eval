import os
import json
import pandas as pd
from glob import glob

base_dir = r"./GeoSQL-Eval/GeoSQL_Syntax_Level_results"
output_excel = os.path.join(base_dir, "evaluation_summary_all_models.xlsx")

summary_files = {
    "execution": "eval_summary_execution.json",
    "semantic_pgtype": "eval_summary_semantic_pgtype.json",
    "with_passn": "eval_summary_with_passn.json",
    "resource": "eval_summary_resource_usage.json"
}

records = []

for model_dir in os.listdir(base_dir):
    model_path = os.path.join(base_dir, model_dir)
    if not os.path.isdir(model_path):
        continue

    record = {"model": model_dir}

    for prefix, filename in summary_files.items():
        file_path = os.path.join(model_path, filename)
        if not os.path.exists(file_path):
            continue
        with open(file_path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                for key, value in data.items():
                    record[f"{prefix}.{key}"] = value
            except Exception as e:
                print(f"Failed to load {file_path}: {e}")

    records.append(record)

# 写入 Excel
df = pd.DataFrame(records)
df.to_excel(output_excel, index=False)

print(f"\nSummary completed, results saved to: {output_excel}")

