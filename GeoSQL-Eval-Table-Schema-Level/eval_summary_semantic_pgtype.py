import json
from collections import defaultdict
import os
model_name = os.environ.get("MODEL_NAME", "default-model")

base_dir = os.environ.get("BASE_DIR", r"./GeoSQL-Eval/GeoSQL_Table_Schema_Level_results")
input_path = os.path.join(base_dir, model_name, "predictions_semantic_pgtype_eval.jsonl")
output_path = os.path.join(base_dir, model_name, "eval_summary_semantic_pgtype.json")
total = 0
count_structure_ok = 0
count_func_hit = 0
match_ratios = []

error_types = defaultdict(int)

with open(input_path, "r", encoding="utf-8") as f:
    for line in f:
        data = json.loads(line)
        total += 1
        if data.get("structure_valid"):
            count_structure_ok += 1
        if data.get("function_hit"):
            count_func_hit += 1
        if isinstance(data.get("param_type_match_ratio"), float):
            match_ratios.append(data["param_type_match_ratio"])
        if "error" in data and data["error"]:
            error_types[data["error"].split(":")[0].strip()] += 1

summary = {
    "total": total,
    "structure_valid_ratio": round(count_structure_ok / total, 4),
    "function_hit_ratio": round(count_func_hit / total, 4),
    "avg_param_type_match_ratio": round(sum(match_ratios) / len(match_ratios), 4) if match_ratios else 0.0
}

print("===== Semantic Param Type Eval Summary =====")
for k, v in summary.items():
    print(f"{k:<30} : {v}")

with open(output_path, "w", encoding="utf-8") as f:
    json.dump(summary, f, indent=2, ensure_ascii=False)

print(f"\nStatistics have been saved to: {output_path}")

