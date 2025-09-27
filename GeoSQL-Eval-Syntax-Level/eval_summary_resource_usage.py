import os
import json

model_name = os.environ.get("MODEL_NAME", "default-model")
base_dir = os.environ.get("BASE_DIR", r"./GeoSQL-Eval/GeoSQL_Syntax_Level_results")
input_path = os.path.join(base_dir, model_name, "predictions_execution_eval.jsonl")
output_path = os.path.join(base_dir, model_name, "eval_summary_resource_usage.json")

with open(input_path, "r", encoding="utf-8") as f:
    lines = [json.loads(line.strip()) for line in f]

sample_count = len(lines)

durations = [item["duration"] for item in lines if "duration" in item]
tokens_list = [item["tokens_used"] for item in lines if "tokens_used" in item]

total_duration = sum(durations)
average_duration = total_duration / (sample_count - 1)
total_tokens = sum(tokens_list)
average_tokens = total_tokens / sample_count

summary = {
    "model_name": model_name,
    "sample_count": sample_count,
    "total_duration_sec": round(total_duration, 3),
    "average_duration_sec": round(average_duration, 3),
    "total_tokens_used": total_tokens,
    "average_tokens_per_sample": round(average_tokens, 3)
}

print("====== Evaluation Resource Usage Summary ======")
for k, v in summary.items():
    print(f"{k}: {v}")

with open(output_path, "w", encoding="utf-8") as f:
    json.dump(summary, f, indent=2, ensure_ascii=False)
print(f"\nSummary written to {output_path}")