import json
import math
import statistics

import os
model_name = os.environ.get("MODEL_NAME", "default-model")

base_dir = os.environ.get("BASE_DIR", r"./GeoSQL-Eval/GeoSQL_Table_Schema_Level_results")
input_path = os.path.join(base_dir, model_name, "predictions_execution_eval.jsonl")
output_path = os.path.join(base_dir, model_name, "eval_summary_with_passn.json")

def compute_passn_metrics():
    with open(input_path, 'r', encoding='utf-8') as f:
        all_data = [json.loads(line) for line in f]

    group_size = 5
    num_groups = len(all_data) // group_size

    pass1_list = []
    pass3_list = []
    pass5_list = []

    for i in range(num_groups):
        group = all_data[i * group_size : (i + 1) * group_size]

        results = [item.get("result_correct") == "correct" for item in group]

        pass1 = any(results[:1])
        pass3 = any(results[:3])
        pass5 = any(results[:5])

        pass1_list.append(pass1)
        pass3_list.append(pass3)
        pass5_list.append(pass5)

    pass1_rate = round(sum(pass1_list) / num_groups, 4)
    pass3_rate = round(sum(pass3_list) / num_groups, 4)
    pass5_rate = round(sum(pass5_list) / num_groups, 4)

    metrics = [pass1_rate, pass3_rate, pass5_rate]
    mu = statistics.mean(metrics)
    sigma = statistics.pstdev(metrics)  # 总体标准差
    cv = round(sigma / mu, 4) if mu != 0 else float('inf')

    sa = 100*round(pass5_rate / (1 + cv), 4) if math.isfinite(cv) else 0.0

    # 输出统计
    print("\n===== Multi-Round Accuracy Evaluation =====")
    print(f"Number of samples (groups)       : {num_groups}")
    print(f"pass@1                           : {pass1_rate:.4f}")
    print(f"pass@3                           : {pass3_rate:.4f}")
    print(f"pass@5                           : {pass5_rate:.4f}")
    print(f"CV (std/mean of pass@5)          : {cv:.4f}")
    print(f"Stability-adjusted accuracy (SA) : {sa:.4f}")
    print("===========================================")

    summary = {
        "num_samples": num_groups,
        "pass@1": pass1_rate,
        "pass@3": pass3_rate,
        "pass@5": pass5_rate,
        "pass@5_std_cv": cv,
        "stability_adjusted_accuracy": sa
    }

    with open(output_path, 'w', encoding='utf-8') as fout:
        json.dump(summary, fout, indent=2)

    print(f"Multi-round accuracy metrics have been saved to: {output_path}")


if __name__ == "__main__":
    compute_passn_metrics()
