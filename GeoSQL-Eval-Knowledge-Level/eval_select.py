import os
import json
import re
import subprocess
from typing import Dict, List, Any
from collections import defaultdict

BASE_DIR = r"./GeoSQL-Eval/GeoSQL_Select_Knowledge_level_results"

model_block = """
# model_name = "claude-3-7-sonnet"
# model_name = "DeepSeek-V3-0324"
# model_name = "DeepSeek-R1-0528"
# model_name = "gemini-2.5-flash"
# model_name = "gpt-4.1"
# model_name = "gpt-4.1-mini"
# model_name = "gpt-4o-mini"
# model_name = "gpt-4"
# model_name = "o4-mini"
# model_name = "qwq-32b"
# model_name = "gpt-5"
# model_name = "geocode-gpt-latest"
# model_name = "deepseek-coder-v2-16b"
# model_name = "gpt-oss-20b"
# model_name = "qwen2.5-coder-32b"
# model_name = "CodeS-3b"
# model_name = "CodeS-7b"
# model_name = "CodeS-15b"
# model_name = "codellama-13b"
# model_name = "XiYanSQL-7b"
# model_name = "XiYanSQL-14b"
# model_name = "XiYanSQL-32b"
# model_name = "qwen3-32b-think"
# model_name = "qwen3-32b-nothink"
"""
all_models: List[str] = [
    match.group(1)
    for line in model_block.strip().splitlines()
    if line.strip() and not line.strip().startswith("#")
    if (match := re.match(r'model_name\s*=\s*["\']([^"\']+)["\']', line.strip()))
]

def _normalize_type_key(t: str) -> str:
    t = str(t or "UNKNOWN").strip().upper()
    t = re.sub(r"[^A-Z0-9]+", "_", t)
    t = t.strip("_")
    return t or "UNKNOWN"

class MCQEvaluator:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir

    def run_clean(self, model_name: str):
        print(f"Running clean.py for {model_name} ...")
        env = os.environ.copy()
        env["MODEL_NAME"] = model_name
        env["BASE_DIR"] = self.base_dir
        result = subprocess.run(
            ["python", "clean.py"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=env,
        )
        if result.returncode != 0:
            print(f"clean.py error: {result.stderr}")
            raise RuntimeError("clean.py execution failed")
        else:
            out = (result.stdout or "").strip()
            if out:
                print(out)

    def load_predictions(self, model_name: str) -> List[Dict[str, Any]]:
        clean_file = os.path.join(self.base_dir, model_name, "predictions_cleaned.jsonl")
        self.run_clean(model_name)

        records: List[Dict[str, Any]] = []
        with open(clean_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return records

    def evaluate(self, model_name: str) -> Dict[str, Any]:

        records = self.load_predictions(model_name)

        total_correct = 0
        total_incorrect = 0
        by_type = defaultdict(lambda: {"correct": 0, "incorrect": 0})

        for record in records:
            if record.get("error"):
                continue

            pred_answer = str(record.get("pred_answer", "")).strip().upper()
            gold_answer = str(record.get("gold_answer", "")).strip().upper()
            if not pred_answer:
                continue

            pred_answer = ",".join(p.strip() for p in pred_answer.split(",") if p.strip())
            gold_answer = ",".join(g.strip() for g in gold_answer.split(",") if g.strip())

            is_correct = (pred_answer == gold_answer)
            tkey = _normalize_type_key(record.get("type", "UNKNOWN"))

            if is_correct:
                total_correct += 1
                by_type[tkey]["correct"] += 1
            else:
                total_incorrect += 1
                by_type[tkey]["incorrect"] += 1

        valid = total_correct + total_incorrect
        overall_accuracy = (total_correct / valid) if valid > 0 else 0.0

        summary: Dict[str, Any] = {
            "overall_correct_count": int(total_correct),
            "overall_incorrect_count": int(total_incorrect),
            "overall_accuracy": round(overall_accuracy, 6),
        }

        for tkey, vals in sorted(by_type.items()):
            t_valid = vals["correct"] + vals["incorrect"]
            t_acc = (vals["correct"] / t_valid) if t_valid > 0 else 0.0
            summary[f"{tkey}_correct_count"] = int(vals["correct"])
            summary[f"{tkey}_incorrect_count"] = int(vals["incorrect"])
            summary[f"{tkey}_accuracy"] = round(t_acc, 6)

        return summary

    def save_select_summary(self, model_name: str, summary: Dict[str, Any]) -> str:
        """保存仅含三项指标（按 type 展开）的结果文件：eval_summary_select.json"""
        model_dir = os.path.join(self.base_dir, model_name)
        os.makedirs(model_dir, exist_ok=True)
        output_path = os.path.join(model_dir, "eval_summary_select.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        return output_path


def main():
    evaluator = MCQEvaluator(BASE_DIR)

    for model_name in all_models:
        print(f"\nStart select (MCQ) evaluation for model: {model_name}")
        try:
            summary = evaluator.evaluate(model_name)
            out_path = evaluator.save_select_summary(model_name, summary)

            print("Evaluation Summary (MCQ, by type, flattened)")
            print("=" * 50)
            print(f"Model                        : {model_name}")
            print(f"Overall Correct Count        : {summary['overall_correct_count']}")
            print(f"Overall Incorrect Count      : {summary['overall_incorrect_count']}")
            print(f"Overall Accuracy             : {summary['overall_accuracy']:.2%}")
            print(f"Saved to                  : {out_path}")

            print("\nAccuracy by Type:")
            for k in sorted(summary.keys()):
                if k.endswith("_accuracy") and not k.startswith("overall_"):
                    tkey = k[:-len("_accuracy")]
                    c = summary.get(f"{tkey}_correct_count", 0)
                    ic = summary.get(f"{tkey}_incorrect_count", 0)
                    acc = summary[k]
                    print(f"- {tkey}: {acc:.2%} ({c}/{c+ic})")

        except Exception as e:
            print(f"Evaluation failed ({model_name}): {e}")
            continue

    print("\nAll select (MCQ) evaluations completed.")


if __name__ == "__main__":
    main()
