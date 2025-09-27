import os
import subprocess
import re
from collections import OrderedDict

scripts = [
    "reorder_data.py",
    "clean.py",
    "deduplicate.py",
    "main_eval_execution_eval.py",
    "main_eval_semantic_pgtype_eval.py",
    "eval_summary_with_passn.py",
    "eval_summary_execution.py",
    "eval_summary_semantic_pgtype.py",
    "eval_summary_resource_usage.py",
]

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

models = []
pattern = re.compile(r'^model_name\s*=\s*["\']([^"\']+)["\']\s*$')

for raw in model_block.strip().splitlines():
    line = raw.strip()
    if not line or line.startswith("#"):
        continue
    m = pattern.match(line)
    if m:
        models.append(m.group(1))

models = list(OrderedDict.fromkeys(models))

if not models:
    raise SystemExit("No uncommented model_name was parsed, please check model_block.")

BASE_DIR = r"./GeoSQL-Eval/GeoSQL_Syntax_Level_results"

for model_name in models:
    os.environ["MODEL_NAME"] = model_name
    os.environ["BASE_DIR"] = BASE_DIR

    print(f"\nStart full evaluation for model: {model_name}")
    for script in scripts:
        print(f"Running script: {script}")
        result = subprocess.run(
            ["python", script],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        print(result.stdout)
        if result.returncode != 0:
            print(f"Error in script: {script}")
            print(result.stderr)
            break
    else:
        print(f"Evaluation finished for model: {model_name}")

print("\nAll evaluations completed.")
