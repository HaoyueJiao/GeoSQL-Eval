# -*- coding: utf-8 -*-
import json
from pathlib import Path
from collections import Counter, defaultdict
import csv
import re
import sys

import pandas as pd

BASE_DIR = Path(r"./GeoSQL-Eval/GeoSQL_Syntax_Level_results")
JSONL_NAME = "error_classified.jsonl"
EXCEL_OUT = BASE_DIR / "error_type_summary_all_models.xlsx"

model_block = """
model_name = "claude-3-7-sonnet"
model_name = "DeepSeek-V3-0324"
model_name = "DeepSeek-R1-0528"
model_name = "gemini-2.5-flash"
model_name = "gpt-4.1"
model_name = "gpt-4.1-mini"
model_name = "gpt-4o-mini"
model_name = "gpt-4"
model_name = "o4-mini"
model_name = "qwq-32b"
model_name = "gpt-5"
model_name = "geocode-gpt-latest"
model_name = "deepseek-coder-v2-16b"
model_name = "gpt-oss-20b"
model_name = "qwen2.5-coder-32b"
model_name = "CodeS-3b"
model_name = "CodeS-7b"
model_name = "CodeS-15b"
model_name = "codellama-13b"
model_name = "XiYanSQL-7b"
model_name = "XiYanSQL-14b"
model_name = "XiYanSQL-32b"
model_name = "qwen3-32b-think"
model_name = "qwen3-32b-nothink"
"""

MODEL_NORMALIZATION = {
    "claude-3-7-sonnet": (1, "Claude3.7-Sonnet"),
    "codellama-13b": (3, "Code-Llama-13B"),
    "CodeS-15b": (5, "CodeS-15B"),
    "CodeS-3b": (5, "CodeS-3B"),
    "CodeS-7b": (5, "CodeS-7B"),
    "deepseek-coder-v2-16b": (3, "DeepSeek-Coder-V2-16B"),
    "DeepSeek-R1-0528": (2, "DeepSeek-R1-0528"),
    "DeepSeek-V3-0324": (1, "DeepSeek-V3-0324"),
    "gemini-2.5-flash": (2, "Gemini2.5-Flash-0520"),
    "geocode-gpt-latest": (4, "GeoCode-GPT-7B"),
    "gpt-4": (6, "SpatialSQL"),
    "gpt-4.1": (1, "GPT-4.1"),
    "gpt-4.1-mini": (1, "GPT-4.1-mini"),
    "gpt-4o-mini": (6, "Monkuu"),
    "gpt-5": (2, "GPT-5"),
    "gpt-oss-20b": (2, "GPT-OSS-20B"),
    "o4-mini": (2, "o4-mini"),
    "qwen2.5-coder-32b": (3, "Qwen2.5-Coder-32B"),
    "qwen3-32b-nothink": (1, "Qwen3-32B"),
    "qwen3-32b-think": (2, "Qwen3-32B-Thinking"),
    "qwq-32b": (2, "QwQ-32B"),
    "XiYanSQL-14b": (5, "XiYan-SQL-14B"),
    "XiYanSQL-32b": (5, "XiYan-SQL-32B"),
    "XiYanSQL-7b": (5, "XiYan-SQL-7B"),
}

ORDERED_NAMES = [
    # Category 1
    "Claude3.7-Sonnet", "DeepSeek-V3-0324", "GPT-4.1", "GPT-4.1-mini",
    "Qwen3-32B",
    # Category 2
    "DeepSeek-R1-0528", "Gemini2.5-Flash-0520", "GPT-5", "GPT-OSS-20B", "o4-mini",
    "Qwen3-32B-Thinking", "QwQ-32B",
    # Category 3
    "Code-Llama-13B", "DeepSeek-Coder-V2-16B",
    "Qwen2.5-Coder-32B",
    # Category 4
    "GeoCode-GPT-7B",
    # Category 5
    "CodeS-15B", "CodeS-3B", "CodeS-7B", "XiYan-SQL-14B", "XiYan-SQL-32B", "XiYan-SQL-7B",
    # Category 6
    "Monkuu", "SpatialSQL"
]
ORDER_INDEX = {name: i for i, name in enumerate(ORDERED_NAMES)}

def parse_active_models(block: str):
    pattern = re.compile(r'^\s*model_name\s*=\s*"([^"]+)"\s*$', re.UNICODE)
    models = []
    for line in block.splitlines():
        s = line.strip("\n")
        if not s.strip():
            continue
        if s.lstrip().startswith("#"):
            continue
        m = pattern.match(s)
        if m:
            models.append(m.group(1))
    if not models:
        raise ValueError("未在 model_block 中找到未注释的模型行。请至少保留一行未注释的 model_name = \"...\"")
    return models

def count_error_types(jsonl_path: Path) -> Counter:
    counter = Counter()
    if not jsonl_path.exists():
        raise FileNotFoundError(f"文件不存在: {jsonl_path}")
    with jsonl_path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                print(f"[WARN] 解析失败，跳过 {jsonl_path} 第 {i} 行", file=sys.stderr)
                continue
            et = obj.get("error_type")
            if et is not None and str(et).strip() != "":
                counter[str(et)] += 1
    return counter

def save_counts_csv(csv_path: Path, counts: Counter):
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["error_type", "count"])
        for k, v in counts.most_common():
            writer.writerow([k, v])

def norm_sort_key(category: int, name: str):
    return (category, ORDER_INDEX.get(name, 10**6))

def main():
    models = parse_active_models(model_block)
    print(f"Processing models (uncommented): {models}")

    long_rows = []
    pivot_map = defaultdict(dict)
    processed_norm_names = set()

    processed_any = False

    for raw_model in models:
        if raw_model not in MODEL_NORMALIZATION:
            print(f"[SKIP] Model not in normalization map, skipping: {raw_model}")
            continue
        category, norm_name = MODEL_NORMALIZATION[raw_model]
        jsonl_path = BASE_DIR / raw_model / JSONL_NAME
        try:
            counts = count_error_types(jsonl_path)
        except FileNotFoundError as e:
            print(f"[SKIP] {e}")
            continue
        except Exception as e:
            print(f"[SKIP] Error processing model {raw_model}: {e}", file=sys.stderr)
            continue

        processed_any = True
        processed_norm_names.add(norm_name)

        total = sum(counts.values())
        print("=" * 60)
        print(f"Model (raw): {raw_model}")
        print(f"Model (normalized): {category}|{norm_name}")
        print(f"File: {jsonl_path}")
        print(f"Entries with non-empty error_type: {total}")
        print("-" * 60)
        if counts:
            # Print and save per-model CSV
            for k, v in counts.most_common():
                print(f"{k}: {v}")
                long_rows.append({
                    "Category": category,
                    "Name": norm_name,
                    "error_type": k,
                    "count": v
                })
                pivot_map[k][norm_name] = v
        else:
            print("No non-empty error_type found.")
        print("=" * 60)

        per_model_csv = (BASE_DIR / raw_model / "error_type_counts.csv")
        save_counts_csv(per_model_csv, counts)
        print(f"[SAVED] {per_model_csv}")

    if not processed_any:
        print("No models successfully processed (paths may not exist, files may be missing, or none are in the normalization map).")
        sys.exit(0)

    long_rows_sorted = sorted(
        long_rows,
        key=lambda r: norm_sort_key(r["Category"], r["Name"])
    )
    all_csv = BASE_DIR / "ALL_error_type_counts.csv"
    with all_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Category", "Name", "error_type", "count"])
        for r in long_rows_sorted:
            writer.writerow([r["Category"], r["Name"], r["error_type"], r["count"]])
    print(f"[SAVED] Consolidated long table: {all_csv}")

    processed_names_sorted = sorted(
        processed_norm_names,
        key=lambda name: (
            min([r["Category"] for r in long_rows if r["Name"] == name]),
            ORDER_INDEX.get(name, 10**6)
        )
    )
    error_types_sorted = sorted(pivot_map.keys())
    pivot_csv = BASE_DIR / "ALL_error_type_pivot.csv"
    with pivot_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["error_type"] + processed_names_sorted)
        for et in error_types_sorted:
            row = [et] + [pivot_map[et].get(name, 0) for name in processed_names_sorted]
            writer.writerow(row)
    print(f"[SAVED] Pivot table: {pivot_csv}")

    error_types_sorted = sorted(pivot_map.keys())

    def _cat_of(name: str) -> int:
        return min(r["Category"] for r in long_rows if r["Name"] == name)

    processed_names_sorted = sorted(
        processed_norm_names,
        key=lambda name: (_cat_of(name), ORDER_INDEX.get(name, 10 ** 6))
    )

    excel_rows = []
    for name in processed_names_sorted:
        row = {
            "Category": _cat_of(name),
            "Name": name,
        }
        for et in error_types_sorted:
            row[et] = pivot_map[et].get(name, 0)
        excel_rows.append(row)

    df_excel = pd.DataFrame(excel_rows, columns=["Category", "Name"] + error_types_sorted)

    with pd.ExcelWriter(EXCEL_OUT, engine="openpyxl") as writer:
        df_excel.to_excel(writer, index=False, sheet_name="model_by_error_type")

    print(f"Excel (columns=Category/Name/Error Types; rows=models): {EXCEL_OUT}")



if __name__ == "__main__":
    main()
