import os
import json
import pandas as pd
from typing import Dict, Any, List

BASE_DIR = r"./GeoSQL-Eval/GeoSQL_Judgment_Knowledge_level_results"

SUMMARY_JSON_NAME = "eval_summary_judgment.json"

OUTPUT_XLSX_NAME = "summary_judgment.xlsx"


def load_summary_json(model_dir: str) -> Dict[str, Any]:
    path = os.path.join(model_dir, SUMMARY_JSON_NAME)
    if not os.path.isfile(path):
        raise FileNotFoundError(f"not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def collect_rows(base_dir: str) -> List[Dict[str, Any]]:
    rows = []
    for name in sorted(os.listdir(base_dir)):
        model_path = os.path.join(base_dir, name)
        if not os.path.isdir(model_path):
            continue
        try:
            data = load_summary_json(model_path)
            row = {"model": name}
            if isinstance(data, dict):
                row.update(data)
            else:
                row["parse_error"] = "summary is not a JSON object"
            rows.append(row)
        except Exception as e:
            rows.append({"model": name, "load_error": str(e)})
    return rows


def main():
    rows = collect_rows(BASE_DIR)
    if not rows:
        print("No model directories found.")
        return

    df = pd.DataFrame(rows)

    cols = ["model"] + [c for c in df.columns if c != "model"]
    df = df[cols]

    out_path = os.path.join(BASE_DIR, OUTPUT_XLSX_NAME)
    df.to_excel(out_path, index=False)
    print(f"Summary completed: {out_path}")
    print(f"Exported {len(df)} rows and {len(df.columns)} columns.")

if __name__ == "__main__":
    main()
