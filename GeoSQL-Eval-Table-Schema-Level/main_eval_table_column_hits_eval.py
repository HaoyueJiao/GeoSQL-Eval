# -*- coding: utf-8 -*-
import json
import os
from collections import defaultdict

MODEL_NAME = os.environ.get("MODEL_NAME", "default-model")
BASE_DIR   = os.environ.get("BASE_DIR", r"./GeoSQL-Eval/GeoSQL_Table_Schema_Level_results")

GEN_OUTPUT_PATH         = os.path.join(BASE_DIR, MODEL_NAME, "predictions_deduplicated_with_dbid.jsonl")
SCHEMA_DATASET_PATH     = r"./GeoSQL-Eval/GeoSQL-Bench/Table_Schema_Retrieval_Question_Explicit.jsonl"
GOLD_PICKED_PATH        = r"./GeoSQL-Eval/GeoSQL-Bench/Table_Schema_Retrieval_Question_table&column_picked.jsonl"
PRED_PICKED_PATH        = os.path.join(BASE_DIR, MODEL_NAME, "predictions_output_picked.jsonl")
SUMMARY_RESULT_PATH     = os.path.join(BASE_DIR, MODEL_NAME, "eval_summary_table_column_hits.json")

from pick_by_tableschema import process_record

def load_jsonl(path):
    with open(path, 'r', encoding='utf-8') as f:
        return [json.loads(line) for line in f if line.strip()]

def save_jsonl(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        for item in data:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')

def _norm(name):
    return (name or "").strip().strip('"').lower()

def build_schema_map(schema_dataset_items):
    out = {}
    for it in schema_dataset_items:
        nid = it.get("new_id")
        schema_text = it.get("schema", "")
        if nid is not None and schema_text:
            out[nid] = schema_text
    return out

def extract_from_predictions(gen_items, id2schema):
    extracted = []
    for it in gen_items:
        _id = it.get("id")
        sql = it.get("pred_sql")
        db_id = it.get("db_id")
        if _id is None or not sql:
            continue

        schema_text = id2schema.get(_id)
        if not schema_text:
            extracted.append({
                "id": _id,
                "db_id": db_id,
                "error": "Missing schema_text for this id"
            })
            continue

        payload = {
            "new_id": _id,
            "db_id": db_id,
            "query": sql,
            "schema": schema_text
        }
        try:
            res = process_record(payload)
            res["id"] = _id
            if "new_id" in res:
                res.pop("new_id", None)
            extracted.append(res)
        except Exception as e:
            extracted.append({
                "id": _id,
                "db_id": db_id,
                "error": f"{type(e).__name__}: {e}"
            })
    return extracted

def build_lookup_map(data, prefer_id=True):
    out = {}
    for item in data:
        key = item.get("id") if prefer_id else None
        if key is None:
            key = item.get("new_id")
        if key is None or "tables" not in item:
            continue
        table_map = defaultdict(set)
        for t in item["tables"]:
            tname = _norm(t.get("table"))
            if not tname:
                continue
            for c in t.get("columns", []):
                table_map[tname].add(_norm(c))
        out[key] = {k: sorted(v) for k, v in table_map.items()}
    return out

def compute_summary_hit_rate(pred_map, gold_map):
    table_hit_total = 0
    table_total = 0
    column_hit_total = 0
    column_total = 0
    matched = 0

    for key, gold_tables in gold_map.items():
        pred_tables = pred_map.get(key)
        if not pred_tables:
            continue
        matched += 1

        gold_tbl_set = set(gold_tables.keys())
        pred_tbl_set = set(pred_tables.keys())

        table_hit_total += len(gold_tbl_set & pred_tbl_set)
        table_total += len(gold_tbl_set)

        for t, gold_cols in gold_tables.items():
            gold_col_set = set(gold_cols)
            pred_col_set = set(pred_tables.get(t, []))
            column_hit_total += len(gold_col_set & pred_col_set)
            column_total += len(gold_col_set)

    return {
        "table_hit_rate": round(table_hit_total / table_total, 4) if table_total else 1.0,
        "column_hit_rate": round(column_hit_total / column_total, 4) if column_total else 1.0,
        "total_gold_items": len(gold_map),
        "matched_items": matched,
        "table_hit_count": table_hit_total,
        "table_total_count": table_total,
        "column_hit_count": column_hit_total,
        "column_total_count": column_total
    }

def main():
    print("Loading prediction file:", GEN_OUTPUT_PATH)
    gen_items = load_jsonl(GEN_OUTPUT_PATH)

    print("Loading Schema dataset (with new_id, schema):", SCHEMA_DATASET_PATH)
    schema_items = load_jsonl(SCHEMA_DATASET_PATH)
    id2schema = build_schema_map(schema_items)  # new_id -> schema

    print("Extracting table/column structures based on pred_sql (reusing process_record)…")
    pred_picked = extract_from_predictions(gen_items, id2schema)
    save_jsonl(PRED_PICKED_PATH, pred_picked)
    print("Prediction extraction results saved:", PRED_PICKED_PATH)

    print("Loading gold standard (already extracted):", GOLD_PICKED_PATH)
    gold_picked = load_jsonl(GOLD_PICKED_PATH)

    print("Calculating hit rate (tables / columns)…")
    # Predictions use id; gold extracted file usually uses new_id
    pred_map = build_lookup_map(pred_picked, prefer_id=True)
    gold_map = build_lookup_map(gold_picked, prefer_id=False)

    summary = compute_summary_hit_rate(pred_map, gold_map)

    os.makedirs(os.path.dirname(SUMMARY_RESULT_PATH), exist_ok=True)
    with open(SUMMARY_RESULT_PATH, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"✅ Hit rate statistics completed, results saved to: {SUMMARY_RESULT_PATH}")
    print("📈 Overall summary results:", summary)


if __name__ == "__main__":
    main()
