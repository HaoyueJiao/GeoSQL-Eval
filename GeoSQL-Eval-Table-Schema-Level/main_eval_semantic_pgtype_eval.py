# -*- coding: utf-8 -*-
import json
import os
import psycopg2
from tqdm import tqdm
from evaluate_semantic_pgtype import evaluate_function_args_dynamic

model_name = os.environ.get("MODEL_NAME", "default-model")
base_dir = os.environ.get("BASE_DIR", r"./GeoSQL-Eval/GeoSQL_Table_Schema_Level_results")

input_path  = os.path.join(base_dir, model_name, "predictions_deduplicated_with_dbid.jsonl")
output_path = os.path.join(base_dir, model_name, "predictions_semantic_pgtype_eval.jsonl")

schema_dataset_path = r"./GeoSQL-Eval/GeoSQL-Bench/Table_Schema_Retrieval_Question_Explicit.jsonl"

# 函数签名文件
signature_path = r"./GeoSQL-Eval/GeoSQL-Bench/function_signatures.json"

def load_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)

def build_id_to_function_map(schema_dataset_path):
    mapping = {}
    missing = 0
    for rec in load_jsonl(schema_dataset_path):
        new_id = rec.get("new_id")
        meta = rec.get("metadata", {}) or {}
        func_name = meta.get("function_name") or meta.get("function")
        if new_id is not None and func_name:
            mapping[new_id] = func_name
        else:
            missing += 1
    return mapping

def main():
    with open(signature_path, "r", encoding="utf-8") as f:
        function_signatures = json.load(f)

    id2func = build_id_to_function_map(schema_dataset_path)

    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="*******",
        host="localhost",
        port=5432
    )
    conn.autocommit = True

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(input_path, "r", encoding="utf-8") as fin, open(output_path, "w", encoding="utf-8") as fout:
        for line in tqdm(fin, desc="Evaluating function param types"):
            item = json.loads(line)
            try:
                sql = item.get("pred_sql", "") or ""
                sample_id = item.get("id")

                function_name = None
                if sample_id is not None:
                    function_name = id2func.get(sample_id)

                if not function_name:
                    function_name = item.get("function_name") or item.get("function")

                if not sql:
                    raise ValueError("Missing pred_sql")
                if not function_name:
                    raise ValueError("Missing function_name (id->new_id mapping or fallback failed)")

                eval_result = evaluate_function_args_dynamic(sql, function_name, conn, function_signatures)

                item["function_name_used"] = function_name
                item.update(eval_result)

            except Exception as e:
                item["error"] = f"{type(e).__name__}: {e}"

            fout.write(json.dumps(item, ensure_ascii=False) + "\n")

    conn.close()
    print(f"\n已保存参数类型语义评估结果至：{output_path}")

if __name__ == "__main__":
    main()
