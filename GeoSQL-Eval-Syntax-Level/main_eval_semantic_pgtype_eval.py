import json
import psycopg2
from tqdm import tqdm
from evaluate_semantic_pgtype import evaluate_function_args_dynamic

import os
model_name = os.environ.get("MODEL_NAME", "default-model")
base_dir = os.environ.get("BASE_DIR", r"./GeoSQL-Eval/GeoSQL_Syntax_Level_results")
input_path = os.path.join(base_dir, model_name, "predictions_deduplicated.jsonl")
output_path = os.path.join(base_dir, model_name, "predictions_semantic_pgtype_eval.jsonl")
signature_path = r"./GeoSQL-Eval/GeoSQL-Bench/function_signatures.json"

# 加载函数签名
with open(signature_path, "r", encoding="utf-8") as f:
    function_signatures = json.load(f)

# 数据库连接
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="*******",
    host="localhost",
    port=5432
)
conn.autocommit = True
with open(input_path, "r", encoding="utf-8") as fin, open(output_path, "w", encoding="utf-8") as fout:
    for line in tqdm(fin, desc="Evaluating function param types"):
        item = json.loads(line)
        try:
            sql = item.get("pred_sql", "")
            function = item.get("function", "")
            eval_result = evaluate_function_args_dynamic(sql, function, conn, function_signatures)
            item.update(eval_result)
        except Exception as e:
            item["error"] = str(e)
        fout.write(json.dumps(item, ensure_ascii=False) + "\n")

conn.close()
print(f"\nParameter type semantic evaluation results have been saved to: {output_path}")

