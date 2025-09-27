import json
import psycopg2
from tqdm import tqdm
from evaluate_execution import evaluate_sql_execution

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'postgres',
    'user': 'postgres',
    'password': '*******'
}

import os
model_name = os.environ.get("MODEL_NAME", "default-model")
base_dir = os.environ.get("BASE_DIR", r"./GeoSQL-Eval/GeoSQL_Syntax_Level_results")
input_path = os.path.join(base_dir, model_name, "predictions_deduplicated.jsonl")
output_path = os.path.join(base_dir, model_name, "predictions_execution_eval.jsonl")

def main():
    with open(input_path, 'r', encoding='utf-8') as fin:
        all_data = [json.loads(line) for line in fin]

    fout = open(output_path, 'w', encoding='utf-8')

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_client_encoding('UTF8')
        for item in tqdm(all_data, desc="eval", ncols=80):
            try:
                sql = item.get("pred_sql", "")
                expected = item.get("expected_result", None)

                eval_result = evaluate_sql_execution(
                    sql_text=sql,
                    db_conn=conn,
                    expected_result=expected
                )

                item.update(eval_result)

            except psycopg2.InterfaceError as conn_err:
                item["executable"] = False
                item["execution_error"] = f"try：{str(conn_err)}"
                item["result_correct"] = "error"
                item["result_comparison"] = {}

                try:
                    conn = psycopg2.connect(**DB_CONFIG)
                except Exception as re_conn_err:
                    item["execution_error"] += f"｜fail：{str(re_conn_err)}"

            except Exception as e:
                item["executable"] = False
                item["execution_error"] = str(e)
                item["result_correct"] = "error"
                item["result_comparison"] = {}

            fout.write(json.dumps(item, ensure_ascii=False) + '\n')

    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        fout.close()

    print(f"Complete：{output_path}")


if __name__ == "__main__":
    main()
