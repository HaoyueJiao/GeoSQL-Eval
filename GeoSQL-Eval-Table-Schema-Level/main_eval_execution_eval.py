import json
import os
import psycopg2
from tqdm import tqdm
from evaluate_execution import evaluate_sql_execution

BASE_DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'password': '*******'
}

model_name = os.environ.get("MODEL_NAME", "default-model")
base_dir = os.environ.get("BASE_DIR", r"./GeoSQL-Eval/GeoSQL_Table_Schema_Level_results")

input_path = os.path.join(base_dir, model_name, "predictions_deduplicated_with_dbid.jsonl")
output_path = os.path.join(base_dir, model_name, "predictions_execution_eval.jsonl")

_conn_cache = {}

def get_connection_for_db(db_id):
    if db_id in _conn_cache:
        return _conn_cache[db_id]
    cfg = BASE_DB_CONFIG.copy()
    cfg['dbname'] = db_id
    conn = psycopg2.connect(**cfg)
    conn.set_client_encoding('UTF8')
    _conn_cache[db_id] = conn
    return conn

def main():
    with open(input_path, 'r', encoding='utf-8') as fin:
        all_data = [json.loads(line) for line in fin]

    with open(output_path, 'w', encoding='utf-8') as fout:
        for item in tqdm(all_data, desc="eval", ncols=80):
            try:
                pred_sql = item.get("pred_sql", "")
                gold_sql = item.get("gold_sql", "")
                expected = item.get("expected_result", None)
                db_id = item.get("db_id", "").strip()

                if not pred_sql or not gold_sql or not db_id:
                    item.update({
                        "executable": False,
                        "execution_error": "missing pred_sql / gold_sql / db_id",
                        "result_correct": "error",
                        "result_comparison": {}
                    })
                else:
                    conn = get_connection_for_db(db_id)
                    eval_result = evaluate_sql_execution(
                        sql_text=pred_sql,
                        db_conn=conn,
                        expected_result=expected,
                        gold_sql=gold_sql
                    )
                    item.update(eval_result)

            except psycopg2.InterfaceError as conn_err:
                item.update({
                    "executable": False,
                    "execution_error": f"connection error: {str(conn_err)}",
                    "result_correct": "error",
                    "result_comparison": {}
                })
                # 下次 get_connection_for_db 会重新连接
                _conn_cache.pop(db_id, None)

            except Exception as e:
                item.update({
                    "executable": False,
                    "execution_error": str(e),
                    "result_correct": "error",
                    "result_comparison": {}
                })

            fout.write(json.dumps(item, ensure_ascii=False) + '\n')

    # 最后关闭所有缓存连接
    for conn in _conn_cache.values():
        try:
            conn.close()
        except:
            pass

    print(f"Complete: {output_path}")

if __name__ == "__main__":
    main()
