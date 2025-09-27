import os
import json
import time
import hashlib
from tqdm import tqdm
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from tqdm.auto import tqdm
from call_language_model import call_language_model


INPUT_PATH = r"./GeoSQL-Eval/GeoSQL-Bench/Table_Schema_Retrieval_Question_Explicit.jsonl"
# INPUT_PATH = r"./GeoSQL-Eval/GeoSQL-Bench/Table_Schema_Retrieval_Question_Underspecified.jsonl"
OUTPUT_DIR = r"./GeoSQL-Eval/GeoSQL_Table_Schema_Level_results"
CONFIG_PATH = r"./llm_config.yaml"

MODELS_TO_TEST = [
    # {'provider': 'ollama', 'name': 'qwen2.5-coder:32b', 'name_simple': 'qwen2.5-coder-32b'},
    # {'provider': 'ollama', 'name': 'codellama:7b', 'name_simple': 'codellama-7b'},
    # {'provider': 'ollama', 'name': 'geocode-gpt:latest', 'name_simple': 'geocode-gpt-latest'},
    # {'provider': 'ollama', 'name': 'deepseek-coder-v2:16b', 'name_simple': 'deepseek-coder-v2-16b'},
    # {'provider': 'ollama', 'name': 'gpt-oss:20b', 'name_simple': 'gpt-oss-20b'},
    # {'provider': 'ollama', 'name': 'CodeS:3b', 'name_simple': 'CodeS-3b'},
    # {'provider': 'ollama', 'name': 'qwen3:32b', 'name_simple': 'qwen3-32b-think'},
    # {'provider': 'ollama', 'name': 'qwen3:32b', 'name_simple': 'qwen3-32b-nothink'},
    # {'provider': 'ollama', 'name': 'CodeS:7b', 'name_simple': 'CodeS-7b'},
    # {'provider': 'ollama', 'name': 'CodeS:15b', 'name_simple': 'CodeS-15b'},
    # {'provider': 'ollama', 'name': 'XiYanSQL:7b', 'name_simple': 'XiYanSQL-7b'},
    # {'provider': 'ollama', 'name': 'XiYanSQL:14b', 'name_simple': 'XiYanSQL-14b'},
    # {'provider': 'ollama', 'name': 'XiYanSQL:32b', 'name_simple': 'XiYanSQL-32b'},
    # {'provider': 'JHY', 'name': 'o4-mini', 'name_simple': 'o4-mini'},
    # {'provider': 'JHY', 'name': 'gpt-4.1-2025-04-14', 'name_simple': 'gpt-4.1'},
    # {'provider': 'JHY', 'name': 'gpt-4', 'name_simple': 'gpt-4'},
    # {'provider': 'JHY', 'name': 'gpt-4o-mini', 'name_simple': 'gpt-4o-mini'},
    # {'provider': 'JHY', 'name': 'gpt-4.1-mini-2025-04-14', 'name_simple': 'gpt-4.1-mini'},
    # {'provider': 'JHY', 'name': 'claude-3-7-sonnet-20250219', 'name_simple': 'claude-3-7-sonnet'},
    # {'provider': 'JHY', 'name': 'gemini-2.5-flash', 'name_simple': 'gemini-2.5-flash'},
    # {'provider': 'JHY', 'name': 'deepseek-ai/DeepSeek-R1-0528', 'name_simple': 'DeepSeek-R1-0528'},
    # {'provider': 'JHY', 'name': 'deepseek-v3-250324', 'name_simple': 'DeepSeek-V3-0324'},
    # {'provider': 'JHY', 'name': 'Qwen/QwQ-32B', 'name_simple': 'qwq-32b'},
    {'provider': 'JHY', 'name': 'gpt-5-2025-08-07', 'name_simple': 'gpt-5'},
]

NUM_ROUNDS = 5
MAX_WORKERS = 1
TEMPERATURE = 0.2
MAX_TOKENS = 12288

SYSTEM_PROMPT = "You are a helpful assistant for generating executable PostGIS SQL statements."

# ==== 函数 ====
def load_dataset(path: str) -> List[Dict]:
    with open(path, 'r', encoding='utf-8') as f:
        return [json.loads(line) for line in f if line.strip()]

def build_prompt(item: Dict) -> str:
    question = item.get('question_en') or item.get('question') or ''
    schema_text = item.get('schema') or ''

    return f"""
You are a PostGIS expert.

Below is the database schema and a few sample rows. Learn it and answer the task with ONE valid SQL query.

Database schema & samples:
{schema_text}

Task:
{question}

Rules:
- Return ONLY a complete executable SQL query (no explanation, no Markdown fences).
- Use PostGIS functions where appropriate.
- Do NOT create tables or insert data unless the task explicitly requires.
- Assume SRIDs exactly as in the schema; cast when needed.
""".strip()


def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path)

def make_unique_key(item: Dict, round_id: int) -> str:
    func_ids = item.get('metadata', {}).get('function_ids')

    if isinstance(func_ids, list):
        func_ids_str = ",".join(str(x) for x in func_ids)
    else:
        func_ids_str = str(func_ids)

    # Prefer new_id if present, otherwise id
    nid = item.get('new_id', item.get('id'))
    q_en = item.get('question_en') or item.get('question') or ''

    raw = f"{nid}__{func_ids_str}__{q_en}__{round_id}"
    return hashlib.md5(raw.encode('utf-8')).hexdigest()


def load_existing_keys(path: str) -> set:
    existing = set()
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    if 'unique_key' in obj:
                        existing.add(obj['unique_key'])
                except:
                    continue
    return existing

import threading

def run_single_prediction(item: Dict, model_cfg: Dict, round_id: int) -> Dict:
    thread_id = threading.get_ident()
    start_time = time.time()

    user_prompt = build_prompt(item)
    sql_text, tokens, error = call_language_model(
        model_provider=model_cfg['provider'],
        model_name=model_cfg['name'],
        system_prompt=SYSTEM_PROMPT,
        user_prompt=user_prompt,
        enable_thinking=False,
        stream=False,
        temperature=TEMPERATURE,
        max_tokens=MAX_TOKENS,
        config_path=CONFIG_PATH
    )

    end_time = time.time()
    duration = end_time - start_time

    if sql_text:
        if sql_text.startswith("```sql"):
            sql_text = sql_text[6:].strip()
        elif sql_text.startswith("```"):
            sql_text = sql_text[3:].strip()
        if sql_text.endswith("```"):
            sql_text = sql_text[:-3].strip()
        sql_text = sql_text.strip('"').strip("'")

    return {
        "id": item["new_id"],
        "function": item.get("metadata", {}).get("function_ids"),  # <- 这里改了
        "question": item["question_en"],
        "gold_sql": item["query"],
        "pred_sql": sql_text,
        "model": model_cfg['name_simple'],
        "round": round_id,
        "error": error,
        "tokens_used": tokens,
        "timestamp": end_time,
        "start_time": start_time,
        "duration": duration,
        "thread_id": thread_id,
        "unique_key": make_unique_key(item, round_id)
    }

def run_model_predictions(model_cfg: Dict, dataset: List[Dict]):
    model_output_dir = os.path.join(OUTPUT_DIR, model_cfg['name_simple'])
    ensure_dir(model_output_dir)
    output_path = os.path.join(model_output_dir, 'predictions.jsonl')

    existing_keys = load_existing_keys(output_path)
    buffer = []
    buffer_lock = threading.Lock()

    total_tasks = len(dataset) * NUM_ROUNDS
    pbar = tqdm(total=total_tasks, desc=f"{model_cfg['name_simple']}", ncols=100)

    def process_single(item, r):
        unique_key = make_unique_key(item, r)
        if unique_key in existing_keys:
            pbar.update(1)
            return
        try:
            result = run_single_prediction(item, model_cfg, r)
            with buffer_lock:
                buffer.append(result)
                if len(buffer) >= 10:
                    with open(output_path, 'a', encoding='utf-8') as f:
                        for entry in buffer:
                            f.write(json.dumps(entry, ensure_ascii=False) + '\n')
                        buffer.clear()
        except Exception as e:
            print(f"Error in model {model_cfg['name_simple']} (id={item['id']}, round={r}): {e}")
        finally:
            pbar.update(1)

    # 多线程调度每条 item+round
    with ThreadPoolExecutor(max_workers=64) as executor:
        futures = []
        for item in dataset:
            for r in range(1, NUM_ROUNDS + 1):
                futures.append(executor.submit(process_single, item, r))
        for _ in as_completed(futures):
            pass

    # 写入剩余 buffer
    if buffer:
        with open(output_path, 'a', encoding='utf-8') as f:
            for entry in buffer:
                f.write(json.dumps(entry, ensure_ascii=False) + '\n')
    pbar.close()
# ==== 主程序入口 ====
def main():
    dataset = load_dataset(INPUT_PATH)
    print(f"Loaded {len(dataset)} examples from dataset.")
    
    for model_cfg in MODELS_TO_TEST:
        run_model_predictions(model_cfg,dataset)
    print("\nAll models finished generating SQL predictions.")

if __name__ == '__main__':
    main()
