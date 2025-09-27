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

INPUT_PATH = r"./GeoSQL-Eval/GeoSQL-Bench/TMultiple_Choice.jsonl"
OUTPUT_DIR = r"./GeoSQL-Eval/GeoSQL_Select_Knowledge_level_results"
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

NUM_ROUNDS = 1
MAX_WORKERS = 1
TEMPERATURE = 0.2
MAX_TOKENS = 1024

SYSTEM_PROMPT = """You are an expert in PostGIS knowledge assessment. Carefully read the question and select ONLY the correct option letter (A/B/C/D). 
Respond with exactly ONE uppercase letter (no explanations, no formatting)."""

def load_dataset(path: str) -> List[Dict]:
    with open(path, 'r', encoding='utf-8') as f:
        return [json.loads(line) for line in f if line.strip()]

def build_prompt(item: Dict) -> str:
    options_str = "\n".join([f"{k}. {v}" for k, v in item["options"].items()])
    return f"""
PostGIS Multiple Choice Question:
{item['question']}

Options:
{options_str}

Answer with ONLY the correct letter (A/B/C/D):
""".strip()

def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path)

def make_unique_key(item: Dict, round_id: int) -> str:
    raw = f"{item['new_id']}__{item['function']}__{item['question']}__{round_id}"
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
    raw_prediction, tokens, error = call_language_model(
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


    prediction = raw_prediction.strip() if raw_prediction else ""

    return {
        "new_id": item["new_id"],
        "id": item["id"],
        "function": item["function"],
        "type": item["type"],
        "question": item["question"],
        "options": item["options"],
        "gold_answer": item["answer"],
        "pred_answer": prediction,
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
    with ThreadPoolExecutor(max_workers=32) as executor:
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
