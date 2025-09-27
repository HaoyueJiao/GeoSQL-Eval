# -*- coding: utf-8 -*-
import os
import re
import json
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from tqdm import tqdm
from call_language_model import call_language_model
from collections import OrderedDict


base_dir = r"./GeoSQL-Eval/GeoSQL_Table_Schema_Level_results"

MODEL_PROVIDER = "JHY"
MODEL_NAME     = "gpt-4o"
CONFIG_PATH    = "./llm_config.yaml"

MAX_WORKERS   =16
TEMPERATURE   = 0.2
MAX_TOKENS    = 64
FLUSH_EVERY   = 20
RESUME        = True

ALLOWED_TYPES = [
    "SQL Syntax Errors",
    "PostGIS Function Errors",
    "Missing Objects",
    "Result Mismatch Errors",
    "Geometry Parsing Errors",
    "SRID/Dimension Mismatch",
    "Environment/Connection Errors",
]

PROMPT = """You are a strict database execution error classifier.
Based on the error message, classify it into exactly one of the following categories,
and output only the label itself:

- SQL Syntax Errors
- PostGIS Function Errors
- Missing Objects
- Result Mismatch Errors
- Geometry Parsing Errors
- SRID/Dimension Mismatch
- Environment/Connection Errors

Error message:
{error_text}
"""


def clean_output(text: str) -> str:
    if not text: return ""
    t = text.strip().strip("`").strip('"').strip("'")
    return t.splitlines()[0].strip()

def classify_error(err_text: str) -> str:
    resp, _, _ = call_language_model(
        model_provider=MODEL_PROVIDER,
        model_name=MODEL_NAME,
        system_prompt="Strict classifier, must output exactly one English label.",
        user_prompt=PROMPT.format(error_text=err_text[:4000]),
        enable_thinking=False,
        stream=False,
        temperature=TEMPERATURE,
        max_tokens=MAX_TOKENS,
        config_path=CONFIG_PATH,
    )
    label = clean_output(resp)
    for t in ALLOWED_TYPES:
        if label.lower() == t.lower():
            return t
    return "Environment/Connection Errors"


def stable_key(rec: dict) -> str:
    if "unique_key" in rec and rec["unique_key"]:
        return rec["unique_key"]
    base = f"{rec.get('id','')}__{rec.get('round','')}__{rec.get('model','')}"
    return hashlib.md5(base.encode()).hexdigest()


def read_done(path: str) -> set:
    done = set()
    if not os.path.exists(path):
        return done
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
                if "error_type" in obj:
                    done.add(stable_key(obj))
            except:
                pass
    return done


def append_lines(path: str, records: list, lock: threading.Lock):
    if not records:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with lock:
        with open(path, "a", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")


def process_model(model_name: str):

    ROOT_DIR   = os.path.join(base_dir, model_name)
    INPUT_PATH = os.path.join(ROOT_DIR, "predictions_execution_eval_with_funcname_with_meta.jsonl")
    output_path = os.path.join(ROOT_DIR, "error_classified.jsonl")

    if not os.path.exists(INPUT_PATH):
        print(f"{model_name}: Input file does not exist, skipping: {INPUT_PATH}")
        return

    # Read input
    data = []
    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        for line in f:
            try:
                data.append(json.loads(line))
            except:
                pass

    if not data:
        print(f"{model_name}: No records to process, skipping.")
        return

    print(f"{model_name}: Loaded {len(data)} records")

    done_keys = read_done(output_path) if RESUME else set()
    if not RESUME and os.path.exists(output_path):
        # Clear old results
        open(output_path, "w", encoding="utf-8").close()

    buffer, lock = [], threading.Lock()
    pbar = tqdm(total=len(data), desc=f"Classifying [{model_name}]", ncols=100)

    def task(rec):
        k = stable_key(rec)
        if k in done_keys:
            pbar.update(1)
            return None
        if rec.get("pred_error"):
            rec["error_type"] = classify_error(rec["pred_error"])
            rec["error_type_model"] = MODEL_NAME
        rec["unique_key"] = k
        pbar.update(1)
        return rec

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = [ex.submit(task, r) for r in data]
            for fut in as_completed(futures):
                res = fut.result()
                if res:
                    buffer.append(res)
                    if len(buffer) >= FLUSH_EVERY:
                        append_lines(output_path, buffer, lock)
                        buffer.clear()
        append_lines(output_path, buffer, lock)
    finally:
        pbar.close()

    print(f"{model_name}: Done. Output -> {output_path}")


def discover_uncommented_models_from_source() -> list:

    script_path = globals().get("__file__", None)
    if not script_path or not os.path.exists(script_path):
        return []

    pat = re.compile(r'^\s*model_name\s*=\s*["\']([^"\']+)["\']\s*$')
    models = []
    with open(script_path, "r", encoding="utf-8") as f:
        for line in f:
            m = pat.match(line)
            if m:
                models.append(m.group(1))

    models = list(OrderedDict((m, None) for m in models).keys())
    return models

# ========== 模型清单区 ==========
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

def main():

    models = discover_uncommented_models_from_source()

    if not models:
        env_models = os.environ.get("MODELS", "")
        if env_models.strip():
            models = [m.strip() for m in env_models.split(",") if m.strip()]

    if not models:
        print("No uncommented model_name lines found, and no MODELS environment variable provided. "
              "Please uncomment the models you want to run in the 'model list section'.")
        return

    print(f"Processing {len(models)} models in sequence: {models}")

    for m in models:
        print(f"\n================ Processing model: {m} ================")
        process_model(m)

if __name__ == "__main__":
    main()
