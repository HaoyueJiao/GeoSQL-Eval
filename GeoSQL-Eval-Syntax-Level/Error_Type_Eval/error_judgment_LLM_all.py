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

MAX_WORKERS   = 32
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
and output only the label itself (do not output any other text or punctuation).

Classification guidelines (for your understanding only, do not output them):
1. SQL Syntax Errors: syntax issues, misplaced keywords, unclosed quotes/parentheses, messages like "syntax error at or near ..."
2. PostGIS Function Errors: PostGIS-related functions, such as "function ... does not exist", "operator/function argument count or type mismatch", "missing required parameter or clause"
3. Missing Objects: missing table/view/column/alias/relation or FROM-clause, such as "relation ... does not exist", "column ... does not exist", "missing FROM-clause entry for table ..."
4. Geometry Parsing Errors: geometry text/binary parsing failures or invalid geometries, such as "parse error - invalid geometry", "cannot mix dimensionality in a geometry", "geometry requires more points"
5. Environment/Connection Errors: connection failures, timeouts, permission/role/database connection or environment issues
6. SRID/Dimension Mismatch: inconsistent SRID or mismatched dimensions, such as "Operation on mixed SRID", "Coordinate dimension mismatch"
7. Result Mismatch Errors: mismatches with expected results, such as "row count mismatch: model returned xx rows, expected xx rows", "expected 1 row, got N rows"

Error message:
{error_text}
"""

def clean_output(text: str) -> str:
    if not text:
        return ""
    t = text.strip().strip("`").strip('"').strip("'")
    return t.splitlines()[0].strip()

def _normalize_label(label: str) -> str:
    for t in ALLOWED_TYPES:
        if label.strip().lower() == t.lower():
            return t
    return "Environment/Connection Errors"

def classify_error(err_text: str) -> str:
    resp, _, _ = call_language_model(
        model_provider=MODEL_PROVIDER,
        model_name=MODEL_NAME,
        system_prompt = "Strict classifier, must output exactly one English label (must match the whitelist exactly).",
        user_prompt=PROMPT.format(error_text=err_text[:4000]),
        enable_thinking=False,
        stream=False,
        temperature=TEMPERATURE,
        max_tokens=MAX_TOKENS,
        config_path=CONFIG_PATH,
    )
    label = clean_output(resp)
    return _normalize_label(label)

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
                k = obj.get("unique_key") or stable_key(obj)
                if k:
                    done.add(k)
            except:
                continue
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
    ROOT_DIR    = os.path.join(base_dir, model_name)
    INPUT_PATH  = os.path.join(ROOT_DIR, "predictions_execution_eval_with_meta.jsonl")
    output_path = os.path.join(ROOT_DIR, "error_classified.jsonl")

    if not os.path.exists(INPUT_PATH):
        print(f"{model_name}: Input file does not exist, skipping: {INPUT_PATH}")
        return

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
        open(output_path, "w", encoding="utf-8").close()

    buffer, lock = [], threading.Lock()
    skipped, total = 0, len(data)
    pbar = tqdm(total=total, desc=f"Classifying [{model_name}]", ncols=100)

    def task(rec):
        nonlocal skipped
        k = stable_key(rec)
        if k in done_keys:
            skipped += 1
            pbar.update(1); return None

        err_text = (rec.get("execution_error") or "").strip()
        if err_text:
            try:
                rec["error_type"] = classify_error(err_text)
                rec["error_type_model"] = MODEL_NAME
            except Exception as e:
                rec["error_type"] = "Environment/Connection Errors"
                rec["error_type_model"] = MODEL_NAME
                rec["error_type_reason"] = f"exception: {type(e).__name__}"

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

    print(f"{model_name}: Done. Output -> {output_path} (跳过 {skipped}/{total})")

def discover_uncommented_models_from_source() -> list:
    script_path = globals().get("__file__", None)
    if not script_path or not os.path.exists(script_path):
        return []
    pat = re.compile(r'^\s*model_name\s*=\s*["\']([^"\']+)["\']\s*$', re.IGNORECASE)
    models = []
    with open(script_path, "r", encoding="utf-8") as f:
        for line in f:
            m = pat.match(line)
            if m:
                models.append(m.group(1))
    models = list(OrderedDict((m, None) for m in models).keys())
    return models

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
        print("No uncommented model_name lines found, and no MODELS environment variable provided.")
        return
    print(f"Processing {len(models)} models in sequence: {models}")
    for m in models:
        print(f"\n================ Processing model: {m} ================")
        process_model(m)


if __name__ == "__main__":
    main()
