import os
import re
import json
import html
from typing import Optional

model_name  = os.environ.get("MODEL_NAME", "default-model")
base_dir    = os.environ.get("BASE_DIR", r"./GeoSQL-Eval/GeoSQL_Judgment_Knowledge_level_results")
input_path  = os.path.join(base_dir, model_name, "predictions.jsonl")
output_path = os.path.join(base_dir, model_name, "predictions_cleaned.jsonl")

def strip_think(text: str) -> str:
    if not text:
        return ""
    t = html.unescape(text)
    t = re.sub(r'<\s*think\b[^>]*>[\s\S]*?<\s*/\s*think\s*>', '', t, flags=re.IGNORECASE)
    t = re.sub(r'<\s*think\b[^>]*>[\s\S]*$', '', t, flags=re.IGNORECASE)
    return t.strip()

def tail(text: str, n: int = 200) -> str:
    text = (text or "").strip()
    return text[-n:] if len(text) > n else text

def normalize_token(tok: str) -> Optional[str]:
    if not tok:
        return None
    if re.fullmatch(r'[A-Da-d]', tok):
        return tok.upper()
    low = tok.lower()
    if low == "true":
        return "True"
    if low == "false":
        return "False"
    return None

def extract_final_answer(raw: str) -> str:
    if not raw:
        return ""
    t = tail(strip_think(raw), 240)
    matches = list(re.finditer(r'(?i)\b(A|B|C|D|True|False)\b', t))
    if not matches:
        return ""
    ans = matches[-1].group(1)
    norm = normalize_token(ans)
    return norm or ""

if not os.path.isfile(input_path):
    raise FileNotFoundError(f"Input file not found: {input_path}")

cleaned = []
with open(input_path, 'r', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue

        raw = item.get("pred_answer", "")
        item["pred_answer"] = extract_final_answer(raw)
        cleaned.append(item)

with open(output_path, 'w', encoding='utf-8') as f:
    for item in cleaned:
        f.write(json.dumps(item, ensure_ascii=False) + '\n')

print(f"Removed <think> and extracted final answer (only A/B/C/D or True/False): {output_path}")
