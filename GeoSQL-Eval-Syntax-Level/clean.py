import json
import re
import os

model_name  = os.environ.get("MODEL_NAME", "default-model")
base_dir    = os.environ.get("BASE_DIR", r"./GeoSQL-Eval/GeoSQL_Syntax_Level_results")
input_path  = os.path.join(base_dir, model_name, "predictions.jsonl")
output_path = os.path.join(base_dir, model_name, "predictions_cleaned.jsonl")

def extract_last_sql(text: str) -> str:
    text = re.sub(r"(?is)<think>.*?</think>", "", text).strip()

    fences = re.findall(
        r"```(?:sql)?\s*([\s\S]*?)```",
        text,
        flags=re.IGNORECASE
    )
    if fences:
        return fences[-1].strip()

    text_for_2b = re.sub(r"`+$", "", text)
    m = re.search(r"```(?:sql)?\s*([\s\S]*)$", text_for_2b, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()

    pattern_end_semicolon = re.compile(
        r"(?i)"                            
        r"((?:SELECT|WITH|UPDATE|DELETE|INSERT|CREATE|DROP|ALTER)\b"  
        r"[\s\S]*?;)"                      
        r"(?=\s*(?:\r?\n|$))"
    )
    candidates = pattern_end_semicolon.findall(text)
    if candidates:
        return candidates[-1].rstrip().strip()

    pattern_to_end = re.compile(
        r"(?i)"                                       
        r"((?:SELECT|WITH|UPDATE|DELETE|INSERT|"      
        r"CREATE|DROP|ALTER)\b[\s\S]*?)"              
        r"$",
        flags=re.MULTILINE
    )
    candidates2 = pattern_to_end.findall(text)
    if candidates2:
        return candidates2[-1].strip()

    return ""

with open(input_path, 'r', encoding='utf-8') as f:
    data = [json.loads(line) for line in f if line.strip()]

for item in data:
    raw_sql = item.get("pred_sql", "")
    item["pred_sql"] = extract_last_sql(raw_sql)

with open(output_path, 'w', encoding='utf-8') as f:
    for item in data:
        f.write(json.dumps(item, ensure_ascii=False) + '\n')

print(f"SQL cleaning completed, output saved to: {output_path}")

