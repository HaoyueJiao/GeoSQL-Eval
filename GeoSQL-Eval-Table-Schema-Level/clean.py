import json
import re
import os

model_name  = os.environ.get("MODEL_NAME", "default-model")
base_dir    = os.environ.get("BASE_DIR", r"./GeoSQL-Eval/GeoSQL_Table_Schema_Level_results")
input_path  = os.path.join(base_dir, model_name, "predictions_reorder.jsonl")
output_path = os.path.join(base_dir, model_name, "predictions_cleaned.jsonl")

def extract_last_sql(text: str) -> str:
    # 1) Remove <think>...</think> and its content
    text = re.sub(r"(?is)<think>.*?</think>", "", text).strip()

    # 2a) Extract fully closed ```...``` code blocks
    fences = re.findall(
        r"```(?:sql)?\s*([\s\S]*?)```",
        text,
        flags=re.IGNORECASE
    )
    if fences:
        return fences[-1].strip()

    # 2b) Remove trailing backticks first, then extract unclosed ```sql... (until the end of text)
    text_for_2b = re.sub(r"`+$", "", text)
    m = re.search(r"```(?:sql)?\s*([\s\S]*)$", text_for_2b, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()

    # 3a) Free text extraction: start with keywords (SELECT/WITH/UPDATE/...)
    #     and end with a semicolon followed by line break or end of text
    pattern_end_semicolon = re.compile(
        r"(?i)"                            # case insensitive
        r"((?:SELECT|WITH|UPDATE|DELETE|INSERT|CREATE|DROP|ALTER)\b"  # start
        r"[\s\S]*?;)"                      # end at semicolon
        r"(?=\s*(?:\r?\n|$))"              # semicolon must be followed by newline or end of text
    )
    candidates = pattern_end_semicolon.findall(text)
    if candidates:
        return candidates[-1].rstrip().strip()

    # 3b) Fallback: if no semicolon found, match from keyword to the end of text (semicolon optional)
    pattern_to_end = re.compile(
        r"(?i)"                                       # case insensitive
        r"((?:SELECT|WITH|UPDATE|DELETE|INSERT|"      # start keywords
        r"CREATE|DROP|ALTER)\b[\s\S]*?)"              # until the end
        r"$",                                         # end of text
        flags=re.MULTILINE
    )
    candidates2 = pattern_to_end.findall(text)
    if candidates2:
        return candidates2[-1].strip()

    # If nothing matched, return empty string
    return ""

# —— Main cleaning process —— #
with open(input_path, 'r', encoding='utf-8') as f:
    data = [json.loads(line) for line in f if line.strip()]

for item in data:
    raw_sql = item.get("pred_sql", "")
    item["pred_sql"] = extract_last_sql(raw_sql)

with open(output_path, 'w', encoding='utf-8') as f:
    for item in data:
        f.write(json.dumps(item, ensure_ascii=False) + '\n')

print(f"SQL cleaning completed, output saved to: {output_path}")
