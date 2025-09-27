# -*- coding: utf-8 -*-
import os
import json

# ===== Configure paths =====
model_name = os.environ.get("MODEL_NAME", "default-model")
base_dir = os.environ.get("BASE_DIR", r"./GeoSQL-Eval/GeoSQL_Table_Schema_Level_results")

schema_file = r"./GeoSQL-Eval/GeoSQL-Bench/Table_Schema_Retrieval_Question_Explicit.jsonl"
input_file = os.path.join(base_dir, model_name, "predictions_cleaned.jsonl")
output_file = os.path.join(base_dir, model_name, "predictions_deduplicated_with_dbid.jsonl")

# ===== Read Table_Schema_Retrieval_Question_Explicit.jsonl and build new_id -> db_id mapping =====
id_to_dbid = {}
with open(schema_file, "r", encoding="utf-8") as f:
    for line in f:
        if line.strip():
            obj = json.loads(line)
            new_id = obj.get("new_id")
            db_id = obj.get("db_id")
            if new_id is not None:
                id_to_dbid[new_id] = db_id

# ===== Read predictions_cleaned.jsonl and add db_id =====
with open(input_file, "r", encoding="utf-8") as fin, \
     open(output_file, "w", encoding="utf-8") as fout:
    for line in fin:
        if line.strip():
            obj = json.loads(line)
            match_id = obj.get("id")
            if match_id in id_to_dbid:
                obj["db_id"] = id_to_dbid[match_id]
            else:
                obj["db_id"] = None  # If not matched, set to None or skip
            fout.write(json.dumps(obj, ensure_ascii=False) + "\n")

print(f"Processing completed, results saved to: {output_file}")
