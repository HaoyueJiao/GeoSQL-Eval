import json
from collections import Counter

import os
model_name = os.environ.get("MODEL_NAME", "default-model")

base_dir = os.environ.get("BASE_DIR", r"./GeoSQL-Eval/GeoSQL_Table_Schema_Level_results")
input_path = os.path.join(base_dir, model_name, "predictions_execution_eval.jsonl")
output_path = os.path.join(base_dir, model_name, "eval_summary_execution.json")

def analyze_results():
    with open(input_path, 'r', encoding='utf-8') as f:
        all_data = [json.loads(line) for line in f]

    stats = Counter()


    stats["total_sql"] = len(all_data)
    stats["total_columns"] = 0
    stats["geometry_columns"] = 0
    stats["text_columns"] = 0

    stats["st_astext_column_pass"] = 0
    stats["st_equals+z_column_pass"] = 0
    stats["value_match_column_pass"] = 0

    stats["correct_sql_count"] = 0
    stats["executable_sql_count"] = 0

    for item in all_data:
        if item.get("executable", False):
            stats["executable_sql_count"] += 1
        if item.get("result_correct") == "correct":
            stats["correct_sql_count"] += 1

        column_types = item.get("column_type", [])
        comparisons = item.get("result_comparison", [])

        for col_type, comp in zip(column_types, comparisons):
            stats["total_columns"] += 1

            if col_type == "geometry":
                stats["geometry_columns"] += 1
                if comp.get("column_pass_by_st_astext"):
                    stats["st_astext_column_pass"] += 1
                if comp.get("column_pass_by_st_equals") and comp.get("column_pass_by_st_z_pass"):
                    stats["st_equals+z_column_pass"] += 1
            elif col_type == "text":
                stats["text_columns"] += 1
                if comp.get("column_pass_by_value_match"):
                    stats["value_match_column_pass"] += 1


    stats["correct_sql_ratio"] = round(stats["correct_sql_count"] / stats["total_sql"], 4) if stats["total_sql"] else 0.0
    stats["executable_sql_ratio"] = round(stats["executable_sql_count"] / stats["total_sql"], 4) if stats["total_sql"] else 0.0

    stats["geometry_st_astext_pass_ratio"] = round(stats["st_astext_column_pass"] / stats["geometry_columns"], 4) if stats["geometry_columns"] else 0.0
    stats["geometry_st_equals+z_pass_ratio"] = (
        round((stats["st_equals+z_column_pass"] + stats["st_astext_column_pass"]) / stats["geometry_columns"], 4)
        if stats["geometry_columns"] else 0.0
    )
    stats["text_value_match_pass_ratio"] = round(stats["value_match_column_pass"] / stats["text_columns"], 4) if stats["text_columns"] else 0.0


    print("\n===== Detailed GeoSQL Evaluation Summary =====")
    print(f"Total SQL statements             : {stats['total_sql']}")
    print(f"Executable SQL statements        : {stats['executable_sql_count']} ({stats['executable_sql_ratio']*100:.2f}%)")
    print(f"Correct SQL statements           : {stats['correct_sql_count']} ({stats['correct_sql_ratio']*100:.2f}%)")
    print(f"Total columns                    : {stats['total_columns']}")
    print(f" - Geometry columns              : {stats['geometry_columns']}")
    print(f" - Text columns                  : {stats['text_columns']}")
    print(f"Geometry column pass rate (AsText)  : {stats['geometry_st_astext_pass_ratio']*100:.2f}%")
    print(f"Geometry column pass rate (Equals+Z)  : {stats['geometry_st_equals+z_pass_ratio']*100:.2f}%")
    print(f"Text column pass rate (ValueMatch)  : {stats['text_value_match_pass_ratio']*100:.2f}%")
    print("==============================================")


    with open(output_path, 'w', encoding='utf-8') as fout:
        json.dump(dict(stats), fout, ensure_ascii=False, indent=2)

    print(f"Summary statistics have been saved to: {output_path}")

if __name__ == "__main__":
    analyze_results()
