import time
import re
from typing import Union, Dict, Any

def to_geom_4326_sql(value: str, param: str = "%s") -> str:
    if is_hex_wkb(value):
        return f"(ST_SetSRID(ST_GeomFromWKB(decode({param}, 'hex')), 4326))"
    elif value.strip().upper().startswith("SRID=4326;"):
        return f"({param})::geography::geometry"
    else:
        return f"ST_SetSRID(ST_GeomFromText({param}), 4326)"

def is_wkt(value: str) -> bool:
    if not isinstance(value, str):
        return False
    value = value.strip().upper()
    return re.match(r"^(SRID=\d+;)?(POINT|LINESTRING|POLYGON|MULTI(POINT|LINESTRING|POLYGON)?|GEOMETRYCOLLECTION)( Z| M| ZM)?\s*\(.*\)", value) is not None

def normalize_expected_result(expected_result, col_names, n_rows):
    if isinstance(expected_result, str):
        return [[expected_result.strip()]]
    elif isinstance(expected_result, list):
        if all(isinstance(row, list) for row in expected_result):
            return expected_result
        elif all(isinstance(row, dict) for row in expected_result):
            return [[row.get(c, "") for c in col_names] for row in expected_result]
    elif isinstance(expected_result, dict):
        return [[expected_result.get(c, "") for c in col_names]]
    return [["" for _ in col_names] for _ in range(n_rows)]

def is_hex_wkb(value: str) -> bool:
    if not isinstance(value, str):
        return False
    return re.fullmatch(r"[0-9A-Fa-f]{16,}", value) is not None

def evaluate_sql_execution(
        sql_text: str,
        db_conn,
        timeout_sec: int = 5,
        expected_result: Union[str, list, dict] = None
) -> Dict[str, Union[bool, str, float, str, list]]:

    result = {
        "executable": False,
        "execution_error": "",
        "execution_time": 0.0,
        "result_correct": "unknown",
        "result_comparison": [],
        "column_type": [],
        "strategy_pass_rate": {
            "st_astext": 0.0,
            "st_equals": 0.0,
            "st_z": 0.0,
            "value_match": 0.0
        }
    }

    try:
        try:
            db_conn.rollback()
        except:
            pass
        with db_conn.cursor() as cursor:
            cursor.execute(f"SET statement_timeout = {timeout_sec * 1000};")
            start_time = time.time()
            cursor.execute(sql_text)
            rows = cursor.fetchall()
            col_desc = cursor.description
            end_time = time.time()

            result["executable"] = True
            result["execution_time"] = round(end_time - start_time, 6)

            if not rows:
                return result  # 无结果直接返回

            col_names = [desc.name for desc in col_desc]
            n_cols = len(col_names)
            n_rows = len(rows)

            expected_matrix = normalize_expected_result(expected_result, col_names, n_rows)

            if len(expected_matrix) != n_rows:
                result["execution_error"] = f"行数不一致：模型返回{n_rows}行，期望{len(expected_matrix)}行"
                result["result_correct"] = "incorrect"
                return result

            comparison = []
            col_types = []

            # 每种策略下整列通过数
            st_astext_col_pass = 0
            st_equals_col_pass = 0
            st_z_col_pass = 0
            value_match_col_pass = 0

            for col_idx in range(n_cols):
                st_astext_pass = 0
                st_equals_pass = 0
                st_z_pass = 0
                value_match_pass = 0

                for row_idx in range(n_rows):
                    model_val = rows[row_idx][col_idx]
                    expected_val = expected_matrix[row_idx][col_idx] if col_idx < len(expected_matrix[row_idx]) else ""

                    model_str = str(model_val).strip()
                    expected_str = str(expected_val).strip()
                    is_geom = is_wkt(expected_str) or is_hex_wkb(expected_str)

                    if is_geom:
                        try:
                            # ST_AsText (EWKT) 比较
                            model_geom_sql = to_geom_4326_sql(model_str)
                            cursor.execute(f"SELECT ST_AsEWKT({model_geom_sql})", (model_str,))
                            model_ewkt = cursor.fetchone()[0]
                            expected_geom_sql = to_geom_4326_sql(expected_str)
                            cursor.execute(f"SELECT ST_AsEWKT({expected_geom_sql})", (expected_str,))
                            expected_ewkt = cursor.fetchone()[0]
                            if model_ewkt.strip().lower() == expected_ewkt.strip().lower():
                                st_astext_pass += 1

                            # ST_Equals 比较
                            cursor.execute(f"""
                                SELECT ST_Equals(
                                    ST_SnapToGrid({model_geom_sql}, 1e-5),
                                    ST_SnapToGrid({expected_geom_sql}, 1e-5)
                                )
                            """, (model_str, expected_str))

                            if cursor.fetchone()[0] is True:
                                st_equals_pass += 1
                            # === ST_Z 值比较（逐点）===
                            try:
                                cursor.execute(f"""
                                    WITH 
                                    model_pts AS (
                                        SELECT ST_Z(dp.geom) AS z
                                        FROM ST_DumpPoints({model_geom_sql}) AS dp
                                    ),
                                    expected_pts AS (
                                        SELECT ST_Z(dp.geom) AS z
                                        FROM ST_DumpPoints({expected_geom_sql}) AS dp
                                    )
                                    SELECT ARRAY_AGG(m.z), ARRAY_AGG(e.z)
                                    FROM model_pts m, expected_pts e;
                                """, (model_str, expected_str))

                                z_model, z_expected = cursor.fetchone()

                                # 两者都为 None（表示都是二维），视为一致
                                if z_model is not None and z_expected is not None and len(z_model) == len(z_expected):
                                    # 如果都是全 None，则视为一致
                                    if all(a is None for a in z_model) and all(b is None for b in z_expected):
                                        st_z_pass += 1
                                    else:
                                        # 正常比较
                                        try:
                                            all_z_match = all(
                                                (a is None and b is None) or (
                                                        a is not None and b is not None and abs(a - b) <= 1e-6)
                                                for a, b in zip(z_model, z_expected)
                                            )
                                            if all_z_match:
                                                st_z_pass += 1
                                        except Exception:
                                            pass

                            except Exception:
                                pass

                        except Exception:
                            pass
                    else:
                        if model_str.lower() == expected_str.lower():
                            value_match_pass += 1

                # 判断整列是否通过
                col_type = "geometry" if is_geom else "text"
                col_types.append(col_type)

                col_pass_by_astext = (st_astext_pass == n_rows) if is_geom else False
                col_pass_by_equals = (st_equals_pass == n_rows) if is_geom else False
                col_pass_by_z_pass = (st_z_pass == n_rows) if is_geom else False
                col_pass_by_value = (value_match_pass == n_rows) if not is_geom else False

                if col_pass_by_astext:
                    st_astext_col_pass += 1
                if col_pass_by_equals:
                    st_equals_col_pass += 1
                if col_pass_by_z_pass:
                    st_z_col_pass += 1
                if col_pass_by_value:
                    value_match_col_pass += 1

                comparison.append({
                    "ST_AsText_pass": st_astext_pass,
                    "ST_Equals_pass": st_equals_pass,
                    "ST_Z_pass": st_z_pass,
                    "value_match_pass": value_match_pass,
                    "total_rows": n_rows,
                    "column_pass_by_st_astext": col_pass_by_astext,
                    "column_pass_by_st_equals": col_pass_by_equals,
                    "column_pass_by_st_z": col_pass_by_z_pass,
                    "column_pass_by_value_match": col_pass_by_value,
                    "column_type": col_type
                })

            result["result_comparison"] = comparison
            result["column_type"] = col_types

            total_cols = len(comparison)
            result["strategy_pass_rate"] = {
                "st_astext": round(st_astext_col_pass / total_cols, 4) if total_cols else 0.0,
                "st_equals": round(st_equals_col_pass / total_cols, 4) if total_cols else 0.0,
                "st_z":round(st_z_col_pass / total_cols, 4) if total_cols else 0.0,
                "value_match": round(value_match_col_pass / total_cols, 4) if total_cols else 0.0
            }

            all_cols_passed = True
            for col in comparison:
                if col["column_type"] == "geometry":
                    if not (
                            col.get("column_pass_by_st_astext") or
                            (col.get("column_pass_by_st_equals") and col.get("column_pass_by_st_z"))
                    ):
                        all_cols_passed = False
                        break

                else:  # text
                    if not col["column_pass_by_value_match"]:
                        all_cols_passed = False
                        break

            result["result_correct"] = "correct" if all_cols_passed else "incorrect"

    except Exception as e:
        db_conn.rollback()
        result["execution_error"] = str(e)

    return result




