import time
import re
from typing import Union, Dict, Any
import pandas as pd
def _normalize_for_order_strict(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    df.columns = _deduplicate_columns(list(df.columns))
    return df

def _columns_equal(cursor, series_gold: pd.Series, series_pred: pd.Series) -> Dict[str, Any]:
    """
    Compare whether two columns are "row-by-row equal":
    - Automatically determine if they are geometry columns
      (only treated as geometry if both sides can be stably converted to geometry).
    - Geometry: EWKT exact match OR (ST_Equals is true AND Z sequence is equal / all None).
    - Text/Number: case-insensitive string equality.

    Returns:
      {
        "equal": bool,       # Whether the entire column is equal
        "col_type": "geometry" | "text",
        "stats": {  # Useful for filling in strategy_pass_rate later
           "ST_AsText_pass": int,
           "ST_Equals_pass": int,
           "ST_Z_pass": int,
           "value_match_pass": int,
           "total_rows": int
        }
      }
    """

    g_vals = series_gold.tolist()
    p_vals = series_pred.tolist()
    n = len(g_vals)

    is_g_gold = _is_geometry_column_via_exec(cursor, g_vals)
    is_g_pred = _is_geometry_column_via_exec(cursor, p_vals)
    is_geom = is_g_gold and is_g_pred

    st_astext_pass = st_equals_pass = st_z_pass = value_match_pass = 0

    if is_geom:
        for i in range(n):
            gv, pv = g_vals[i], p_vals[i]
            try:
                g_is, g_ewkt = _cell_to_ewkt(cursor, gv)
                p_is, p_ewkt = _cell_to_ewkt(cursor, pv)
                if g_is and p_is and g_ewkt and p_ewkt:
                    # AsText（EWKT）全等
                    if (g_ewkt or "").strip().lower() == (p_ewkt or "").strip().lower():
                        st_astext_pass += 1
                    # ST_Equals
                    cursor.execute("""
                        SELECT ST_Equals(
                            ST_SnapToGrid(ST_GeomFromEWKT(%s), 1e-5),
                            ST_SnapToGrid(ST_GeomFromEWKT(%s), 1e-5)
                        )
                    """, (g_ewkt, p_ewkt))
                    if cursor.fetchone()[0] is True:
                        st_equals_pass += 1

                    try:
                        cursor.execute("""
                            WITH 
                            gpts AS (SELECT ST_Z(dp.geom) AS z FROM ST_DumpPoints(ST_GeomFromEWKT(%s)) dp),
                            ppts AS (SELECT ST_Z(dp.geom) AS z FROM ST_DumpPoints(ST_GeomFromEWKT(%s)) dp)
                            SELECT ARRAY_AGG(g.z), ARRAY_AGG(p.z) FROM gpts g, ppts p;
                        """, (g_ewkt, p_ewkt))
                        z_g, z_p = cursor.fetchone()
                        if z_g is not None and z_p is not None and len(z_g) == len(z_p):
                            if all(a is None for a in z_g) and all(b is None for b in z_p):
                                st_z_pass += 1
                            else:
                                all_z_match = all(
                                    (a is None and b is None) or
                                    (a is not None and b is not None and abs(a - b) <= 1e-6)
                                    for a, b in zip(z_g, z_p)
                                )
                                if all_z_match:
                                    st_z_pass += 1
                    except Exception:
                        pass
            except Exception:
                pass


        col_equal = (st_astext_pass == n) or ((st_equals_pass == n) and (st_z_pass == n))
        return {
            "equal": col_equal,
            "col_type": "geometry",
            "stats": {
                "ST_AsText_pass": st_astext_pass,
                "ST_Equals_pass": st_equals_pass,
                "ST_Z_pass": st_z_pass,
                "value_match_pass": 0,
                "total_rows": n
            }
        }
    else:
        for i in range(n):
            gv, pv = g_vals[i], p_vals[i]
            if str(pv).strip().lower() == str(gv).strip().lower():
                value_match_pass += 1
        col_equal = (value_match_pass == n)
        return {
            "equal": col_equal,
            "col_type": "text",
            "stats": {
                "ST_AsText_pass": 0,
                "ST_Equals_pass": 0,
                "ST_Z_pass": 0,
                "value_match_pass": value_match_pass,
                "total_rows": n
            }
        }

def _max_bipartite_match(pred_to_gold_ok: Dict[int, list], gold_count: int) -> Dict[int, int]:

    match_gold = {}
    def dfs(p, seen):
        for g in pred_to_gold_ok.get(p, []):
            if g in seen:
                continue
            seen.add(g)
            if g not in match_gold or dfs(match_gold[g], seen):
                match_gold[g] = p
                return True
        return False

    pred2gold = {}
    for p in pred_to_gold_ok.keys():
        if dfs(p, set()):
            # 反向查找当前 p 匹配到的 g
            for g, pp in match_gold.items():
                if pp == p:
                    pred2gold[p] = g
                    break
    return pred2gold

def _deduplicate_columns(cols):
    seen = {}
    out = []
    for c in cols:
        if c not in seen:
            seen[c] = 0
            out.append(c)
        else:
            seen[c] += 1
            out.append(f"{c}_{seen[c]}")
    return out

def _parse_order_by(sql_text: str):
    s = (sql_text or "").strip().rstrip(";").lower()
    m = re.search(r"\border\s+by\s+(.*?)(\s+limit\b|;|$)", s, flags=re.DOTALL)
    if not m:
        return []
    raw = m.group(1)
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    out = []
    for p in parts:
        mm = re.match(r"([\w\.\"`]+)(?:\s+(asc|desc))?$", p)
        if not mm:
            continue
        col = mm.group(1).strip('`"')
        asc = (mm.group(2) or "asc").lower() != "desc"
        out.append((col, asc))
    return out

def _cell_sort_key(v):

    try:
        s = str(v).strip()
        return s.lower()
    except Exception:
        return ""

def _normalize_table(df: pd.DataFrame, sql_text: str = None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    df.columns = _deduplicate_columns(list(df.columns))
    df = df.drop_duplicates()

    order_spec = _parse_order_by(sql_text)
    if order_spec:
        by_cols = [c for c, _ in order_spec if c in df.columns]
        ascending = [asc for c, asc in order_spec if c in df.columns]
        if by_cols:
            df = df.sort_values(by=by_cols, ascending=ascending)
    else:

        df = df.reindex(sorted(df.columns), axis=1)


        sortkey_df = df.applymap(_cell_sort_key)

        sort_cols = list(sortkey_df.columns)
        ordered_idx = sortkey_df.sort_values(by=sort_cols).index
        df = df.loc[ordered_idx]

    return df.reset_index(drop=True)

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

def _cell_to_ewkt(cursor, cell) -> "tuple[bool, str]":
    if cell is None:
        return (False, "")
    try:
        if isinstance(cell, (bytes, memoryview)):
            hex_str = (cell.tobytes() if isinstance(cell, memoryview) else cell).hex()
            geom_sql = to_geom_4326_sql(hex_str)
            cursor.execute(f"SELECT ST_AsEWKT({geom_sql})", (hex_str,))
            ewkt = cursor.fetchone()[0]
            return (True, ewkt or "")
        s = str(cell).strip()
        if not s:
            return (False, "")
        if is_hex_wkb(s) or is_wkt(s) or s.upper().startswith("SRID="):
            geom_sql = to_geom_4326_sql(s)
            cursor.execute(f"SELECT ST_AsEWKT({geom_sql})", (s,))
            ewkt = cursor.fetchone()[0]
            return (True, ewkt or "")
        return (False, "")
    except Exception:
        return (False, "")

def _is_geometry_column_via_exec(cursor, col_values) -> bool:
    any_geom = False
    for v in col_values:
        if v is None or str(v).strip() == "":
            continue
        isg, ewkt = _cell_to_ewkt(cursor, v)
        if isg and ewkt:
            any_geom = True
        else:
            return False
    return any_geom
def evaluate_sql_execution(
        sql_text: str,
        db_conn,
        timeout_sec: int = 5,
        gold_sql: str = None
) -> Dict[str, Union[bool, str, float, str, list]]:

    if not gold_sql:
        raise ValueError("gold_sql is required.")

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
        },
        "gold_executable": False,
        "gold_execution_time": 0.0,
        "gold_error": "",
        "pred_error": ""
    }

    try:
        try:
            db_conn.rollback()
        except:
            pass
        with db_conn.cursor() as cursor:
            cursor.execute(f"SET statement_timeout = {timeout_sec * 1000};")

            df_gold = pd.DataFrame()
            gold_cols = []
            try:
                t0 = time.time()
                cursor.execute(gold_sql)
                gold_rows = cursor.fetchall()
                gold_desc = cursor.description or []
                gold_time = time.time() - t0
                gold_cols = [d[0] for d in gold_desc]
                df_gold = pd.DataFrame(gold_rows, columns=gold_cols)
                result["gold_executable"] = True
                result["gold_execution_time"] = round(gold_time, 6)
            except Exception as ge:
                result["gold_error"] = str(ge)

            df_pred = pd.DataFrame()
            try:
                t1 = time.time()
                cursor.execute(sql_text)
                pred_rows = cursor.fetchall()
                pred_desc = cursor.description or []
                pred_time = time.time() - t1
                pred_cols = [d[0] for d in pred_desc]
                df_pred = pd.DataFrame(pred_rows, columns=pred_cols)

                result["executable"] = True
                result["execution_time"] = round(pred_time, 6)
            except Exception as pe:
                result["pred_error"] = str(pe)
                return result

            if not result["gold_executable"]:
                return result

            if df_pred.empty and df_gold.empty:
                return result

            df_gold_n = _normalize_for_order_strict(df_gold)
            df_pred_n = _normalize_for_order_strict(df_pred)

            if df_gold_n.shape[0] != df_pred_n.shape[0]:
                result["execution_error"] = (
                    f"Row count mismatch (strict comparison, order preserved): pred {df_pred_n.shape[0]} vs gold {df_gold_n.shape[0]}"
                )
                result["result_correct"] = "incorrect"
                return result

            n_rows = len(df_gold_n)
            pred_cols = list(df_pred_n.columns)
            gold_cols = list(df_gold_n.columns)


            pred_to_gold_ok = {}
            col_compare_cache = {}

            with db_conn.cursor() as cursor_cmp:
                for p_idx, p_name in enumerate(pred_cols):
                    ok_list = []
                    for g_idx, g_name in enumerate(gold_cols):
                        key = (p_idx, g_idx)
                        details = _columns_equal(cursor_cmp, df_gold_n[g_name], df_pred_n[p_name])
                        col_compare_cache[key] = details
                        if details["equal"]:
                            ok_list.append(g_idx)
                    pred_to_gold_ok[p_idx] = ok_list

            pred2gold = _max_bipartite_match(pred_to_gold_ok, len(gold_cols))
            if len(pred2gold) != len(pred_cols):

                not_matched = [pred_cols[i] for i in range(len(pred_cols)) if i not in pred2gold]
                result["execution_error"] = (
                    "Column subset match failed: some pred columns could not find an equal value counterpart in gold."
                    f" Unmatched pred columns: {not_matched}"
                )
                result["result_correct"] = "incorrect"
                return result

            comparison = []
            col_types = []
            st_astext_col_pass = st_equals_col_pass = st_z_col_pass = value_match_col_pass = 0

            for p_idx, g_idx in pred2gold.items():
                details = col_compare_cache[(p_idx, g_idx)]
                ctype = details["col_type"]
                stats = details["stats"]
                col_types.append(ctype)

                # 列级通过布尔（与你原规则一致）
                if ctype == "geometry":
                    col_pass_by_astext = (stats["ST_AsText_pass"] == stats["total_rows"])
                    col_pass_by_equals = (stats["ST_Equals_pass"] == stats["total_rows"])
                    col_pass_by_z = (stats["ST_Z_pass"] == stats["total_rows"])
                    col_pass_by_value = False
                    if col_pass_by_astext: st_astext_col_pass += 1
                    if col_pass_by_equals: st_equals_col_pass += 1
                    if col_pass_by_z:      st_z_col_pass += 1
                else:
                    col_pass_by_astext = col_pass_by_equals = col_pass_by_z = False
                    col_pass_by_value = (stats["value_match_pass"] == stats["total_rows"])
                    if col_pass_by_value:  value_match_col_pass += 1

                comparison.append({
                    "ST_AsText_pass": stats["ST_AsText_pass"],
                    "ST_Equals_pass": stats["ST_Equals_pass"],
                    "ST_Z_pass": stats["ST_Z_pass"],
                    "value_match_pass": stats["value_match_pass"],
                    "total_rows": stats["total_rows"],
                    "column_pass_by_st_astext": col_pass_by_astext,
                    "column_pass_by_st_equals": col_pass_by_equals,
                    "column_pass_by_st_z": col_pass_by_z,
                    "column_pass_by_value_match": col_pass_by_value,
                    "column_type": ctype,
                    "pred_col": pred_cols[p_idx],
                    "gold_col": gold_cols[g_idx]
                })

            result["result_comparison"] = comparison
            result["column_type"] = col_types

            total_cols = len(comparison) or 1
            result["strategy_pass_rate"] = {
                "st_astext": round(st_astext_col_pass / total_cols, 4),
                "st_equals": round(st_equals_col_pass / total_cols, 4),
                "st_z": round(st_z_col_pass / total_cols, 4),
                "value_match": round(value_match_col_pass / total_cols, 4)
            }

            result["result_correct"] = "correct"

    except Exception as e:
        try:
            db_conn.rollback()
        except:
            pass
        result["execution_error"] = str(e)

    return result






