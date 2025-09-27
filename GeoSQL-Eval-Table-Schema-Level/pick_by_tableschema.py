# -*- coding: utf-8 -*-

import sys
import os
import json
import re
from collections import OrderedDict

DEFAULT_INPUT = r"./GeoSQL-Eval/GeoSQL-Bench/Table_Schema_Retrieval_Question_Explicit.jsonl"

RESERVED = {
    'select','as','distinct','case','when','then','else','end','null','true','false',
    'limit','offset','from','where','group','by','order','having','asc','desc','on','join',
    'left','right','inner','outer','full','cross','union','all'
}

MANAGEMENT_FUNCS = {
    "dropgeometrytable":        {"table_args": [(0,), (0, 1)], "col_args": []},
    "addgeometrycolumn": {
        "table_args": [(0,), (0, 1)],
        "col_args": [1, 2]
    },
    "dropgeometrycolumn": {
        "table_args": [(0,), (0, 1)],
        "col_args": [1, 2]
    },
    "find_srid": {
        "table_args": [(0, 1)],
        "col_args": [2]
    },
    "recovergeometrycolumn":    {"table_args": [(0, 1)],       "col_args": [2]},
    "populate_geometry_columns":{"table_args": [(0,)],         "col_args": []},
    "updategeometrysrid": {
    "table_args": [(0,), (0, 1)],
    "col_args": [1, 2]
},
    "st_estimatedextent":       {"table_args": [(0,)],         "col_args": [1]},
}

import re

def _split_args_top(args_blob: str):
    args, buf = [], []
    depth, in_str = 0, False
    i, n = 0, len(args_blob)
    while i < n:
        ch = args_blob[i]
        if in_str:
            buf.append(ch)
            if ch == "'" and i + 1 < n and args_blob[i+1] == "'":
                buf.append(args_blob[i+1]); i += 1
            elif ch == "'":
                in_str = False
        else:
            if ch == "'":
                in_str = True; buf.append(ch)
            elif ch == "(":
                depth += 1; buf.append(ch)
            elif ch == ")":
                depth -= 1; buf.append(ch)
            elif ch == "," and depth == 0:
                args.append("".join(buf).strip()); buf = []
            else:
                buf.append(ch)
        i += 1
    last = "".join(buf).strip()
    if last:
        args.append(last)
    return args

def _unquote_str_like(s: str):
    s = s.strip()
    s = re.sub(r'::\s*\w+\s*$', '', s)  # 去掉 ::regclass 等
    if len(s) >= 2 and s[0] == s[-1] == "'":
        return s[1:-1].replace("''", "'")
    return None

def find_management_refs(sql: str, known_tables: set, table_cols_map: dict):
    out = OrderedDict()
    fn_pat = re.compile(r'\b([A-Za-z_]\w*)\s*\(', flags=re.I)
    pos, n = 0, len(sql)
    while True:
        m = fn_pat.search(sql, pos)
        if not m:
            break
        fn = m.group(1).lower()
        pos = m.end()
        if fn not in MANAGEMENT_FUNCS:
            continue

        depth, j = 1, pos
        while j < n and depth > 0:
            if sql[j] == "(":
                depth += 1
            elif sql[j] == ")":
                depth -= 1
            j += 1
        args_blob = sql[pos:j-1] if depth == 0 else ""
        args = _split_args_top(args_blob)

        table_name = None
        for combo in MANAGEMENT_FUNCS[fn]["table_args"]:
            if max(combo, default=-1) < len(args):
                if len(combo) == 1:
                    t0 = _unquote_str_like(args[combo[0]])
                    if t0:
                        table_name = t0
                        break
                elif len(combo) == 2:
                    s0 = _unquote_str_like(args[combo[0]])
                    s1 = _unquote_str_like(args[combo[1]])
                    if s0 and s1:
                        table_name = s1
                        break

        cols = []
        for k in MANAGEMENT_FUNCS[fn]["col_args"]:
            if k < len(args):
                c = _unquote_str_like(args[k])
                if c:
                    cols.append(c)

        if table_name and table_name in known_tables:
            if table_name not in out:
                out[table_name] = []
            valid_cols = table_cols_map.get(table_name, [])
            for c in cols:
                if c in valid_cols and c not in out[table_name]:
                    out[table_name].append(c)

        pos = j
    return out

def sanitize_sql(sql: str) -> str:
    sql = re.sub(r'/\*.*?\*/', ' ', sql, flags=re.S)
    sql = re.sub(r'--[^\n\r]*', ' ', sql)
    sql = re.sub(r"\'([^']|\'\')*\'", ' ', sql)
    return sql

QUALIFIED_COL_RE = re.compile(
    r'\b("?[A-Za-z_][\w\$]*"?)[\s]*\.[\s]*("?[A-Za-z_][\w\$]*"?)\b'
)
import re

def find_management_tables(sql: str, known_tables):
    hits = []
    pattern = re.compile(r'\b([A-Za-z_]\w*)\s*\(\s*', flags=re.I)
    pos = 0
    while True:
        m = pattern.search(sql, pos)
        if not m:
            break
        func = m.group(1).lower()
        pos = m.end()
        if func not in MANAGEMENT_FUNCS:
            continue

        depth = 1
        j = pos
        n = len(sql)
        while j < n and depth > 0:
            ch = sql[j]
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            j += 1
        args_blob = sql[pos:j-1] if depth == 0 else ""

        args = []
        buf, d2, in_s, prev = [], 0, False, ''
        for ch in args_blob:
            if in_s:
                buf.append(ch)
                if ch == "'" and prev == "'":
                    ch = ''
                elif ch == "'" and prev != "'":
                    in_s = False
                prev = ch
                continue
            if ch == "'":
                in_s = True
                buf.append(ch)
                prev = ch
                continue
            prev = ch
            if ch == '(':
                d2 += 1; buf.append(ch)
            elif ch == ')':
                d2 -= 1; buf.append(ch)
            elif ch == ',' and d2 == 0:
                piece = ''.join(buf).strip()
                if piece:
                    args.append(piece)
                buf = []
            else:
                buf.append(ch)
        last = ''.join(buf).strip()
        if last:
            args.append(last)

        def unquote_str(s):
            s = s.strip()
            s = re.sub(r'::\s*\w+\s*$', '', s)
            if len(s) >= 2 and s[0] == s[-1] == "'":
                return s[1:-1].replace("''", "'")
            return None

        tname = None
        if args:
            a0 = unquote_str(args[0])
            a1 = unquote_str(args[1]) if len(args) >= 2 else None
            if a0 and a1:
                tname = a1
            elif a0:
                tname = a0

        if tname and tname in known_tables and tname not in hits:
            hits.append(tname)

        pos = j
    return hits

def find_qualified_columns(sql: str):
    clean = sanitize_sql(sql)
    hits = []
    for m in QUALIFIED_COL_RE.finditer(clean):
        left = m.group(1).strip('"')
        col  = m.group(2).strip('"')
        hits.append((left, col))
    return hits

BARE_COL_RE = re.compile(r'(?<!\.)\b([A-Za-z_]\w*)\b(?!\s*\()')

def find_bare_columns(sql: str):
    clean = sanitize_sql(sql)
    cols = []
    seen = set()
    for m in BARE_COL_RE.finditer(clean):
        tok = m.group(1)
        if tok.lower() in RESERVED:
            continue
        if tok not in seen:
            cols.append(tok)
            seen.add(tok)
    return cols

def _open_file_dialog():
    try:
        import tkinter as tk
        from tkinter import filedialog
        root = tk.Tk(); root.withdraw()
        return filedialog.askopenfilename(
            title="Please select a JSON/JSONL input file",
            filetypes=[("JSON Lines", "*.jsonl"), ("JSON", "*.json"), ("All Files", "*.*")]
        ) or None
    except Exception:
        return None

def load_lines(path):
    with open(path, 'r', encoding='utf-8-sig') as f:
        for line in f:
            line = line.rstrip('\n\r')
            if line.strip():
                yield line

def parse_schema_text(schema_text: str):
    tables = OrderedDict()

    i = 0
    n = len(schema_text)
    while i < n:
        m = re.search(r'#[ \t]*([A-Za-z_]\w*)\s*\(', schema_text[i:], flags=re.S)
        if not m:
            break
        tname = m.group(1)
        start = i + m.end()
        depth = 1
        j = start
        while j < n and depth > 0:
            ch = schema_text[j]
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            j += 1
        if depth != 0:
            i = i + m.end()
            continue

        cols_blob = schema_text[start:j-1]
        cols = []
        buf = []
        d2 = 0
        for ch in cols_blob:
            if ch == '(':
                d2 += 1
                buf.append(ch)
            elif ch == ')':
                d2 -= 1
                buf.append(ch)
            elif ch == ',' and d2 == 0:
                piece = ''.join(buf).strip()
                if piece:
                    mcol = re.match(r'["`]?([A-Za-z_]\w*)["`]?', piece)
                    if mcol:
                        cols.append(mcol.group(1))
                buf = []
            else:
                buf.append(ch)

        piece = ''.join(buf).strip()
        if piece:
            mcol = re.match(r'["`]?([A-Za-z_]\w*)["`]?', piece)
            if mcol:
                cols.append(mcol.group(1))

        if tname not in tables:
            tables[tname] = cols

        i = j

    return tables


def parse_tables_with_alias(sql: str):
    order_tables = []
    alias_map = {}
    for m in re.finditer(
        r'\b(from|join)\b\s+("?[A-Za-z_][\w\.]*"?)(?:\s+(?:as\s+)?([A-Za-z_]\w*))?',
        sql, flags=re.I
    ):
        raw_table = m.group(2).strip('"`')
        base = raw_table.split('.')[-1]
        alias = m.group(3)

        if base not in order_tables:
            order_tables.append(base)
        alias_map[base] = base
        alias_map[raw_table] = base
        if alias:
            alias_map[alias] = base
    return order_tables, alias_map

def extract_by_tableschema(sql, table_cols_map, order_tables, alias_map, include_empty_from_tables=False):
    result_by_table = OrderedDict((t, []) for t in order_tables)
    seen_by_table = {t: set() for t in order_tables}

    for alias_or_table, col in find_qualified_columns(sql):
        base = alias_map.get(alias_or_table)
        if not base:
            continue
        if base in table_cols_map and col in table_cols_map[base]:
            if col not in seen_by_table[base]:
                result_by_table[base].append(col)
                seen_by_table[base].add(col)

    bare_tokens = find_bare_columns(sql)
    for tok in bare_tokens:
        if tok in alias_map or tok.lower() in RESERVED:
            continue
        owners = [t for t in order_tables if tok in set(table_cols_map.get(t, []))]
        if len(owners) == 1:
            t = owners[0]
            if tok not in seen_by_table[t]:
                result_by_table[t].append(tok)
                seen_by_table[t].add(tok)

    out = []
    for t in order_tables:
        if result_by_table[t]:
            out.append({"table": t, "columns": result_by_table[t]})
        elif include_empty_from_tables and t in table_cols_map:
            out.append({"table": t, "columns": []})
    return out

def process_record(rec: dict):
    new_id = rec.get('new_id')
    db_id = rec.get('db_id')   # 这里新增
    sql = rec.get('query', '') or ''
    schema_text = rec.get('schema', '') or ''

    table_cols_map = parse_schema_text(schema_text)
    order_tables, alias_map = parse_tables_with_alias(sql)

    mgmt_refs = find_management_refs(sql, set(table_cols_map.keys()), table_cols_map)  # OrderedDict

    order_tables_all = []
    seen = set()
    for t in order_tables + list(mgmt_refs.keys()):
        if t not in seen:
            order_tables_all.append(t)
            seen.add(t)

    tables = extract_by_tableschema(
        sql, table_cols_map, order_tables_all, alias_map,
        include_empty_from_tables=True
    )

    by_table = {t["table"]: t for t in tables}
    for tname, cols in mgmt_refs.items():
        if tname not in by_table:
            by_table[tname] = {"table": tname, "columns": []}
            tables.append(by_table[tname])
        for c in cols:
            if c not in by_table[tname]["columns"]:
                by_table[tname]["columns"].append(c)

    return {"new_id": new_id, "db_id": db_id, "tables": tables}

def derive_output_path(input_path):
    root, ext = os.path.splitext(input_path)
    return root + "_picked.jsonl"

def main():
    args = sys.argv[1:]
    input_path = args[0] if len(args) >= 1 else None
    output_path = args[1] if len(args) >= 2 else None

    if not input_path:
        if os.path.isfile(DEFAULT_INPUT):
            input_path = DEFAULT_INPUT
            print(f"No input path provided, using default: {input_path}")
        else:
            print("No input path provided, default path not found, opening file dialog…")
            input_path = _open_file_dialog()
            if not input_path:
                print("No file selected. You can also run: python pick_by_tableschema.py input.jsonl [output.jsonl]")
                sys.exit(1)

    if not os.path.isfile(input_path):
        print(f"Input file not found: {input_path}")
        sys.exit(1)

    if not output_path:
        output_path = derive_output_path(input_path)

    total = ok = failed = 0
    with open(output_path, 'w', encoding='utf-8') as fout:
        for idx, line in enumerate(load_lines(input_path), 1):
            total += 1
            try:
                rec = json.loads(line)
                res = process_record(rec)
                fout.write(json.dumps(res, ensure_ascii=False) + "\n")
                ok += 1
            except Exception as e:
                nid = None
                try:
                    nid = rec.get('new_id') if isinstance(rec, dict) else None
                except Exception:
                    pass
                fout.write(json.dumps({
                    "new_id": nid,
                    "error": f"line {idx}: {type(e).__name__}: {e}"
                }, ensure_ascii=False) + "\n")
                failed += 1

    print(f"Completed: {output_path} | Stats: total={total} success={ok} failed={failed}")


if __name__ == "__main__":
    main()
