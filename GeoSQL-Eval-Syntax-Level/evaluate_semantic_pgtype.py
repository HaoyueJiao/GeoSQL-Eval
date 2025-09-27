
from typing import Dict, Union, List
from pglast import parse_sql
from pglast.visitors import Visitor
from pglast.stream import RawStream

class FunctionCallVisitor(Visitor):
    def __init__(self, target_func):
        super().__init__()
        self.target_func = target_func.lower()
        self.matches = []

    def visit_FuncCall(self, ancestors, node):
        func_name = ".".join(n.sval for n in node.funcname)
        if func_name.lower() == self.target_func:
            self.matches.append(node)

def get_pg_typeof(expr_sql: str, db_conn) -> str:
    try:
        with db_conn.cursor() as cursor:
            cursor.execute(f"SELECT pg_typeof({expr_sql})")
            return cursor.fetchone()[0]
    except Exception as e:
        return f"ERROR: {e}"


def evaluate_function_args_dynamic(
    sql_text: str,
    function_name: str,
    db_conn,
    function_signatures: List[Dict]
) -> Dict[str, Union[bool, float, List[str], str]]:
    result = {
        "structure_valid": False,
        "function_hit": False,
        "param_type_match_ratio": 0.0,
        "actual_arg_types": [],
        "expected_arg_types": [],
        "param_type_match_detail": [],
        "error": ""
    }
    # 1. AST parse
    try:
        tree = parse_sql(sql_text)
        result["structure_valid"] = True
    except Exception as e:
        result["error"] = f"AST parse failed: {e}"
        return result

    # 2. Find function call
    visitor = FunctionCallVisitor(function_name)
    visitor(tree)
    if not visitor.matches:
        result["error"] = "Function not found"
        return result

    result["function_hit"] = True
    func_node = visitor.matches[0]
    args = func_node.args or []
    arg_exprs = [RawStream()(arg).strip() for arg in args]
    actual_types = [get_pg_typeof(expr, db_conn) for expr in arg_exprs]
    result["actual_arg_types"] = actual_types

    # 3. Match all signature overloads and keep the best match
    best_ratio = 0
    best_detail = []
    best_expected = []

    for sig in function_signatures:
        if sig["function_name"].lower() != function_name.lower():
            continue
        expected_types = sig["input_types"]
        is_variadic = str(sig.get("variadic", "False")).lower() == "true"

        if not is_variadic and len(actual_types) != len(expected_types):
            continue

        if is_variadic:
            if len(actual_types) < len(expected_types):
                continue
            fixed_len = len(expected_types)
            match_count = 0
            match_detail = []
            all_match = True

            for i in range(fixed_len):
                a, e = actual_types[i], expected_types[i]
                if a == e:
                    match_count += 1
                    match_detail.append(f"✔ {a}")
                else:
                    match_detail.append(f"✘ {a} ≠ {e}")
                    all_match = False

            variadic_type = expected_types[-1]
            for i in range(fixed_len, len(actual_types)):
                a = actual_types[i]
                if a == variadic_type:
                    match_count += 1
                    match_detail.append(f"✔ {a}")
                else:
                    match_detail.append(f"✘ {a} ≠ {variadic_type}")
                    all_match = False

            ratio = match_count / len(actual_types)

        else:
            match_count = 0
            match_detail = []
            all_match = True
            for a, e in zip(actual_types, expected_types):
                if a == e:
                    match_count += 1
                    match_detail.append(f"✔ {a}")
                else:
                    match_detail.append(f"✘ {a} ≠ {e}")
                    all_match = False
            ratio = match_count / len(expected_types)

        if ratio > best_ratio:
            best_ratio = ratio
            best_detail = match_detail
            best_expected = expected_types

    result["expected_arg_types"] = best_expected
    result["param_type_match_ratio"] = round(best_ratio, 4)
    result["param_type_match_detail"] = best_detail

    return result