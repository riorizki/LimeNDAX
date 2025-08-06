import json
from tabulate import tabulate
from datetime import datetime, timedelta


def parse_dt(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except Exception:
        try:
            return datetime.strptime(s, "%Y-%m-%d %H:%M")
        except Exception:
            return None


def parse_float(val):
    try:
        return float(val)
    except:
        return None


def parse_duration(duration_str):
    try:
        parts = [int(p) for p in duration_str.split(":")]
        if len(parts) == 3:
            h, m, s = parts
        elif len(parts) == 2:
            h, m, s = 0, *parts
        else:
            return None
        return h * 3600 + m * 60 + s
    except:
        return None


def find_rest_step(step_list, target_minutes=40):
    target_sec = target_minutes * 60
    best_step = None
    best_diff = float("inf")
    for step in step_list:
        typ = step.get("step_type", "").replace("_", " ").lower()
        if typ == "rest":
            dur = parse_duration(step.get("step_time", ""))
            if dur is None:
                continue
            diff = abs(dur - target_sec)
            if diff < best_diff:
                best_diff = diff
                best_step = step
    return best_step


def get_aux_in_window(aux, start, end):
    sdt = parse_dt(start) if isinstance(start, str) else start
    edt = parse_dt(end) if isinstance(end, str) else end
    if not sdt or not edt:
        return aux
    return [
        row
        for row in aux
        if sdt <= parse_dt(row.get("date") or row.get("datetime") or "") <= edt
    ]


def check_step_6(step_list, aux):
    result_table = [["check", "RESULT", "DETAIL", "REASON"]]

    # Find best rest step (≈40min)
    rest_step = find_rest_step(step_list)
    if not rest_step:
        result_table.append(
            ["step_find", "NG", "No rest step", "Not found in step_list"]
        )
        return result_table

    start = rest_step.get("oneset_date")
    end = rest_step.get("oneset_end_date")
    aux_win = get_aux_in_window(aux, start, end)

    # 1. max - min < 20mV at end of rest
    last_row = aux_win[-1] if aux_win else {}
    # Try string voltages first, fall back to cell voltages
    string_voltages = [
        parse_float(last_row[k]) for k in last_row if "string_voltage_v" in k
    ]
    string_voltages = [v for v in string_voltages if v is not None]
    if string_voltages:
        spread_mv = (max(string_voltages) - min(string_voltages)) * 1000  # V to mV
        if spread_mv < 20:
            result_table.append(
                ["string_voltage_spread_end", "PASS", f"Δ={spread_mv:.2f}mV", "<20mV"]
            )
        else:
            result_table.append(
                ["string_voltage_spread_end", "NG", f"Δ={spread_mv:.2f}mV", "≥20mV"]
            )
    else:
        # Cell voltages fallback
        cell_voltages = [
            parse_float(v)
            for k, v in last_row.items()
            if k.startswith("cell_volt_mv_") and k.endswith("_mv")
        ]
        cell_voltages = [v / 1000.0 for v in cell_voltages if v is not None]
        if cell_voltages:
            spread_mv = (max(cell_voltages) - min(cell_voltages)) * 1000
            if spread_mv < 20:
                result_table.append(
                    ["cell_voltage_spread_end", "PASS", f"Δ={spread_mv:.2f}mV", "<20mV"]
                )
            else:
                result_table.append(
                    ["cell_voltage_spread_end", "NG", f"Δ={spread_mv:.2f}mV", "≥20mV"]
                )
        else:
            result_table.append(
                [
                    "cell_voltage_spread_end",
                    "NG",
                    "No voltage data",
                    "No string/cell voltages in aux end row",
                ]
            )

    # 2. Reduction <= 20°C (max temp probe difference from start to end)
    temp_fields = [f"bms_temp_{i}_c" for i in range(1, 5)]
    temp_reductions = []
    if aux_win:
        for tfield in temp_fields:
            t_start = parse_float(aux_win[0].get(tfield))
            t_end = parse_float(aux_win[-1].get(tfield))
            if t_start is not None and t_end is not None:
                temp_reductions.append((tfield, t_start - t_end))
        max_reduction = (
            max([abs(d) for k, d in temp_reductions]) if temp_reductions else None
        )
        detail = ", ".join([f"{k}: Δ={d:.2f}°C" for k, d in temp_reductions])
        if max_reduction is not None and max_reduction <= 20:
            result_table.append(
                ["temp_reduction", "PASS", detail, "All temp reductions ≤ 20°C"]
            )
        elif max_reduction is not None:
            result_table.append(["temp_reduction", "NG", detail, "Reduction > 20°C"])
        else:
            result_table.append(["temp_reduction", "NG", "No data", "No temp data"])
    else:
        result_table.append(["temp_reduction", "NG", "No aux data", "No aux in window"])

    # 3. max-min temp < 3°C (any sample in rest window)
    max_min_spreads = []
    for row in aux_win:
        temps = [parse_float(row.get(t)) for t in temp_fields if row.get(t) is not None]
        temps = [t for t in temps if t is not None]
        if temps:
            spread = max(temps) - min(temps)
            max_min_spreads.append(spread)
    max_spread = max(max_min_spreads) if max_min_spreads else None
    if max_spread is not None and max_spread < 3.0:
        result_table.append(
            ["max_min_temp", "PASS", f"Max spread = {max_spread:.2f}°C", "<3°C"]
        )
    elif max_spread is not None:
        result_table.append(
            ["max_min_temp", "NG", f"Max spread = {max_spread:.2f}°C", "≥3°C found"]
        )
    else:
        result_table.append(["max_min_temp", "NG", "No data", "No temp data"])

    return result_table


def main(json_path):
    with open(json_path) as f:
        data = json.load(f)
    step_list = data["data"].get("step", [])
    aux = data["data"].get("auxDBC") or data["data"].get("aux_dbc", [])
    result = check_step_6(step_list, aux)
    print(tabulate(result, headers="firstrow", tablefmt="github"))


if __name__ == "__main__":
    main(
        "/Users/riorizki/development/repos/ION-Mobility-Team/mes/LimeNDAX/result/LG_2_EOL_test_15-1-4-20250428125222_response.json"
    )
