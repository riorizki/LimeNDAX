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


def parse_float(val):
    try:
        return float(val)
    except:
        return None


def find_last_rest_10s_step(step_list, target_sec=10):
    best_step = None
    best_diff = float("inf")
    for step in reversed(step_list):
        typ = step.get("step_type", "").replace("_", " ").lower()
        if typ == "rest":
            duration = parse_duration(step.get("step_time", ""))
            if duration is None:
                continue
            diff = abs(duration - target_sec)
            if diff < best_diff:
                best_step = step
                best_diff = diff
    return best_step


def get_aux_in_window(aux, start, end):
    sdt = parse_dt(start) if isinstance(start, str) else start
    edt = parse_dt(end) if isinstance(end, str) else end
    if not sdt or not edt:
        return aux
    selected = []
    for row in aux:
        dt = row.get("date") or row.get("datetime")
        dt = parse_dt(dt)
        if dt and sdt <= dt <= edt:
            selected.append(row)
    return selected


def check_step_8(step_list, aux):
    result_table = [["check", "RESULT", "DETAIL", "REASON"]]
    # 1. Find step
    rest_step = find_last_rest_10s_step(step_list)
    if not rest_step:
        result_table.append(["rest_step", "NG", "No Rest 10s step found", ""])
        return result_table
    start = rest_step.get("oneset_date")
    end = rest_step.get("oneset_end_date")
    aux_win = get_aux_in_window(aux, start, end)
    if not aux_win:
        result_table.append(["rest_step", "NG", "No aux data in window", ""])
        return result_table

    # 2. Pack voltage at end
    pack_v = parse_float(aux_win[-1].get("pack_voltage_v"))
    if pack_v is not None and 70 <= pack_v <= 78:
        result_table.append(
            ["pack_voltage_end", "PASS", f"{pack_v:.2f}V", "Within 70-78V"]
        )
    elif pack_v is not None:
        result_table.append(
            ["pack_voltage_end", "NG", f"{pack_v:.2f}V", "Out of 70-78V"]
        )
    else:
        result_table.append(["pack_voltage_end", "NG", "No pack_voltage_v", ""])

    # 3. Temp probe ±1°C fluctuation
    temp_fields = [f"bms_temp_{i}_c" for i in range(1, 5)]
    temp_flucts = []
    for t in temp_fields:
        vals = [parse_float(row.get(t)) for row in aux_win if row.get(t) is not None]
        vals = [v for v in vals if v is not None]
        if vals:
            fluct = max(vals) - min(vals)
            temp_flucts.append((t, fluct))
    fail_temps = [(k, v) for k, v in temp_flucts if v > 2.0]  # ±1°C → 2°C spread
    detail = ", ".join([f"{k}: Δ={d:.2f}°C" for k, d in temp_flucts])
    if not temp_flucts:
        result_table.append(
            ["temp_fluctuation", "NG", "No data", "No probe temps in aux"]
        )
    elif not fail_temps:
        result_table.append(
            ["temp_fluctuation", "PASS", detail, "All probes within ±1°C"]
        )
    else:
        fails = ", ".join([f"{k}: Δ={v:.2f}°C" for k, v in fail_temps])
        result_table.append(
            ["temp_fluctuation", "NG", detail, f"Probe(s) >±1°C: {fails}"]
        )

    # 4. String voltage ±1mV fluctuation (all cell_volt_mv_xx_mv)
    cell_fields = [
        k
        for k in aux_win[0].keys()
        if k.startswith("cell_volt_mv_") and k.endswith("_mv")
    ]
    cell_flucts = []
    for c in cell_fields:
        vals = [parse_float(row.get(c)) for row in aux_win if row.get(c) is not None]
        vals = [v for v in vals if v is not None]
        if vals:
            fluct = max(vals) - min(vals)
            cell_flucts.append((c, fluct))
    fail_cells = [(k, v) for k, v in cell_flucts if v > 1.0]
    detail = ", ".join([f"{k}: Δ={d:.2f}mV" for k, d in cell_flucts])
    if not cell_flucts:
        result_table.append(
            ["cell_fluctuation", "NG", "No data", "No cell voltage in aux"]
        )
    elif not fail_cells:
        result_table.append(
            ["cell_fluctuation", "PASS", detail, "All cells within ±1mV"]
        )
    else:
        fails = ", ".join([f"{k}: Δ={v:.2f}mV" for k, v in fail_cells])
        result_table.append(
            ["cell_fluctuation", "NG", detail, f"Cell(s) >±1mV: {fails}"]
        )

    # 5. (Skip CAN/serial checks unless data is present)
    return result_table


def short(text, maxlen=120):
    if isinstance(text, str) and len(text) > maxlen:
        return text[: maxlen - 3] + "..."
    return text


# In your main print section, after tabulate:
def print_result_short(result_table):
    # Make a short version for each row
    short_table = [result_table[0]]  # header
    for row in result_table[1:]:
        short_table.append([row[0], row[1], short(row[2]), short(row[3])])
    print(tabulate(short_table, headers="firstrow", tablefmt="github"))


def main(json_path):
    with open(json_path) as f:
        data = json.load(f)
    step_list = data["data"].get("step", [])
    aux = data["data"].get("auxDBC") or data["data"].get("aux_dbc", [])
    result = check_step_8(step_list, aux)
    print_result_short(result)


if __name__ == "__main__":
    main(
        "/Users/riorizki/development/repos/ION-Mobility-Team/mes/LimeNDAX/result/LG_2_EOL_test_15-1-4-20250428125222_response.json"
    )
