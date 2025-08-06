# checker/step_2_charge.py

import json
from tabulate import tabulate
from datetime import datetime
import re


def parse_dt(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except Exception:
        try:
            return datetime.strptime(s, "%Y-%m-%d %H:%M")
        except Exception:
            return None


def get_aux_in_window(aux, start, end):
    sdt = parse_dt(start)
    edt = parse_dt(end)
    if not sdt or not edt:
        return aux
    selected = []
    for row in aux:
        dt = row.get("date") or row.get("datetime")
        if not dt:
            continue
        dt = parse_dt(dt)
        if dt and sdt <= dt <= edt:
            selected.append(row)
    return selected


def check_charge_step(step, aux_all):
    idx = step.get("step_index", "")
    name = step.get("step_name", "")
    result_table = [["step", "", "", f"Step {idx} - {name}"]]

    aux = get_aux_in_window(
        aux_all, step.get("oneset_date", ""), step.get("end_date", "")
    )

    # 1. No BMS errors reported
    bms_err_cols = [k for k in aux[0].keys() if "bms_err" in k.lower()] if aux else []
    bms_error_found = any(
        float(row[k]) != 0
        for row in aux
        for k in bms_err_cols
        if k in row and row[k] not in ("", None)
    )
    if bms_error_found:
        result_table.append(["bms_error", "FAIL", "BMS error reported", ""])
    else:
        result_table.append(["bms_error", "PASS", "No BMS errors", ""])

    # 2. All reported temp between 20-50C (only the 4 bms_temp_x_c as requested)
    temp_keys = ["bms_temp_1_c", "bms_temp_2_c", "bms_temp_3_c", "bms_temp_4_c"]
    temps = []
    for row in aux:
        for k in temp_keys:
            try:
                val = float(row[k])
                temps.append(val)
            except Exception:
                pass
    if temps and all(20 <= t <= 50 for t in temps):
        result_table.append(["temp_range", "PASS", f"All 20–50°C", ""])
    else:
        failtemps = [t for t in temps if t < 20 or t > 50]
        result_table.append(["temp_range", "FAIL", f"Out of range: {failtemps}", ""])

    # 3. Temp rise < 10C (across all those 4 temp keys, min at start vs max at end)
    t_start = [
        float(aux[0][k])
        for k in temp_keys
        if aux and k in aux[0] and aux[0][k] not in ("", None)
    ]
    t_end = [
        float(aux[-1][k])
        for k in temp_keys
        if aux and k in aux[-1] and aux[-1][k] not in ("", None)
    ]
    temp_rise = (max(t_end) if t_end else 0) - (min(t_start) if t_start else 0)
    if t_start and t_end and temp_rise < 10:
        result_table.append(["temp_rise", "PASS", f"ΔT={temp_rise:.2f}°C", ""])
    else:
        result_table.append(["temp_rise", "FAIL", f"ΔT={temp_rise}", ""])

    # 4. Charge capacity 41–45 Ah (use capacity_ah or chg_cap_ah from step)
    cap = None
    for key in ("capacity_ah", "chg_cap_ah"):
        if key in step:
            try:
                cap = float(step[key])
                break
            except Exception:
                cap = None
    if cap is not None and 41 <= cap <= 45:
        result_table.append(["charge_capacity", "PASS", f"capacity={cap:.3f}Ah", ""])
    else:
        result_table.append(["charge_capacity", "FAIL", f"capacity={cap}", ""])

    # 5. max-min temp < 3C (all temp_keys, at any time)
    temp_spread = (max(temps) - min(temps)) if temps else None
    if temp_spread is not None and temp_spread < 3:
        result_table.append(["temp_spread", "PASS", f"ΔT={temp_spread:.2f}°C", ""])
    else:
        result_table.append(["temp_spread", "FAIL", f"ΔT={temp_spread}", ""])

    # 6. MOSFET temp < 75C (mos_temp)
    mos_temps = []
    for row in aux:
        try:
            mos_temps.append(float(row["mos_temp"]))
        except Exception:
            pass
    if mos_temps and max(mos_temps) < 75:
        result_table.append(["mosfet_temp", "PASS", f"max={max(mos_temps):.2f}°C", ""])
    else:
        result_table.append(
            ["mosfet_temp", "FAIL", f"max={max(mos_temps) if mos_temps else None}", ""]
        )

    return result_table


def main(json_path):
    with open(json_path) as f:
        data = json.load(f)

    step_list = data["data"].get("step", [])
    aux = data["data"].get("auxDBC") or data["data"].get("aux_dbc", [])

    print("Available step names in your parsed JSON:")
    for step in step_list:
        print(f"  - Step {step.get('step_index', '')}: '{step.get('step_type', '')}'")

    charge_pattern = re.compile(r"chg|charge", re.IGNORECASE)
    for step in step_list:
        if charge_pattern.search(step.get("step_type", "")):
            charge_result = check_charge_step(step, aux)
            print(tabulate(charge_result, headers="firstrow", tablefmt="github"))
            break
    else:
        print("No 'Charge' step found in parsed JSON.")


if __name__ == "__main__":
    main("/Users/rio.wijaya/Downloads/LimeNDAX/parsed_output.json")
