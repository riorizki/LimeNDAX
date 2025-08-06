import json
from tabulate import tabulate
from datetime import datetime


def parse_dt(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except Exception:
        try:
            return datetime.strptime(s, "%Y-%m-%d %H:%M")
        except Exception:
            return None


def parse_duration(duration_str):
    """Convert hh:mm:ss or mm:ss to total seconds."""
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


def find_cccv_step(step_list, target_seconds=10800):
    # Find CCCV steps and pick the closest to 3 hours
    best_step = None
    best_diff = float("inf")
    for step in step_list:
        typ = step.get("step_type", "").replace("_", " ").lower()
        if typ == "cccv chg":
            duration = parse_duration(step.get("step_time", ""))
            if duration is None:
                continue
            diff = abs(duration - target_seconds)
            if diff < best_diff:
                best_step = step
                best_diff = diff
    return best_step


def check_charge_step(step, aux_all, cycle):
    aux = get_aux_in_window(
        aux_all, step.get("oneset_date", ""), step.get("end_date", "")
    )

    result_table = [["check", "RESULT", "DETAIL", "REASON"]]

    # 1. No BMS errors during charging (all bms_err_1~22 == 0)
    error_fields = [f"bms_err_{i}" for i in range(1, 23)]
    error_found = False
    for row in aux:
        for field in error_fields:
            if row.get(field, "0") not in ("0", 0):
                error_found = True
                break
        if error_found:
            break
    if not aux:
        result_table.append(
            ["bms_errors", "NG", "No aux data in window", "Cannot check BMS errors"]
        )
    elif not error_found:
        result_table.append(
            ["bms_errors", "PASS", "No BMS errors found", "All bms_err_* fields = 0"]
        )
    else:
        result_table.append(
            ["bms_errors", "NG", "BMS errors reported", "At least one bms_err_* ≠ 0"]
        )

    # Only use temp fields 1–4 for all temperature checks!
    temp_fields = [f"bms_temp_{i}_c" for i in range(1, 5)]

    # 2. All reported temperature values between 20–50°C
    temps = []
    out_of_range = False
    for row in aux:
        for k in temp_fields:
            try:
                fval = float(row.get(k, -100))
                if fval != -40 and fval != -100:
                    temps.append(fval)
                    if not (20 <= fval <= 50):
                        out_of_range = True
            except Exception:
                continue
    if not aux or not temps:
        result_table.append(
            ["temp_within_20_50", "NG", "No temperature data", "No data to check"]
        )
    elif not out_of_range:
        result_table.append(
            [
                "temp_within_20_50",
                "PASS",
                f"All temps in 20–50°C",
                "All temperature probes within spec",
            ]
        )
    else:
        outvals = [t for t in temps if t < 20 or t > 50]
        result_table.append(
            [
                "temp_within_20_50",
                "NG",
                f"Out of range: {outvals}",
                "At least one temp probe <20°C or >50°C",
            ]
        )

    # 3. Temperature rise from start to finish <10°C (use all temp probes)
    temp_rises = []
    for k in temp_fields:
        try:
            series = [
                float(row.get(k, -100))
                for row in aux
                if float(row.get(k, -100)) != -40 and float(row.get(k, -100)) != -100
            ]
            if series:
                rise = max(series) - min(series)
                temp_rises.append(rise)
        except Exception:
            continue
    max_rise = max(temp_rises) if temp_rises else None
    if max_rise is not None and max_rise < 10:
        result_table.append(
            ["temp_rise", "PASS", f"Max rise = {max_rise:.2f}°C", "Within 10°C"]
        )
    elif max_rise is not None:
        result_table.append(
            ["temp_rise", "NG", f"Max rise = {max_rise:.2f}°C", "Exceeds 10°C"]
        )
    else:
        result_table.append(
            ["temp_rise", "NG", "No temp rise data", "No temperature series found"]
        )

    # 4. Charge capacity 41–45Ah (prefer step, fallback to cycle)
    cap = None
    try:
        cap = float(step.get("capacity_ah", "0"))
        if cap == 0:
            cap = float(cycle.get("chg_cap_ah", "0"))
    except Exception:
        cap = None
    if cap is not None and 41 <= cap <= 45:
        result_table.append(
            ["charge_capacity", "PASS", f"Capacity={cap:.3f}Ah", "Within 41–45Ah"]
        )
    elif cap is not None:
        result_table.append(
            ["charge_capacity", "NG", f"Capacity={cap:.3f}Ah", "Out of range 41–45Ah"]
        )
    else:
        result_table.append(
            ["charge_capacity", "NG", "Capacity not found", "No data to check"]
        )

    # 5. Max-min temp <3°C (probe delta at any time)
    min_deltas = []
    for row in aux:
        rowtemps = []
        for k in temp_fields:
            try:
                fval = float(row.get(k, -100))
                if fval != -40 and fval != -100:
                    rowtemps.append(fval)
            except Exception:
                continue
        if rowtemps:
            min_deltas.append(max(rowtemps) - min(rowtemps))
    max_probe_delta = max(min_deltas) if min_deltas else None
    if max_probe_delta is not None and max_probe_delta < 3:
        result_table.append(
            [
                "temp_probe_delta",
                "PASS",
                f"Max ΔT={max_probe_delta:.2f}°C",
                "Within 3°C",
            ]
        )
    elif max_probe_delta is not None:
        result_table.append(
            ["temp_probe_delta", "NG", f"Max ΔT={max_probe_delta:.2f}°C", "Exceeds 3°C"]
        )
    else:
        result_table.append(
            ["temp_probe_delta", "NG", "No probe delta data", "No data to check"]
        )

    # 6. MOSFET temp <75°C (mos_temp)
    mos_temps = []
    for row in aux:
        try:
            fval = float(row.get("mos_temp", -100))
            if fval != -100:
                mos_temps.append(fval)
        except Exception:
            continue
    over_75 = any(mt > 75 for mt in mos_temps)
    if not aux or not mos_temps:
        result_table.append(
            ["mosfet_temp", "NG", "No MOSFET temp data", "No data to check"]
        )
    elif not over_75:
        result_table.append(
            [
                "mosfet_temp",
                "PASS",
                f"Max MOSFET temp = {max(mos_temps):.2f}°C",
                "All ≤ 75°C",
            ]
        )
    else:
        overvals = [mt for mt in mos_temps if mt > 75]
        result_table.append(
            ["mosfet_temp", "NG", f"Over 75°C: {overvals}", "MOSFET temp exceeds 75°C"]
        )

    return result_table


# =============== MAIN CHARGE STEP CHECKER ===============
def main(json_path):
    with open(json_path) as f:
        data = json.load(f)
    step_list = data["data"].get("step", [])
    aux = data["data"].get("auxDBC") or data["data"].get("aux_dbc", [])
    cycle = data["data"].get("cycle", [{}])[0]  # For chg_cap_ah

    cccv_step = find_cccv_step(step_list)
    if cccv_step:
        print(
            f"\n🔍 Checking step: {cccv_step.get('step_name', '')} | Duration: {cccv_step.get('step_time', '')}"
        )
        charge_result = check_charge_step(cccv_step, aux, cycle)
        print(tabulate(charge_result, headers="firstrow", tablefmt="github"))
    else:
        print("No suitable CCCV Chg step found.")


if __name__ == "__main__":
    main(
        "/Users/riorizki/development/repos/ION-Mobility-Team/mes/LimeNDAX/result/LG_2_EOL_test_15-1-4-20250428125222_response.json"
    )
