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


def find_cc_chg_step(step_list, target_seconds=7200):
    best_step = None
    best_diff = float("inf")
    for step in step_list:
        typ = step.get("step_type", "").replace("_", " ").lower()
        if typ == "cc chg":
            duration = parse_duration(step.get("step_time", ""))
            if duration is None:
                continue
            diff = abs(duration - target_seconds)
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


def check_step_7(step_list, aux):
    result_table = [["check", "RESULT", "DETAIL", "REASON"]]

    # 1. Find the step
    cc_chg_step = find_cc_chg_step(step_list)
    if not cc_chg_step:
        result_table.append(["max_temp_probe", "NG", "No CC Chg step found", "No step"])
        return result_table

    start = cc_chg_step.get("oneset_date")
    end = cc_chg_step.get("oneset_end_date")
    aux_win = get_aux_in_window(aux, start, end)

    # 2. Check temp probes T1-T4
    temp_fields = [f"bms_temp_{i}_c" for i in range(1, 5)]
    max_temps = []
    for t in temp_fields:
        vals = [
            parse_float(row.get(t))
            for row in aux_win
            if t in row and parse_float(row.get(t)) is not None
        ]
        if vals:
            max_temps.append((t, max(vals)))
    fail_temps = [(k, v) for k, v in max_temps if v >= 50]
    detail = ", ".join([f"{k}: {v:.2f}°C" for k, v in max_temps])
    if not max_temps:
        result_table.append(
            ["max_temp_probe", "NG", "No data", "No probe temps in aux"]
        )
    elif not fail_temps:
        result_table.append(["max_temp_probe", "PASS", detail, "All <50°C"])
    else:
        fails = ", ".join([f"{k}: {v:.2f}°C" for k, v in fail_temps])
        result_table.append(["max_temp_probe", "NG", detail, f"Over 50°C: {fails}"])

    # 3. Check MOSFET temperature (now using 'mos_temp')
    mosfet_field = "mos_temp"
    mosfet_vals = [
        parse_float(row.get(mosfet_field))
        for row in aux_win
        if mosfet_field in row and parse_float(row.get(mosfet_field)) is not None
    ]
    if mosfet_vals:
        max_mosfet = max(mosfet_vals)
        if max_mosfet < 75:
            result_table.append(
                ["charge_mosfet_temp", "PASS", f"Max: {max_mosfet:.2f}°C", "<75°C"]
            )
        else:
            result_table.append(
                ["charge_mosfet_temp", "NG", f"Max: {max_mosfet:.2f}°C", "≥75°C"]
            )
    else:
        result_table.append(
            [
                "charge_mosfet_temp",
                "INFO",
                "No MOSFET temp data",
                "Field not found/empty",
            ]
        )

    return result_table


def main(json_path):
    with open(json_path) as f:
        data = json.load(f)
    step_list = data["data"].get("step", [])
    aux = data["data"].get("auxDBC") or data["data"].get("aux_dbc", [])
    result = check_step_7(step_list, aux)
    print(tabulate(result, headers="firstrow", tablefmt="github"))


if __name__ == "__main__":
    main(
        "/Users/riorizki/development/repos/ION-Mobility-Team/mes/LimeNDAX/result/LG_2_EOL_test_15-1-4-20250428125222_response.json"
    )
