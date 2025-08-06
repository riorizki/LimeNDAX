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


def check_topup_charge_step(step, aux_all):
    idx = step.get("step_index", "")
    name = step.get("step_type", "")
    result_table = [["step", "", "", f"Step {idx} - {name}"]]
    aux = get_aux_in_window(
        aux_all, step.get("oneset_date", ""), step.get("end_date", "")
    )

    # 1. Max of T1-T4 < 50°C
    temp_keys = ["bms_temp_1_c", "bms_temp_2_c", "bms_temp_3_c", "bms_temp_4_c"]
    max_temp = None
    all_temps = []
    for row in aux:
        for k in temp_keys:
            try:
                val = float(row[k])
                all_temps.append(val)
            except Exception:
                pass
    if all_temps:
        max_temp = max(all_temps)
        if max_temp < 50:
            result_table.append(["max_probe_temp", "PASS", f"max={max_temp:.2f}°C", ""])
        else:
            result_table.append(["max_probe_temp", "FAIL", f"max={max_temp:.2f}°C", ""])
    else:
        result_table.append(["max_probe_temp", "FAIL", "No probe temp data", ""])

    # 2. MOSFET temp < 75°C (mos_temp)
    mos_temps = []
    for row in aux:
        try:
            mos_temps.append(float(row["mos_temp"]))
        except Exception:
            pass
    if mos_temps:
        max_mos = max(mos_temps)
        if max_mos < 75:
            result_table.append(["mosfet_temp", "PASS", f"max={max_mos:.2f}°C", ""])
        else:
            result_table.append(["mosfet_temp", "FAIL", f"max={max_mos:.2f}°C", ""])
    else:
        result_table.append(["mosfet_temp", "FAIL", "No MOSFET temp data", ""])

    return result_table


def main(json_path):
    with open(json_path) as f:
        data = json.load(f)

    step_list = data["data"].get("step", [])
    aux = data["data"].get("auxDBC") or data["data"].get("aux_dbc", [])

    # Match any step_type containing "chg" (case-insensitive)
    chg_pattern = re.compile(r"chg", re.IGNORECASE)
    found = False
    for step in step_list:
        if chg_pattern.search(step.get("step_type", "")):
            result = check_topup_charge_step(step, aux)
            print("\n🔍", result[0][3])
            print(tabulate(result, headers="firstrow", tablefmt="github"))
            found = True
            # If you want *all* chg steps, remove the 'break'
            break
    if not found:
        print("No 'chg' (charge) step found in parsed JSON.")


if __name__ == "__main__":
    main("/Users/rio.wijaya/Downloads/LimeNDAX/parsed_output.json")
