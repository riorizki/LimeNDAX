import json
from tabulate import tabulate
from datetime import datetime
import re


def summarize_list(lst, max_len=5):
    if len(lst) > max_len:
        return f"{lst[:max_len]} ..."
    else:
        return str(lst)


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


def check_discharge_capacity_step(step, aux_all, cycle_data=None):
    idx = step.get("step_index", "")
    name = step.get("step_type", "")
    result_table = [["step", "", "", f"Step {idx} - {name}"]]
    aux = get_aux_in_window(
        aux_all, step.get("oneset_date", ""), step.get("end_date", "")
    )

    # 1. Discharge capacity (from step/cycle): 53–57Ah
    cap = None
    if "capacity_ah" in step:
        try:
            cap = float(step["capacity_ah"])
        except Exception:
            cap = None
    if cap is None and cycle_data:
        # Try get dchg_cap_ah from cycle table (by index)
        try:
            cap = float(cycle_data[0].get("dchg_cap_ah", 0))
        except Exception:
            cap = None
    if cap is not None and 53 <= cap <= 57:
        result_table.append(["discharge_capacity", "PASS", f"capacity={cap:.3f}Ah", ""])
    else:
        result_table.append(["discharge_capacity", "FAIL", f"capacity={cap}", ""])

    # 2. Absolute current error <2.4A (4%)
    target_current = float(step.get("current_a", 0) or 0)
    measured_currents = [
        float(row.get("bms_current_a_a", 0) or 0)
        for row in aux
        if "bms_current_a_a" in row
    ]
    max_abs_error = None
    if measured_currents and target_current != 0:
        max_abs_error = max([abs(c - target_current) for c in measured_currents])
        if max_abs_error < 2.4:
            result_table.append(
                ["current_error", "PASS", f"max error={max_abs_error:.2f}A", ""]
            )
        else:
            result_table.append(
                ["current_error", "FAIL", f"max error={max_abs_error:.2f}A", ""]
            )
    elif not measured_currents:
        result_table.append(
            ["current_error", "FAIL", "No valid BMS current readings", ""]
        )
    else:
        result_table.append(["current_error", "FAIL", "Target current is 0", ""])

    # 3. Max temp before/after discharge < 30C (use 4 probe temps)
    temp_keys = ["bms_temp_1_c", "bms_temp_2_c", "bms_temp_3_c", "bms_temp_4_c"]
    max_temp = None
    temps = []
    for row in aux:
        for k in temp_keys:
            try:
                val = float(row[k])
                temps.append(val)
            except Exception:
                pass
    if temps:
        max_temp = max(temps)
        if max_temp < 30:
            result_table.append(["max_temp", "PASS", f"max={max_temp:.1f}°C", ""])
        else:
            result_table.append(["max_temp", "FAIL", f"max={max_temp:.1f}°C", ""])
    else:
        result_table.append(["max_temp", "FAIL", "No temperature data", ""])

    # 4. max-min temp < 3C
    temp_spread = (max(temps) - min(temps)) if temps else None
    if temp_spread is not None and temp_spread < 3:
        result_table.append(["temp_spread", "PASS", f"spread={temp_spread:.2f}°C", ""])
    else:
        result_table.append(["temp_spread", "FAIL", f"spread={temp_spread}", ""])

    # 5. Low voltage warning: pack <64V or string <3.2V
    low_voltage_found = False
    for row in aux:
        try:
            v = float(row.get("bms_volt_v_v", 1e6))
            if v < 64:
                low_voltage_found = True
                break
        except Exception:
            pass
        for k in [k for k in row if "cell_volt" in k.lower()]:
            try:
                vcell = float(row[k])
                if vcell < 3200:
                    low_voltage_found = True
                    break
            except Exception:
                pass
    if low_voltage_found:
        result_table.append(
            ["low_voltage_alert", "PASS", "Alert: voltage < threshold", ""]
        )
    else:
        result_table.append(
            ["low_voltage_alert", "FAIL", "No low voltage detected", ""]
        )

    # 6. String voltage range (max-min < 40mV)
    cell_volt_keys = (
        [k for k in aux[0].keys() if "cell_volt" in k.lower()] if aux else []
    )
    string_voltages = []
    for row in aux:
        for k in cell_volt_keys:
            try:
                val = float(row[k])
                if 0 < val < 6000:
                    string_voltages.append(val)
            except Exception:
                pass
    if string_voltages:
        spread = max(string_voltages) - min(string_voltages)
        if spread < 40:
            result_table.append(
                ["string_voltage_range", "PASS", f"spread={spread:.1f}mV", ""]
            )
        else:
            result_table.append(
                ["string_voltage_range", "FAIL", f"spread={spread:.1f}mV", ""]
            )
    else:
        result_table.append(["string_voltage_range", "FAIL", "No cell voltages", ""])

    return result_table


def main(json_path):
    with open(json_path) as f:
        data = json.load(f)
    step_list = data["data"].get("step", [])
    aux = data["data"].get("auxDBC") or data["data"].get("aux_dbc", [])
    cycle = data["data"].get("cycle", [])

    dchg_pattern = re.compile(r"dchg|discharge", re.IGNORECASE)
    for step in step_list:
        if dchg_pattern.search(step.get("step_type", "")):
            result = check_discharge_capacity_step(step, aux, cycle)
            print("\n🔍", result[0][3])
            print(tabulate(result, headers="firstrow", tablefmt="github"))
            break
    else:
        print("No 'Discharge' step found in parsed JSON.")


if __name__ == "__main__":
    main("/Users/rio.wijaya/Downloads/LimeNDAX/parsed_output.json")
