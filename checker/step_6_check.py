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


def check_rest_after_discharge_step(step, aux_all):
    idx = step.get("step_index", "")
    name = step.get("step_type", "")
    result_table = [["step", "", "", f"Step {idx} - {name}"]]
    aux = get_aux_in_window(
        aux_all, step.get("oneset_date", ""), step.get("end_date", "")
    )

    # --- 1. max-min cell voltage < 20mV at end of rest ---
    cell_volt_keys = (
        [k for k in aux[0].keys() if "cell_volt" in k.lower()] if aux else []
    )
    end_voltages = []
    if aux:
        last = aux[-1]
        for k in cell_volt_keys:
            try:
                val = float(last[k])
                # mV plausible check: 0 < v < 6000
                if 0 < val < 6000:
                    end_voltages.append(val)
            except Exception:
                pass
    spread = max(end_voltages) - min(end_voltages) if end_voltages else None
    if spread is not None and spread < 20:
        result_table.append(
            ["cell_voltage_spread", "PASS", f"spread={spread:.1f}mV", ""]
        )
    else:
        result_table.append(["cell_voltage_spread", "FAIL", f"spread={spread}", ""])

    # --- 2. Reduction < 20°C (max start temp - min end temp, 4 probe keys) ---
    temp_keys = ["bms_temp_1_c", "bms_temp_2_c", "bms_temp_3_c", "bms_temp_4_c"]
    reduction = None
    if aux:
        try:
            start_temps = [float(aux[0][k]) for k in temp_keys]
            end_temps = [float(aux[-1][k]) for k in temp_keys]
            reduction = max(start_temps) - min(end_temps)
        except Exception:
            pass
    if reduction is not None and reduction < 20:
        result_table.append(
            ["temp_reduction", "PASS", f"reduction={reduction:.1f}°C", ""]
        )
    else:
        result_table.append(["temp_reduction", "FAIL", f"reduction={reduction}", ""])

    # --- 3. max-min temp < 3°C (all 4 probes, at any time in window) ---
    all_temps = []
    for row in aux:
        for k in temp_keys:
            try:
                all_temps.append(float(row[k]))
            except Exception:
                pass
    temp_spread = max(all_temps) - min(all_temps) if all_temps else None
    if temp_spread is not None and temp_spread < 3:
        result_table.append(["temp_spread", "PASS", f"spread={temp_spread:.2f}°C", ""])
    else:
        result_table.append(["temp_spread", "FAIL", f"spread={temp_spread}", ""])

    return result_table


def main(json_path):
    with open(json_path) as f:
        data = json.load(f)
    step_list = data["data"].get("step", [])
    aux = data["data"].get("auxDBC") or data["data"].get("aux_dbc", [])

    # Find the Rest step after Discharge (usually step_type == "Rest" and after "DChg")
    # If you know which index, you can select it specifically!
    rest_indices = [
        i
        for i, step in enumerate(step_list)
        if step.get("step_type", "").lower() == "rest"
    ]
    # For demo, pick the *last* Rest step
    if rest_indices:
        rest_idx = rest_indices[-1]
        step = step_list[rest_idx]
        result = check_rest_after_discharge_step(step, aux)
        print("\n🔍", result[0][3])
        print(tabulate(result, headers="firstrow", tablefmt="github"))
    else:
        print("No 'Rest' step found in parsed JSON.")


if __name__ == "__main__":
    main("/Users/rio.wijaya/Downloads/LimeNDAX/parsed_output.json")
