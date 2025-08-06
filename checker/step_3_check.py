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


def check_rest_step(step, aux_all):
    idx = step.get("step_index", "")
    name = step.get("step_type", "")  # <<<<<<<< IMPORTANT: Use step_type!
    result_table = [["step", "", "", f"Step {idx} - {name}"]]

    aux = get_aux_in_window(
        aux_all, step.get("oneset_date", ""), step.get("end_date", "")
    )

    # --- 1. <30mV decrease in string voltage (any cell) start to end ---
    # We'll use all "cell_volt_mv_X_mv" keys (and assume they're in mV)
    cell_keys = (
        [k for k in aux[0] if "cell_volt" in k and k.endswith("_mv")] if aux else []
    )
    start_cells = [float(aux[0][k]) for k in cell_keys if k in aux[0]]
    end_cells = [float(aux[-1][k]) for k in cell_keys if k in aux[-1]]
    cell_delta = [start - end for start, end in zip(start_cells, end_cells)]
    max_decrease = max(cell_delta) if cell_delta else None
    if max_decrease is not None and max_decrease < 30:
        result_table.append(
            ["cell_voltage_drop", "PASS", f"Δcell={max_decrease:.1f}mV", ""]
        )
    else:
        result_table.append(["cell_voltage_drop", "FAIL", f"Δcell={max_decrease}", ""])

    # --- 2. <1V decrease in pack voltage (bms_volt_v_v or oneset_volt_v in step) ---
    pack_start = float(aux[0].get("bms_volt_v_v", 0)) if aux else 0
    pack_end = float(aux[-1].get("bms_volt_v_v", 0)) if aux else 0
    pack_drop = pack_start - pack_end
    if pack_drop is not None and pack_drop < 1:
        result_table.append(
            ["pack_voltage_drop", "PASS", f"Δpack={pack_drop:.3f}V", ""]
        )
    else:
        result_table.append(["pack_voltage_drop", "FAIL", f"Δpack={pack_drop}", ""])

    # --- 3. max-min cell voltage < 20mV at end of rest ---
    if end_cells:
        cell_range = max(end_cells) - min(end_cells)
    else:
        cell_range = None
    if cell_range is not None and cell_range < 20:
        result_table.append(
            ["cell_voltage_spread", "PASS", f"spread={cell_range:.1f}mV", ""]
        )
    else:
        result_table.append(["cell_voltage_spread", "FAIL", f"spread={cell_range}", ""])

    # --- 4. 0 <= temp decrease in each probe <= 5C (use only 4 probes) ---
    temp_keys = ["bms_temp_1_c", "bms_temp_2_c", "bms_temp_3_c", "bms_temp_4_c"]
    probe_deltas = []
    temp_check = "PASS"
    for k in temp_keys:
        t_start = float(aux[0].get(k, 0)) if aux else 0
        t_end = float(aux[-1].get(k, 0)) if aux else 0
        delta = t_start - t_end
        probe_deltas.append(delta)
        if not (0 <= delta <= 5):
            temp_check = "FAIL"
    result_table.append(["probe_temp_drop", temp_check, f"deltas={probe_deltas}", ""])

    # --- 5. max-min temp < 3C over rest (across all probes, all times) ---
    all_temps = []
    for row in aux:
        for k in temp_keys:
            try:
                all_temps.append(float(row[k]))
            except Exception:
                pass
    temp_spread = (max(all_temps) - min(all_temps)) if all_temps else None
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

    # Pick the next Rest step after a Chg/Dchg or by index
    for step in step_list:
        if step.get("step_type", "").lower() == "rest":
            rest_result = check_rest_step(step, aux)
            print("\n🔍", rest_result[0][3])
            print(tabulate(rest_result, headers="firstrow", tablefmt="github"))
            break
    else:
        print("No 'Rest' step found in parsed JSON.")


if __name__ == "__main__":
    main("/Users/rio.wijaya/Downloads/LimeNDAX/parsed_output.json")
