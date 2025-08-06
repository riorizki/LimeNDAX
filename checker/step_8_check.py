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


def extract_serial_from_aux(aux):
    serial_fields = [f"bms_serial_num_{i}" for i in range(1, 18)]
    for row in reversed(aux):  # last row first
        chars = []
        for k in serial_fields:
            v = row.get(k)
            try:
                if v is not None and int(v) != 0:
                    chars.append(chr(int(v)))
            except Exception:
                pass
        # If valid ASCII chars found and not all are weird or null
        serial_str = "".join(chars)
        if serial_str and any(c.isalnum() for c in serial_str):
            return serial_str
    return None


def check_final_rest_step(step, aux_all, test_info):
    idx = step.get("step_index", "")
    name = step.get("step_type", "")
    result_table = [["step", "", "", f"Step {idx} - {name}"]]
    aux = get_aux_in_window(
        aux_all, step.get("oneset_date", ""), step.get("end_date", "")
    )

    # 1. End voltage 70–78V
    v = None
    if aux:
        for k in aux[-1]:
            if k.lower() in ("bms_volt_v_v", "bms_volt_v"):
                try:
                    v = float(aux[-1][k])
                except Exception:
                    pass
                break
    if v is not None and 70.0 <= v <= 78.0:
        result_table.append(["end_voltage", "PASS", f"end={v:.3f}V", ""])
    else:
        result_table.append(["end_voltage", "FAIL", f"end={v}", ""])

    # 2. Temp stability: within ±1°C in each probe
    temp_keys = ["bms_temp_1_c", "bms_temp_2_c", "bms_temp_3_c", "bms_temp_4_c"]
    probes_ok = True
    probe_fluct = []
    if aux:
        for k in temp_keys:
            vals = []
            for row in aux:
                try:
                    vals.append(float(row[k]))
                except Exception:
                    pass
            if vals:
                spread = max(vals) - min(vals)
                probe_fluct.append(spread)
                if spread > 1.0:
                    probes_ok = False
    if probe_fluct and probes_ok:
        result_table.append(["temp_stability", "PASS", f"fluct={probe_fluct}", ""])
    else:
        result_table.append(["temp_stability", "FAIL", f"fluct={probe_fluct}", ""])

    # 3. String voltage not fluctuating: all cell voltages within 1mV at end
    cell_volt_keys = (
        [k for k in aux[-1].keys() if "cell_volt" in k.lower()] if aux else []
    )
    cell_volts = []
    if aux and cell_volt_keys:
        for k in cell_volt_keys:
            try:
                val = float(aux[-1][k])
                cell_volts.append(val)
            except Exception:
                pass
        if cell_volts:
            spread = max(cell_volts) - min(cell_volts)
            if spread <= 1:
                result_table.append(
                    ["string_voltage", "PASS", f"spread={spread:.2f}mV", ""]
                )
            else:
                result_table.append(
                    ["string_voltage", "FAIL", f"spread={spread:.2f}mV", ""]
                )
        else:
            result_table.append(["string_voltage", "FAIL", "No cell voltages", ""])
    else:
        result_table.append(["string_voltage", "FAIL", "No cell voltages", ""])

    # 4. BMS CAN packets
    if aux and len(aux) > 0:
        result_table.append(["bms_packets", "PASS", f"{len(aux)} BMS CAN packets", ""])
    else:
        result_table.append(["bms_packets", "FAIL", "No BMS CAN packets", ""])

    # 5. Serial number (prefer aux, fallback test_info['barcode'])
    serial = extract_serial_from_aux(aux)
    test_barcode = test_info.get("barcode", "") if test_info else ""
    if test_barcode:
        result_table.append(
            ["serial_number", "PASS", f"from Barcode={test_barcode}", ""]
        )
    elif serial:
        result_table.append(["serial_number", "PASS", f"from aux Serial={serial}", ""])
    else:
        result_table.append(["serial_number", "FAIL", "No serial/Barcode found", ""])

    return result_table


def main(json_path):
    with open(json_path) as f:
        data = json.load(f)

    step_list = data["data"].get("step", [])
    aux = data["data"].get("auxDBC") or data["data"].get("aux_dbc", [])
    test_info = data["data"].get("test", {}).get("test_information", {})

    # Find last rest step (or pick by index as needed)
    rest_steps = [
        step for step in step_list if step.get("step_type", "").lower() == "rest"
    ]
    if rest_steps:
        rest_result = check_final_rest_step(rest_steps[-1], aux, test_info)
        print("\n🔍", rest_result[0][3])
        print(tabulate(rest_result, headers="firstrow", tablefmt="github"))
    else:
        print("No 'Rest' step found in parsed JSON.")


if __name__ == "__main__":
    main("/Users/rio.wijaya/Downloads/LimeNDAX/parsed_output.json")
