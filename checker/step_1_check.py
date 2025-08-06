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
    name = step.get("step_name", "")
    result_table = [["step", "", "", f"Step {idx} - {name}"]]

    aux = get_aux_in_window(
        aux_all, step.get("oneset_date", ""), step.get("end_date", "")
    )

    # Start voltage 68–73V (prefer BMS CAN if present)
    v = step.get("oneset_volt_v")
    try:
        v = float(v)
    except:
        v = None

    bms_vs = []
    for row in aux:
        for k in row:
            if k.lower() in ("bms_volt_v_v", "bms_volt_v"):
                try:
                    val = float(row[k])
                    if val > 1:
                        bms_vs.append(val)
                except Exception:
                    pass
    aux_bms_v = sum(bms_vs) / len(bms_vs) if bms_vs else None
    voltage_for_check = aux_bms_v if (aux_bms_v is not None) else v
    if voltage_for_check is not None and 68.0 <= voltage_for_check <= 73.0:
        result_table.append(
            ["start_voltage", "PASS", f"Voltage={voltage_for_check:.4f}V", ""]
        )
    else:
        failval = voltage_for_check if voltage_for_check is not None else "N/A"
        result_table.append(["start_voltage", "FAIL", f"Voltage={failval}", ""])

    # Temp stability (only bms_temp_1_c ~ bms_temp_4_c, ignore -40C which means unused)
    temps = []
    for row in aux:
        for k in ["bms_temp_1_c", "bms_temp_2_c", "bms_temp_3_c", "bms_temp_4_c"]:
            val = row.get(k)
            try:
                fval = float(val)
                # Exclude -40 (unused), and must be realistic
                if fval != -40 and -30 < fval < 90:
                    temps.append(fval)
            except Exception:
                pass

    if temps and (max(temps) - min(temps)) <= 1.0:
        result_table.append(
            ["temp_stability", "PASS", f"ΔT={max(temps)-min(temps):.2f}°C", ""]
        )
    else:
        result_table.append(
            [
                "temp_stability",
                "FAIL",
                f"ΔT={'N/A' if not temps else f'{max(temps)-min(temps):.2f}°C'}",
                "",
            ]
        )

    # String voltage delta (all cell_volt_mv_xx_mv, convert mV to V, must be in 0–5V)
    cell_volt_keys = (
        [
            k
            for k in aux[0].keys()
            if k.startswith("cell_volt_mv_") and k.endswith("_mv")
        ]
        if aux
        else []
    )
    string_voltages = []
    for row in aux:
        for k in cell_volt_keys:
            try:
                val = float(row[k]) / 1000.0  # mV to V
                if 2.0 < val < 5.0:
                    string_voltages.append(val)
            except Exception:
                pass

    if string_voltages and (max(string_voltages) - min(string_voltages)) < 0.001:
        result_table.append(
            [
                "string_voltage_delta",
                "PASS",
                f"ΔV={max(string_voltages)-min(string_voltages):.6f}V",
                "",
            ]
        )
    else:
        result_table.append(
            [
                "string_voltage_delta",
                "FAIL",
                f"ΔV={'N/A' if not string_voltages else f'{max(string_voltages)-min(string_voltages):.6f}V'}",
                "",
            ]
        )

    # MOSFET status = 1 at end of step (dcdc_mos_status)
    mosfet = None
    if aux:
        try:
            mosfet = int(float(aux[-1].get("dcdc_mos_status", None)))
        except Exception:
            mosfet = aux[-1].get("dcdc_mos_status", None)

    if mosfet is not None and int(mosfet) == 1:
        result_table.append(["mosfet_status", "PASS", f"MOSFET={mosfet}", ""])
    else:
        result_table.append(["mosfet_status", "FAIL", f"MOSFET={mosfet}", ""])

    # BMS CAN packets
    if aux and len(aux) > 0:
        result_table.append(["bms_packets", "PASS", f"BMS CAN packets found", ""])
    else:
        result_table.append(["bms_packets", "FAIL", "No BMS CAN packets", ""])

    # Serial check (not implemented)
    result_table.append(["serial_number", "SKIP", "Serial check not implemented", ""])
    return result_table


def main(json_path):
    with open(json_path) as f:
        data = json.load(f)

    step_list = data["data"].get("step", [])
    aux = data["data"].get("auxDBC") or data["data"].get("aux_dbc", [])

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
