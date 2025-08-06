import json
from tabulate import tabulate
from datetime import datetime


def parse_dt(s):
    """Parse datetime string to datetime object."""
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except Exception:
        try:
            return datetime.strptime(s, "%Y-%m-%d %H:%M")
        except Exception:
            return None


def get_aux_in_window(aux, start, end):
    """Filter aux_dbc data within the time window."""
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


def extract_serial(row):
    """Concatenate bms_serial_num_1~17 as ASCII string if possible."""
    serial_bytes = []
    for i in range(1, 18):
        key = f"bms_serial_num_{i}"
        val = row.get(key, "0")
        try:
            byte = int(val)
        except Exception:
            byte = 0
        serial_bytes.append(byte)
    # Convert ASCII codes to string, substitute non-printable with '.'
    serial_str = "".join(chr(b) if 32 <= b <= 126 else "." for b in serial_bytes)
    return serial_str.strip()


def check_rest_step(step, aux_all):
    idx = step.get("step_index", "")
    name = step.get("step_name", "")
    # Remove rightmost column; header now ["check", "RESULT", "DETAIL", "REASON"]
    result_table = [["check", "RESULT", "DETAIL", "REASON"]]

    # 1. Get aux_dbc rows in this step's time window
    aux = get_aux_in_window(
        aux_all, step.get("oneset_date", ""), step.get("end_date", "")
    )

    # 2. Start voltage 68–73V (from BMS CAN if present)
    bms_vs = []
    for row in aux:
        for k in row:
            if k.lower() in ("bms_volt_v_v", "bms_volt_v"):
                try:
                    val = float(row[k])
                    if val > 1:  # filter out zeros/nulls
                        bms_vs.append(val)
                except Exception:
                    pass
    voltage_for_check = sum(bms_vs) / len(bms_vs) if bms_vs else None
    if voltage_for_check is not None and 68.0 <= voltage_for_check <= 73.0:
        result_table.append(
            [
                "start_voltage",
                "PASS",
                f"Voltage={voltage_for_check:.4f}V",
                "Within limit (68–73V)",
            ]
        )
    else:
        failval = voltage_for_check if voltage_for_check is not None else "N/A"
        result_table.append(
            ["start_voltage", "NG", f"Voltage={failval}", "Voltage out of 68–73V range"]
        )

    # 3. Temp stability (bms_temp_1~4_c, ignore -40C)
    temps = []
    for row in aux:
        for k in ["bms_temp_1_c", "bms_temp_2_c", "bms_temp_3_c", "bms_temp_4_c"]:
            val = row.get(k)
            try:
                fval = float(val)
                if fval != -40 and -30 < fval < 90:
                    temps.append(fval)
            except Exception:
                pass
    if temps and (max(temps) - min(temps)) <= 1.0:
        result_table.append(
            [
                "temp_stability",
                "PASS",
                f"ΔT={max(temps)-min(temps):.2f}°C",
                "Within limit (≤ 1°C)",
            ]
        )
    else:
        delta = f"{max(temps)-min(temps):.2f}°C" if temps else "N/A"
        result_table.append(
            [
                "temp_stability",
                "NG",
                f"ΔT={delta}",
                "Temperature fluctuation exceeds 1°C",
            ]
        )

    # 4. String voltage delta (cell_volt_mv_*, must be in 2V~5V, ΔV<1mV)
    cell_volt_keys = (
        [
            k
            for k in aux[0].keys()
            if k.startswith("cell_volt_mv_") and k.endswith("_mv")
        ]
        if aux
        else []
    )
    cell_vs = []
    for row in aux:
        for k in cell_volt_keys:
            try:
                val = float(row[k]) / 1000.0
                if 2.0 < val < 5.0:
                    cell_vs.append(val)
            except Exception:
                pass
    if cell_vs and (max(cell_vs) - min(cell_vs)) < 0.001:
        result_table.append(
            [
                "string_voltage_delta",
                "PASS",
                f"ΔV={max(cell_vs)-min(cell_vs):.6f}V",
                "Within limit (<1mV)",
            ]
        )
    else:
        delta = f"{max(cell_vs)-min(cell_vs):.6f}V" if cell_vs else "N/A"
        result_table.append(
            ["string_voltage_delta", "NG", f"ΔV={delta}", "ΔV exceeds 1mV limit"]
        )

    # 5. MOSFET status = 1 at end of step (dcdc_mos_status)
    mosfet = None
    if aux:
        try:
            mosfet = int(float(aux[-1].get("dcdc_mos_status", None)))
        except Exception:
            mosfet = aux[-1].get("dcdc_mos_status", None)
    if mosfet is not None and int(mosfet) == 1:
        result_table.append(
            [
                "mosfet_status",
                "PASS",
                f"MOSFET={mosfet}",
                "MOSFET status = 1 at end of step",
            ]
        )
    else:
        result_table.append(
            [
                "mosfet_status",
                "NG",
                f"MOSFET={mosfet}",
                "MOSFET status not 1 at end of step",
            ]
        )

    # 6. BMS CAN packets (simply: aux rows exist)
    if aux and len(aux) > 0:
        result_table.append(
            ["bms_packets", "PASS", "BMS CAN packets found", "CAN packets present"]
        )
    else:
        result_table.append(
            ["bms_packets", "NG", "No BMS CAN packets", "No CAN packets found"]
        )

    # 7. Serial number check (all serials in window must be same, valid, non-empty)
    serials = set()
    for row in aux:
        serial_str = extract_serial(row)
        if serial_str and serial_str != "." * 17 and serial_str != "0" * 17:
            serials.add(serial_str)
    if len(serials) == 1:
        result_table.append(
            [
                "serial_number",
                "PASS",
                f"Serial={list(serials)[0]}",
                "Serial number consistent",
            ]
        )
    elif len(serials) > 1:
        result_table.append(
            [
                "serial_number",
                "NG",
                f"Inconsistent serials: {serials}",
                "Serial numbers inconsistent",
            ]
        )
    else:
        result_table.append(
            [
                "serial_number",
                "NG",
                "Serial number missing or invalid",
                "Serial missing/invalid",
            ]
        )

    return result_table


def main(json_path):
    with open(json_path) as f:
        data = json.load(f)
    step_list = data["data"].get("step", [])
    aux = data["data"].get("auxDBC") or data["data"].get("aux_dbc", [])

    for step in step_list:
        # Only do the first rest step with 10 seconds (your control plan step 1)
        if step.get("step_type", "").lower() == "rest" and step.get("step_time") in (
            "00:00:10",
            "10 seconds",
        ):
            rest_result = check_rest_step(step, aux)
            print(tabulate(rest_result, headers="firstrow", tablefmt="github"))
            break
    else:
        print("No matching 'Rest' step found in parsed JSON.")


if __name__ == "__main__":
    main(
        "/Users/riorizki/development/repos/ION-Mobility-Team/mes/LimeNDAX/result/LG_2_EOL_test_15-1-4-20250428125222_response.json"
    )
