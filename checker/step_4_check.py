import json
from tabulate import tabulate
from datetime import datetime
import re


def summarize_list(lst, max_len=5):
    """Summarize a long list for table display."""
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


def check_discharge_step(step, aux_all):
    idx = step.get("step_index", "")
    name = step.get("step_type", "")
    result_table = [["step", "", "", f"Step {idx} - {name}"]]
    aux = get_aux_in_window(
        aux_all, step.get("oneset_date", ""), step.get("end_date", "")
    )

    # 1. Error <= 6% (Current sensor accuracy)
    target_current = float(step.get("current_a", 0) or 0)
    measured_currents = [
        float(row.get("bms_current_a_a", 0) or 0)
        for row in aux
        if "bms_current_a_a" in row
    ]
    if measured_currents and target_current != 0:
        max_error = max(
            [
                abs(c - target_current) / abs(target_current) * 100
                for c in measured_currents
            ]
        )
        if max_error <= 6:
            result_table.append(
                ["current_error", "PASS", f"max error={max_error:.2f}%", ""]
            )
        else:
            result_table.append(
                ["current_error", "FAIL", f"max error={max_error:.2f}%", ""]
            )
    elif not measured_currents:
        result_table.append(
            ["current_error", "FAIL", "No valid BMS current readings found", ""]
        )
    else:
        result_table.append(["current_error", "FAIL", "Target current is 0", ""])

    # 2. Current reading = 0 for first 3 seconds in this step
    first_dt = parse_dt(step.get("oneset_date", ""))
    zero_current = True
    start_currents = []
    for row in aux:
        row_dt = parse_dt(row.get("date") or row.get("datetime"))
        if not row_dt or not first_dt:
            continue
        delta_s = (row_dt - first_dt).total_seconds()
        if 0 <= delta_s <= 3:
            try:
                current = float(row.get("bms_current_a_a", 0) or 0)
                start_currents.append(current)
                if abs(current) > 0.1:
                    zero_current = False
            except:
                continue
    summary_currents = summarize_list(start_currents)
    if not start_currents:
        result_table.append(
            ["zero_start_current", "FAIL", "No data in first 3 seconds", ""]
        )
    elif zero_current:
        result_table.append(["zero_start_current", "PASS", "0A in first 3 seconds", ""])
    else:
        result_table.append(
            ["zero_start_current", "FAIL", f"Start currents={summary_currents}", ""]
        )

    # 3. String voltage delta (<20mV as an example)
    cell_volt_keys = (
        [k for k in aux[0].keys() if "cell_volt" in k.lower()] if aux else []
    )
    string_voltages = []
    for row in aux:
        for k in cell_volt_keys:
            try:
                val = float(row[k])
                # plausible: 0 < v < 6000 (mV)
                if 0 < val < 6000:
                    string_voltages.append(val)
            except Exception:
                pass
    if string_voltages:
        spread = max(string_voltages) - min(string_voltages)
        if spread < 20:
            result_table.append(
                ["string_voltage_delta", "PASS", f"spread={spread:.1f}mV", ""]
            )
        else:
            result_table.append(
                ["string_voltage_delta", "FAIL", f"spread={spread:.1f}mV", ""]
            )
    else:
        result_table.append(["string_voltage_delta", "FAIL", "No cell voltages", ""])

    # 4. No BMS errors, current within 1% setpoint
    bms_err_cols = [k for k in aux[0].keys() if "bms_err" in k.lower()] if aux else []
    bms_error_found = any(
        float(row[k]) != 0
        for row in aux
        for k in bms_err_cols
        if k in row and row[k] not in ("", None)
    )
    current_within_1pct = True
    if measured_currents and target_current != 0:
        for c in measured_currents:
            if abs(c - target_current) / abs(target_current) > 0.01:
                current_within_1pct = False
                break
    if not bms_error_found and current_within_1pct:
        result_table.append(
            [
                "bms_current_accuracy",
                "PASS",
                "No BMS errors and all current within 1%",
                "",
            ]
        )
    else:
        reason = []
        if bms_error_found:
            reason.append("BMS error found")
        if not current_within_1pct:
            reason.append("Current out of 1% band")
        result_table.append(["bms_current_accuracy", "FAIL", ", ".join(reason), ""])

    # 5. Temp probes: 0 <= temp change <= 2C for each probe
    temp_keys = ["bms_temp_1_c", "bms_temp_2_c", "bms_temp_3_c", "bms_temp_4_c"]
    temp_deltas = []
    if aux:
        start = aux[0]
        end = aux[-1]
        for k in temp_keys:
            try:
                delta = float(end[k]) - float(start[k])
                temp_deltas.append(delta)
            except Exception:
                temp_deltas.append(None)
    temp_deltas_summary = summarize_list(temp_deltas)
    temp_pass = all((d is not None and 0 <= d <= 2) for d in temp_deltas)
    if temp_pass:
        result_table.append(
            ["probe_temp_delta", "PASS", f"deltas={temp_deltas_summary}", ""]
        )
    else:
        result_table.append(
            ["probe_temp_delta", "FAIL", f"deltas={temp_deltas_summary}", ""]
        )

    return result_table


def main(json_path):
    with open(json_path) as f:
        data = json.load(f)

    step_list = data["data"].get("step", [])
    aux = data["data"].get("auxDBC") or data["data"].get("aux_dbc", [])

    dchg_pattern = re.compile(r"dchg|discharge", re.IGNORECASE)
    for step in step_list:
        if dchg_pattern.search(step.get("step_type", "")):
            result = check_discharge_step(step, aux)
            print(tabulate(result, headers="firstrow", tablefmt="github"))
            break
    else:
        print("No 'Discharge' step found in parsed JSON.")


if __name__ == "__main__":
    main("/Users/rio.wijaya/Downloads/LimeNDAX/parsed_output.json")
