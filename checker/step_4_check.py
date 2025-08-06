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


def match_record_by_time(records, target_dt, tolerance=timedelta(seconds=1)):
    best_row = None
    best_delta = None
    for row in records:
        dt = row.get("date") or row.get("datetime")
        if not dt:
            continue
        row_dt = parse_dt(dt)
        if not row_dt:
            continue
        delta = abs((row_dt - target_dt).total_seconds())
        if delta <= tolerance.total_seconds():
            if best_delta is None or delta < best_delta:
                best_delta = delta
                best_row = row
    return best_row


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


def find_cc_dchg_step(step_list, target_seconds=3600):
    best_step = None
    best_diff = float("inf")
    for step in step_list:
        typ = step.get("step_type", "").replace("_", " ").lower()
        if typ == "cc dchg":
            duration = parse_duration(step.get("step_time", ""))
            if duration is None:
                continue
            diff = abs(duration - target_seconds)
            if diff < best_diff:
                best_step = step
                best_diff = diff
    return best_step


def check_current_sensor_accuracy(step, aux_all, records):
    result_table = [["check", "RESULT", "DETAIL", "REASON"]]

    # Get first 180 seconds window for this step
    step_start = parse_dt(step.get("oneset_date", ""))
    aux = []
    if step_start:
        step_180s_end = step_start + timedelta(seconds=180)
        for row in aux_all:
            dt = row.get("date") or row.get("datetime")
            if not dt:
                continue
            dt = parse_dt(dt)
            if dt and step_start <= dt <= step_180s_end:
                aux.append(row)

    # Compute BMS and record errors
    errors = []
    record_errors = []
    for row in aux:
        dt = row.get("date") or row.get("datetime")
        if not dt:
            continue
        row_dt = parse_dt(dt)
        if not row_dt or not step_start:
            continue
        seconds_from_start = (row_dt - step_start).total_seconds()
        set_current = min(int(seconds_from_start // 3) * 10, 100)
        try:
            meas_current = float(row.get("bms_current_a_a", 0))
            if abs(set_current) > 0.5:
                error_pct = abs(meas_current - set_current) / abs(set_current) * 100
                errors.append(error_pct)
        except Exception:
            continue

        # Compare to records current
        rec = match_record_by_time(records, row_dt)
        record_current = float(rec["current_a"]) if rec and "current_a" in rec else None
        if record_current is not None and abs(set_current) > 0.5:
            rec_error_pct = abs(record_current - set_current) / abs(set_current) * 100
            record_errors.append(rec_error_pct)
        elif abs(set_current) > 0.5:
            record_errors.append(float("nan"))

    max_error = max(errors) if errors else None
    rec_valid_errors = [v for v in record_errors if v == v]  # filter out nan
    max_rec_error = max(rec_valid_errors) if rec_valid_errors else None

    # 1. BMS error
    if max_error is not None and max_error <= 6.0:
        result_table.append(
            [
                "current_error",
                "PASS",
                f"Max error = {max_error:.2f}%",
                "All errors ≤ 6%",
            ]
        )
    elif max_error is not None:
        result_table.append(
            ["current_error", "NG", f"Max error = {max_error:.2f}%", "Error > 6% found"]
        )
    else:
        result_table.append(
            ["current_error", "NG", "No data to check", "No valid set/current"]
        )

    # 2. Record error (added section, always after BMS error)
    if max_rec_error is not None and max_rec_error <= 6.0:
        result_table.append(
            [
                "record_current_error",
                "PASS",
                f"Max error = {max_rec_error:.2f}%",
                "All errors ≤ 6%",
            ]
        )
    elif max_rec_error is not None:
        result_table.append(
            [
                "record_current_error",
                "NG",
                f"Max error = {max_rec_error:.2f}%",
                "Error > 6% found",
            ]
        )
    else:
        result_table.append(
            [
                "record_current_error",
                "NG",
                "No data to check",
                "No valid set/current",
            ]
        )

    # 3. Current reading = 0 for first 3s
    first3s_dt = step_start + timedelta(seconds=3) if step_start else None
    current_nonzero = False
    for row in aux:
        dt = row.get("date") or row.get("datetime")
        if not dt:
            continue
        dt = parse_dt(dt)
        if dt and dt <= first3s_dt:
            try:
                curr = float(row.get("bms_current_a_a", 0))
                if abs(curr) > 0.1:
                    current_nonzero = True
                    break
            except Exception:
                continue
    if not aux:
        result_table.append(
            ["current_zero_first3s", "NG", "No aux data", "No data to check"]
        )
    elif not current_nonzero:
        result_table.append(
            ["current_zero_first3s", "PASS", "Current=0 for first 3s", "OK"]
        )
    else:
        result_table.append(
            [
                "current_zero_first3s",
                "NG",
                "Nonzero current in first 3s",
                "Should be zero",
            ]
        )

    # 4. String voltage delta (TBD - just report for review)
    cell_vs_end = []
    if aux:
        for k, v in aux[-1].items():
            if k.startswith("cell_volt_mv_") and k.endswith("_mv"):
                try:
                    cell_vs_end.append(float(v) / 1000.0)
                except Exception:
                    pass
        if cell_vs_end:
            spread = (max(cell_vs_end) - min(cell_vs_end)) * 1000  # mV
            result_table.append(
                [
                    "cell_voltage_spread_end",
                    "INFO",
                    f"{min(cell_vs_end):.4f}V ~ {max(cell_vs_end):.4f}V (Δ={spread:.2f}mV)",
                    "For review (no spec limit in matrix)",
                ]
            )
        else:
            result_table.append(
                [
                    "cell_voltage_spread_end",
                    "INFO",
                    "No cell voltages",
                    "No data to check",
                ]
            )
    else:
        result_table.append(
            ["cell_voltage_spread_end", "INFO", "No aux data", "No data to check"]
        )

    # 5. 0 <= temp change in each probe ≤ 2°C over window (use bms_temp_1~4_c)
    temp_fields = [f"bms_temp_{i}_c" for i in range(1, 5)]
    temp_changes = []
    if aux:
        for k in temp_fields:
            try:
                t_start = float(aux[0].get(k, -100))
                t_end = float(aux[-1].get(k, -100))
                if all([-30 < t < 90 and t != -40 for t in [t_start, t_end]]):
                    temp_changes.append((k, t_end - t_start))
            except Exception:
                pass
        ngs = []
        for k, delta in temp_changes:
            if not (0 <= delta <= 2):
                ngs.append((k, delta))
        if not temp_changes:
            result_table.append(
                ["temp_probe_change", "NG", "No temp data", "No data to check"]
            )
        elif not ngs:
            result_table.append(
                [
                    "temp_probe_change",
                    "PASS",
                    ", ".join([f"{k}: Δ={d:.2f}°C" for k, d in temp_changes]),
                    "All probe change 0~2°C",
                ]
            )
        else:
            result_table.append(
                [
                    "temp_probe_change",
                    "NG",
                    ", ".join([f"{k}: Δ={d:.2f}°C" for k, d in ngs]),
                    "At least one probe change not in 0~2°C",
                ]
            )
    else:
        result_table.append(
            ["temp_probe_change", "NG", "No aux data", "No data to check"]
        )

    return result_table


# ================= MAIN FOR STEP 4 (first 180s discharge) ==================
def main(json_path):
    with open(json_path) as f:
        data = json.load(f)
    step_list = data["data"].get("step", [])
    aux = data["data"].get("auxDBC") or data["data"].get("aux_dbc", [])
    records = data["data"].get("records", [])

    cc_dchg_step = find_cc_dchg_step(step_list)
    if cc_dchg_step:
        print(
            f"\n🔍 Checking step: {cc_dchg_step.get('step_name', '')} | Duration: {cc_dchg_step.get('step_time', '')} (First 180s only)"
        )
        result = check_current_sensor_accuracy(cc_dchg_step, aux, records)
        print(tabulate(result, headers="firstrow", tablefmt="github"))
    else:
        print("No suitable CC DChg step found.")


if __name__ == "__main__":
    main(
        "/Users/riorizki/development/repos/ION-Mobility-Team/mes/LimeNDAX/result/LG_2_EOL_test_15-1-4-20250428125222_response.json"
    )
