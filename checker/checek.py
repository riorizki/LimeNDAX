import json
from datetime import datetime, timedelta


def parse_dt(s):
    """Parse datetime string to datetime object."""
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except Exception:
        try:
            return datetime.strptime(s, "%Y-%m-%d %H:%M")
        except Exception:
            return None


def parse_duration(duration_str):
    """Convert hh:mm:ss or mm:ss to total seconds."""
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
    """Parse string to float safely."""
    try:
        return float(val)
    except:
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


def get_string_voltage(row):
    """Sum up all cell voltages for a row (in volts)."""
    string_v = 0.0
    for k, v in row.items():
        if k.startswith("cell_volt_mv_") and k.endswith("_mv"):
            try:
                string_v += float(v) / 1000.0
            except Exception:
                pass
    return string_v


def find_step_by_type_and_duration(
    step_list, step_type, target_seconds, tolerance_percent=10
):
    """Find step by type and duration."""
    best_step = None
    best_diff = float("inf")
    tolerance = target_seconds * (tolerance_percent / 100)

    for step in step_list:
        typ = step.get("step_type", "").replace("_", " ").lower()
        if "rest" in step_type.lower() and "rest" in typ:
            duration = parse_duration(step.get("step_time", ""))
            if duration is None:
                continue
            diff = abs(duration - target_seconds)
            if diff <= tolerance:
                return step
        elif step_type.lower() in typ:
            duration = parse_duration(step.get("step_time", ""))
            if duration is None:
                continue
            diff = abs(duration - target_seconds)
            if diff <= tolerance:
                return step
    return best_step


def find_cc_dchg_step(step_list, target_seconds=3600):
    """Find CC discharge step."""
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


def find_cccv_step(step_list, target_seconds=10800):
    """Find CCCV charge step."""
    best_step = None
    best_diff = float("inf")
    for step in step_list:
        typ = step.get("step_type", "").replace("_", " ").lower()
        if typ == "cccv chg":
            duration = parse_duration(step.get("step_time", ""))
            if duration is None:
                continue
            diff = abs(duration - target_seconds)
            if diff < best_diff:
                best_step = step
                best_diff = diff
    return best_step


def find_cc_chg_step(step_list, target_seconds=7200):
    """Find CC charge step."""
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


def find_rest_step(step_list, target_seconds):
    """Find rest step with specific duration."""
    best_step = None
    best_diff = float("inf")
    for step in step_list:
        typ = step.get("step_type", "").replace("_", " ").lower()
        if typ == "rest":
            duration = parse_duration(step.get("step_time", ""))
            if duration is None:
                continue
            diff = abs(duration - target_seconds)
            if diff < best_diff:
                best_step = step
                best_diff = diff
    return best_step


def check_step_1(step, aux_all):
    """Check step 1: Rest step with 10 seconds."""
    aux = get_aux_in_window(
        aux_all, step.get("oneset_date", ""), step.get("end_date", "")
    )
    results = []

    # 1. Start voltage 68-73V
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
    voltage_for_check = sum(bms_vs) / len(bms_vs) if bms_vs else None
    if voltage_for_check is not None and 68.0 <= voltage_for_check <= 73.0:
        results.append(
            {
                "check": "start_voltage",
                "result": "PASS",
                "detail": f"Voltage={voltage_for_check:.4f}V",
                "reason": "Within limit (68-73V)",
            }
        )
    else:
        failval = voltage_for_check if voltage_for_check is not None else "N/A"
        results.append(
            {
                "check": "start_voltage",
                "result": "NG",
                "detail": f"Voltage={failval}",
                "reason": "Voltage out of 68-73V range",
            }
        )

    # 2. Temp stability
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
        results.append(
            {
                "check": "temp_stability",
                "result": "PASS",
                "detail": f"ΔT={max(temps)-min(temps):.2f}°C",
                "reason": "Within limit (≤ 1°C)",
            }
        )
    else:
        delta = f"{max(temps)-min(temps):.2f}°C" if temps else "N/A"
        results.append(
            {
                "check": "temp_stability",
                "result": "NG",
                "detail": f"ΔT={delta}",
                "reason": "Temperature fluctuation exceeds 1°C",
            }
        )

    # 3. String voltage delta
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
        results.append(
            {
                "check": "string_voltage_delta",
                "result": "PASS",
                "detail": f"ΔV={max(cell_vs)-min(cell_vs):.6f}V",
                "reason": "Within limit (<1mV)",
            }
        )
    else:
        delta = f"{max(cell_vs)-min(cell_vs):.6f}V" if cell_vs else "N/A"
        results.append(
            {
                "check": "string_voltage_delta",
                "result": "NG",
                "detail": f"ΔV={delta}",
                "reason": "ΔV exceeds 1mV limit",
            }
        )

    # 4. MOSFET status
    mosfet = None
    if aux:
        try:
            mosfet = int(float(aux[-1].get("dcdc_mos_status", None)))
        except Exception:
            mosfet = aux[-1].get("dcdc_mos_status", None)
    if mosfet is not None and int(mosfet) == 1:
        results.append(
            {
                "check": "mosfet_status",
                "result": "PASS",
                "detail": f"MOSFET={mosfet}",
                "reason": "MOSFET status = 1 at end of step",
            }
        )
    else:
        results.append(
            {
                "check": "mosfet_status",
                "result": "NG",
                "detail": f"MOSFET={mosfet}",
                "reason": "MOSFET status not 1 at end of step",
            }
        )

    # 5. BMS CAN packets
    if aux and len(aux) > 0:
        results.append(
            {
                "check": "bms_packets",
                "result": "PASS",
                "detail": "BMS CAN packets found",
                "reason": "CAN packets present",
            }
        )
    else:
        results.append(
            {
                "check": "bms_packets",
                "result": "NG",
                "detail": "No BMS CAN packets",
                "reason": "No CAN packets found",
            }
        )

    # 6. Serial number check
    serials = set()
    for row in aux:
        serial_str = extract_serial(row)
        if serial_str and serial_str != "." * 17 and serial_str != "0" * 17:
            serials.add(serial_str)
    if len(serials) == 1:
        results.append(
            {
                "check": "serial_number",
                "result": "PASS",
                "detail": f"Serial={list(serials)[0]}",
                "reason": "Serial number consistent",
            }
        )
    elif len(serials) > 1:
        results.append(
            {
                "check": "serial_number",
                "result": "NG",
                "detail": f"Inconsistent serials: {serials}",
                "reason": "Serial numbers inconsistent",
            }
        )
    else:
        results.append(
            {
                "check": "serial_number",
                "result": "NG",
                "detail": "Serial number missing or invalid",
                "reason": "Serial missing/invalid",
            }
        )

    return results


def check_step_2(step, aux_all, cycle):
    """Check step 2: CCCV charge step (3 hours)."""
    aux = get_aux_in_window(
        aux_all, step.get("oneset_date", ""), step.get("end_date", "")
    )
    results = []

    # 1. No BMS errors during charging
    error_fields = [f"bms_err_{i}" for i in range(1, 23)]
    error_found = False
    for row in aux:
        for field in error_fields:
            if row.get(field, "0") not in ("0", 0):
                error_found = True
                break
        if error_found:
            break
    if not aux:
        results.append(
            {
                "check": "bms_errors",
                "result": "NG",
                "detail": "No aux data in window",
                "reason": "Cannot check BMS errors",
            }
        )
    elif not error_found:
        results.append(
            {
                "check": "bms_errors",
                "result": "PASS",
                "detail": "No BMS errors found",
                "reason": "All bms_err_* fields = 0",
            }
        )
    else:
        results.append(
            {
                "check": "bms_errors",
                "result": "NG",
                "detail": "BMS errors reported",
                "reason": "At least one bms_err_* ≠ 0",
            }
        )

    # 2. All reported temperature values between 20-50°C
    temp_fields = [f"bms_temp_{i}_c" for i in range(1, 5)]
    temps = []
    out_of_range = False
    for row in aux:
        for k in temp_fields:
            try:
                fval = float(row.get(k, -100))
                if fval != -40 and fval != -100:
                    temps.append(fval)
                    if not (20 <= fval <= 50):
                        out_of_range = True
            except Exception:
                continue
    if not aux or not temps:
        results.append(
            {
                "check": "temp_within_20_50",
                "result": "NG",
                "detail": "No temperature data",
                "reason": "No data to check",
            }
        )
    elif not out_of_range:
        results.append(
            {
                "check": "temp_within_20_50",
                "result": "PASS",
                "detail": "All temps in 20-50°C",
                "reason": "All temperature probes within spec",
            }
        )
    else:
        outvals = [t for t in temps if t < 20 or t > 50]
        results.append(
            {
                "check": "temp_within_20_50",
                "result": "NG",
                "detail": f"Out of range: {outvals}",
                "reason": "At least one temp probe <20°C or >50°C",
            }
        )

    # 3. Temperature rise from start to finish <10°C
    temp_rises = []
    for k in temp_fields:
        try:
            series = [
                float(row.get(k, -100))
                for row in aux
                if float(row.get(k, -100)) != -40 and float(row.get(k, -100)) != -100
            ]
            if series:
                rise = max(series) - min(series)
                temp_rises.append(rise)
        except Exception:
            continue
    max_rise = max(temp_rises) if temp_rises else None
    if max_rise is not None and max_rise < 10:
        results.append(
            {
                "check": "temp_rise",
                "result": "PASS",
                "detail": f"Max rise = {max_rise:.2f}°C",
                "reason": "Within 10°C",
            }
        )
    elif max_rise is not None:
        results.append(
            {
                "check": "temp_rise",
                "result": "NG",
                "detail": f"Max rise = {max_rise:.2f}°C",
                "reason": "Exceeds 10°C",
            }
        )
    else:
        results.append(
            {
                "check": "temp_rise",
                "result": "NG",
                "detail": "No temp rise data",
                "reason": "No temperature series found",
            }
        )

    # 4. Charge capacity 41-45Ah
    cap = None
    try:
        cap = float(step.get("capacity_ah", "0"))
        if cap == 0:
            cap = float(cycle.get("chg_cap_ah", "0"))
    except Exception:
        cap = None
    if cap is not None and 41 <= cap <= 45:
        results.append(
            {
                "check": "charge_capacity",
                "result": "PASS",
                "detail": f"Capacity={cap:.3f}Ah",
                "reason": "Within 41-45Ah",
            }
        )
    elif cap is not None:
        results.append(
            {
                "check": "charge_capacity",
                "result": "NG",
                "detail": f"Capacity={cap:.3f}Ah",
                "reason": "Out of range 41-45Ah",
            }
        )
    else:
        results.append(
            {
                "check": "charge_capacity",
                "result": "NG",
                "detail": "Capacity not found",
                "reason": "No data to check",
            }
        )

    # 5. Max-min temp <3°C
    min_deltas = []
    for row in aux:
        rowtemps = []
        for k in temp_fields:
            try:
                fval = float(row.get(k, -100))
                if fval != -40 and fval != -100:
                    rowtemps.append(fval)
            except Exception:
                continue
        if rowtemps:
            min_deltas.append(max(rowtemps) - min(rowtemps))
    max_probe_delta = max(min_deltas) if min_deltas else None
    if max_probe_delta is not None and max_probe_delta < 3:
        results.append(
            {
                "check": "temp_probe_delta",
                "result": "PASS",
                "detail": f"Max ΔT={max_probe_delta:.2f}°C",
                "reason": "Within 3°C",
            }
        )
    elif max_probe_delta is not None:
        results.append(
            {
                "check": "temp_probe_delta",
                "result": "NG",
                "detail": f"Max ΔT={max_probe_delta:.2f}°C",
                "reason": "Exceeds 3°C",
            }
        )
    else:
        results.append(
            {
                "check": "temp_probe_delta",
                "result": "NG",
                "detail": "No probe delta data",
                "reason": "No data to check",
            }
        )

    # 6. MOSFET temp <75°C
    mos_temps = []
    for row in aux:
        try:
            fval = float(row.get("mos_temp", -100))
            if fval != -100:
                mos_temps.append(fval)
        except Exception:
            continue
    over_75 = any(mt > 75 for mt in mos_temps)
    if not aux or not mos_temps:
        results.append(
            {
                "check": "mosfet_temp",
                "result": "NG",
                "detail": "No MOSFET temp data",
                "reason": "No data to check",
            }
        )
    elif not over_75:
        results.append(
            {
                "check": "mosfet_temp",
                "result": "PASS",
                "detail": f"Max MOSFET temp = {max(mos_temps):.2f}°C",
                "reason": "All ≤ 75°C",
            }
        )
    else:
        overvals = [mt for mt in mos_temps if mt > 75]
        results.append(
            {
                "check": "mosfet_temp",
                "result": "NG",
                "detail": f"Over 75°C: {overvals}",
                "reason": "MOSFET temp exceeds 75°C",
            }
        )

    return results


def check_step_3(step, aux_all):
    """Check step 3: Rest step with 30 minutes."""
    aux = get_aux_in_window(
        aux_all, step.get("oneset_date", ""), step.get("end_date", "")
    )
    results = []

    # 1. String voltage decrease <30mV
    if aux:
        string_v_start = get_string_voltage(aux[0])
        string_v_end = get_string_voltage(aux[-1])
        string_v_delta = string_v_start - string_v_end
        if string_v_delta < 0.03:
            results.append(
                {
                    "check": "string_voltage_decrease",
                    "result": "PASS",
                    "detail": f"{string_v_start:.4f}V → {string_v_end:.4f}V (Δ={string_v_delta*1000:.2f}mV)",
                    "reason": "<30mV decrease",
                }
            )
        else:
            results.append(
                {
                    "check": "string_voltage_decrease",
                    "result": "NG",
                    "detail": f"{string_v_start:.4f}V → {string_v_end:.4f}V (Δ={string_v_delta*1000:.2f}mV)",
                    "reason": "Decrease ≥30mV",
                }
            )
    else:
        results.append(
            {
                "check": "string_voltage_decrease",
                "result": "NG",
                "detail": "No aux data",
                "reason": "No data to check",
            }
        )

    # 2. Pack voltage decrease <1V
    pack_v_start = pack_v_end = None
    if aux:
        try:
            pack_v_start = float(aux[0].get("bms_volt_v_v", 0))
            pack_v_end = float(aux[-1].get("bms_volt_v_v", 0))
            pack_v_delta = pack_v_start - pack_v_end
            if pack_v_delta < 1.0:
                results.append(
                    {
                        "check": "pack_voltage_decrease",
                        "result": "PASS",
                        "detail": f"{pack_v_start:.4f}V → {pack_v_end:.4f}V (Δ={pack_v_delta:.4f}V)",
                        "reason": "<1V decrease",
                    }
                )
            else:
                results.append(
                    {
                        "check": "pack_voltage_decrease",
                        "result": "NG",
                        "detail": f"{pack_v_start:.4f}V → {pack_v_end:.4f}V (Δ={pack_v_delta:.4f}V)",
                        "reason": "Decrease ≥1V",
                    }
                )
        except Exception:
            results.append(
                {
                    "check": "pack_voltage_decrease",
                    "result": "NG",
                    "detail": "Could not parse voltages",
                    "reason": "Parse error",
                }
            )
    else:
        results.append(
            {
                "check": "pack_voltage_decrease",
                "result": "NG",
                "detail": "No aux data",
                "reason": "No data to check",
            }
        )

    # 3. Cell voltage spread at end <20mV
    cell_vs_end = []
    if aux:
        for k, v in aux[-1].items():
            if k.startswith("cell_volt_mv_") and k.endswith("_mv"):
                try:
                    cell_vs_end.append(float(v) / 1000.0)
                except Exception:
                    pass
        if cell_vs_end:
            spread = (max(cell_vs_end) - min(cell_vs_end)) * 1000
            if spread < 20:
                results.append(
                    {
                        "check": "cell_voltage_spread_end",
                        "result": "PASS",
                        "detail": f"{min(cell_vs_end):.4f}V ~ {max(cell_vs_end):.4f}V (Δ={spread:.2f}mV)",
                        "reason": "Spread <20mV",
                    }
                )
            else:
                results.append(
                    {
                        "check": "cell_voltage_spread_end",
                        "result": "NG",
                        "detail": f"{min(cell_vs_end):.4f}V ~ {max(cell_vs_end):.4f}V (Δ={spread:.2f}mV)",
                        "reason": "Spread ≥20mV",
                    }
                )
        else:
            results.append(
                {
                    "check": "cell_voltage_spread_end",
                    "result": "NG",
                    "detail": "No cell voltages",
                    "reason": "No data to check",
                }
            )
    else:
        results.append(
            {
                "check": "cell_voltage_spread_end",
                "result": "NG",
                "detail": "No aux data",
                "reason": "No data to check",
            }
        )

    # 4. Temperature decrease 0-5°C per probe
    temp_fields = [f"bms_temp_{i}_c" for i in range(1, 5)]
    temp_decreases = []
    if aux:
        for k in temp_fields:
            try:
                t_start = float(aux[0].get(k, -100))
                t_end = float(aux[-1].get(k, -100))
                if all([-30 < t < 90 and t != -40 for t in [t_start, t_end]]):
                    temp_decreases.append((k, t_start - t_end))
            except Exception:
                pass
        ngs = []
        for k, delta in temp_decreases:
            if not (0 <= delta <= 5):
                ngs.append((k, delta))
        if not temp_decreases:
            results.append(
                {
                    "check": "temp_probe_decrease",
                    "result": "NG",
                    "detail": "No temp data",
                    "reason": "No data to check",
                }
            )
        elif not ngs:
            detail_str = ", ".join([f"{k}: Δ={d:.2f}°C" for k, d in temp_decreases])
            results.append(
                {
                    "check": "temp_probe_decrease",
                    "result": "PASS",
                    "detail": detail_str,
                    "reason": "All probe decrease 0-5°C",
                }
            )
        else:
            detail_str = ", ".join([f"{k}: Δ={d:.2f}°C" for k, d in ngs])
            results.append(
                {
                    "check": "temp_probe_decrease",
                    "result": "NG",
                    "detail": detail_str,
                    "reason": "At least one probe decrease not in 0-5°C",
                }
            )
    else:
        results.append(
            {
                "check": "temp_probe_decrease",
                "result": "NG",
                "detail": "No aux data",
                "reason": "No data to check",
            }
        )

    # 5. Max-min temp <3°C
    min_deltas = []
    for row in aux:
        rowtemps = []
        for k in temp_fields:
            try:
                fval = float(row.get(k, -100))
                if fval != -40 and -30 < fval < 90:
                    rowtemps.append(fval)
            except Exception:
                continue
        if rowtemps:
            min_deltas.append(max(rowtemps) - min(rowtemps))
    max_probe_delta = max(min_deltas) if min_deltas else None
    if max_probe_delta is not None and max_probe_delta < 3:
        results.append(
            {
                "check": "temp_probe_delta",
                "result": "PASS",
                "detail": f"Max ΔT={max_probe_delta:.2f}°C",
                "reason": "Within 3°C",
            }
        )
    elif max_probe_delta is not None:
        results.append(
            {
                "check": "temp_probe_delta",
                "result": "NG",
                "detail": f"Max ΔT={max_probe_delta:.2f}°C",
                "reason": "Exceeds 3°C",
            }
        )
    else:
        results.append(
            {
                "check": "temp_probe_delta",
                "result": "NG",
                "detail": "No probe delta data",
                "reason": "No data to check",
            }
        )

    return results


def check_step_4(step, aux_all, records):
    """Check step 4: CC discharge current sensor accuracy (first 180s)."""
    results = []
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

    # 1. Current sensor accuracy
    errors, record_errors = [], []
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

        # Match to records for record current error
        rec = None
        best_delta = 1.1
        for r in records:
            rdt = r.get("date") or r.get("datetime")
            if not rdt:
                continue
            r_dt = parse_dt(rdt)
            if r_dt:
                delta = abs((r_dt - row_dt).total_seconds())
                if delta < best_delta:
                    best_delta = delta
                    rec = r
        record_current = float(rec.get("current_a", 0)) if rec else None
        if record_current is not None and abs(set_current) > 0.5:
            rec_error_pct = abs(record_current - set_current) / abs(set_current) * 100
            record_errors.append(rec_error_pct)
        elif abs(set_current) > 0.5:
            record_errors.append(float("nan"))

    max_error = max(errors) if errors else None
    rec_valid_errors = [v for v in record_errors if v == v]
    max_rec_error = max(rec_valid_errors) if rec_valid_errors else None

    if max_error is not None and max_error <= 6.0:
        results.append(
            {
                "check": "current_error",
                "result": "PASS",
                "detail": f"Max error = {max_error:.2f}%",
                "reason": "All errors ≤ 6%",
            }
        )
    elif max_error is not None:
        results.append(
            {
                "check": "current_error",
                "result": "NG",
                "detail": f"Max error = {max_error:.2f}%",
                "reason": "Error > 6% found",
            }
        )
    else:
        results.append(
            {
                "check": "current_error",
                "result": "NG",
                "detail": "No data to check",
                "reason": "No valid set/current",
            }
        )

    if records:
        if max_rec_error is not None and max_rec_error <= 6.0:
            results.append(
                {
                    "check": "record_current_error",
                    "result": "PASS",
                    "detail": f"Max error = {max_rec_error:.2f}%",
                    "reason": "All errors ≤ 6%",
                }
            )
        elif max_rec_error is not None:
            results.append(
                {
                    "check": "record_current_error",
                    "result": "NG",
                    "detail": f"Max error = {max_rec_error:.2f}%",
                    "reason": "Error > 6% found",
                }
            )
        else:
            results.append(
                {
                    "check": "record_current_error",
                    "result": "NG",
                    "detail": "No data to check",
                    "reason": "No valid set/current",
                }
            )
    else:
        results.append(
            {
                "check": "record_current_error",
                "result": "INFO",
                "detail": "No records provided",
                "reason": "Field not in JSON",
            }
        )

    # 2. Current zero for first 3s
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
        results.append(
            {
                "check": "current_zero_first3s",
                "result": "NG",
                "detail": "No aux data",
                "reason": "No data to check",
            }
        )
    elif not current_nonzero:
        results.append(
            {
                "check": "current_zero_first3s",
                "result": "PASS",
                "detail": "Current=0 for first 3s",
                "reason": "OK",
            }
        )
    else:
        results.append(
            {
                "check": "current_zero_first3s",
                "result": "NG",
                "detail": "Nonzero current in first 3s",
                "reason": "Should be zero",
            }
        )

    # 3. Temperature change 0-2°C
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
            results.append(
                {
                    "check": "temp_probe_change",
                    "result": "NG",
                    "detail": "No temp data",
                    "reason": "No data to check",
                }
            )
        elif not ngs:
            detail_str = ", ".join([f"{k}: Δ={d:.2f}°C" for k, d in temp_changes])
            results.append(
                {
                    "check": "temp_probe_change",
                    "result": "PASS",
                    "detail": detail_str,
                    "reason": "All probe change 0-2°C",
                }
            )
        else:
            detail_str = ", ".join([f"{k}: Δ={d:.2f}°C" for k, d in ngs])
            results.append(
                {
                    "check": "temp_probe_change",
                    "result": "NG",
                    "detail": detail_str,
                    "reason": "At least one probe change not in 0-2°C",
                }
            )
    else:
        results.append(
            {
                "check": "temp_probe_change",
                "result": "NG",
                "detail": "No aux data",
                "reason": "No data to check",
            }
        )

    # 4. Cell voltage spread at end (for review)
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
            results.append(
                {
                    "check": "cell_voltage_spread_end",
                    "result": "INFO",
                    "detail": f"{min(cell_vs_end):.4f}V ~ {max(cell_vs_end):.4f}V (Δ={spread:.2f}mV)",
                    "reason": "For review (no spec limit in matrix)",
                }
            )
        else:
            results.append(
                {
                    "check": "cell_voltage_spread_end",
                    "result": "INFO",
                    "detail": "No cell voltages",
                    "reason": "No data to check",
                }
            )
    else:
        results.append(
            {
                "check": "cell_voltage_spread_end",
                "result": "INFO",
                "detail": "No aux data",
                "reason": "No data to check",
            }
        )

    return results


def check_step_5(step_list, aux, records):
    """Check step 5: CC discharge (1 hour)."""
    # Use the CC discharge step found by find_cc_dchg_step
    dchg_step = find_cc_dchg_step(step_list, 3600)
    if not dchg_step:
        return [
            {
                "check": "step_find",
                "result": "NG",
                "detail": "No CC DChg step",
                "reason": "Not found",
            }
        ]

    aux_win = get_aux_in_window(
        aux, dchg_step.get("oneset_date", ""), dchg_step.get("end_date", "")
    )
    results = []

    # 1. Discharge capacity
    capacity = parse_float(dchg_step.get("capacity_ah"))
    if capacity is not None and 53 <= capacity <= 57:
        results.append(
            {
                "check": "capacity",
                "result": "PASS",
                "detail": f"{capacity:.2f}Ah",
                "reason": "Within 53-57Ah",
            }
        )
    elif capacity is not None:
        results.append(
            {
                "check": "capacity",
                "result": "NG",
                "detail": f"{capacity:.2f}Ah",
                "reason": "Outside 53-57Ah",
            }
        )
    else:
        results.append(
            {
                "check": "capacity",
                "result": "NG",
                "detail": "No data",
                "reason": "Missing capacity_ah in step",
            }
        )

    # 2. Current sensor error
    errors = []
    set_current = 57.6
    for row in aux_win:
        bms_current = parse_float(row.get("bms_current_a_a"))
        if bms_current is not None:
            abs_error = abs(bms_current - set_current)
            errors.append(abs_error)
    max_error = max(errors) if errors else None
    if max_error is not None and max_error < 2.4:
        results.append(
            {
                "check": "current_error",
                "result": "PASS",
                "detail": f"Max error = {max_error:.2f}A",
                "reason": "All errors < 2.4A",
            }
        )
    elif max_error is not None:
        results.append(
            {
                "check": "current_error",
                "result": "NG",
                "detail": f"Max error = {max_error:.2f}A",
                "reason": "Error ≥ 2.4A found",
            }
        )
    else:
        results.append(
            {
                "check": "current_error",
                "result": "NG",
                "detail": "No data",
                "reason": "Missing bms_current_a_a in aux",
            }
        )

    # 3. Max temp rise <30°C
    temp_fields = [f"bms_temp_{i}_c" for i in range(1, 5)]
    temp_rise = []
    if aux_win:
        for tfield in temp_fields:
            t_start = parse_float(aux_win[0].get(tfield))
            t_end = parse_float(aux_win[-1].get(tfield))
            if t_start is not None and t_end is not None:
                temp_rise.append((tfield, t_end - t_start))
        fails = [f for f in temp_rise if abs(f[1]) >= 30]
        detail = ", ".join([f"{k}: Δ={d:.2f}°C" for k, d in temp_rise])
        if not temp_rise:
            results.append(
                {
                    "check": "max_temp_rise",
                    "result": "NG",
                    "detail": "No data",
                    "reason": "No temp data",
                }
            )
        elif not fails:
            results.append(
                {
                    "check": "max_temp_rise",
                    "result": "PASS",
                    "detail": detail,
                    "reason": "All temp rises < 30°C",
                }
            )
        else:
            results.append(
                {
                    "check": "max_temp_rise",
                    "result": "NG",
                    "detail": detail,
                    "reason": "One or more temp rises ≥ 30°C",
                }
            )
    else:
        results.append(
            {
                "check": "max_temp_rise",
                "result": "NG",
                "detail": "No aux data",
                "reason": "No aux in window",
            }
        )

    # 4. Max-min temp <3°C
    max_min_spreads = []
    for row in aux_win:
        temps = [parse_float(row.get(t)) for t in temp_fields if row.get(t) is not None]
        temps = [t for t in temps if t is not None]
        if temps:
            spread = max(temps) - min(temps)
            max_min_spreads.append(spread)
    max_spread = max(max_min_spreads) if max_min_spreads else None
    if max_spread is not None and max_spread < 3.0:
        results.append(
            {
                "check": "max_min_temp",
                "result": "PASS",
                "detail": f"Max spread = {max_spread:.2f}°C",
                "reason": "<3°C",
            }
        )
    elif max_spread is not None:
        results.append(
            {
                "check": "max_min_temp",
                "result": "NG",
                "detail": f"Max spread = {max_spread:.2f}°C",
                "reason": "≥3°C found",
            }
        )
    else:
        results.append(
            {
                "check": "max_min_temp",
                "result": "NG",
                "detail": "No data",
                "reason": "No temp data",
            }
        )

    # 5. Low voltage warning
    low_voltage_flag = False
    low_v_reason = ""
    for row in aux_win:
        pack_v = parse_float(row.get("pack_voltage_v"))
        string_vs = [parse_float(row[k]) for k in row if "string_voltage_v" in k]
        if pack_v is not None and pack_v < 64:
            low_voltage_flag = True
            low_v_reason = f"PackV={pack_v:.2f}V"
            break
        if any(v is not None and v < 3.2 for v in string_vs):
            low_voltage_flag = True
            low_v_reason = (
                f"StringV={min([v for v in string_vs if v is not None]):.2f}V"
            )
            break
    if low_voltage_flag:
        results.append(
            {
                "check": "low_voltage_warning",
                "result": "PASS",
                "detail": low_v_reason,
                "reason": "<64V or <3.2V triggered",
            }
        )
    else:
        results.append(
            {
                "check": "low_voltage_warning",
                "result": "NG",
                "detail": "No alert triggered",
                "reason": "No pack/string < threshold",
            }
        )

    # 6. Max-min cell voltage <40mV at end
    cell_voltages = []
    if aux_win:
        for k, v in aux_win[-1].items():
            if k.startswith("cell_volt_mv_") and k.endswith("_mv"):
                val = parse_float(v)
                if val is not None:
                    cell_voltages.append(val / 1000.0)

    if cell_voltages:
        spread_mv = (max(cell_voltages) - min(cell_voltages)) * 1000
        if spread_mv < 40:
            results.append(
                {
                    "check": "cell_voltage_spread_end",
                    "result": "PASS",
                    "detail": f"Δ={spread_mv:.2f}mV",
                    "reason": "<40mV",
                }
            )
        else:
            results.append(
                {
                    "check": "cell_voltage_spread_end",
                    "result": "NG",
                    "detail": f"Δ={spread_mv:.2f}mV",
                    "reason": "≥40mV",
                }
            )
    else:
        results.append(
            {
                "check": "cell_voltage_spread_end",
                "result": "NG",
                "detail": "No cell voltage data",
                "reason": "No data in aux[-1]",
            }
        )

    return results


def check_step_6(step, aux_all):
    """Check step 6: Rest step with 40 minutes."""
    aux = get_aux_in_window(
        aux_all, step.get("oneset_date", ""), step.get("end_date", "")
    )
    results = []

    # 1. String/cell voltage spread at end <20mV
    last_row = aux[-1] if aux else {}
    string_voltages = [
        parse_float(last_row[k]) for k in last_row if "string_voltage_v" in k
    ]
    string_voltages = [v for v in string_voltages if v is not None]

    if string_voltages:
        spread_mv = (max(string_voltages) - min(string_voltages)) * 1000
        if spread_mv < 20:
            results.append(
                {
                    "check": "string_voltage_spread_end",
                    "result": "PASS",
                    "detail": f"Δ={spread_mv:.2f}mV",
                    "reason": "<20mV",
                }
            )
        else:
            results.append(
                {
                    "check": "string_voltage_spread_end",
                    "result": "NG",
                    "detail": f"Δ={spread_mv:.2f}mV",
                    "reason": "≥20mV",
                }
            )
    else:
        # Fallback to cell voltages
        cell_voltages = [
            parse_float(v)
            for k, v in last_row.items()
            if k.startswith("cell_volt_mv_") and k.endswith("_mv")
        ]
        cell_voltages = [v / 1000.0 for v in cell_voltages if v is not None]
        if cell_voltages:
            spread_mv = (max(cell_voltages) - min(cell_voltages)) * 1000
            if spread_mv < 20:
                results.append(
                    {
                        "check": "cell_voltage_spread_end",
                        "result": "PASS",
                        "detail": f"Δ={spread_mv:.2f}mV",
                        "reason": "<20mV",
                    }
                )
            else:
                results.append(
                    {
                        "check": "cell_voltage_spread_end",
                        "result": "NG",
                        "detail": f"Δ={spread_mv:.2f}mV",
                        "reason": "≥20mV",
                    }
                )
        else:
            results.append(
                {
                    "check": "cell_voltage_spread_end",
                    "result": "NG",
                    "detail": "No voltage data",
                    "reason": "No string/cell voltages in aux end row",
                }
            )

    # 2. Temperature reduction ≤20°C
    temp_fields = [f"bms_temp_{i}_c" for i in range(1, 5)]
    temp_reductions = []
    if aux:
        for tfield in temp_fields:
            t_start = parse_float(aux[0].get(tfield))
            t_end = parse_float(aux[-1].get(tfield))
            if t_start is not None and t_end is not None:
                temp_reductions.append((tfield, t_start - t_end))
        max_reduction = (
            max([abs(d) for k, d in temp_reductions]) if temp_reductions else None
        )
        detail = ", ".join([f"{k}: Δ={d:.2f}°C" for k, d in temp_reductions])
        if max_reduction is not None and max_reduction <= 20:
            results.append(
                {
                    "check": "temp_reduction",
                    "result": "PASS",
                    "detail": detail,
                    "reason": "All temp reductions ≤ 20°C",
                }
            )
        elif max_reduction is not None:
            results.append(
                {
                    "check": "temp_reduction",
                    "result": "NG",
                    "detail": detail,
                    "reason": "Reduction > 20°C",
                }
            )
        else:
            results.append(
                {
                    "check": "temp_reduction",
                    "result": "NG",
                    "detail": "No data",
                    "reason": "No temp data",
                }
            )
    else:
        results.append(
            {
                "check": "temp_reduction",
                "result": "NG",
                "detail": "No aux data",
                "reason": "No aux in window",
            }
        )

    # 3. Max-min temp <3°C
    max_min_spreads = []
    for row in aux:
        temps = [parse_float(row.get(t)) for t in temp_fields if row.get(t) is not None]
        temps = [t for t in temps if t is not None]
        if temps:
            spread = max(temps) - min(temps)
            max_min_spreads.append(spread)
    max_spread = max(max_min_spreads) if max_min_spreads else None
    if max_spread is not None and max_spread < 3.0:
        results.append(
            {
                "check": "max_min_temp",
                "result": "PASS",
                "detail": f"Max spread = {max_spread:.2f}°C",
                "reason": "<3°C",
            }
        )
    elif max_spread is not None:
        results.append(
            {
                "check": "max_min_temp",
                "result": "NG",
                "detail": f"Max spread = {max_spread:.2f}°C",
                "reason": "≥3°C found",
            }
        )
    else:
        results.append(
            {
                "check": "max_min_temp",
                "result": "NG",
                "detail": "No data",
                "reason": "No temp data",
            }
        )

    return results


def check_step_7(step, aux_all):
    """Check step 7: CC charge step with 2 hours."""
    aux = get_aux_in_window(
        aux_all, step.get("oneset_date", ""), step.get("end_date", "")
    )
    results = []

    # 1. Max temp probe <50°C
    temp_fields = [f"bms_temp_{i}_c" for i in range(1, 5)]
    max_temps = []
    for t in temp_fields:
        vals = [
            parse_float(row.get(t))
            for row in aux
            if t in row and parse_float(row.get(t)) is not None
        ]
        if vals:
            max_temps.append((t, max(vals)))
    fail_temps = [(k, v) for k, v in max_temps if v >= 50]
    detail = ", ".join([f"{k}: {v:.2f}°C" for k, v in max_temps])
    if not max_temps:
        results.append(
            {
                "check": "max_temp_probe",
                "result": "NG",
                "detail": "No data",
                "reason": "No probe temps in aux",
            }
        )
    elif not fail_temps:
        results.append(
            {
                "check": "max_temp_probe",
                "result": "PASS",
                "detail": detail,
                "reason": "All <50°C",
            }
        )
    else:
        fails = ", ".join([f"{k}: {v:.2f}°C" for k, v in fail_temps])
        results.append(
            {
                "check": "max_temp_probe",
                "result": "NG",
                "detail": detail,
                "reason": f"Over 50°C: {fails}",
            }
        )

    # 2. MOSFET temperature <75°C
    mosfet_field = "mos_temp"
    mosfet_vals = [
        parse_float(row.get(mosfet_field))
        for row in aux
        if mosfet_field in row and parse_float(row.get(mosfet_field)) is not None
    ]
    if mosfet_vals:
        max_mosfet = max(mosfet_vals)
        if max_mosfet < 75:
            results.append(
                {
                    "check": "charge_mosfet_temp",
                    "result": "PASS",
                    "detail": f"Max: {max_mosfet:.2f}°C",
                    "reason": "<75°C",
                }
            )
        else:
            results.append(
                {
                    "check": "charge_mosfet_temp",
                    "result": "NG",
                    "detail": f"Max: {max_mosfet:.2f}°C",
                    "reason": "≥75°C",
                }
            )
    else:
        results.append(
            {
                "check": "charge_mosfet_temp",
                "result": "INFO",
                "detail": "No MOSFET temp data",
                "reason": "Field not found/empty",
            }
        )

    return results


def check_step_8(step, aux_all):
    """Check step 8: Final rest step with 10 seconds."""
    aux = get_aux_in_window(
        aux_all, step.get("oneset_date", ""), step.get("end_date", "")
    )
    results = []

    # 1. Pack voltage at end
    pack_v = parse_float(aux[-1].get("pack_voltage_v")) if aux else None
    if pack_v is not None and 70 <= pack_v <= 78:
        results.append(
            {
                "check": "pack_voltage_end",
                "result": "PASS",
                "detail": f"{pack_v:.2f}V",
                "reason": "Within 70-78V",
            }
        )
    elif pack_v is not None:
        results.append(
            {
                "check": "pack_voltage_end",
                "result": "NG",
                "detail": f"{pack_v:.2f}V",
                "reason": "Out of 70-78V",
            }
        )
    else:
        results.append(
            {
                "check": "pack_voltage_end",
                "result": "NG",
                "detail": "No pack_voltage_v",
                "reason": "No data",
            }
        )

    # 2. Temperature fluctuation ±1°C
    temp_fields = [f"bms_temp_{i}_c" for i in range(1, 5)]
    temp_flucts = []
    for t in temp_fields:
        vals = [parse_float(row.get(t)) for row in aux if row.get(t) is not None]
        vals = [v for v in vals if v is not None]
        if vals:
            fluct = max(vals) - min(vals)
            temp_flucts.append((t, fluct))
    fail_temps = [(k, v) for k, v in temp_flucts if v > 2.0]
    detail = ", ".join([f"{k}: Δ={d:.2f}°C" for k, d in temp_flucts])
    if not temp_flucts:
        results.append(
            {
                "check": "temp_fluctuation",
                "result": "NG",
                "detail": "No data",
                "reason": "No probe temps in aux",
            }
        )
    elif not fail_temps:
        results.append(
            {
                "check": "temp_fluctuation",
                "result": "PASS",
                "detail": detail,
                "reason": "All probes within ±1°C",
            }
        )
    else:
        fails = ", ".join([f"{k}: Δ={v:.2f}°C" for k, v in fail_temps])
        results.append(
            {
                "check": "temp_fluctuation",
                "result": "NG",
                "detail": detail,
                "reason": f"Probe(s) >±1°C: {fails}",
            }
        )

    # 3. Cell voltage fluctuation ±1mV
    cell_fields = [
        k
        for k in (aux[0].keys() if aux else [])
        if k.startswith("cell_volt_mv_") and k.endswith("_mv")
    ]
    cell_flucts = []
    for c in cell_fields:
        vals = [parse_float(row.get(c)) for row in aux if row.get(c) is not None]
        vals = [v for v in vals if v is not None]
        if vals:
            fluct = max(vals) - min(vals)
            cell_flucts.append((c, fluct))
    fail_cells = [(k, v) for k, v in cell_flucts if v > 1.0]
    detail = ", ".join([f"{k}: Δ={d:.2f}mV" for k, d in cell_flucts])
    if not cell_flucts:
        results.append(
            {
                "check": "cell_fluctuation",
                "result": "NG",
                "detail": "No data",
                "reason": "No cell voltage in aux",
            }
        )
    elif not fail_cells:
        results.append(
            {
                "check": "cell_fluctuation",
                "result": "PASS",
                "detail": detail,
                "reason": "All cells within ±1mV",
            }
        )
    else:
        fails = ", ".join([f"{k}: Δ={v:.2f}mV" for k, v in fail_cells])
        results.append(
            {
                "check": "cell_fluctuation",
                "result": "NG",
                "detail": detail,
                "reason": f"Cell(s) >±1mV: {fails}",
            }
        )

    # 4. CAN packet check
    required_can_packets = [
        [f"bms_status_{i}" for i in range(1, 10)],
        [f"bms_id_{i}" for i in range(1, 4)],
        ["bms_error_1"],
        ["bms_obc_config"],
    ]
    can_fail = []
    for packet_group in required_can_packets:
        found = any(any(pkt in row for pkt in packet_group) for row in aux)
        if not found:
            can_fail.extend(packet_group)
    if not can_fail:
        results.append(
            {
                "check": "can_packets",
                "result": "PASS",
                "detail": "All required CAN packets present",
                "reason": "",
            }
        )
    else:
        results.append(
            {
                "check": "can_packets",
                "result": "NG",
                "detail": f"Missing: {can_fail}",
                "reason": "Some CAN packets missing in window",
            }
        )

    # 5. Serial number check
    serials = set()
    for row in aux:
        serial_str = extract_serial(row)
        if serial_str and serial_str != "." * 17 and serial_str != "0" * 17:
            serials.add(serial_str)
    if len(serials) == 1:
        results.append(
            {
                "check": "serial_number",
                "result": "PASS",
                "detail": f"Serial={list(serials)[0]}",
                "reason": "Serial number consistent",
            }
        )
    elif len(serials) > 1:
        results.append(
            {
                "check": "serial_number",
                "result": "NG",
                "detail": f"Inconsistent serials: {serials}",
                "reason": "Serial numbers inconsistent",
            }
        )
    else:
        results.append(
            {
                "check": "serial_number",
                "result": "NG",
                "detail": "Serial number missing or invalid",
                "reason": "Serial missing/invalid",
            }
        )

    return results


def run_all_checks(json_path):
    """Run all step checks and return JSON response."""
    with open(json_path) as f:
        data = json.load(f)

    step_list = data["data"].get("step", [])
    aux = data["data"].get("auxDBC") or data["data"].get("aux_dbc", [])
    records = data["data"].get("records", [])
    cycle = data["data"].get("cycle", [{}])[0]

    # Find steps using specialized functions
    rest_10s_step = find_rest_step(step_list, 10)
    cccv_step = find_cccv_step(step_list, 10800)  # 3 hours
    rest_30m_step = find_rest_step(step_list, 1800)  # 30 minutes
    cc_dchg_step = find_cc_dchg_step(step_list, 3600)  # 1 hour
    cc_chg_2h_step = find_cc_chg_step(step_list, 7200)  # 2 hours
    rest_40m_step = find_rest_step(step_list, 2400)  # 40 minutes
    final_rest_10s_step = find_rest_step(step_list, 10)  # 10 seconds

    # Run checks for available steps
    response = {"json_file": json_path, "number_of_steps": 8, "tests": []}

    # Step 1
    if rest_10s_step:
        results = check_step_1(rest_10s_step, aux)
    else:
        results = [
            {
                "check": "step_find",
                "result": "NG",
                "detail": "No Rest 10s step",
                "reason": "Not found",
            }
        ]
    response["tests"].append({"step": "1", "results": results})

    # Step 2
    if cccv_step:
        results = check_step_2(cccv_step, aux, cycle)
    else:
        results = [
            {
                "check": "step_find",
                "result": "NG",
                "detail": "No CCCV Chg step",
                "reason": "Not found",
            }
        ]
    response["tests"].append({"step": "2", "results": results})

    # Step 3
    if rest_30m_step:
        results = check_step_3(rest_30m_step, aux)
    else:
        results = [
            {
                "check": "step_find",
                "result": "NG",
                "detail": "No Rest 30min step",
                "reason": "Not found",
            }
        ]
    response["tests"].append({"step": "3", "results": results})

    # Step 4 - Use the CC discharge step found earlier
    if cc_dchg_step:
        results = check_step_4(cc_dchg_step, aux, records)
    else:
        results = [
            {
                "check": "step_find",
                "result": "NG",
                "detail": "No CC DChg step",
                "reason": "Not found",
            }
        ]
    response["tests"].append({"step": "4", "results": results})

    # Step 5
    results = check_step_5(step_list, aux, records)
    response["tests"].append({"step": "5", "results": results})

    # Step 6
    if rest_40m_step:
        results = check_step_6(rest_40m_step, aux)
    else:
        results = [
            {
                "check": "step_find",
                "result": "NG",
                "detail": "No Rest 40min step",
                "reason": "Not found",
            }
        ]
    response["tests"].append({"step": "6", "results": results})

    # Step 7
    if cc_chg_2h_step:
        results = check_step_7(cc_chg_2h_step, aux)
    else:
        results = [
            {
                "check": "step_find",
                "result": "NG",
                "detail": "No CC Chg 2h step",
                "reason": "Not found",
            }
        ]
    response["tests"].append({"step": "7", "results": results})

    # Step 8
    if final_rest_10s_step:
        results = check_step_8(final_rest_10s_step, aux)
    else:
        results = [
            {
                "check": "step_find",
                "result": "NG",
                "detail": "No Rest 10s step",
                "reason": "Not found",
            }
        ]
    response["tests"].append({"step": "8", "results": results})

    return response


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        json_path = sys.argv[1]
    else:
        json_path = "/Users/riorizki/development/repos/ION-Mobility-Team/mes/LimeNDAX/result/LG_2_EOL_test_15-1-4-20250428125222_response.json"

    result = run_all_checks(json_path)
    print(json.dumps(result, indent=2))
