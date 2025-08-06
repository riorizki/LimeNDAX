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


def parse_float(val):
    try:
        return float(val)
    except:
        return None


def get_discharge_window(step_list):
    for step in step_list:
        typ = step.get("step_type", "").replace("_", " ").lower()
        if typ == "cc dchg":
            start = step.get("oneset_date")
            end = step.get("oneset_end_date")
            return parse_dt(start), parse_dt(end)
    return None, None


def get_aux_in_window(aux, start, end):
    sdt = parse_dt(start) if isinstance(start, str) else start
    edt = parse_dt(end) if isinstance(end, str) else end
    if not sdt or not edt:
        return aux
    return [
        row
        for row in aux
        if sdt <= parse_dt(row.get("date") or row.get("datetime") or "") <= edt
    ]


def check_step_5(step_list, aux, records):
    result_table = [["check", "RESULT", "DETAIL", "REASON"]]
    # Get discharge window
    dchg_start, dchg_end = get_discharge_window(step_list)
    aux_win = get_aux_in_window(aux, dchg_start, dchg_end)

    # 1. Discharge capacity (Assume 'capacity_ah' in step)
    dchg_step = next(
        (
            s
            for s in step_list
            if s.get("step_type", "").replace("_", " ").lower() == "cc dchg"
        ),
        None,
    )
    capacity = parse_float(dchg_step.get("capacity_ah")) if dchg_step else None
    if capacity is not None and 53 <= capacity <= 57:
        result_table.append(["capacity", "PASS", f"{capacity:.2f}Ah", "Within 53-57Ah"])
    elif capacity is not None:
        result_table.append(["capacity", "NG", f"{capacity:.2f}Ah", "Outside 53-57Ah"])
    else:
        result_table.append(
            ["capacity", "NG", "No data", "Missing capacity_ah in step"]
        )

    # 2. Current sensor error (compare bms_current_a_a vs 57.6A)
    errors = []
    set_current = 57.6  # fixed for this test
    for row in aux_win:
        bms_current = parse_float(row.get("bms_current_a_a"))
        if bms_current is not None:
            abs_error = abs(bms_current - set_current)
            errors.append(abs_error)
    max_error = max(errors) if errors else None
    if max_error is not None and max_error < 2.4:
        result_table.append(
            [
                "current_error",
                "PASS",
                f"Max error = {max_error:.2f}A",
                "All errors < 2.4A",
            ]
        )
    elif max_error is not None:
        result_table.append(
            [
                "current_error",
                "NG",
                f"Max error = {max_error:.2f}A",
                "Error >= 2.4A found",
            ]
        )
    else:
        result_table.append(
            [
                "current_error",
                "NG",
                "No data",
                "Missing bms_current_a_a in aux",
            ]
        )

    # 3. Max temp rise < 30°C (for each probe)
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
            result_table.append(["max_temp_rise", "NG", "No data", "No temp data"])
        elif not fails:
            result_table.append(
                ["max_temp_rise", "PASS", detail, "All temp rises < 30°C"]
            )
        else:
            result_table.append(
                ["max_temp_rise", "NG", detail, "One or more temp rises ≥ 30°C"]
            )
    else:
        result_table.append(["max_temp_rise", "NG", "No aux data", "No aux in window"])

    # 4. Max-min temp < 3°C (across all probes at any sample)
    max_min_spreads = []
    for row in aux_win:
        temps = [parse_float(row.get(t)) for t in temp_fields if row.get(t) is not None]
        temps = [t for t in temps if t is not None]
        if temps:
            spread = max(temps) - min(temps)
            max_min_spreads.append(spread)
    max_spread = max(max_min_spreads) if max_min_spreads else None
    if max_spread is not None and max_spread < 3.0:
        result_table.append(
            ["max_min_temp", "PASS", f"Max spread = {max_spread:.2f}°C", "<3°C"]
        )
    elif max_spread is not None:
        result_table.append(
            ["max_min_temp", "NG", f"Max spread = {max_spread:.2f}°C", "≥3°C found"]
        )
    else:
        result_table.append(["max_min_temp", "NG", "No data", "No temp data"])

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
        result_table.append(
            ["low_voltage_warning", "PASS", low_v_reason, "<64V or <3.2V triggered"]
        )
    else:
        result_table.append(
            [
                "low_voltage_warning",
                "NG",
                "No alert triggered",
                "No pack/string < threshold",
            ]
        )

    # 6. Max-min cell voltage <40mV (at end)
    cell_voltages = (
        [
            parse_float(v)
            for k, v in aux_win[-1].items()
            if k.startswith("cell_volt_mv_") and k.endswith("_mv")
        ]
        if aux_win
        else []
    )
    cell_voltages = [v / 1000.0 for v in cell_voltages if v is not None]
    if cell_voltages:
        spread_mv = (max(cell_voltages) - min(cell_voltages)) * 1000  # mV
        if spread_mv < 40:
            result_table.append(
                ["cell_voltage_spread_end", "PASS", f"Δ={spread_mv:.2f}mV", "<40mV"]
            )
        else:
            result_table.append(
                ["cell_voltage_spread_end", "NG", f"Δ={spread_mv:.2f}mV", "≥40mV"]
            )
    else:
        result_table.append(
            [
                "cell_voltage_spread_end",
                "NG",
                "No cell voltage data",
                "No data in aux[-1]",
            ]
        )

    return result_table


def main(json_path):
    with open(json_path) as f:
        data = json.load(f)
    step_list = data["data"].get("step", [])
    aux = data["data"].get("auxDBC") or data["data"].get("aux_dbc", [])
    records = data["data"].get("records", [])
    result = check_step_5(step_list, aux, records)
    print(tabulate(result, headers="firstrow", tablefmt="github"))


if __name__ == "__main__":
    main(
        "/Users/riorizki/development/repos/ION-Mobility-Team/mes/LimeNDAX/result/LG_2_EOL_test_15-1-4-20250428125222_response.json"
    )
