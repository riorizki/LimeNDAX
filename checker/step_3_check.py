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


def find_rest_step(step_list, target_seconds=1800):
    # Find Rest steps and pick the closest to 30 minutes (1800s)
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


def get_string_voltage(row):
    # Sum up all cell voltages for a row (in volts)
    string_v = 0.0
    for k, v in row.items():
        if k.startswith("cell_volt_mv_") and k.endswith("_mv"):
            try:
                string_v += float(v) / 1000.0
            except Exception:
                pass
    return string_v


def check_rest30_step(step, aux_all):
    aux = get_aux_in_window(
        aux_all, step.get("oneset_date", ""), step.get("end_date", "")
    )
    result_table = [["check", "RESULT", "DETAIL", "REASON"]]

    # --- 1. <30mV string voltage decrease ---
    if aux:
        string_v_start = get_string_voltage(aux[0])
        string_v_end = get_string_voltage(aux[-1])
        string_v_delta = string_v_start - string_v_end
        if string_v_delta < 0.03:
            result_table.append(
                [
                    "string_voltage_decrease",
                    "PASS",
                    f"{string_v_start:.4f}V → {string_v_end:.4f}V (Δ={string_v_delta*1000:.2f}mV)",
                    "<30mV decrease",
                ]
            )
        else:
            result_table.append(
                [
                    "string_voltage_decrease",
                    "NG",
                    f"{string_v_start:.4f}V → {string_v_end:.4f}V (Δ={string_v_delta*1000:.2f}mV)",
                    "Decrease ≥30mV",
                ]
            )
    else:
        result_table.append(
            ["string_voltage_decrease", "NG", "No aux data", "No data to check"]
        )

    # --- 2. <1V pack voltage decrease ---
    pack_v_start = pack_v_end = None
    if aux:
        try:
            pack_v_start = float(aux[0].get("bms_volt_v_v", 0))
            pack_v_end = float(aux[-1].get("bms_volt_v_v", 0))
            pack_v_delta = pack_v_start - pack_v_end
            if pack_v_delta < 1.0:
                result_table.append(
                    [
                        "pack_voltage_decrease",
                        "PASS",
                        f"{pack_v_start:.4f}V → {pack_v_end:.4f}V (Δ={pack_v_delta:.4f}V)",
                        "<1V decrease",
                    ]
                )
            else:
                result_table.append(
                    [
                        "pack_voltage_decrease",
                        "NG",
                        f"{pack_v_start:.4f}V → {pack_v_end:.4f}V (Δ={pack_v_delta:.4f}V)",
                        "Decrease ≥1V",
                    ]
                )
        except Exception:
            result_table.append(
                [
                    "pack_voltage_decrease",
                    "NG",
                    "Could not parse voltages",
                    "Parse error",
                ]
            )
    else:
        result_table.append(
            ["pack_voltage_decrease", "NG", "No aux data", "No data to check"]
        )

    # --- 3. max-min <20mV at end (cell voltage spread at end) ---
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
            if spread < 20:
                result_table.append(
                    [
                        "cell_voltage_spread_end",
                        "PASS",
                        f"{min(cell_vs_end):.4f}V ~ {max(cell_vs_end):.4f}V (Δ={spread:.2f}mV)",
                        "Spread <20mV",
                    ]
                )
            else:
                result_table.append(
                    [
                        "cell_voltage_spread_end",
                        "NG",
                        f"{min(cell_vs_end):.4f}V ~ {max(cell_vs_end):.4f}V (Δ={spread:.2f}mV)",
                        "Spread ≥20mV",
                    ]
                )
        else:
            result_table.append(
                [
                    "cell_voltage_spread_end",
                    "NG",
                    "No cell voltages",
                    "No data to check",
                ]
            )
    else:
        result_table.append(
            ["cell_voltage_spread_end", "NG", "No aux data", "No data to check"]
        )

    # --- 4. 0 <= temp decrease <= 5°C per probe ---
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
            result_table.append(
                ["temp_probe_decrease", "NG", "No temp data", "No data to check"]
            )
        elif not ngs:
            result_table.append(
                [
                    "temp_probe_decrease",
                    "PASS",
                    ", ".join([f"{k}: Δ={d:.2f}°C" for k, d in temp_decreases]),
                    "All probe decrease 0~5°C",
                ]
            )
        else:
            result_table.append(
                [
                    "temp_probe_decrease",
                    "NG",
                    ", ".join([f"{k}: Δ={d:.2f}°C" for k, d in ngs]),
                    "At least one probe decrease not in 0~5°C",
                ]
            )
    else:
        result_table.append(
            ["temp_probe_decrease", "NG", "No aux data", "No data to check"]
        )

    # --- 5. max-min temp <3°C (at any time) ---
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
        result_table.append(
            [
                "temp_probe_delta",
                "PASS",
                f"Max ΔT={max_probe_delta:.2f}°C",
                "Within 3°C",
            ]
        )
    elif max_probe_delta is not None:
        result_table.append(
            ["temp_probe_delta", "NG", f"Max ΔT={max_probe_delta:.2f}°C", "Exceeds 3°C"]
        )
    else:
        result_table.append(
            ["temp_probe_delta", "NG", "No probe delta data", "No data to check"]
        )

    return result_table


# ================= MAIN FILE FOR STEP 3 ==================
def main(json_path):
    with open(json_path) as f:
        data = json.load(f)
    step_list = data["data"].get("step", [])
    aux = data["data"].get("auxDBC") or data["data"].get("aux_dbc", [])

    rest30_step = find_rest_step(step_list)
    if rest30_step:
        print(
            f"\n🔍 Checking step: {rest30_step.get('step_name', '')} | Duration: {rest30_step.get('step_time', '')}"
        )
        rest30_result = check_rest30_step(rest30_step, aux)
        print(tabulate(rest30_result, headers="firstrow", tablefmt="github"))
    else:
        print("No suitable Rest (30min) step found.")


if __name__ == "__main__":
    main(
        "/Users/riorizki/development/repos/ION-Mobility-Team/mes/LimeNDAX/result/LG_2_EOL_test_15-1-4-20250428125222_response.json"
    )
