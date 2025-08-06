import pandas as pd
import re
import json
from typing import Optional, Dict, List, Any


def normalize_header(header):
    if not isinstance(header, str):
        header = str(header)
    header = header.strip()
    header = re.sub(r"[？?]+", "", header)
    header = re.sub(r"[^\x00-\x7F]+", "", header)
    header = re.sub(r"o_b_c", "obc", header, flags=re.IGNORECASE)
    header = re.sub(r"b_m_s", "bms", header, flags=re.IGNORECASE)
    header = re.sub(r"m_o_s", "mos", header, flags=re.IGNORECASE)
    header = re.sub(r"_(?=[a-z]_)", "", header)
    header = re.sub(r"_+", "_", header)
    header = header.replace("\n", " ")
    header = header.replace("-", " ")
    header = header.replace(".", "_")
    header = header.replace("(", "_")
    header = header.replace(")", "")
    header = header.replace("/", "_")
    header = header.replace("%", "percent")
    header = header.replace("℃", "c")
    header = header.replace("Δ", "delta")
    header = header.replace("δ", "delta")
    header = header.replace("±", "")
    header = header.replace("Ω", "ohm")
    header = re.sub(r"\s+", "_", header)
    header = header.lower()
    header = re.sub(r"_+", "_", header)
    header = header.strip("_")
    header = header.replace(" ", "")
    return header


def clean_dataframe(df: pd.DataFrame, drop_no_column: bool = True) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if drop_no_column and len(df.columns) > 0:
        if str(df.columns[0]).lower() in ["no.", "no"]:
            df = df.drop(df.columns[0], axis=1)
    df.columns = [normalize_header(col) for col in df.columns]
    df = df.dropna(axis=1, how="all")
    df = df.dropna(axis=0, how="all")
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str).str.replace(",", ".", regex=False)
    return df


def load_workbook(file_path: str) -> Dict[str, pd.DataFrame]:
    """Loads all sheets in the Excel workbook (with no header by default)."""
    try:
        # Loads ALL sheets as DataFrames; headers handled per parser
        sheets = pd.read_excel(
            file_path, sheet_name=None, engine="openpyxl", header=None
        )
        return sheets
    except Exception as e:
        print(f"Failed to load workbook: {e}")
        return {}


def parse_aux_dbc(df: Optional[pd.DataFrame]) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    # In your original: header=1, so reset columns
    df.columns = df.iloc[1].values  # Use second row as header
    df = df.iloc[2:]  # Skip first two rows
    df = clean_dataframe(df, drop_no_column=True)
    if df.empty:
        return []
    data = df.to_dict(orient="records")
    return data if data else []


def parse_cycle(df: Optional[pd.DataFrame]) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    # header=0 (already loaded with no header, so set header)
    df.columns = df.iloc[0].values
    df = df.iloc[1:]
    df = clean_dataframe(df, drop_no_column=False)
    if df.empty:
        return []
    data = df.to_dict(orient="records")
    return data if data else []


def parse_idle(df: Optional[pd.DataFrame]) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    df.columns = df.iloc[0].values
    df = df.iloc[1:]
    df = clean_dataframe(df, drop_no_column=True)
    if df.empty:
        return []
    data = df.to_dict(orient="records")
    return data if data else []


def parse_log(df: Optional[pd.DataFrame]) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    df.columns = df.iloc[0].values
    df = df.iloc[1:]
    df = clean_dataframe(df, drop_no_column=True)
    if df.empty:
        return []
    data = df.to_dict(orient="records")
    return data if data else []


def parse_others(df: Optional[pd.DataFrame]) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    df.columns = df.iloc[1].values  # Use second row as header
    df = df.iloc[2:]
    df = clean_dataframe(df, drop_no_column=True)
    if df.empty:
        return []
    data = df.to_dict(orient="records")
    return data if data else []


def parse_record(df: Optional[pd.DataFrame]) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    df.columns = df.iloc[0].values
    df = df.iloc[1:]
    df = clean_dataframe(df, drop_no_column=False)
    if df.empty:
        return []
    data = df.to_dict(orient="records")
    return data if data else []


def parse_step(df: Optional[pd.DataFrame]) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    df.columns = df.iloc[0].values
    df = df.iloc[1:]
    df = clean_dataframe(df, drop_no_column=False)
    if df.empty:
        return []
    data = df.to_dict(orient="records")
    return data if data else []


def parse_test(df: Optional[pd.DataFrame]) -> dict:
    if df is None or df.empty:
        print("Sheet 'test' is empty!")
        return {}

    print("=== TEST SHEET (first 10 rows) ===")
    print(df.head(10))

    field_map = {
        "start step id": "start_step_id",
        "cycle count": "cycle_count",
        "record settings": "record_settings",
        "voltage range": "voltage_range",
        "current range": "current_range",
        "active material": "active_material",
        "volt. upper": "volt_upper",
        "volt upper": "volt_upper",
        "volt. lower": "volt_lower",
        "volt lower": "volt_lower",
        "curr. upper": "curr_upper",
        "curr upper": "curr_upper",
        "curr. lower": "curr_lower",
        "curr lower": "curr_lower",
        "start time": "start_time",
        "nominal capacity": "nominal_capacity",
        "p/n": "p_n",
        "builder": "builder",
        "remarks": "remarks",
        "barcode": "barcode",
    }
    test_info_clean = {v: "" for v in field_map.values()}

    # Loop rows 1-6 (inclusive)
    for i in range(1, 7):
        for j in range(0, df.shape[1], 3):
            field_cell = (
                str(df.iloc[i, j]).strip().lower() if pd.notnull(df.iloc[i, j]) else ""
            )
            value_cell = (
                str(df.iloc[i, j + 2]).strip()
                if (j + 2 < df.shape[1] and pd.notnull(df.iloc[i, j + 2]))
                else ""
            )
            print(f"Row {i}, Col {j}: field='{field_cell}' | value='{value_cell}'")
            if field_cell and field_cell in field_map:
                key = field_map[field_cell]
                test_info_clean[key] = value_cell

    # Step plan extraction
    step_plan_header_idx = None
    for idx in range(df.shape[0]):
        if (
            pd.notnull(df.iloc[idx, 0])
            and str(df.iloc[idx, 0]).lower().strip() == "step index"
        ):
            step_plan_header_idx = idx
            break
    step_plan = []
    if step_plan_header_idx is not None:
        step_headers = df.iloc[step_plan_header_idx, :].tolist()
        step_headers_norm = [normalize_header(h) for h in step_headers]
        for i in range(step_plan_header_idx + 1, df.shape[0]):
            row = df.iloc[i, :].tolist()
            if (
                pd.isnull(row[0])
                or str(row[0]).strip() == ""
                or str(row[0]).lower().startswith("nan")
            ):
                continue
            step_dict = {}
            for k, v in zip(step_headers_norm, row):
                if k and k != "nan":
                    step_dict[k] = v if pd.notnull(v) else ""
            step_plan.append(step_dict)
    return {"test_information": test_info_clean, "step_plan": step_plan}


def parse_unit(df: Optional[pd.DataFrame]) -> Dict[str, Any]:
    if df is None or df.empty:
        return {}
    try:
        device_parts = []
        for i in range(1, 4):
            if i < df.shape[1] and pd.notnull(df.iloc[1, i]):
                device_parts.append(str(int(df.iloc[1, i])))
        device = " ".join(device_parts) if device_parts else ""
        start_time = (
            str(df.iloc[2, 2])
            if (2 < df.shape[1] and pd.notnull(df.iloc[2, 2]))
            else ""
        )
        end_time = (
            str(df.iloc[2, 6])
            if (6 < df.shape[1] and pd.notnull(df.iloc[2, 6]))
            else ""
        )
        list_of_unit_plans = {}
        if df.shape[0] > 6:
            headers = df.iloc[5, :].tolist() if 5 < df.shape[0] else []
            units = df.iloc[6, :].tolist() if 6 < df.shape[0] else []
            for h, u in zip(headers, units):
                if pd.notnull(h) and pd.notnull(u):
                    key = str(h).strip().lower().replace(" ", "_")
                    value = str(u).strip()
                    list_of_unit_plans[key] = value
        result = {
            "device": device,
            "start_time": start_time,
            "end_time": end_time,
            "list_of_unit_plans": list_of_unit_plans,
        }
        if not device and not start_time and not end_time and not list_of_unit_plans:
            return {}
        return result
    except Exception as e:
        print(f"Error parsing unit sheet: {e}")
        return {}


def parse_all_sheets(file_path: str) -> Dict[str, Any]:
    data = {}
    dict_sheets = ["test", "unit"]
    # Load all sheets ONCE
    sheets = load_workbook(file_path)
    parser_map = {
        "auxDBC": parse_aux_dbc,
        "cycle": parse_cycle,
        "idle": parse_idle,
        "log": parse_log,
        "record": parse_record,
        "step": parse_step,
        "test": parse_test,
        "unit": parse_unit,
    }
    for sheet_name, parser_func in parser_map.items():
        df = sheets.get(sheet_name)
        try:
            if parser_func == parse_others:
                parsed_data = parser_func(df) if df is not None else []
            elif sheet_name in dict_sheets:
                parsed_data = parser_func(df) if df is not None else {}
            else:
                parsed_data = parser_func(df) if df is not None else []
            data[sheet_name] = parsed_data
        except Exception as e:
            print(f"Error parsing {sheet_name}: {e}")
            data[sheet_name] = {} if sheet_name in dict_sheets else []
    return {"file_path": file_path, "data": data}


def parse_excel_file(file_path: str) -> Dict[str, Any]:
    return parse_all_sheets(file_path)


def parse_sheet(file_path: str, sheet_name: str, **kwargs) -> Any:
    sheets = load_workbook(file_path)
    parser_map = {
        "auxdbc": parse_aux_dbc,
        "aux_dbc": parse_aux_dbc,
        "cycle": parse_cycle,
        "idle": parse_idle,
        "log": parse_log,
        "record": parse_record,
        "step": parse_step,
        "test": parse_test,
        "unit": parse_unit,
    }
    sheet_key = sheet_name.lower().replace(" ", "_")
    df = sheets.get(sheet_key)
    if sheet_key in parser_map and df is not None:
        return parser_map[sheet_key](df)
    elif df is not None:
        return parse_others(df)
    else:
        print(f"Sheet {sheet_name} not found in file.")
        return {} if sheet_key in ["test", "unit"] else []


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python parser.py <file_path> [sheet_name]")
        print("Examples:")
        print("  python parser.py file.xlsx           # Parse all sheets")
        print("  python parser.py file.xlsx test      # Parse specific sheet")
        print("")
        print("Output format for all sheets:")
        print("{")
        print('  "file_path": "path/to/file.xlsx",')
        print('  "data": {')
        print('    "test": {...},')
        print('    "unit": {...},')
        print('    "aux_dbc": [...],')
        print('    "cycle": [...],')
        print('    "idle": [...],')
        print('    "log": [...],')
        print('    "record": [...],')
        print('    "step": [...]')
        print("  }")
        print("}")
        sys.exit(1)
    file_path = sys.argv[1]
    if len(sys.argv) >= 3:
        sheet_name = sys.argv[2]
        result = parse_sheet(file_path, sheet_name)
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        result = parse_excel_file(file_path)
        with open("parsed_output.json", "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        print("Parsed data written to parsed_output.json")
