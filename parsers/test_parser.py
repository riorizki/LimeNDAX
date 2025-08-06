import pandas as pd
import json

file_path = "LG_2_EOL_test_15-1-4-20250428125222.xlsx"
sheet_name = "test"
df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, engine="openpyxl")


import re


def normalize_header(header):
    if not isinstance(header, str):
        header = str(header)
    header = header.strip()
    header = header.replace("\n", " ")
    header = header.replace("-", " ")
    header = header.replace("(", "_")
    header = header.replace(")", "")
    header = header.replace(".", "_")
    header = header.replace("/", "_")
    header = header.replace("℃", "c")
    header = header.replace("Δ", "delta")
    header = header.replace("δ", "delta")
    header = header.replace("±", "")
    header = header.replace("Ω", "ohm")
    header = header.replace("%", "percent")
    # Insert underscore between lowercase-letter-number or letter-uppercase or number-letter
    header = re.sub(r"(?<=[a-zA-Z])(?=[A-Z0-9])", "_", header)
    header = re.sub(r"(?<=[0-9])(?=[a-zA-Z])", "_", header)
    header = re.sub(r"\s+", "_", header)
    header = header.lower()
    # Remove repeated underscores
    header = re.sub(r"_+", "_", header)
    # Remove leading/trailing underscores
    header = header.strip("_")
    # Fix common patterns for your sheet:
    header = header.replace("curra", "curr_a")
    header = header.replace("ah", "a_h")
    header = header.replace("wh", "w_h")
    header = header.replace("mv", "m_v")
    header = header.replace("mw", "m_w")
    header = header.replace("mohm", "m_ohm")
    header = header.replace("record1", "record_1")
    header = header.replace("record2", "record_2")
    header = header.replace("viv", "vi_v")
    header = header.replace("tic", "ti_c")
    header = header.replace("auxch", "aux_ch")
    return header


# --- Targeted extraction by known cell positions (adjust if you need) ---
test_info = {
    "start_step_id": str(df.iloc[0, 2]),
    "volt_upper": str(df.iloc[0, 5]),
    "cycle_count": str(df.iloc[1, 2]),
    "volt_lower": str(df.iloc[1, 5]),
    "record_settings": str(df.iloc[2, 2]),
    "curr_upper": str(df.iloc[2, 5]),
    "voltage_range": str(df.iloc[3, 2]),
    "curr_lower": str(df.iloc[3, 5]),
    "current_range": str(df.iloc[4, 2]),
    "start_time": str(df.iloc[4, 5]),
    "active_material": str(df.iloc[5, 2]),
    "nominal_capacity": str(df.iloc[5, 5]),
    "p_n": str(df.iloc[0, 8]),
    "builder": str(df.iloc[1, 8]),
    "remarks": str(df.iloc[2, 8]),
    "barcode": str(df.iloc[4, 8]),
}

# Lower-case and normalize keys, clean up 'nan' values
test_info_clean = {}
for k, v in test_info.items():
    key = k.strip().lower().replace(" ", "_")
    value = "" if v.lower() == "nan" else v
    test_info_clean[key] = value

# --- Step plan with normalized headers ---
step_plan_header_idx = df[
    df.iloc[:, 0].astype(str).str.lower().str.strip() == "step index"
].index[0]
step_headers = df.iloc[step_plan_header_idx, :].tolist()
step_headers_norm = [normalize_header(h) for h in step_headers]


step_plan = []
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

parsed = {"test_information": test_info_clean, "step_plan": step_plan}

print(json.dumps(parsed, indent=2, ensure_ascii=False))
