import pandas as pd
import re
import json


def normalize_header(header):
    if not isinstance(header, str):
        header = str(header)
    header = header.strip()
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
    # For things like Chg_ and DChg_ unify to chg_, dchg_
    header = header.replace("chg_", "chg_")
    header = header.replace("dchg_", "dchg_")
    return header


file_path = "LG_2_EOL_test_15-1-4-20250428125222.xlsx"
sheet_name = "cycle"
df = pd.read_excel(file_path, sheet_name=sheet_name, header=0, engine="openpyxl")

# Normalize headers
df.columns = [normalize_header(col) for col in df.columns]

# Remove empty columns if any
df = df.dropna(axis=1, how="all")
# Remove empty rows if any
df = df.dropna(axis=0, how="all")

# Convert ',' decimal to '.' if needed (for number columns as in your screenshot)
for col in df.columns:
    if df[col].dtype == "object":
        df[col] = df[col].astype(str).str.replace(",", ".", regex=False)

# Convert to dicts
data = df.to_dict(orient="records")

print(json.dumps(data, indent=2, ensure_ascii=False))
