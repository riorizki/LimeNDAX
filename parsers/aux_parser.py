import json
import re

import pandas as pd


def normalize_header(header):
    header = header.strip()
    header = header.replace(" ", "_").replace("-", "_").replace(".", "_")
    header = header.replace("(", "_").replace(")", "")
    header = header.replace("/", "_")
    header = header.replace("%", "pct")
    header = header.replace("℃", "c")
    header = header.replace("Δ", "delta").replace("δ", "delta")
    header = header.replace("±", "")
    header = header.replace("Ω", "ohm")
    header = re.sub(r"__+", "_", header)
    header = header.lower()
    header = re.sub(
        r"(?<!^)(?=[A-Z])", "_", header
    )  # underscore before caps not at start
    header = re.sub(r"_+", "_", header)
    header = header.strip("_")
    return header


file_path = "LG_2_EOL_test_15-1-4-20250428125222.xlsx"
sheet_name = "auxDBC"
df = pd.read_excel(file_path, sheet_name=sheet_name, header=1, engine="openpyxl")


# Drop "No." column if it exists
if df.columns[0].lower() in ["no.", "no"]:
    df = df.drop(df.columns[0], axis=1)

# Normalize headers
df.columns = [normalize_header(col) for col in df.columns]

# Remove empty columns/rows if any
df = df.dropna(axis=1, how="all")
df = df.dropna(axis=0, how="all")

# Convert ',' decimal to '.' for string-number columns
for col in df.columns:
    if df[col].dtype == "object":
        df[col] = df[col].astype(str).str.replace(",", ".", regex=False)

# Convert to dicts
data = df.to_dict(orient="records")

# Print only the first 10 records
print(json.dumps(data[:10], indent=2, ensure_ascii=False))
