import pandas as pd
import json

file_path = "LG_2_EOL_test_15-1-4-20250428125222.xlsx"
sheet_name = "unit"
df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, engine="openpyxl")

# Device info: row 1, col 1-3
device = " ".join(
    str(int(df.iloc[1, i])) for i in range(1, 4) if pd.notnull(df.iloc[1, i])
)

# Start time: row 2, col 2
start_time = df.iloc[2, 2] if pd.notnull(df.iloc[2, 2]) else ""
# End time: row 2, col 6
end_time = df.iloc[2, 6] if pd.notnull(df.iloc[2, 6]) else ""

# List of unit plans: headers and units
headers = df.iloc[5, :].tolist()
units = df.iloc[6, :].tolist()
list_of_unit_plans = {}
for h, u in zip(headers, units):
    if pd.notnull(h) and pd.notnull(u):
        list_of_unit_plans[str(h).strip().lower().replace(" ", "_")] = str(u).strip()

parsed = {
    "device": device,
    "start_time": str(start_time),
    "end_time": str(end_time),
    "list_of_unit_plans": list_of_unit_plans,
}

print(json.dumps(parsed, indent=2, ensure_ascii=False))
