import pandas as pd
import json

# Read all sheets
excel_file = pd.ExcelFile('./Data.xlsx')

# Convert each sheet to JSON
all_data = {}
for sheet_name in excel_file.sheet_names:
    df = pd.read_excel(excel_file, sheet_name=sheet_name)
    all_data[sheet_name] = df.to_dict(orient='records')

# Save to JSON file
with open('output.json', 'w', encoding='utf-8') as f:
    json.dump(all_data, f, indent=2, ensure_ascii=False)