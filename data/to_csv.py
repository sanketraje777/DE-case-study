import pandas as pd

# Load the Excel file
excel_path = "DE - Case Study sample data set.xlsx"
xls = pd.ExcelFile(excel_path)

# List all sheet names
sheet_names = xls.sheet_names
sheet_names

# Save each sheet as a CSV file
output_dir = "."
csv_paths = {}

for sheet in sheet_names:
    df = xls.parse(sheet)
    csv_file = f"{output_dir}/{sheet}.csv"
    df.to_csv(csv_file, index=False)
    csv_paths[sheet] = csv_file

csv_paths
