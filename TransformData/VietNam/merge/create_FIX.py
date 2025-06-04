import pandas as pd
import sys
import os
sys.path.append('/app/')
sys.path.append('/app/TransformData/VietNam')
from base.PATH_UPDATE import *
from VAR_GLOBAL_CONFIG import *
from base.Financial import CafeF,VietStock
from base.Setup import *
from Flow.ulis import *

print(f"{PATH_COMPARE}/{YEAR_FINANCAIL_FIX_FILE}.xlsx")
df = pd.read_excel(f"{PATH_COMPARE}/{YEAR_FINANCAIL_FIX_FILE}.xlsx",sheet_name="Sheet1")
# def determine_fix(row):
#     if row['Compare'] == 1:
#         return row['2023_x'] if not pd.isna(row['2023_x']) else row['2023_y']
#     elif row['Compare'] == 2:
#         return row['2023_x'] if not pd.isna(row['2023_x']) else row['2023_y']
#     else:
#         return "Skip"

# # Apply the function to create the FIX column
# df['FIX'] = df.apply(determine_fix, axis=1)
# Define FIX logic

def determine_fix(row):
    if row['Compare'] == "1" or row['Compare'] == 1:
        return row['2023_x'] if not pd.isna(row['2023_x']) else row['2023_y']
    elif row['Compare'] == "2" or row['Compare'] == 2:
        return row['2023_x'] if not pd.isna(row['2023_x']) else row['2023_y']
    else:
        return "Skip"

df['FIX'] = df.apply(determine_fix, axis=1)

print("First few rows of the DataFrame:")
print(df.head())

print("Unique values in 'Compare' column:")
print(df['Compare'].unique())

print("Count of non-NaN values in '2023_x' and '2023_y':")
print(f"2023_x: {df['2023_x'].notna().sum()}, 2023_y: {df['2023_y'].notna().sum()}")

# Save the result to a new Excel file
df.to_excel("Financial_Year_with_FIX.xlsx", index=False)
# Save the result to the same folder as the input file
output_file_path = os.path.join(PATH_COMPARE, f"{YEAR_FINANCAIL_FIX_FILE}_with_FIX.xlsx")
df.to_excel(output_file_path, index=False)
print(f"File saved at: {output_file_path}")