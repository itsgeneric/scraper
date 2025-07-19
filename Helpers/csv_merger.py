import pandas as pd
import glob
import os

# Define the directory containing all your CSVs
input_folder = "../Datasets"
output_file = "../Datasets/merged.csv"

# Use glob to get all CSV files in the directory
csv_files = glob.glob(os.path.join(input_folder, "*.csv"))

print(f"📂 Found {len(csv_files)} CSV files to merge:")
for file in csv_files:
    print(f" - {file}")

# Read and concatenate all CSV files
dfs = []
for file in csv_files:
    try:
        df = pd.read_csv(file)
        dfs.append(df)
    except Exception as e:
        print(f"⚠️ Failed to read {file}: {e}")

# Merge and save
if dfs:
    merged_df = pd.concat(dfs, ignore_index=True)
    merged_df.to_csv(output_file, index=False)
    print(f"\n✅ Merged CSV saved as: {output_file}")
    print(f"📊 Total rows: {len(merged_df)}")
else:
    print("❌ No valid CSVs found to merge.")
