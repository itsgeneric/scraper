import pandas as pd
import glob

# List of CSV files to merge
csv_files = ['wanderingearl.csv', 'sciencedaily.csv', 'apnews_articles.csv', 'tngo_articles.csv', 'tribunal_docs_clean.csv', 'worldhistory.csv']

# Read and concatenate all CSVs
merged_df = pd.concat([pd.read_csv(file) for file in csv_files], ignore_index=True)

# Save to a new CSV file
merged_df.to_csv('merged.csv', index=False)
