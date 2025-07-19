import pandas as pd
import glob

# List of CSV files to merge
csv_files = ['ap_news_articles.csv', 'finance.csv', 'legal_gov.csv', 'papers.csv', 'research_papers.csv', 'sciencedaily.csv', 'tech_docs.csv', 'tngo_articles.csv', 'tribunal_docs.csv', 'wanderingearl.csv', 'wikipedia_articles_1.csv', 'worldhistory.csv']

# Read and concatenate all CSVs
merged_df = pd.concat([pd.read_csv(file) for file in csv_files], ignore_index=True)

# Save to a new CSV file
merged_df.to_csv('merged.csv', index=False)
