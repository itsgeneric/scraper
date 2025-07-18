import csv
import os

# List your CSV file names here
csv_files = [
    "/Users/user/Downloads/scraper/Datasets/apnews_articles.csv",
    "/Users/user/Downloads/scraper/Datasets/finance.csv",
    "/Users/user/Downloads/scraper/Datasets/tech_docs.csv",
    "/Users/user/Downloads/scraper/Datasets/tngo_articles.csv",
    "/Users/user/Downloads/scraper/Datasets/tribunal_docs_clean.csv",
    "/Users/user/Downloads/scraper/Datasets/wanderingearl_blogs.csv",
    "/Users/user/Downloads/scraper/Datasets/wikipedia_articles.csv",
    "/Users/user/Downloads/scraper/Datasets/sciencedaily.csv",
]

total_rows = 0

for filename in csv_files:
    if not os.path.isfile(filename):
        print(f"❌ File not found: {filename}")
        continue

    with open(filename, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        row_count = sum(1 for row in reader) - 1  # subtract header
        print(f"📄 {filename}: {row_count} rows")
        total_rows += row_count

print(f"\n🧮 Total rows across all CSV files: {total_rows}")
