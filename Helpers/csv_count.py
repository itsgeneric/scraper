import csv
import os

# List your CSV file names here
csv_files = [
    "D:/BNMIT/Semester 7/Final Year Project/scraper/Datasets/ap_news_articles.csv",
    "D:/BNMIT/Semester 7/Final Year Project/scraper/Datasets/finance.csv",
    "D:/BNMIT/Semester 7/Final Year Project/scraper/Datasets/papers.csv",
    "D:/BNMIT/Semester 7/Final Year Project/scraper/Datasets/tech_docs.csv",
    "D:/BNMIT/Semester 7/Final Year Project/scraper/Datasets/tngo_articles.csv",
    "D:/BNMIT/Semester 7/Final Year Project/scraper/Datasets/tribunal_docs.csv",
    "D:/BNMIT/Semester 7/Final Year Project/scraper/Datasets/wanderingearl_blogs.csv",
    "D:/BNMIT/Semester 7/Final Year Project/scraper/Datasets/wikipedia_articles_1.csv",
    "D:/BNMIT/Semester 7/Final Year Project/scraper/Datasets/sciencedaily.csv",
    "D:/BNMIT/Semester 7/Final Year Project/scraper/Datasets/legal_gov.csv",
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
