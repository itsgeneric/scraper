import csv

input_file = "/Users/user/Downloads/scraper/Scrapers/tribunal_docs.csv"
output_file = "tribunal_docs_clean.csv"

with open(input_file, newline='', encoding='utf-8') as infile, open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()

    for row in reader:
        # Check if all fields are non-empty after stripping whitespace
        if all(row[field].strip() for field in fieldnames):
            writer.writerow(row)

print(f"✅ Cleaned data saved to '{output_file}' (rows with any empty field removed).")
