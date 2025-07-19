import csv

input_file = "D:/BNMIT/Semester 7/Final Year Project/scraper/Datasets/research_papers.csv"
output_file = "../Datasets/papers.csv"

seen_rows = set()
rows_written = 0

with open(input_file, newline='', encoding='utf-8') as infile, open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()

    for row in reader:
        # Remove leading/trailing whitespace from all fields
        cleaned_row = {k: v.strip() for k, v in row.items()}

        # Skip rows with any empty fields
        if not all(cleaned_row.values()):
            continue

        # Create a tuple of values to check for duplicates
        row_key = tuple(cleaned_row.values())

        if row_key not in seen_rows:
            writer.writerow(cleaned_row)
            seen_rows.add(row_key)
            rows_written += 1

print(f"✅ Cleaned data saved to '{output_file}' ({rows_written} unique, complete rows written).")
