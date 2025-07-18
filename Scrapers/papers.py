import requests
from bs4 import BeautifulSoup
import csv
import os
from concurrent.futures import ThreadPoolExecutor
import time
import logging
from datetime import datetime

# ------------------- Configuration -------------------
MAX_RECORDS = 1000
OUTPUT_DIR = os.path.join(os.getcwd(), "scraped_data")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "papers.csv")
HEADERS = ["Title", "Description", "Price", "URL", "Date of Publication"]
BASE_URL = "https://books.toscrape.com/catalogue/page-{}.html"
LOG_FILE = os.path.join(OUTPUT_DIR, 'webscraper_errors.log')

# ------------------- Logging -------------------
os.makedirs(OUTPUT_DIR, exist_ok=True)
logging.basicConfig(filename=LOG_FILE, level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# ------------------- Utility Functions -------------------

def clean_field(field):
    """Cleans text fields by stripping whitespace and replacing missing values."""
    return field.strip() if field and isinstance(field, str) else "N/A"

def get_description_from_detail_page(url):
    """Visits the product detail page and extracts the description."""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract description from <p> under #product_description
        description_tag = soup.select_one('#product_description ~ p')
        if description_tag:
            return clean_field(description_tag.text)

    except Exception as e:
        logging.error(f"Error fetching description from {url}: {str(e)}")
    return "N/A"

def parse_books_page(url):
    """Parses a single page from Books to Scrape and returns valid book entries."""
    print(f"Scraping: {url}")
    data = []
    base_url_prefix = "https://books.toscrape.com/catalogue/"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        books = soup.select('article.product_pod')

        for book in books:
            title = clean_field(book.h3.a['title'])
            price = clean_field(book.select_one('.price_color').text.replace('£', ''))
            relative_link = book.find('a')['href']
            relative_link = relative_link.replace('../../../', '')
            full_link = base_url_prefix + relative_link
            pub_date = datetime.today().strftime('%Y-%m-%d')

            # Visit the detail page to get the description
            desc = get_description_from_detail_page(full_link)

            if all([title, price, full_link, pub_date, desc]):
                data.append([title, desc, price, full_link, pub_date])

    except Exception as e:
        error_msg = f"Failed to scrape {url}, Error: {str(e)}"
        logging.error(error_msg)
        print("⚠️", error_msg)

    return data

def scrape_all_books():
    """Scrapes multiple pages in parallel until MAX_RECORDS is collected."""
    all_data = []
    page = 1

    with ThreadPoolExecutor(max_workers=5) as executor:
        while len(all_data) < MAX_RECORDS:
            futures = []
            for i in range(page, page + 5):  # Batch scrape 5 pages
                url = BASE_URL.format(i)
                futures.append(executor.submit(parse_books_page, url))
            page += 5

            for future in futures:
                records = future.result()
                all_data.extend(records)

            print(f"Collected so far: {len(all_data)}")

    return all_data[:MAX_RECORDS]

def save_to_csv(data):
    """Saves cleaned data to a CSV file with headers."""
    try:
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(HEADERS)
            writer.writerows(data)
        print(f"✅ CSV saved to '{OUTPUT_FILE}'")
    except Exception as e:
        logging.error(f"Failed to save CSV: {str(e)}")
        print("❌ Failed to save CSV:", str(e))

# ------------------- Main Function -------------------

def main():
    print("🚀 Starting data scrape...")

    # Scrape data
    records = scrape_all_books()

    # Deduplicate and clean
    unique_data = [list(item) for item in {tuple(row) for row in records}]
    cleaned_data = [row for row in unique_data if all(row)]

    print(f"✅ Scraping complete! Total cleaned records: {len(cleaned_data)}")

    # Save to CSV
    save_to_csv(cleaned_data)

    # Print sample
    print("\n📋 Sample Row:")
    if cleaned_data:
        print(cleaned_data[0])

    # Open folder
    try:
        import webbrowser
        webbrowser.open(f'file://{OUTPUT_DIR}')
    except:
        pass

# ------------------- Entry Point -------------------

if __name__ == "__main__":
    main()
