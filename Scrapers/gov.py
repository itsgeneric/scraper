import os
import csv
import time
import requests
from bs4 import BeautifulSoup
import logging
from urllib.parse import urlparse
from datetime import datetime

# ---------------- CONFIG ----------------
BASE_URL = "https://catalog.data.gov"
START_URL = f"{BASE_URL}/dataset"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; OpenDataScraper/1.0; +http://example.com/bot)"
}
OUTPUT_DIR = "scraped_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "legal_gov.csv")
LOG_FILE = os.path.join(OUTPUT_DIR, "scraper_errors.log")

MAX_PAGES = 100  # maximum pages to scrape
DELAY = 1  # seconds between requests

FIELDNAMES = ["title", "content", "date", "url", "author", "domain", "categories"]

# ---------------- HELPERS ----------------
def clean_text(text):
    return text.strip().replace("\n", " ").replace("\r", "")[:5000] if text else "N/A"

def extract_domain(url):
    return urlparse(url).netloc or "data.gov"

def extract_tags_from_dataset_page(url):
    """ Visit the dataset details URL and extract tags """
    try:
        res = requests.get(url, headers=HEADERS, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')
        tags = soup.select("section.tags li a")
        return [tag.text.strip() for tag in tags if tag.text.strip()]
    except Exception as e:
        logging.error(f"Failed to extract tags from {url}: {e}")
        return []

def parse_dataset_item(item):
    try:
        title_tag = item.select_one("h3 a")
        desc_tag = item.select_one(".notes")
        title = clean_text(title_tag.text) if title_tag else "N/A"
        relative_url = title_tag.get("href") if title_tag else ""
        url = BASE_URL + relative_url if relative_url else "N/A"
        content = clean_text(desc_tag.text) if desc_tag else "N/A"
        tags = extract_tags_from_dataset_page(url)
        time.sleep(DELAY)

        if not all([title, content, url]):
            return None

        return {
            "title": title,
            "content": content,
            "date": datetime.utcnow().strftime("%Y-%m-%d"),
            "url": url,
            "author": "data.gov",
            "domain": extract_domain(url),
            "categories": ", ".join(tags) if tags else "legal, government"
        }

    except Exception as e:
        logging.error(f"Error processing dataset item: {e}")
        return None

# ---------------- SCRAPER ----------------
def scrape_dataset_list(page):
    print(f"🔍 Scraping page {page}")
    page_url = f"{START_URL}?page={page}"
    try:
        r = requests.get(page_url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        dataset_items = soup.select(".dataset-content")

        results = []
        for item in dataset_items:
            record = parse_dataset_item(item)
            if record:
                results.append(record)

        return results
    except Exception as e:
        logging.error(f"Failed to scrape page {page_url}: {e}")
        return []

def scrape_all_datasets():
    all_data = []
    for page in range(1, MAX_PAGES + 1):
        results = scrape_dataset_list(page)
        all_data.extend(results)
        time.sleep(DELAY)
    return all_data

def deduplicate_and_save_csv(data, output_file):
    seen = set()
    final = []
    for row in data:
        key = (row['title'], row['url'])
        if key not in seen and all(row[k] and row[k] != "N/A" for k in FIELDNAMES):
            seen.add(key)
            final.append(row)

    with open(output_file, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(final)

    print(f"\n✅ Saved {len(final)} clean records to {output_file}")

# ---------------- MAIN ----------------
def main():
    print("🚀 Scraping legal-related datasets from data.gov ...")
    datasets = scrape_all_datasets()

    if datasets:
        deduplicate_and_save_csv(datasets, OUTPUT_CSV)
        print("\n📌 Sample Record:")
        print(datasets[0])
    else:
        print("⚠️ No datasets scraped.")

if __name__ == "__main__":
    main()
