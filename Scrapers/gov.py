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

MAX_PAGES = 100  # Pages to scrape (adjust as needed)
DELAY = 1        # Delay between requests

# ✅ Required output fieldnames
FIELDNAMES = ["title", "content", "date", "url", "author", "domain", "categories"]

# ---------------- HELPER FUNCTIONS ----------------
def clean_text(text):
    return text.strip().replace('\n', ' ').replace('\r', '')[:5000] if text else "N/A"

def extract_domain(url):
    """Extract domain from a URL"""
    return urlparse(url).netloc or "data.gov"

def extract_tags_from_dataset_page(url):
    """Visit dataset detail page to extract category tags"""
    try:
        res = requests.get(url, headers=HEADERS, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')
        tags = soup.select("section.tags li a")
        return [tag.text.strip() for tag in tags if tag.text.strip()]
    except Exception as e:
        logging.error(f"Failed to extract tags from {url}: {e}")
        return []

# ---------------- PARSE ONE DATASET ITEM ----------------
def parse_dataset_item(item):
    try:
        title_tag = item.select_one("h3 a")
        desc_tag = item.select_one(".notes")

        title = clean_text(title_tag.text) if title_tag else "N/A"
        relative_url = title_tag.get("href") if title_tag else ""
        dataset_url = BASE_URL + relative_url if relative_url else "N/A"
        content = clean_text(desc_tag.text) if desc_tag else "N/A"
        tags = extract_tags_from_dataset_page(dataset_url)
        time.sleep(DELAY)

        return {
            "title": title,
            "content": content,
            "date": datetime.utcnow().strftime("%Y-%m-%d"),
            "url": dataset_url,
            "author": "data.gov",
            "domain": extract_domain(dataset_url),
            "categories": ", ".join(tags) if tags else "government, dataset"
        }

    except Exception as e:
        logging.error(f"Error processing dataset item: {e}")
        return None

# ---------------- SCRAPE PAGE ----------------
def scrape_dataset_list(page):
    print(f"🔍 Scraping page {page}")
    page_url = f"{START_URL}?page={page}"
    try:
        res = requests.get(page_url, headers=HEADERS, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        dataset_items = soup.select(".dataset-content")
        results = []

        for item in dataset_items:
            parsed = parse_dataset_item(item)
            if parsed and all(parsed.get(fld) for fld in FIELDNAMES):
                results.append(parsed)

        return results
    except Exception as e:
        logging.error(f"Failed to parse page {page_url}: {e}")
        return []

# ---------------- SCRAPE ALL ----------------
def scrape_all_datasets():
    all_data = []
    for page in range(1, MAX_PAGES + 1):
        page_data = scrape_dataset_list(page)
        if not page_data:
            break
        all_data.extend(page_data)
        time.sleep(DELAY)
    return all_data

# ---------------- SAVE CLEAN CSV ----------------
def deduplicate_and_save_csv(data, output_file):
    seen = set()
    cleaned = []

    for row in data:
        key = (row['title'], row['url'])
        if key not in seen:
            seen.add(key)
            cleaned.append(row)

    with open(output_file, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(cleaned)

    print(f"\n✅ Saved {len(cleaned)} clean records to {output_file}")

# ---------------- MAIN ----------------
def main():
    print("🚀 Scraping legal/government datasets from data.gov ...")
    datasets = scrape_all_datasets()

    if datasets:
        deduplicate_and_save_csv(datasets, OUTPUT_CSV)
        print("\n📌 Sample Record:")
        for key, val in datasets[0].items():
            print(f"{key}: {val[:100]}{'...' if len(val) > 100 else ''}")
    else:
        print("❌ No data scraped.")

if __name__ == "__main__":
    main()
