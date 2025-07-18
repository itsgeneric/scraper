import os
import csv
import time
import requests
from bs4 import BeautifulSoup
import logging

# ---------------- CONFIG ----------------
BASE_URL = "https://catalog.data.gov"
START_URL = f"{BASE_URL}/dataset"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; OpenDataScraper/1.0; + http://example.com/bot)"
}
OUTPUT_DIR = "scraped_data"
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "legal_gov.csv")
LOG_FILE = os.path.join(OUTPUT_DIR, "scrape_errors.log")
MAX_PAGES = 60     # 🔁 adjust this to scrape more pages
DELAY = 1         # seconds

# Setup
os.makedirs(OUTPUT_DIR, exist_ok=True)
logging.basicConfig(filename=LOG_FILE, level=logging.ERROR)

# ---------------- HELPERS ----------------
def extract_tags_from_dataset_page(url):
    """ Visit the dataset details URL and extract tags """
    try:
        res = requests.get(url, headers=HEADERS, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')
        tags = soup.select("section.tags li a")
        return [tag.text.strip() for tag in tags]
    except Exception as e:
        logging.error(f"Failed to extract tags from {url}: {e}")
        return []

# ---------------- SCRAPER ----------------
def scrape_dataset_list(page):
    print(f"🔍 Scraping page {page}")
    page_url = f"{START_URL}?page={page}"
    try:
        r = requests.get(page_url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        results = []

        for item in soup.select(".dataset-content"):
            title_tag = item.select_one("h3 a")
            desc_tag = item.select_one(".notes")

            title = title_tag.text.strip() if title_tag else "N/A"
            relative_url = title_tag.get("href") if title_tag else ""
            dataset_url = BASE_URL + relative_url if relative_url else "N/A"
            description = desc_tag.text.strip() if desc_tag else "N/A"

            # ➡️ Visit the dataset page to extract tags
            tags = extract_tags_from_dataset_page(dataset_url)
            time.sleep(DELAY)

            results.append({
                "title": title,
                "description": description,
                "tags": ", ".join(tags),
                "dataset_url": dataset_url,
                "source": "data.gov"
            })

        return results
    except Exception as e:
        logging.error(f"Failed to parse page {page_url}: {e}")
        return []

# ---------------- RUNNER ----------------
def scrape_data():
    all_data = []
    for page in range(1, MAX_PAGES + 1):
        page_data = scrape_dataset_list(page)
        all_data.extend(page_data)
        time.sleep(DELAY)
    return all_data

def save_to_csv(data):
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["title", "description", "tags", "dataset_url", "source"])
        writer.writeheader()
        writer.writerows(data)

    print(f"\n✅ Saved {len(data)} records to {OUTPUT_CSV}")

# ---------------- MAIN ----------------
if __name__ == "__main__":
    datasets = scrape_data()

    if datasets:
        save_to_csv(datasets)
        print("\n📌 Sample Record:")
        print(datasets[0])
    else:
        print("⚠️ No datasets scraped.")
