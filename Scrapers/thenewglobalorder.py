import requests
from bs4 import BeautifulSoup
import csv
import time
import random
from datetime import datetime
from xml.etree import ElementTree as ET
from urllib.parse import urlparse

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

def fetch_sitemap_urls(sitemap_url, limit=1000):
    print(f"Fetching sitemap: {sitemap_url}")
    res = requests.get(sitemap_url, headers=HEADERS, timeout=20)
    res.raise_for_status()
    root = ET.fromstring(res.text)

    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = []
    for url_el in root.findall("ns:url", ns):
        loc = url_el.find("ns:loc", ns).text
        if "/world-news" in loc:  # Year-based filter
            urls.append(loc)
        if len(urls) >= limit:
            break

    print(f"[+] Collected {len(urls)} article URLs")
    return urls

def extract_article_data(url):
    print(f"Scraping: {url}")
    res = requests.get(url, headers=HEADERS, timeout=15)
    soup = BeautifulSoup(res.text, "html.parser")

    # Title
    title_tag = soup.select_one("h1.entry-title")
    title = title_tag.get_text(strip=True) if title_tag else ""

    # Author
    author_tag = soup.select_one("span.author.vcard a")
    author = author_tag.get_text(strip=True) if author_tag else None

    # Date
    date_tag = soup.select_one("time.published")
    if date_tag and date_tag.has_attr("datetime"):
        try:
            dt = datetime.fromisoformat(date_tag["datetime"].replace("Z", "+00:00"))
            date = dt.strftime("%B %d, %Y")
        except ValueError:
            date = date_tag.get_text(strip=True)
    else:
        date = None

    # Categories
    category_tag = soup.select_one("span.bl_categ a")
    category = category_tag.get_text(strip=True) if category_tag else ""

    # Content
    content_div = soup.select_one("div.entry-content")
    content_paragraphs = []
    if content_div:
        for p in content_div.find_all("p"):
            text = p.get_text(strip=True)
            if text:
                content_paragraphs.append(text)
    content = "\n".join(content_paragraphs)

    # Domain
    domain = urlparse(url).netloc

    return {
        "title": title,
        "content": content,
        "date": date,
        "url": url,
        "author": author,
        "domain": domain,
        "categories": category
    }

def save_csv(records, filename="tngo_articles.csv"):
    keys = ["title", "content", "date", "url", "author", "domain", "categories"]
    with open(filename, "w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=keys)
        writer.writeheader()
        writer.writerows(records)
    print(f"[✓] Saved {len(records)} records to '{filename}'")

def main():
    sitemap_url = "https://thenewglobalorder.com/sitemap-1.xml"
    limit = 500  # Set to 1000 if needed

    urls = fetch_sitemap_urls(sitemap_url, limit=limit)
    print(f"\nFound {len(urls)} article URLs. Sample:")
    for u in urls[:5]:
        print(f"  → {u}")

    confirmation = input("\nType 'yes' to proceed with scraping (or anything else to abort): ").strip().lower()
    if confirmation != "yes":
        print("❌ Aborted by user")
        return

    print("\n⏳ Starting article scraping...")
    data_records = []
    for i, url in enumerate(urls, 1):
        try:
            print(f"Processing {i}/{len(urls)}: {url[:80]}...")
            record = extract_article_data(url)
            if record and record.get("content"):
                data_records.append(record)
        except Exception as e:
            print(f"⚠️ Error on {url}: {e}")

        time.sleep(0.75 + random.random() * 0.5)

    save_csv(data_records)
    print("✅ Done!")

if __name__ == "__main__":
    main()
