import random
import requests
from bs4 import BeautifulSoup
import csv
import time
from xml.etree import ElementTree as ET
from urllib.parse import urlparse, urljoin

HEADERS = {"User-Agent": "Mozilla/5.0"}

def fetch_sitemap_urls(sitemap_url, limit=1000):
    print(f"Fetching sitemap: {sitemap_url}")
    res = requests.get(sitemap_url, headers=HEADERS, timeout=20)
    res.raise_for_status()
    root = ET.fromstring(res.text)

    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = []
    for url_el in root.findall("ns:url", ns):
        loc = url_el.find("ns:loc", ns).text
        # Only select article pages, exclude live/newsletter pages
        if "/article/" in loc and "/live/" not in loc:
            urls.append(loc)
        if len(urls) >= limit:
            break

    print(f"[+] Collected {len(urls)} article URLs")
    return urls


import json
from datetime import datetime, timedelta


def extract_article_data(url):
    print(f"Scraping: {url}")
    res = requests.get(url, headers = HEADERS, timeout = 15)
    soup = BeautifulSoup(res.text, "html.parser")

    # Title
    title = soup.select_one(".Page-headline")
    title = title.get_text(strip = True) if title else ""

    # Content - main article text is in the RichTextStoryBody div
    content_block = soup.select_one(".RichTextStoryBody")
    if content_block:
        # Get all paragraphs, excluding any ads or non-content elements
        content_paragraphs = []
        for p in content_block.find_all("p"):
            # Skip empty paragraphs and ad containers
            if p.get_text(strip = True) and not p.find_parent(class_ = ["Advertisement", "FreeStar"]):
                content_paragraphs.append(p.get_text(strip = True))
        content = "\n".join(content_paragraphs)
    else:
        content = None

    # Extract date from JSON-LD script tag
    date = None
    script_tag = soup.find('script', type = 'application/ld+json')
    if script_tag:
        try:
            json_data = json.loads(script_tag.string)
            if isinstance(json_data, list):
                json_data = json_data[0]  # Take first item if it's a list

            date_published = json_data.get('datePublished')
            if date_published:
                # Convert ISO format date to more readable format
                dt = datetime.strptime(date_published, "%Y-%m-%dT%H:%M:%SZ")
                date = dt.strftime("%B %d, %Y")  # e.g. "July 13, 2025"
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            print(f"Error parsing date from JSON-LD: {e}")

    # Author - in the Page-authors section
    author_element = soup.select_one(".Page-authors a")
    if not author_element:
        author_element = soup.select_one(".Page-authors")
    author = author_element.get_text(strip = True) if author_element else None

    # Domain
    domain = urlparse(url).netloc

    # Categories - from breadcrumb
    cats = [a.get_text(strip = True) for a in soup.select(".Page-breadcrumbs a") if a.get("href")]
    category = ", ".join(cats)

    return {
        "title": title,
        "content": content,
        "date": date,
        "url": url,
        "author": author,
        "domain": domain,
        "categories": category
    }

def save_csv(records, filename="ap_news_articles.csv"):
    keys = ["title", "content", "date", "url", "author", "domain", "categories"]
    with open(filename, "w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=keys)
        writer.writeheader()
        writer.writerows(records)
    print(f"[✓] Saved {len(records)} records to '{filename}'")


def fetch_archive_articles(limit=1000):
    """Scrape articles from AP News archive pages"""
    base_url = "https://apnews.com"
    article_urls = set()

    # 1. Archive Crawling (Month-by-Month)
    current_date = datetime.now()
    months_to_check = 12  # Go back 12 months

    for i in range(months_to_check):
        if len(article_urls) >= limit:
            break

        date = current_date -   timedelta(days = 30 * i)
        archive_url = f"{base_url}/hub/archives?month={date.month}&year={date.year}"

        try:
            print(f"Checking archive: {date.strftime('%B %Y')}")
            res = requests.get(archive_url, headers = HEADERS, timeout = 15)
            soup = BeautifulSoup(res.text, "html.parser")

            for link in soup.select('a[href*="/article/"]:not([href*="/live/"])'):
                full_url = urljoin(base_url, link['href'])
                article_urls.add(full_url)
                if len(article_urls) >= limit:
                    break

            time.sleep(1 + random.random())
        except Exception as e:
            print(f"Archive failed for {date.strftime('%B %Y')}: {e}")

    # 2. Aggressive Topic Hub Crawling
    if len(article_urls) < limit:
        hubs = [
            "politics", "business", "technology", "science",
            "entertainment", "sports", "health", "europe",
            "asia-pacific", "latin-america", "africa"
        ]

        for hub in hubs:
            if len(article_urls) >= limit:
                break

            hub_url = f"{base_url}/hub/{hub}"
            try:
                print(f"Scraping hub: {hub}")
                res = requests.get(hub_url, headers = HEADERS, timeout = 15)
                soup = BeautifulSoup(res.text, "html.parser")

                # Get all pagination pages (up to 5 pages per hub)
                for page in range(1, 6):
                    if len(article_urls) >= limit:
                        break

                    page_url = f"{hub_url}?page={page}"
                    res = requests.get(page_url, headers = HEADERS, timeout = 15)
                    soup = BeautifulSoup(res.text, "html.parser")

                    for link in soup.select('a[href*="/article/"]:not([href*="/live/"])'):
                        full_url = urljoin(base_url, link['href'])
                        article_urls.add(full_url)
                        if len(article_urls) >= limit:
                            break

                    time.sleep(1 + random.random())
            except Exception as e:
                print(f"Hub {hub} failed: {e}")

    # 3. Related Articles Extraction
    if len(article_urls) < limit:
        # Make a copy to avoid modifying set during iteration
        existing_urls = list(article_urls)[:100]  # Check first 100 articles
        for url in existing_urls:
            if len(article_urls) >= limit:
                break

            try:
                print(f"Checking related articles for: {url[:60]}...")
                res = requests.get(url, headers = HEADERS, timeout = 15)
                soup = BeautifulSoup(res.text, "html.parser")

                for link in soup.select('a[href*="/article/"]:not([href*="/live/"])'):
                    full_url = urljoin(base_url, link['href'])
                    article_urls.add(full_url)
                    if len(article_urls) >= limit:
                        break

                time.sleep(1 + random.random())
            except Exception as e:
                print(f"Failed to get related articles for {url}: {e}")

    return list(article_urls)[:limit]
# (Keep your existing fetch_sitemap_urls, extract_article_data, save_csv functions)

def main():
    print("🚀 Starting URL discovery...")
    urls = fetch_archive_articles(limit = 1000)
    print(f"\nFound {len(urls)} article URLs. Sample:")
    for url in urls[:5]:
        print(f"  → {url}")

    # SANITY CHECK
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
            if record and record.get('content'):
                data_records.append(record)
        except Exception as e:
            print(f"⚠️ Failed on {url}: {e}")

        # Dynamic delay
        delay = 0.5 + (0.5 if i % 100 == 0 else 0)
        time.sleep(delay + random.random() * 0.5)

    save_csv(data_records)
    print("✅ All done!")


if __name__ == "__main__":
    main()