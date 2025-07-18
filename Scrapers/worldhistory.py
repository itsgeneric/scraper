import requests
import xml.etree.ElementTree as ET
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import csv
import time

HEADERS = {"User-Agent": "Mozilla/5.0"}
SITEMAP_URL = "https://www.worldhistory.org/sitemap.xml"
CSV_FILE = "worldhistory.csv"

def get_sitemap_entries(sitemap_url):
    try:
        r = requests.get(sitemap_url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        return [loc.text for loc in root.findall(".//{*}loc") if loc.text]
    except Exception as e:
        print(f"❌ Failed to fetch {sitemap_url}: {e}")
        return []

def crawl_sitemaps(sitemap_url):
    entries = get_sitemap_entries(sitemap_url)
    urls = []
    if not entries:
        return urls

    if all(e.endswith(".xml") for e in entries):
        for child_sitemap in entries:
            urls += crawl_sitemaps(child_sitemap)
    else:
        urls += [url for url in entries if "/article/" in url]

    return urls

def extract_article_data(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, "html.parser")

        # Title
        title_tag = soup.select_one('div#title_bar h1#page_title_text')
        title = title_tag.get_text(strip=True) if title_tag else ""

        # Content
        content_container = soup.select_one('div.text.body article')
        content_paragraphs = content_container.find_all("p") if content_container else []
        content = "\n".join(p.get_text(strip=True) for p in content_paragraphs)

        # Author from <meta>
        author_tag = soup.find("meta", attrs={"name": "author"})
        author = author_tag["content"] if author_tag and "content" in author_tag.attrs else ""

        # Date from <time>
        date = ""
        date_tag = soup.find("time")
        if date_tag:
            date = date_tag.get_text(strip=True)

        return {
            "title": title,
            "content": content,
            "date": date,
            "author": author,
            "url": url,
            "domain": urlparse(url).netloc,
            "categories": "History"
        }
    except Exception as e:
        print(f"⚠️ Error extracting {url}: {e}")
        return None

def save_to_csv(data, filename):
    keys = ["title", "content", "date", "author", "url", "domain", "categories"]
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for row in data:
            if row:
                writer.writerow(row)

def main():
    print(f"🌐 Crawling sitemaps from: {SITEMAP_URL}")
    all_article_urls = crawl_sitemaps(SITEMAP_URL)
    print(f"✅ Found {len(all_article_urls)} article URLs")

    scraped_data = []
    for i, url in enumerate(all_article_urls, start=1):
        print(f"🔍 [{i}/{len(all_article_urls)}] Scraping: {url}")
        data = extract_article_data(url)
        if data:
            scraped_data.append(data)
        time.sleep(1)  # be polite to the server

    print(f"\n💾 Saving {len(scraped_data)} articles to {CSV_FILE}")
    save_to_csv(scraped_data, CSV_FILE)
    print("✅ Done!")

if __name__ == "__main__":
    main()
