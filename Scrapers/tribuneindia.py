import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import json
import csv
import time

HEADERS = {"User-Agent": "Mozilla/5.0"}
SITEMAP_INDEX = "https://www.tribuneindia.com/sitemap.xml"

def get_sitemap_urls(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
    except Exception as e:
        print(f"❌ Failed to fetch {url}: {e}")
        return []
    root = ET.fromstring(r.content)
    return [loc.text for loc in root.findall(".//{*}loc") if loc.text]

def extract_article_data(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, "html.parser")

        json_ld = soup.find("script", type="application/ld+json")
        if not json_ld:
            print(f"⚠️ No JSON-LD found for {url}")
            return None

        data = json.loads(json_ld.string)

        title = data.get("headline", "")
        author = data.get("author", {}).get("name", "Unknown")
        date = data.get("datePublished", "")
        content = data.get("articleBody", "")
        domain = urlparse(url).netloc
        category = url.split("/")[3] if len(url.split("/")) > 3 else "Uncategorized"

        return {
            "title": title,
            "content": content,
            "date": date,
            "author": author,
            "url": url,
            "domain": domain,
            "categories": category,
        }

    except Exception as e:
        print(f"❌ Failed to scrape {url}: {e}")
        return None

def main():
    print("🔍 Fetching sitemap index...")
    sitemaps = get_sitemap_urls(SITEMAP_INDEX)

    print(f"📄 Found {len(sitemaps)} sitemap files. Downloading them...")
    urls = []
    for sm in sitemaps:
        urls += get_sitemap_urls(sm)

    print(f"🔎 Filtering URLs for '/news'...")
    news_urls = [u for u in urls if "/news" in u]
    print(f"\n✅ Found {len(news_urls)} '/news' URLs.")

    print(f"\n💾 Saving scraped articles to 'tribunal_docs.csv'...\n")
    with open("tribunal_docs.csv", mode="w", newline='', encoding="utf-8") as csvfile:
        fieldnames = ["title", "content", "date", "author", "url", "domain", "categories"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for i, url in enumerate(news_urls, 1):
            data = extract_article_data(url)
            if data:
                print(f"✅ Article {i}: {data['title'][:80]}...")
                writer.writerow(data)

            # optional polite delay
            time.sleep(0.3)

    print(f"\n✅ Finished scraping {i} articles into 'tribunal_docs.csv'.")

if __name__ == "__main__":
    main()
