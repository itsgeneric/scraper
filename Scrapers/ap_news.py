import requests
from bs4 import BeautifulSoup
import csv
import time
from xml.etree import ElementTree as ET
from urllib.parse import urlparse

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
from datetime import datetime


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

def save_csv(records, filename="apnews_articles.csv"):
    keys = ["title", "content", "date", "url", "author", "domain", "categories"]
    with open(filename, "w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=keys)
        writer.writeheader()
        writer.writerows(records)
    print(f"[✓] Saved {len(records)} records to '{filename}'")

def main():
    sitemap = "https://apnews.com/news-sitemap-content.xml"
    urls = fetch_sitemap_urls(sitemap, limit=1000)
    data_records = []

    for u in urls:
        try:
            record = extract_article_data(u)
            data_records.append(record)
        except Exception as e:
            print(f"[!] Failed to scrape {u}: {e}")
        time.sleep(0.5)

    save_csv(data_records)

if __name__ == "__main__":
    main()
