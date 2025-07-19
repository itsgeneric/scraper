import requests
from bs4 import BeautifulSoup
import csv
import time
from urllib.parse import urljoin, urlparse
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed

# ------------ CONFIGURATION ------------ #
BASE_URL = "https://en.wikipedia.org"
START_CATEGORY = urljoin(BASE_URL, "/wiki/Category:Computer_science")
OUTPUT_FILE = "../Datasets/wikipedia_articles_2.csv"
FIELDS = ["title", "content", "date", "url", "author", "domain", "categories"]
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SuperScraper/5.0)"}

MAX_ARTICLES = 10000
MAX_SUBCATEGORIES = 10000
MAX_THREADS = 120              # 💥 High parallelism
TIMEOUT = 10
DELAY = 0.05                   # tiny polite delay

# ------------ SCRAPING FUNCTIONS ------------ #
def get_all_article_links(start_url):
    print(f"🔍 Scanning for up to {MAX_ARTICLES} Wiki article URLs...")

    seen_articles = set()
    seen_subcats = set()
    queue = deque([start_url])

    while queue and len(seen_articles) < MAX_ARTICLES and len(seen_subcats) < MAX_SUBCATEGORIES:
        url = queue.popleft()
        if url in seen_subcats:
            continue
        seen_subcats.add(url)

        try:
            response = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            soup = BeautifulSoup(response.text, "html.parser")

            # Extract articles
            for link in soup.select("#mw-pages a[href^='/wiki/']"):
                href = link['href']
                if (not any(href.startswith(f"/wiki/{p}") for p in ["Category:", "File:", "Template:", "Special:", "Help:", "Wikipedia:"])
                        and len(seen_articles) < MAX_ARTICLES):
                    seen_articles.add(urljoin(BASE_URL, href))

            # Discover new subcategories
            for sc_link in soup.select("#mw-subcategories a[href^='/wiki/Category:']"):
                subcat_url = urljoin(BASE_URL, sc_link["href"])
                if subcat_url not in seen_subcats:
                    queue.append(subcat_url)

            # Handle pagination
            next_page = soup.find("a", string=lambda t: t and "next page" in t.lower())
            if next_page and next_page.get("href"):
                queue.append(urljoin(BASE_URL, next_page["href"]))

        except Exception as e:
            print(f"[!] Error scanning {url}: {e}")
            continue

        print(f"  🌐 Articles: {len(seen_articles)} | Subcats queued: {len(queue)}")

    return list(seen_articles)


def extract_article(url):
    try:
        res = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")

        title = soup.find("h1", id="firstHeading").text.strip()
        content_div = soup.find("div", class_="mw-parser-output")
        if not content_div:
            return None

        for tag in content_div.select(".reflist, table, script, .navbox, .toc, style, .infobox, .mw-editsection"):
            tag.decompose()
        content = content_div.get_text(separator="\n", strip=True)

        # Metadata
        mod = soup.find("li", id="footer-info-lastmod")
        date = mod.text.replace("This page was last edited on ", "").split(",")[0] if mod else "N/A"

        cat_div = soup.find("div", id="catlinks")
        cats = [a.text.strip() for a in cat_div.select("a[href^='/wiki/Category:']")] if cat_div else []

        return {
            "title": title,
            "content": content,
            "date": date,
            "url": url,
            "author": "Wikipedia Contributors",
            "domain": urlparse(url).netloc,
            "categories": ", ".join(cats)
        }

    except Exception as e:
        print(f"[!] {url} failed: {e}")
        return None

def save_csv(records):
    with open(OUTPUT_FILE, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(records)
    print(f"\n📁 Saved {len(records)} records to {OUTPUT_FILE}")

# ------------ MAIN SCRIPT ------------ #
def main():
    t0 = time.time()
    print("== 🧠 High-Speed Wikipedia Scraper (10k+) ==\n")
    urls = get_all_article_links(START_CATEGORY)

    seen_urls = set()
    entries = []

    print(f"\n🚀 Extracting {len(urls)} articles in parallel using {MAX_THREADS} threads...\n")

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(extract_article, url): url for url in urls}
        for idx, future in enumerate(as_completed(futures), 1):
            result = future.result()
            if result and result["url"] not in seen_urls:
                entries.append(result)
                seen_urls.add(result["url"])
            if idx % 100 == 0 or len(entries) >= MAX_ARTICLES:
                print(f"  ✅ Processed: {idx} | Valid: {len(entries)}")
            if len(entries) >= MAX_ARTICLES:
                break

    save_csv(entries)
    print(f"\n⏱ Finished in {round(time.time() - t0, 2)} sec")

if __name__ == "__main__":
    main()
