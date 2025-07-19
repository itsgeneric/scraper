import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import deque
from datetime import datetime
import csv
import os
import time
import argparse

# ------------------ Config ------------------ #
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; TechDocsScraper/1.0)"
}
OUTPUT_FILE = "../Datasets/tech_docs.csv"
os.makedirs("scraped_data", exist_ok=True)
FIELDNAMES = ["title", "content", "date", "url", "author", "domain", "categories"]
SESSION = requests.Session()

# ------------------ Utils ------------------ #
def get_date():
    return datetime.utcnow().strftime("%Y-%m-%d")

def clean_text(text):
    return text.strip().replace("\n", " ").replace("\r", "")[:5000] if text else "N/A"

def extract_domain(url):
    return urlparse(url).netloc

# ------------------ Scraper Core ------------------ #
def scrape_site(start_urls, base_url, source_name, path_func, max_pages, max_depth=2):
    print(f"🔍 Scraping: {source_name}")
    content = []
    visited = set()
    queue = deque([(url, 0) for url in start_urls])
    count = 0

    while queue and count < max_pages:
        url, depth = queue.popleft()
        if url in visited or depth > max_depth:
            continue
        visited.add(url)

        try:
            response = SESSION.get(url, headers=HEADERS, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Title
            title_tag = soup.find("h1")
            title = clean_text(title_tag.get_text()) if title_tag else "Untitled"

            # Content
            main = soup.find("main") or soup.select_one("div.body") or soup.select_one("div.content")
            if not main:
                continue

            for bad in main.select("nav, aside, footer, script, style"):
                bad.decompose()

            body = clean_text(main.get_text(separator="\n", strip=True))
            if len(body) < 50:
                continue

            record = {
                "title": title,
                "content": body,
                "date": get_date(),
                "url": url,
                "author": source_name + " Docs Team",
                "domain": extract_domain(url),
                "categories": source_name.lower()
            }

            content.append(record)
            count += 1
            print(f"[+] ({count}) {title[:60]}...")

            if count >= max_pages:
                break

            # Enqueue sub-URLs
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if path_func(href):
                    full_url = urljoin(base_url, href)
                    if full_url not in visited:
                        queue.append((full_url, depth + 1))

            time.sleep(0.5)

        except Exception as e:
            print(f"[❌] Failed to scrape {url}: {str(e)}")
            continue

    return content

# ------------------ Site Definitions ------------------ #
def scrape_mdn(max_pages=400):
    base = "https://developer.mozilla.org"
    start = [f"{base}/en-US/docs/Web", f"{base}/en-US/docs/Learn"]
    return scrape_site(start, base, "MDN", lambda h: h.startswith("/en-US/docs/"), max_pages)

def scrape_python_docs(max_pages=400):
    base = "https://docs.python.org"
    start = [f"{base}/3/tutorial/", f"{base}/3/library/", f"{base}/3/howto/"]
    return scrape_site(start, base, "Python", lambda h: h.startswith("/3/"), max_pages)

def scrape_kubernetes_docs(max_pages=300):
    base = "https://kubernetes.io"
    start = [f"{base}/docs/home/"]
    return scrape_site(start, base, "Kubernetes", lambda h: h.startswith("/docs/"), max_pages)

def scrape_docker_docs(max_pages=300):
    base = "https://docs.docker.com"
    start = [f"{base}/"]
    return scrape_site(start, base, "Docker", lambda h: h.startswith("/"), max_pages)

# ------------------ Saver ------------------ #
def save_to_csv(data):
    unique = []
    seen = set()
    for row in data:
        key = (row["url"].strip(), row["title"])
        if all(row[f] and row[f] != "N/A" for f in FIELDNAMES) and key not in seen:
            unique.append(row)
            seen.add(key)

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(unique)
    print(f"\n✅ Saved {len(unique)} unique technical docs to {OUTPUT_FILE}")

# ------------------ Main ------------------ #
def main():
    parser = argparse.ArgumentParser(description="Scrape technical documentation into structured CSV")
    parser.add_argument("--max_mdn", type=int, default=400)
    parser.add_argument("--max_python", type=int, default=400)
    parser.add_argument("--max_k8s", type=int, default=300)
    parser.add_argument("--max_docker", type=int, default=300)
    args = parser.parse_args()

    print(f"🏁 Starting scrape to collect ~1000–1500 entries...\n")

    data = []
    data += scrape_mdn(args.max_mdn)
    data += scrape_python_docs(args.max_python)
    data += scrape_kubernetes_docs(args.max_k8s)
    data += scrape_docker_docs(args.max_docker)

    print(f"\n📦 Total collected before deduplication: {len(data)}")

    save_to_csv(data)

    if data:
        print("\n📌 Sample:")
        for k in FIELDNAMES:
            print(f"{k}: {data[0][k][:100]}{'...' if len(data[0][k]) > 100 else ''}")
    else:
        print("❌ No data scraped.")

if __name__ == "__main__":
    main()
