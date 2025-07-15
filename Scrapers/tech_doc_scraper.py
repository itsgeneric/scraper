import requests
from bs4 import BeautifulSoup
import csv
import time
from urllib.parse import urljoin, urlparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from playwright.sync_api import sync_playwright
import wikipediaapi
from datetime import datetime
from collections import deque

# Base URLs and headers
BASE_URLS = {
    "mdn": "https://developer.mozilla.org/en-US/docs/Web",
    "tensorflow": "https://www.tensorflow.org/api_docs/python/",
    "kubernetes": "https://kubernetes.io/docs/",
    "github": "https://docs.github.com/en",
    "docker": "https://docs.docker.com/"
}
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.google.com/"
}

# Set up session with retries
session = requests.Session()
retries = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

# Wikipedia API setup
wiki_api = wikipediaapi.Wikipedia(
    language="en",
    user_agent="KnowledgeBaseScraper/1.0 (https://example.com)"
)

def get_wikipedia_articles(max_articles=10000, max_depth=3):
    """Recursively collect articles from Wikipedia's Computer science category and subcategories."""
    print("[*] Collecting Wikipedia articles...")
    article_data = []
    visited_categories = set()
    queue = deque([("Category:Computer_science", 0)])
    count = 0

    while queue and count < max_articles:
        category_title, depth = queue.popleft()
        if category_title in visited_categories or depth > max_depth:
            continue
        visited_categories.add(category_title)
        category_page = wiki_api.page(category_title)
        if not category_page.exists():
            continue

        for member_title, member_page in category_page.categorymembers.items():
            if count >= max_articles:
                break
            if member_page.ns == 0:  # Article
                try:
                    content = member_page.text[:10000]  # Limit content size
                    date = datetime.now().strftime("%Y-%m-%d")  # Fallback date
                    article_data.append({
                        "title": member_title,
                        "content": content,
                        "date": date,
                        "url": member_page.fullurl,
                        "domain": "en.wikipedia.org",
                        "source": "Wikipedia"
                    })
                    count += 1
                    print(f"[+] Collected Wikipedia article: {member_title}")
                except Exception as e:
                    print(f"[!] Error collecting Wikipedia article {member_title}: {e}")
            elif member_page.ns == 14 and depth < max_depth:  # Subcategory
                queue.append((member_title, depth + 1))
        time.sleep(0.5)  # Respect rate limits

    print(f"[+] Total Wikipedia articles collected: {len(article_data)}")
    return article_data

def get_tech_doc_links(base_url, source, max_articles=100):
    """Collect documentation page URLs from technical documentation sites using Playwright."""
    print(f"[*] Collecting {source} URLs...")
    article_urls = set()
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(base_url)
            page.wait_for_timeout(5000)
            content = page.content()
            browser.close()

        soup = BeautifulSoup(content, 'html.parser')
        nav = soup.find("nav") or soup.find("aside") or soup.find("ul")
        if nav:
            links = nav.find_all("a", href=True)
            for link in links[:max_articles]:
                href = link.get("href")
                if href and not href.startswith("http"):
                    full_url = urljoin(base_url, href)
                    article_urls.add(full_url)
                elif href and href.startswith(base_url):
                    article_urls.add(href)

        print(f"[+] Found {len(article_urls)} {source} pages.")
        return list(article_urls)
    except Exception as e:
        print(f"[!] Error fetching {source} articles: {e}")
        return []

def extract_tech_doc_data(url, source):
    """Extract title, content, and date from a technical documentation page using Playwright."""
    print(f"[+] Extracting {source} page: {url}")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url)
            page.wait_for_timeout(5000)
            content = page.content()
            browser.close()

        soup = BeautifulSoup(content, 'html.parser')
        title = soup.find("h1").get_text(strip=True) if soup.find("h1") else "N/A"
        date = "N/A"
        content_div = soup.find("main") or soup.find("div", class_=lambda x: x and "content" in x)
        if content_div:
            for unwanted in content_div.find_all(["nav", "aside", "footer"]):
                unwanted.decompose()
            content_text = content_div.get_text(separator="\n", strip=True)
            content_text = "\n".join(line for line in content_text.split("\n") if line.strip())
        else:
            content_text = "N/A"

        domain = urlparse(url).netloc
        return {
            "title": title,
            "content": content_text,
            "date": date,
            "url": url,
            "domain": domain,
            "source": source.capitalize()
        }
    except Exception as e:
        print(f"[!] Error extracting {source} page {url}: {e}")
        return None

def save_to_csv(data, filename="tech_docs.csv"):
    """Save the scraped data to a CSV file."""
    keys = ["title", "content", "date", "url", "domain", "source"]
    with open(filename, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for row in data:
            if row:
                writer.writerow(row)
    print(f"[+] Data saved to {filename}")

def main():
    """Main function to orchestrate the scraping and saving process."""
    # Step 1: Collect Wikipedia articles
    wiki_data = get_wikipedia_articles(max_articles=10000, max_depth=3)

    # Step 2: Collect and extract data from technical documentation sites
    tech_sources = ["mdn", "tensorflow", "kubernetes", "github", "docker"]
    tech_data = []
    for source in tech_sources:
        base_url = BASE_URLS[source]
        article_urls = get_tech_doc_links(base_url, source, max_articles=100)
        for url in article_urls:
            data = extract_tech_doc_data(url, source)
            if data:
                tech_data.append(data)
            time.sleep(1.5)  # Polite delay

    # Step 3: Combine and save data
    all_data = wiki_data + tech_data
    if all_data:
        save_to_csv(all_data)
    else:
        print("[!] No data collected from any source.")

# Entry point
if __name__ == "__main__":
    main()