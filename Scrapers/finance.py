import requests
import csv
import time
import argparse
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from playwright.sync_api import sync_playwright

# Set up session with retries
session = requests.Session()
retries = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

# Headers for Investopedia scraping
HEADERS = {
    "User-Agent": "FinanceScraper/1.0 (https://example.com; contact@example.com)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5"
}

def estimate_total_rows(max_investopedia, max_worldbank, max_imf):
    """Estimate the total number of data rows to be generated."""
    total_rows = max_investopedia + max_worldbank + max_imf
    return total_rows

def fetch_investopedia_articles(query, max_articles=10000):
    """Scrape financial articles and definitions from Investopedia using Playwright."""
    print("[*] Collecting Investopedia articles...")
    articles = []
    visited_urls = set()
    base_url = "https://www.investopedia.com"
    start_urls = [
        "https://www.investopedia.com/financial-term-dictionary-4769738",
        f"{base_url}/search?q={query.replace(' ', '+')}"
    ]
    count = 0

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            # Collect article URLs from start pages
            article_urls = set()
            for start_url in start_urls:
                print(f"[*] Fetching Investopedia page: {start_url}")
                page.goto(start_url)
                page.wait_for_timeout(5000)
                content = page.content()
                soup = BeautifulSoup(content, 'html.parser')
                links = soup.find_all("a", href=True)
                print(f"[*] Found {len(links)} links on {start_url}")
                for link in links:
                    href = link['href']
                    if href and ("/terms/" in href or "/articles/" in href or "/investing/" in href):
                        full_url = urljoin(base_url, href)
                        if full_url not in visited_urls and count < max_articles:
                            article_urls.add(full_url)

            # Scrape each article
            for url in article_urls:
                if count >= max_articles:
                    break
                try:
                    print(f"[*] Scraping Investopedia article: {url}")
                    page.goto(url)
                    page.wait_for_timeout(5000)
                    content = page.content()
                    soup = BeautifulSoup(content, 'html.parser')

                    # Extract title
                    title_tag = soup.find("h1")
                    title = title_tag.get_text(strip=True) if title_tag else "N/A"

                    # Extract content
                    content_div = soup.find("div", class_=lambda x: x and "article-content" in x) or soup.find("main")
                    if content_div:
                        for unwanted in content_div.find_all(["nav", "aside", "footer", "script"]):
                            unwanted.decompose()
                        content = content_div.get_text(separator="\n", strip=True)[:10000]
                    else:
                        content = "N/A"

                    if content != "N/A" and title != "N/A":
                        articles.append({
                            'title': title,
                            'content': content,
                            'url': url,
                            'source': 'Investopedia'
                        })
                        count += 1
                        print(f"[+] Collected Investopedia article: {title}")
                    visited_urls.add(url)
                    time.sleep(0.5)  # Rate limiting
                except Exception as e:
                    print(f"[!] Error scraping Investopedia article {url}: {e}")
                    continue

            browser.close()
        print(f"[+] Total Investopedia articles collected: {len(articles)}")
        return articles
    except Exception as e:
        print(f"[!] Error fetching Investopedia pages: {e}")
        return articles

def fetch_worldbank_datasets(query, max_datasets=1000):
    """Fetch datasets from World Bank Open Data API."""
    print("[*] Collecting World Bank datasets...")
    datasets = []
    rows = 50  # Results per request
    page = 1
    base_url = "https://api.worldbank.org/v2/indicator"

    while len(datasets) < max_datasets:
        params = {
            "format": "json",
            "per_page": rows,
            "page": page
        }
        try:
            response = session.get(base_url, params=params, timeout=20)
            response.raise_for_status()
            data = response.json()
            if not data or len(data) < 2:
                print("[!] World Bank API request failed.")
                break
            results = data[1]  # Data[1] contains the actual results
            for item in results:
                if len(datasets) >= max_datasets:
                    break
                if query.lower() in item.get('name', '').lower() or query.lower() in item.get('sourceNote', '').lower():
                    datasets.append({
                        'title': item.get('name', 'N/A'),
                        'content': item.get('sourceNote', 'N/A'),
                        'url': f"https://data.worldbank.org/indicator/{item.get('id', 'N/A')}",
                        'source': 'World Bank'
                    })
                    print(f"[+] Collected World Bank dataset: {item.get('name', 'N/A')}")
            if len(results) < rows:
                break
            page += 1
            time.sleep(1)  # Rate limiting
        except Exception as e:
            print(f"[!] Error fetching World Bank datasets: {e}")
            break
    print(f"[+] Total World Bank datasets collected: {len(datasets)}")
    return datasets

def fetch_imf_datasets(query, max_datasets=1000):
    """Fetch reports from IMF publication search API."""
    print("[*] Collecting IMF datasets...")
    datasets = []
    rows = 10
    page = 1
    base_url = "https://www.imf.org/en/Search"

    while len(datasets) < max_datasets:
        params = {
            "query": query,
            "max": rows,
            "page": page,
            "type": "Publication"
        }
        try:
            response = session.get(base_url, params=params, timeout=20)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            results = soup.find_all("div", class_=lambda x: x and "search-result" in x)
            if not results:
                print("[!] No more IMF results found.")
                break
            for item in results:
                if len(datasets) >= max_datasets:
                    break
                title_tag = item.find("h2")
                title = title_tag.get_text(strip=True) if title_tag else "N/A"
                url_tag = item.find("a", href=True)
                url = urljoin("https://www.imf.org", url_tag['href']) if url_tag else "N/A"
                content_tag = item.find("p", class_=lambda x: x and "description" in x)
                content = content_tag.get_text(strip=True)[:10000] if content_tag else "N/A"
                if title != "N/A" and content != "N/A":
                    datasets.append({
                        'title': title,
                        'content': content,
                        'url': url,
                        'source': 'IMF'
                    })
                    print(f"[+] Collected IMF dataset: {title}")
            if len(results) < rows:
                break
            page += 1
            time.sleep(1)  # Rate limiting
        except Exception as e:
            print(f"[!] Error fetching IMF datasets: {e}")
            break
    print(f"[+] Total IMF datasets collected: {len(datasets)}")
    return datasets

def save_to_csv(data, filename="finance.csv"):
    """Save the scraped data to a CSV file."""
    keys = ['source', 'title', 'content', 'url']
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for row in data:
            writer.writerow(row)
    print(f"[+] Data saved to {filename}")

def main():
    """Main function to orchestrate the scraping and saving process."""
    # Set up command-line argument parser
    parser = argparse.ArgumentParser(description="Web scraper for financial data from Investopedia, World Bank, and IMF.")
    parser.add_argument("--investopedia_query", default="financial analysis OR investment OR portfolio",
                        help="Query for Investopedia search")
    parser.add_argument("--worldbank_query", default="finance OR economics",
                        help="Query for World Bank datasets")
    parser.add_argument("--imf_query", default="finance OR economics",
                        help="Query for IMF reports")
    parser.add_argument("--max_articles", type=int, default=10000,
                        help="Maximum number of Investopedia articles to fetch")
    parser.add_argument("--max_datasets", type=int, default=1000,
                        help="Maximum number of datasets per portal")
    args = parser.parse_args()

    # Estimate and display the target number of data rows
    estimated_rows = estimate_total_rows(args.max_articles, args.max_datasets, args.max_datasets)
    print(f"[*] Estimated total data rows to be generated: {estimated_rows}")

    # Fetch data from each source
    investopedia_articles = fetch_investopedia_articles(args.investopedia_query, max_articles=args.max_articles)
    worldbank_datasets = fetch_worldbank_datasets(args.worldbank_query, max_datasets=args.max_datasets)
    imf_datasets = fetch_imf_datasets(args.imf_query, max_datasets=args.max_datasets)

    # Combine all data
    all_data = []
    for article in investopedia_articles:
        all_data.append({
            'source': article['source'],
            'title': article['title'],
            'content': article['content'],
            'url': article['url']
        })
    for dataset in worldbank_datasets + imf_datasets:
        all_data.append({
            'source': dataset['source'],
            'title': dataset['title'],
            'content': dataset['content'],
            'url': dataset['url']
        })

    # Save to CSV
    if all_data:
        save_to_csv(all_data)
    else:
        print("[!] No data collected from any source.")
    print(f"[+] Total data rows actually collected: {len(all_data)}")

# Entry point
if __name__ == "__main__":
    main()