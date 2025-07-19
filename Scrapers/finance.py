import requests
import csv
import time
import argparse
import os
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from playwright.sync_api import sync_playwright
from datetime import datetime

# Set up session with retries
session = requests.Session()
retries = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

# Headers for scraping
HEADERS = {
    "User-Agent": "FinanceScraper/1.0 (https://example.com; contact@example.com)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5"
}


def extract_domain(url):
    """Extract domain from URL."""
    try:
        domain = urlparse(url).netloc
        return domain if domain else "N/A"
    except:
        return "N/A"


def get_current_date():
    """Get current date in YYYY-MM-DD format."""
    return datetime.now().strftime("%Y-%m-%d")


def clean_text(text):
    """Clean and truncate text content."""
    if not text or text.strip() == "":
        return "N/A"
    return text.strip()[:5000]  # Limit content length


def estimate_total_rows(max_investopedia, max_worldbank, max_imf, max_reuters):
    """Estimate the total number of data rows to be generated."""
    total_rows = max_investopedia + max_worldbank + max_imf + max_reuters
    return total_rows


def fetch_investopedia_articles(query, max_articles=400):
    """Scrape financial articles and definitions from Investopedia using Playwright."""
    print("[*] Collecting Investopedia articles...")
    articles = []
    visited_urls = set()
    base_url = "https://www.investopedia.com"

    # Expanded start URLs to get more articles
    start_urls = [
        "https://www.investopedia.com/financial-term-dictionary-4769738",
        "https://www.investopedia.com/investing-4427685",
        "https://www.investopedia.com/personal-finance-4427760",
        "https://www.investopedia.com/markets-4689504",
        "https://www.investopedia.com/economy-4689801",
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
                if len(article_urls) >= max_articles * 2:  # Get more URLs than needed
                    break
                print(f"[*] Fetching Investopedia page: {start_url}")
                try:
                    page.goto(start_url, timeout=30000)
                    page.wait_for_timeout(3000)

                    # Scroll down to load more content
                    for _ in range(3):
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        page.wait_for_timeout(2000)

                    content = page.content()
                    soup = BeautifulSoup(content, 'html.parser')
                    links = soup.find_all("a", href=True)
                    print(f"[*] Found {len(links)} links on {start_url}")

                    for link in links:
                        href = link['href']
                        if href and (
                                "/terms/" in href or "/articles/" in href or "/investing/" in href or "/personal-finance/" in href):
                            full_url = urljoin(base_url, href)
                            if full_url not in visited_urls:
                                article_urls.add(full_url)

                except Exception as e:
                    print(f"[!] Error fetching start page {start_url}: {e}")
                    continue

            print(f"[*] Collected {len(article_urls)} unique article URLs")

            # Scrape each article
            for i, url in enumerate(list(article_urls)[:max_articles]):
                if count >= max_articles:
                    break
                try:
                    print(f"[*] Scraping Investopedia article {count + 1}/{max_articles}: {url}")
                    page.goto(url, timeout=30000)
                    page.wait_for_timeout(2000)
                    content = page.content()
                    soup = BeautifulSoup(content, 'html.parser')

                    # Extract title
                    title_tag = soup.find("h1") or soup.find("title")
                    title = clean_text(title_tag.get_text()) if title_tag else "N/A"

                    # Extract author
                    author_tag = soup.find("span", class_=lambda x: x and "author" in x.lower()) or \
                                 soup.find("div", class_=lambda x: x and "author" in x.lower()) or \
                                 soup.find("a", class_=lambda x: x and "author" in x.lower())
                    author = clean_text(author_tag.get_text()) if author_tag else "Investopedia Editorial Team"

                    # Extract date
                    date_tag = soup.find("time") or \
                               soup.find("span", class_=lambda x: x and "date" in x.lower()) or \
                               soup.find("div", class_=lambda x: x and "date" in x.lower())
                    date = clean_text(date_tag.get_text()) if date_tag else get_current_date()

                    # Extract content
                    content_div = soup.find("div", class_=lambda x: x and "article-content" in x.lower()) or \
                                  soup.find("main") or soup.find("article")
                    if content_div:
                        # Remove unwanted elements
                        for unwanted in content_div.find_all(["nav", "aside", "footer", "script", "style", "ad"]):
                            unwanted.decompose()
                        content = clean_text(content_div.get_text(separator=" "))
                    else:
                        content = "N/A"

                    # Only add if we have valid title and content
                    if title != "N/A" and content != "N/A" and len(content) > 50:
                        articles.append({
                            'title': title,
                            'content': content,
                            'date': date,
                            'url': url,
                            'author': author,
                            'domain': "investopedia.com",
                            'categories': "finance, investment, financial education"
                        })
                        count += 1
                        print(f"[+] Collected Investopedia article: {title}")

                    visited_urls.add(url)
                    time.sleep(0.5)  # Reduced delay for faster scraping

                except Exception as e:
                    print(f"[!] Error scraping Investopedia article {url}: {e}")
                    continue

            browser.close()
        print(f"[+] Total Investopedia articles collected: {len(articles)}")
        return articles
    except Exception as e:
        print(f"[!] Error fetching Investopedia pages: {e}")
        return articles


def fetch_worldbank_datasets(query, max_datasets=400):
    """Fetch datasets from World Bank Open Data API with pagination."""
    print("[*] Collecting World Bank datasets...")
    datasets = []
    rows = 100  # Increased results per request
    page = 1
    base_url = "https://api.worldbank.org/v2/indicator"

    while len(datasets) < max_datasets:
        params = {
            "format": "json",
            "per_page": rows,
            "page": page
        }
        try:
            print(f"[*] Fetching World Bank page {page}")
            response = session.get(base_url, params=params, timeout=20, headers=HEADERS)
            response.raise_for_status()
            data = response.json()

            if not data or len(data) < 2 or not data[1]:
                print("[!] No more World Bank data available.")
                break

            results = data[1]  # Data[1] contains the actual results

            for item in results:
                if len(datasets) >= max_datasets:
                    break

                title = clean_text(item.get('name', ''))
                content = clean_text(item.get('sourceNote', ''))

                if title != "N/A" and content != "N/A" and len(content) > 30:
                    datasets.append({
                        'title': title,
                        'content': content,
                        'date': get_current_date(),
                        'url': f"https://data.worldbank.org/indicator/{item.get('id', 'unknown')}",
                        'author': "World Bank Group",
                        'domain': "data.worldbank.org",
                        'categories': "economics, development, statistics, global data"
                    })
                    print(f"[+] Collected World Bank dataset: {title[:50]}...")

            if len(results) < rows:
                print("[!] Reached end of World Bank results.")
                break

            page += 1
            time.sleep(0.5)  # Reduced delay

        except Exception as e:
            print(f"[!] Error fetching World Bank datasets: {e}")
            break

    print(f"[+] Total World Bank datasets collected: {len(datasets)}")
    return datasets


def fetch_imf_datasets(query, max_datasets=400):
    """Fetch reports from IMF using web scraping with multiple pages."""
    print("[*] Collecting IMF datasets...")
    datasets = []
    base_url = "https://www.imf.org"

    # Multiple entry points for more data
    search_urls = [
        f"{base_url}/en/Publications",
        f"{base_url}/en/Publications/WEO",
        f"{base_url}/en/Publications/GFSR",
        f"{base_url}/en/Publications/REO",
        f"{base_url}/en/Publications/WP"
    ]

    for search_url in search_urls:
        if len(datasets) >= max_datasets:
            break

        try:
            print(f"[*] Fetching IMF publications from: {search_url}")
            response = session.get(search_url, timeout=20, headers=HEADERS)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Find publication links
            publication_links = []
            for link in soup.find_all("a", href=True):
                href = link.get("href", "")
                if ("/en/Publications/" in href or "/external/pubs/" in href) and href not in publication_links:
                    full_url = urljoin(base_url, href)
                    publication_links.append(full_url)

            print(f"[*] Found {len(publication_links)} IMF publication links from {search_url}")

            for i, url in enumerate(publication_links[:100]):  # Limit per source
                if len(datasets) >= max_datasets:
                    break

                try:
                    print(f"[*] Scraping IMF publication {len(datasets) + 1}/{max_datasets}")
                    response = session.get(url, timeout=15, headers=HEADERS)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, 'html.parser')

                    # Extract title
                    title_tag = soup.find("h1") or soup.find("title")
                    title = clean_text(title_tag.get_text()) if title_tag else "N/A"

                    # Extract content/description
                    content_tags = soup.find_all("p")
                    content_parts = []
                    for p in content_tags[:8]:  # Increased to get more content
                        text = clean_text(p.get_text())
                        if text != "N/A" and len(text) > 20:
                            content_parts.append(text)

                    content = " ".join(content_parts) if content_parts else "N/A"

                    if title != "N/A" and content != "N/A" and len(content) > 50:
                        datasets.append({
                            'title': title,
                            'content': content,
                            'date': get_current_date(),
                            'url': url,
                            'author': "International Monetary Fund",
                            'domain': "imf.org",
                            'categories': "economics, monetary policy, global finance, IMF reports"
                        })
                        print(f"[+] Collected IMF publication: {title[:50]}...")

                    time.sleep(0.5)  # Reduced delay

                except Exception as e:
                    print(f"[!] Error scraping IMF publication {url}: {e}")
                    continue

        except Exception as e:
            print(f"[!] Error fetching IMF publications from {search_url}: {e}")
            continue

    print(f"[+] Total IMF datasets collected: {len(datasets)}")
    return datasets


def fetch_reuters_articles(query, max_articles=400):
    """Fetch financial articles from Reuters Business section."""
    print("[*] Collecting Reuters articles...")
    articles = []
    base_url = "https://www.reuters.com"

    # Multiple Reuters sections for more content
    reuters_urls = [
        f"{base_url}/business/finance/",
        f"{base_url}/markets/",
        f"{base_url}/business/",
        f"{base_url}/world/us/"
    ]

    try:
        for reuters_url in reuters_urls:
            if len(articles) >= max_articles:
                break

            print(f"[*] Fetching Reuters articles from: {reuters_url}")
            response = session.get(reuters_url, timeout=20, headers=HEADERS)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Find article links
            article_links = []
            for link in soup.find_all("a", href=True):
                href = link.get("href", "")
                if "/business/" in href or "/markets/" in href or "/world/" in href:
                    if href.startswith("/"):
                        full_url = base_url + href
                    else:
                        full_url = href
                    if full_url not in article_links:
                        article_links.append(full_url)

            print(f"[*] Found {len(article_links)} Reuters article links")

            for url in article_links[:100]:  # Limit per section
                if len(articles) >= max_articles:
                    break

                try:
                    print(f"[*] Scraping Reuters article {len(articles) + 1}/{max_articles}")
                    response = session.get(url, timeout=15, headers=HEADERS)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, 'html.parser')

                    # Extract title
                    title_tag = soup.find("h1") or soup.find("title")
                    title = clean_text(title_tag.get_text()) if title_tag else "N/A"

                    # Extract content
                    content_divs = soup.find_all("p")
                    content_parts = []
                    for p in content_divs[:10]:
                        text = clean_text(p.get_text())
                        if text != "N/A" and len(text) > 20:
                            content_parts.append(text)

                    content = " ".join(content_parts) if content_parts else "N/A"

                    if title != "N/A" and content != "N/A" and len(content) > 50:
                        articles.append({
                            'title': title,
                            'content': content,
                            'date': get_current_date(),
                            'url': url,
                            'author': "Reuters Editorial Team",
                            'domain': "reuters.com",
                            'categories': "news, finance, business, markets"
                        })
                        print(f"[+] Collected Reuters article: {title[:50]}...")

                    time.sleep(0.5)

                except Exception as e:
                    print(f"[!] Error scraping Reuters article {url}: {e}")
                    continue

    except Exception as e:
        print(f"[!] Error fetching Reuters articles: {e}")

    print(f"[+] Total Reuters articles collected: {len(articles)}")
    return articles


def validate_row(row):
    """Validate that row has all required fields and no empty values."""
    required_fields = ["title", "content", "date", "url", "author", "domain", "categories"]

    for field in required_fields:
        value = row.get(field, "")
        if not value or value.strip() == "" or value == "N/A":
            return False

    return True


def save_to_csv(data, filename="finance.csv"):
    """Save the scraped data to a CSV file with complete fields."""
    REQUIRED_FIELDS = ["title", "content", "date", "url", "author", "domain", "categories"]

    # Create output directory if needed
    output_dir = os.path.dirname(filename)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    # Filter out rows with missing data
    valid_data = []
    seen_titles = set()  # For deduplication

    for row in data:
        # Ensure all fields exist and have values
        cleaned_row = {
            "title": clean_text(row.get("title", "")),
            "content": clean_text(row.get("content", "")),
            "date": row.get("date", get_current_date()),
            "url": row.get("url", "N/A"),
            "author": clean_text(row.get("author", "Unknown")),
            "domain": row.get("domain") or extract_domain(row.get("url", "")),
            "categories": row.get("categories", "general")
        }

        # Only include rows where critical fields are not "N/A" and no duplicates
        if (cleaned_row["title"] != "N/A" and
                cleaned_row["content"] != "N/A" and
                cleaned_row["url"] != "N/A" and
                len(cleaned_row["content"]) > 50 and
                cleaned_row["title"] not in seen_titles):
            valid_data.append(cleaned_row)
            seen_titles.add(cleaned_row["title"])

    print(f"[*] Writing {len(valid_data)} valid records to CSV (filtered from {len(data)} total)")

    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=REQUIRED_FIELDS)
        writer.writeheader()
        writer.writerows(valid_data)

    print(f"[+] Data saved to {filename}")
    print(f"[+] Total valid records: {len(valid_data)}")

    # Show sample data
    if valid_data:
        print("\n[+] Sample record:")
        sample = valid_data[0]
        for field in REQUIRED_FIELDS:
            print(f"  {field}: {sample[field][:100]}{'...' if len(sample[field]) > 100 else ''}")


def main():
    """Main function to orchestrate the scraping and saving process."""
    # Set up command-line argument parser
    parser = argparse.ArgumentParser(
        description="Web scraper for financial data from Investopedia, World Bank, IMF, and Reuters.")
    parser.add_argument("--investopedia_query", default="financial analysis OR investment OR portfolio",
                        help="Query for Investopedia search")
    parser.add_argument("--worldbank_query", default="finance OR economics",
                        help="Query for World Bank datasets")
    parser.add_argument("--imf_query", default="finance OR economics",
                        help="Query for IMF reports")
    parser.add_argument("--max_investopedia", type=int, default=400,
                        help="Maximum number of Investopedia articles to fetch")
    parser.add_argument("--max_worldbank", type=int, default=400,
                        help="Maximum number of World Bank datasets to fetch")
    parser.add_argument("--max_imf", type=int, default=400,
                        help="Maximum number of IMF datasets to fetch")
    parser.add_argument("--max_reuters", type=int, default=400,
                        help="Maximum number of Reuters articles to fetch")
    parser.add_argument("--output", default="finance.csv",
                        help="Output CSV file name")
    args = parser.parse_args()

    # Estimate and display the target number of data rows
    estimated_rows = estimate_total_rows(args.max_investopedia, args.max_worldbank, args.max_imf, args.max_reuters)
    print(f"[*] Estimated total data rows to be generated: {estimated_rows}")

    # Fetch data from each source
    print("\n" + "=" * 60)
    investopedia_articles = fetch_investopedia_articles(args.investopedia_query, max_articles=args.max_investopedia)

    print("\n" + "=" * 60)
    worldbank_datasets = fetch_worldbank_datasets(args.worldbank_query, max_datasets=args.max_worldbank)

    print("\n" + "=" * 60)
    imf_datasets = fetch_imf_datasets(args.imf_query, max_datasets=args.max_imf)

    print("\n" + "=" * 60)
    reuters_articles = fetch_reuters_articles("finance", max_articles=args.max_reuters)

    # Combine all data
    all_data = investopedia_articles + worldbank_datasets + imf_datasets + reuters_articles

    # Save to CSV
    print("\n" + "=" * 60)
    if all_data:
        save_to_csv(all_data, filename=args.output)
    else:
        print("[!] No data collected from any source.")

    print(f"[+] Total data rows actually collected: {len(all_data)}")
    print("[+] Scraping completed successfully!")


# Entry point
if __name__ == "__main__":
    main()
