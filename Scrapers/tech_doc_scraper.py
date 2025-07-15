import requests
import csv
import time
import argparse
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from collections import deque

# Set up session with retries
session = requests.Session()
retries = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

# Headers for scraping
HEADERS = {
    "User-Agent": "TechDocsScraper/1.0 (https://example.com; contact@example.com)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5"
}

def estimate_total_rows(max_mdn, max_python, max_kubernetes, max_docker):
    """Estimate the total number of data rows to be generated."""
    total_rows = max_mdn + max_python + max_kubernetes + max_docker
    return total_rows

def fetch_mdn_content(max_pages=8000, max_depth=2):
    """Scrape tutorials, API references, and code snippets from MDN Web Docs."""
    print("[*] Collecting MDN Web Docs content...")
    content = []
    visited_urls = set()
    base_url = "https://developer.mozilla.org"
    start_urls = [
        f"{base_url}/en-US/docs/Web",
        f"{base_url}/en-US/docs/Learn"
    ]
    queue = deque([(url, 0) for url in start_urls])
    count = 0

    while queue and count < max_pages:
        url, depth = queue.popleft()
        if url in visited_urls or depth > max_depth:
            continue
        visited_urls.add(url)
        try:
            response = session.get(url, headers=HEADERS, timeout=20)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Extract title
            title_tag = soup.find("h1")
            title = title_tag.get_text(strip=True) if title_tag else "N/A"

            # Extract content
            content_div = soup.find("main") or soup.find("div", class_=lambda x: x and "content" in x.lower())
            if content_div:
                for unwanted in content_div.find_all(["nav", "aside", "footer", "script"]):
                    unwanted.decompose()
                text = content_div.get_text(separator="\n", strip=True)[:10000]
            else:
                text = "N/A"

            if title != "N/A" and text != "N/A":
                content.append({
                    'title': title,
                    'content': text,
                    'url': url,
                    'source': 'MDN Web Docs'
                })
                count += 1
                print(f"[+] Collected MDN content: {title}")

            # Collect subpage URLs
            links = soup.find_all("a", href=True)
            print(f"[*] Found {len(links)} links on {url}")
            for link in links:
                href = link['href']
                if href.startswith("/en-US/docs/"):
                    sub_url = urljoin(base_url, href)
                    if sub_url not in visited_urls and count < max_pages and depth < max_depth:
                        queue.append((sub_url, depth + 1))
            time.sleep(0.5)  # Rate limiting
        except Exception as e:
            print(f"[!] Error scraping MDN content {url}: {e}")
            continue

    print(f"[+] Total MDN content collected: {len(content)}")
    return content

def fetch_python_docs(max_pages=2000, max_depth=2):
    """Scrape tutorials, references, and code snippets from Python Docs."""
    print("[*] Collecting Python Docs content...")
    content = []
    visited_urls = set()
    base_url = "https://docs.python.org"
    start_urls = [
        f"{base_url}/3/tutorial/",
        f"{base_url}/3/library/",
        f"{base_url}/3/howto/"
    ]
    queue = deque([(url, 0) for url in start_urls])
    count = 0

    while queue and count < max_pages:
        url, depth = queue.popleft()
        if url in visited_urls or depth > max_depth:
            continue
        visited_urls.add(url)
        try:
            response = session.get(url, headers=HEADERS, timeout=20)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Extract title
            title_tag = soup.find("h1")
            title = title_tag.get_text(strip=True) if title_tag else "N/A"

            # Extract content
            content_div = soup.find("div", class_="body") or soup.find("main")
            if content_div:
                for unwanted in content_div.find_all(["nav", "aside", "footer", "script"]):
                    unwanted.decompose()
                text = content_div.get_text(separator="\n", strip=True)[:10000]
            else:
                text = "N/A"

            if title != "N/A" and text != "N/A":
                content.append({
                    'title': title,
                    'content': text,
                    'url': url,
                    'source': 'Python Docs'
                })
                count += 1
                print(f"[+] Collected Python Docs content: {title}")

            # Collect subpage URLs
            links = soup.find_all("a", href=True)
            print(f"[*] Found {len(links)} links on {url}")
            for link in links:
                href = link['href']
                if href.startswith("/3/"):
                    sub_url = urljoin(base_url, href)
                    if sub_url not in visited_urls and count < max_pages and depth < max_depth:
                        queue.append((sub_url, depth + 1))
            time.sleep(0.5)  # Rate limiting
        except Exception as e:
            print(f"[!] Error scraping Python Docs content {url}: {e}")
            continue

    print(f"[+] Total Python Docs content collected: {len(content)}")
    return content

def fetch_kubernetes_docs(max_pages=1000, max_depth=2):
    """Scrape tutorials and references from Kubernetes Docs."""
    print("[*] Collecting Kubernetes Docs content...")
    content = []
    visited_urls = set()
    base_url = "https://kubernetes.io"
    start_url = f"{base_url}/docs/home/"
    queue = deque([(start_url, 0)])
    count = 0

    while queue and count < max_pages:
        url, depth = queue.popleft()
        if url in visited_urls or depth > max_depth:
            continue
        visited_urls.add(url)
        try:
            response = session.get(url, headers=HEADERS, timeout=20)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Extract title
            title_tag = soup.find("h1")
            title = title_tag.get_text(strip=True) if title_tag else "N/A"

            # Extract content
            content_div = soup.find("main") or soup.find("div", class_=lambda x: x and "content" in x.lower())
            if content_div:
                for unwanted in content_div.find_all(["nav", "aside", "footer", "script"]):
                    unwanted.decompose()
                text = content_div.get_text(separator="\n", strip=True)[:10000]
            else:
                text = "N/A"

            if title != "N/A" and text != "N/A":
                content.append({
                    'title': title,
                    'content': text,
                    'url': url,
                    'source': 'Kubernetes Docs'
                })
                count += 1
                print(f"[+] Collected Kubernetes Docs content: {title}")

            # Collect subpage URLs
            links = soup.find_all("a", href=True)
            print(f"[*] Found {len(links)} links on {url}")
            for link in links:
                href = link['href']
                if href.startswith("/docs/"):
                    sub_url = urljoin(base_url, href)
                    if sub_url not in visited_urls and count < max_pages and depth < max_depth:
                        queue.append((sub_url, depth + 1))
            time.sleep(0.5)  # Rate limiting
        except Exception as e:
            print(f"[!] Error scraping Kubernetes Docs content {url}: {e}")
            continue

    print(f"[+] Total Kubernetes Docs content collected: {len(content)}")
    return content

def fetch_docker_docs(max_pages=1000, max_depth=2):
    """Scrape tutorials and references from Docker Docs."""
    print("[*] Collecting Docker Docs content...")
    content = []
    visited_urls = set()
    base_url = "https://docs.docker.com"
    start_url = f"{base_url}/"
    queue = deque([(start_url, 0)])
    count = 0

    while queue and count < max_pages:
        url, depth = queue.popleft()
        if url in visited_urls or depth > max_depth:
            continue
        visited_urls.add(url)
        try:
            response = session.get(url, headers=HEADERS, timeout=20)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Extract title
            title_tag = soup.find("h1")
            title = title_tag.get_text(strip=True) if title_tag else "N/A"

            # Extract content
            content_div = soup.find("main") or soup.find("div", class_=lambda x: x and "content" in x.lower())
            if content_div:
                for unwanted in content_div.find_all(["nav", "aside", "footer", "script"]):
                    unwanted.decompose()
                text = content_div.get_text(separator="\n", strip=True)[:10000]
            else:
                text = "N/A"

            if title != "N/A" and text != "N/A":
                content.append({
                    'title': title,
                    'content': text,
                    'url': url,
                    'source': 'Docker Docs'
                })
                count += 1
                print(f"[+] Collected Docker Docs content: {title}")

            # Collect subpage URLs
            links = soup.find_all("a", href=True)
            print(f"[*] Found {len(links)} links on {url}")
            for link in links:
                href = link['href']
                if href.startswith("/"):
                    sub_url = urljoin(base_url, href)
                    if sub_url not in visited_urls and count < max_pages and depth < max_depth:
                        queue.append((sub_url, depth + 1))
            time.sleep(0.5)  # Rate limiting
        except Exception as e:
            print(f"[!] Error scraping Docker Docs content {url}: {e}")
            continue

    print(f"[+] Total Docker Docs content collected: {len(content)}")
    return content

def save_to_csv(data, filename="tech_docs.csv"):
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
    parser = argparse.ArgumentParser(description="Web scraper for programming and tech docs from MDN, Python, Kubernetes, and Docker.")
    parser.add_argument("--max_mdn", type=int, default=8000,
                        help="Maximum number of MDN Web Docs pages to fetch")
    parser.add_argument("--max_python", type=int, default=2000,
                        help="Maximum number of Python Docs pages to fetch")
    parser.add_argument("--max_kubernetes", type=int, default=1000,
                        help="Maximum number of Kubernetes Docs pages to fetch")
    parser.add_argument("--max_docker", type=int, default=1000,
                        help="Maximum number of Docker Docs pages to fetch")
    args = parser.parse_args()

    # Estimate and display the target number of data rows
    estimated_rows = estimate_total_rows(args.max_mdn, args.max_python, args.max_kubernetes, args.max_docker)
    print(f"[*] Estimated total data rows to be generated: {estimated_rows}")

    # Fetch data from each source
    mdn_content = fetch_mdn_content(max_pages=args.max_mdn, max_depth=2)
    python_content = fetch_python_docs(max_pages=args.max_python, max_depth=2)
    kubernetes_content = fetch_kubernetes_docs(max_pages=args.max_kubernetes, max_depth=2)
    docker_content = fetch_docker_docs(max_pages=args.max_docker, max_depth=2)

    # Combine all data
    all_data = []
    for item in mdn_content + python_content + kubernetes_content + docker_content:
        all_data.append({
            'source': item['source'],
            'title': item['title'],
            'content': item['content'],
            'url': item['url']
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