import requests
from bs4 import BeautifulSoup
import csv
import time
from urllib.parse import urljoin, urlparse

# Base URLs and headers
BASE_URL = "https://en.wikipedia.org"
CATEGORY_URL = urljoin(BASE_URL, "/wiki/Category:Computer_science")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; AcademicScraper/1.0; +https://example.com)"
}

def get_all_article_links(start_category_url, max_articles=1000, max_subcategories=10):
    """
    Collect article URLs from a Wikipedia category and its subcategories with pagination.
    """
    print("[*] Collecting article URLs from", start_category_url)
    article_urls = set()
    subcategory_urls = set([start_category_url])
    visited_subcategories = set()
    visited_pages = set()

    while subcategory_urls and len(article_urls) < max_articles and len(visited_subcategories) < max_subcategories:
        current_category_url = subcategory_urls.pop()
        if current_category_url in visited_subcategories:
            continue
        visited_subcategories.add(current_category_url)
        next_page_url = current_category_url

        print(f"[*] Processing category: {current_category_url}")

        while next_page_url and next_page_url not in visited_pages and len(article_urls) < max_articles:
            print(f"[*] Scanning page: {next_page_url}...")
            try:
                res = requests.get(next_page_url, headers=HEADERS, timeout=10)
                if res.status_code != 200:
                    print(f"[!] Failed to fetch {next_page_url}: HTTP {res.status_code}")
                    break

                soup = BeautifulSoup(res.text, 'html.parser')

                # Find the category content section (articles)
                pages_div = soup.find("div", id="mw-pages")
                if pages_div:
                    links = pages_div.find_all("a", href=True)
                    for link in links:
                        href = link['href']
                        if href.startswith("/wiki/") and not (
                            href.startswith("/wiki/Category:") or
                            href.startswith("/wiki/File:") or
                            href.startswith("/wiki/Template:") or
                            href.startswith("/wiki/Special:") or
                            href.startswith("/wiki/Wikipedia:") or
                            href.startswith("/wiki/Help:")
                        ):
                            full_url = urljoin(BASE_URL, href)
                            article_urls.add(full_url)

                # Find subcategories
                subcat_div = soup.find("div", id="mw-subcategories")
                if subcat_div:
                    subcat_links = subcat_div.find_all("a", href=True)
                    for link in subcat_links:
                        if link['href'].startswith("/wiki/Category:"):
                            full_subcat_url = urljoin(BASE_URL, link['href'])
                            if full_subcat_url not in visited_subcategories:
                                subcategory_urls.add(full_subcat_url)

                # Find the "next page" link for pagination
                next_page_link = soup.find("a", string=lambda text: text and "next page" in text.lower())
                next_page_url = urljoin(BASE_URL, next_page_link['href']) if next_page_link else None
                visited_pages.add(next_page_url)

                print(f"[*] Articles found so far: {len(article_urls)}, Subcategories queued: {len(subcategory_urls)}")

                time.sleep(1)  # Polite delay to respect Wikipedia's servers

            except Exception as e:
                print(f"[!] Error fetching {next_page_url}: {e}")
                break

    print(f"[+] Total: Found {len(article_urls)} articles across categories.")
    return list(article_urls)[:max_articles]

def extract_article_data(url):
    """
    Extract title, content, last modified date, categories, and other details from a Wikipedia article.
    """
    print(f"[+] Extracting: {url}")
    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(res.text, 'html.parser')

        # Extract the article title
        title = soup.find("h1", id="firstHeading")
        title = title.get_text(strip=True) if title else "N/A"

        # Extract the last modified date
        last_modified = soup.find("li", id="footer-info-lastmod")
        date = last_modified.get_text(strip=True).replace("This page was last edited on ", "") if last_modified else "N/A"

        # Extract article categories
        cat_div = soup.find("div", id="catlinks")
        categories = []
        if cat_div:
            cat_links = cat_div.find_all("a", href=True)
            categories = [link.get_text(strip=True) for link in cat_links if link['href'].startswith("/wiki/Category:")]

        # Extract the main content of the article
        content_div = soup.find("div", class_="mw-parser-output")
        if content_div:
            # Remove unwanted elements (tables, references, infoboxes, navboxes, etc.)
            for element in content_div.find_all(
                ["table", "sup", "div", "ul"],
                class_=["infobox", "reflist", "navbox", "toc", "mw-editsection", "thumb"]
            ):
                element.decompose()
            content = content_div.get_text(separator="\n", strip=True)
            # Remove excessive newlines and whitespace
            content = "\n".join(line for line in content.split("\n") if line.strip())
        else:
            content = "N/A"

        # Wikipedia articles have no explicit authors
        author = "Wikipedia Contributors"

        # Extract the domain from the URL
        domain = urlparse(url).netloc

        return {
            "title": title,
            "content": content,
            "date": date,
            "url": url,
            "author": author,
            "domain": domain,
            "categories": ", ".join(categories)
        }
    except Exception as e:
        print(f"[!] Error fetching {url}: {e}")
        return None

def save_to_csv(data, filename="wikipedia_articles.csv"):
    """
    Save the scraped article data to a CSV file.
    """
    keys = ["title", "content", "date", "url", "author", "domain", "categories"]
    with open(filename, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for row in data:
            if row:
                writer.writerow(row)
    print(f"[+] Data saved to {filename}")

def main():
    """
    Main function to orchestrate the scraping and saving process.
    """
    # Step 1: Collect article URLs from the category and subcategories
    article_links = get_all_article_links(CATEGORY_URL, max_articles=1000, max_subcategories=10)
    if not article_links:
        print("[!] No articles found. Check category URL, HTML parsing, or network connectivity.")
        return

    # Step 2: Extract data from each article
    article_data = []
    for link in article_links:
        data = extract_article_data(link)
        if data:
            article_data.append(data)
        time.sleep(0.5)  # Polite delay to avoid overloading Wikipedia's servers

    # Step 3: Save the data to a CSV file
    save_to_csv(article_data)

# Entry point
if __name__ == "__main__":
    main()