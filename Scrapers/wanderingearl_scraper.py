import requests
from bs4 import BeautifulSoup
import csv
import time
from urllib.parse import urljoin, urlparse

BASE_URL = "https://wanderingearl.com"
BLOG_URL = urljoin(BASE_URL, "/blog/")
HEADERS = {"User-Agent": "Mozilla/5.0"}

def get_all_blog_post_links():
    print("[*] Collecting blog post URLs...")
    post_urls = set()
    page = 1

    while True:
        print(f"[*] Scanning page {page}...")
        url = f"{BLOG_URL}page/{page}/"
        res = requests.get(url, headers=HEADERS)
        if res.status_code != 200:
            break

        soup = BeautifulSoup(res.text, 'html.parser')
        articles = soup.find_all("h2", class_="entry-title")

        if not articles:
            break

        for article in articles:
            a_tag = article.find("a")
            if a_tag and a_tag.get("href"):
                post_urls.add(a_tag['href'])

        page += 1
        time.sleep(1)

    print(f"[+] Found {len(post_urls)} blog posts.")
    return list(post_urls)

def extract_post_data(url):
    print(f"[+] Extracting: {url}")
    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(res.text, 'html.parser')

        title = soup.find("h1", class_="entry-title")
        title = title.get_text(strip=True) if title else "N/A"

        date = soup.find("time", class_="entry-date")
        date = date.get_text(strip=True) if date else "N/A"

        content_div = soup.find("div", class_="entry-content")
        info = content_div.get_text(separator="\n", strip=True) if content_div else "N/A"

        author_tag = soup.find("span", class_="author vcard")
        author = author_tag.get_text(strip=True) if author_tag else "Earl"

        domain = urlparse(url).netloc

        return {
            "title": title,
            "info": info,
            "date": date,
            "url": url,
            "author": author,
            "domain": domain
        }
    except Exception as e:
        print(f"[!] Error fetching {url}: {e}")
        return None

def save_to_csv(data, filename="wanderingearl_blogs.csv"):
    keys = ["title", "info", "date", "url", "author", "domain"]
    with open(filename, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for row in data:
            if row:
                writer.writerow(row)
    print(f"[+] Data saved to {filename}")

def main():
    blog_links = get_all_blog_post_links()
    blog_data = []

    for link in blog_links:
        data = extract_post_data(link)
        if data:
            blog_data.append(data)
        time.sleep(0.5)

    save_to_csv(blog_data)

if __name__ == "__main__":
    main()
