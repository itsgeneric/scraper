import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import csv
import time

BASE_URL = "https://wanderingearl.com"
BLOG_URL = f"{BASE_URL}/blog/"
HEADERS = {"User-Agent": "Mozilla/5.0"}

def get_all_blog_post_links():
    print("[*] Collecting blog post URLs using requests...")
    post_urls = set()
    page = 1

    while True:
        url = f"{BLOG_URL}page/{page}/"
        print(f"[*] Scanning {url}")
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
        time.sleep(0.5)

    print(f"[+] Found {len(post_urls)} blog posts.")
    return list(post_urls)

def setup_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(options=options)

def extract_post_data(driver, url):
    print(f"[+] Extracting: {url}")
    try:
        driver.get(url)
        time.sleep(2)

        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Title
        title_tag = soup.find("h1", class_="entry-title")
        title = title_tag.get_text(strip=True) if title_tag else "N/A"

        # Date
        date = "N/A"
        meta_wrapper = soup.find("div", class_="fusion-meta-info-wrapper")
        if meta_wrapper:
            for span in meta_wrapper.find_all("span"):
                text = span.get_text(strip=True)
                if text and "," in text:  # crude filter for real date
                    date = text
                    break

        # Content (first 150 words)
        content_div = soup.find("div", class_="post-content")
        if content_div:
            paragraphs = content_div.find_all("p")
            full_text = " ".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
            words = full_text.split()
            content = " ".join(words[:150]) + ("..." if len(words) > 150 else "")
        else:
            content = "N/A"

        # Author
        author_tag = soup.select_one("span.vcard span.fn a")
        author = author_tag.get_text(strip=True) if author_tag else "Earl"

        domain = urlparse(url).netloc

        return {
            "title": title,
            "content": content,
            "date": date,
            "url": url,
            "author": author,
            "domain": domain,
            "categories": "Travel"
        }
    except Exception as e:
        print(f"[!] Error extracting {url}: {e}")
        return None

def save_to_csv(data, filename="wanderingearl_all_posts.csv"):
    keys = ["title", "content", "date", "url", "author", "domain", "categories"]
    with open(filename, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for row in data:
            if row:
                writer.writerow(row)
    print(f"[+] Saved {len(data)} blog posts to {filename}")

def main():
    driver = setup_driver()
    try:
        blog_links = get_all_blog_post_links()
        blog_data = []

        print(f"[+] Total URLs fetched: {len(blog_links)} — limiting to first 10")

        for i, link in enumerate(blog_links):
            print(f"({i+1}/10)")
            data = extract_post_data(driver, link)
            if data:
                blog_data.append(data)
            time.sleep(1)

        save_to_csv(blog_data, filename= "../Datasets/wanderingearl.csv")
    finally:
        driver.quit()


if __name__ == "__main__":
    main()
