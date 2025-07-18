import requests
from bs4 import BeautifulSoup
import csv
import xml.etree.ElementTree as ET
from urllib.parse import urlparse

# Step 1: Read first 12000 URLs from local sitemap file
def extract_urls_from_sitemap(path, limit=12000):
    with open(path, "r", encoding="utf-8") as file:
        tree = ET.parse(file)
        root = tree.getroot()
        ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        urls = [url.find("ns:loc", ns).text for url in root.findall("ns:url", ns)]
        return urls[:limit]

sitemap_path = "/Users/user/Downloads/sitemap-releases-2024.txt"
urls = extract_urls_from_sitemap(sitemap_path)

# Step 2: Scraper logic
HEADERS = {"User-Agent": "Mozilla/5.0"}

def extract_data_from_url(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, "html.parser")

        # Title
        title = soup.find("h1").get_text(strip=True) if soup.find("h1") else None

        # Content
        content_div = soup.find("div", {"id": "text"})
        paragraphs = content_div.find_all("p") if content_div else []
        content = "\n".join(p.get_text(strip=True) for p in paragraphs)

        # Date and Author from <dl>
        date, author = None, None
        dl = soup.find("dl", class_="dl-horizontal dl-custom")
        if dl:
            dt_tags = dl.find_all("dt")
            for dt in dt_tags:
                label = dt.get_text(strip=True)
                dd = dt.find_next_sibling("dd")
                if label == "Date:":
                    date = dd.get_text(strip=True) if dd else None
                elif label == "Source:":
                    author = dd.get_text(strip=True) if dd else None

        domain = urlparse(url).netloc
        categories = "Science"

        return {
            "title": title,
            "content": content,
            "date": date,
            "author": author,
            "url": url,
            "domain": domain,
            "categories": categories,
        }

    except Exception as e:
        print(f"❌ Error scraping {url}: {e}")
        return {
            "title": None,
            "content": None,
            "date": None,
            "author": None,
            "url": url,
            "domain": urlparse(url).netloc,
            "categories": "Science",
        }

# Step 3: Extract and show progress
data = []
for idx, url in enumerate(urls, start=1):
    print(f"🔄 Scraping URL {idx}/{len(urls)}: {url}")
    result = extract_data_from_url(url)
    data.append(result)

# Step 4: Save to CSV
with open("sciencedaily.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["title", "content", "date", "author", "url", "domain", "categories"])
    writer.writeheader()
    writer.writerows(data)

print("✅ Scraping complete. Data saved to sciencedaily.csv")
