import requests
from bs4 import BeautifulSoup
import csv
import os
import time
from datetime import datetime
from urllib.parse import urlparse
from datetime import datetime

# ---------------- Config ----------------
OUTPUT_DIR = "scraped_papers"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "research_papers.csv")
FIELDS = ["title", "content", "date", "url", "author", "domain", "categories"]
MAX_ARTICLES = 1000  # Total articles to scrape

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ResearchScraper/1.0)"
}

# ---------------- Helper Functions ----------------

def extract_domain(url):
    return urlparse(url).netloc

def clean_text(text):
    return text.strip().replace("\n", " ").replace("\r", "")[:5000] if text else "N/A"

# ---------------- Scrapers ----------------

def scrape_arxiv_paginated(query="machine learning", total_articles=1000):
    print("🔎 Scraping arXiv with pagination...")
    articles = []
    BATCH = 100
    start = 0

    while len(articles) < total_articles:
        try:
            print(f"➡️ arXiv batch {start}–{start+BATCH}")
            api_url = f"https://export.arxiv.org/api/query?search_query=all:{query.replace(' ', '+')}&start={start}&max_results={BATCH}"
            response = requests.get(api_url, headers=HEADERS)
            soup = BeautifulSoup(response.text, features="xml")
            entries = soup.find_all("entry")

            if not entries:
                break  # No more results

            for entry in entries:
                title = clean_text(entry.title.text)
                content = clean_text(entry.summary.text)
                url = entry.id.text
                date = entry.published.text.split("T")[0] if entry.published else datetime.now().strftime("%Y-%m-%d")
                authors = ", ".join([a.find("name").text for a in entry.find_all("author")])
                domain = extract_domain(url)
                categories = ", ".join([c['term'] for c in entry.find_all("category")])

                if (title and content and url):
                    articles.append({
                        "title": title,
                        "content": content,
                        "date": date,
                        "url": url,
                        "author": authors or "Multiple Authors",
                        "domain": domain,
                        "categories": categories or "arxiv, research"
                    })

                if len(articles) >= total_articles:
                    break

            start += BATCH
            time.sleep(1)

        except Exception as e:
            print(f"[Error] arXiv pagination failed: {e}")
            break

    print(f"✅ Total arXiv records: {len(articles)}")
    return articles

def scrape_plos_paginated(total_articles=400):
    print("🔎 Scraping PLOS ONE with pagination...")
    articles = []
    page = 0

    while len(articles) < total_articles:
        try:
            url_page = f"https://journals.plos.org/plosone/browse?resultView=cards&page={page}"
            res = requests.get(url_page, headers=HEADERS)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, 'html.parser')
            links = soup.select("a[href^='/plosone/article']")

            if not links:
                break

            for link in links:
                href = link.get("href", "")
                full_url = "https://journals.plos.org" + href
                if len(articles) >= total_articles:
                    break

                try:
                    art_res = requests.get(full_url, headers=HEADERS)
                    art_soup = BeautifulSoup(art_res.text, "html.parser")

                    title_tag = art_soup.find("h1")
                    content_tag = art_soup.find("div", class_="abstract")
                    authors = art_soup.select("ul.authors li")
                    author_str = ", ".join(a.get_text(strip=True) for a in authors) or "PLOS Editorial Team"

                    date_tag = art_soup.find("meta", {"name": "citation_publication_date"})
                    date = date_tag["content"] if date_tag else datetime.now().strftime("%Y-%m-%d")

                    article = {
                        "title": clean_text(title_tag.text if title_tag else "Untitled"),
                        "content": clean_text(content_tag.text if content_tag else "No summary."),
                        "date": date,
                        "url": full_url,
                        "author": author_str,
                        "domain": "journals.plos.org",
                        "categories": "science, public health, biology, plos",
                    }

                    if all(article.values()) and len(article["content"]) > 50:
                        articles.append(article)

                except Exception as e:
                    print(f"[PLOS 🚫] Skipped article: {e}")

            page += 1
            time.sleep(1)

        except Exception as e:
            print(f"[PLOS ❌] Page fetch failed: {e}")
            break

    print(f"✅ Total PLOS articles collected: {len(articles)}")
    return articles

def scrape_biorxiv(query="neuro", max_articles=200):
    print("🔎 Scraping bioRxiv...")
    articles = []
    base_url = f"https://www.biorxiv.org/search/{query}%20numresults%3A100%20sort%3Arelevance-rank"

    try:
        response = requests.get(base_url, headers=HEADERS)
        soup = BeautifulSoup(response.text, "html.parser")
        preview_links = soup.select("span.highwire-cite-title > a")

        for tag in preview_links[:max_articles]:
            try:
                link = "https://www.biorxiv.org" + tag["href"]
                art = requests.get(link, headers=HEADERS)
                art_soup = BeautifulSoup(art.text, "html.parser")

                title = art_soup.find("h1", class_="highwire-cite-title").get_text(strip=True)
                abstract = art_soup.find("div", class_="section abstract").get_text(strip=True)
                author_list = art_soup.select(".highwire-citation-authors span.highwire-citation-author")
                authors = ", ".join(a.get_text(strip=True) for a in author_list)
                date = datetime.now().strftime("%Y-%m-%d")

                articles.append({
                    "title": clean_text(title),
                    "content": clean_text(abstract),
                    "date": date,
                    "url": link,
                    "author": authors or "bioRxiv Authors",
                    "domain": "biorxiv.org",
                    "categories": "neuroscience, life sciences, biorxiv"
                })

                if len(articles) >= max_articles:
                    break

            except Exception as e:
                print(f"[bioRxiv ❌] Skipped article: {e}")
                continue

        print(f"✅ Total bioRxiv articles collected: {len(articles)}")

    except Exception as e:
        print(f"[bioRxiv ❌] Failed fetching main page: {e}")

    return articles

def scrape_plos_paginated(total_articles=500):
    print("🔎 Scraping PLOS ONE with pagination...")
    articles = []
    page = 0
    BATCH = 20

    while len(articles) < total_articles:
        try:
            url_page = f"https://journals.plos.org/plosone/browse?resultView=cards&page={page}"
            res = requests.get(url_page, headers=HEADERS)
            soup = BeautifulSoup(res.text, 'html.parser')
            links = soup.select("div.search-results-item-meta h2 a")

            if not links:
                break

            for link in links:
                if len(articles) >= total_articles:
                    break

                try:
                    url = "https://journals.plos.org" + link["href"]
                    content_res = requests.get(url, headers=HEADERS)
                    article_soup = BeautifulSoup(content_res.text, "html.parser")

                    title = clean_text(link.text)
                    abstract = article_soup.find("div", class_="abstract")
                    content = clean_text(abstract.text) if abstract else "N/A"

                    author_tag = article_soup.select_one("ul.authors li")
                    author = clean_text(author_tag.text) if author_tag else "PLOS Editorial Team"

                    date_tag = article_soup.find("meta", {"name": "citation_publication_date"})
                    pub_date = date_tag["content"] if date_tag else get_current_date()

                    if content != "N/A" and len(content) > 50:
                        articles.append({
                            "title": title,
                            "content": content,
                            "date": pub_date,
                            "url": url,
                            "author": author,
                            "domain": extract_domain(url),
                            "categories": "plos, open access, research"
                        })

                except Exception as e:
                    print(f"[!] Error in PLOS URL: {e}")
                    continue

            page += 1
            time.sleep(1)

        except Exception as e:
            print(f"[!] PLOS scrape failed: {e}")
            break

    print(f"✅ Total PLOS ONE records: {len(articles)}")
    return articles

def scrape_nature(max_articles=100):
    print("🔎 Scraping Nature...")
    articles = []
    try:
        url = "https://www.nature.com/news"
        soup = BeautifulSoup(requests.get(url, headers=HEADERS).text, "html.parser")
        items = soup.select("li.app-article-list-row__item")
        for item in items[:max_articles]:
            try:
                a_tag = item.find("a", href=True)
                link = "https://www.nature.com" + a_tag["href"]
                detail = requests.get(link, headers=HEADERS)
                detail_soup = BeautifulSoup(detail.text, "html.parser")
                content_paragraphs = detail_soup.select("div.c-article-body p")
                article = {
                    "title": clean_text(a_tag.text),
                    "content": clean_text(" ".join(p.text for p in content_paragraphs)),
                    "date": get_date(),
                    "url": link,
                    "author": "Nature Editors",
                    "domain": "nature.com",
                    "categories": "Nature, research, science"
                }
                articles.append(article)
            except:
                continue
    except Exception as e:
        print(f"[!] Nature scrape failed: {e}")
    print(f"✅ Nature articles: {len(articles)}")
    return articles

# ---------------- Save to CSV ----------------

def save_to_csv(records):
    with open(OUTPUT_FILE, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        for row in records:
            if all(row.get(k) and row[k] != "N/A" for k in FIELDS):  # Ensure no empty fields
                writer.writerow(row)
    print(f"✅ CSV saved: {OUTPUT_FILE} ({len(records)} entries)")

# ---------------- Main ----------------

def main():
    print("🚀 Starting large-scale research scraper to gather 1000+ records...\n")

    # arXiv pagination with multiple topics
    arxiv = []
    for query in ["machine learning", "climate", "neuroscience", "statistics"]:
        arxiv += scrape_arxiv_paginated(query=query, total_articles=250)

    # biorxiv
    biorxiv = scrape_biorxiv(query="neuro", max_articles=300)

    # plos paginated
    plos = scrape_plos_paginated(total_articles=300)

    # nature (smaller)
    nature = scrape_nature(max_articles=50)

    combined = arxiv + biorxiv + plos + nature

    seen = set()
    final = []
    for row in combined:
        if row["url"] not in seen:
            seen.add(row["url"])
            final.append(row)

    print(f"\n✅ Final dataset size: {len(final)}")
    save_to_csv(final)

if __name__ == "__main__":
    main()
