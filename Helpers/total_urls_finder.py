import requests
import xml.etree.ElementTree as ET

HEADERS = {"User-Agent": "Mozilla/5.0"}

def get_sitemap_entries(sitemap_url):
    """Parses a sitemap XML and returns the list of URLs or nested sitemap URLs."""
    try:
        response = requests.get(sitemap_url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        root = ET.fromstring(response.content)
        entries = [loc.text for loc in root.findall(".//{*}loc")]
        return entries
    except Exception as e:
        print(f"❌ Failed to fetch or parse {sitemap_url}: {e}")
        return []

def is_sitemap(url):
    """Heuristically decides if a URL is a sitemap (rather than a page URL)."""
    return url.endswith(".xml")

def crawl_sitemaps(sitemap_url, depth=0):
    indent = "  " * depth
    entries = get_sitemap_entries(sitemap_url)
    total_urls = 0

    if not entries:
        return 0

    if is_sitemap(entries[0]):  # If child entries are sitemaps
        print(f"{indent}📂 {sitemap_url} → contains {len(entries)} nested sitemaps")
        for sm in entries:
            total_urls += crawl_sitemaps(sm, depth + 1)
    else:  # If child entries are actual page URLs
        filtered_urls = [url for url in entries if "/article/" in url]
        print(f"{indent}📄 {sitemap_url} → contains {len(filtered_urls)} filtered URLs (matched '/article/')")
        total_urls += len(filtered_urls)


    return total_urls

if __name__ == "__main__":
    root_sitemap = "https://www.worldhistory.org/sitemap.xml"
    print("🔍 Starting sitemap crawl...\n")
    total = crawl_sitemaps(root_sitemap)
    print(f"\n✅ Total URLs found: {total}")
