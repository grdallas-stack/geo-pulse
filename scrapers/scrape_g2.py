# scrape_g2.py — G2 review scraper via Google News RSS + Reddit
# G2 blocks direct scraping, so we capture review discussions indexed elsewhere.
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from urllib.parse import quote

import requests

SAVE_PATH = "data/scraped_g2.json"
CONFIG_PATH = "config/sources.json"

HEADERS = {
    "User-Agent": "GeoPulse/1.0 (market-intelligence)",
}


def _load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    return {}


def _google_news_g2(product_name, max_results=20):
    """Search Google News for G2 reviews of a product."""
    posts = []
    queries = [
        f'site:g2.com "{product_name}" review',
        f'"{product_name}" G2 review',
    ]

    for query in queries:
        try:
            encoded = quote(query)
            url = f"https://news.google.com/rss/search?q={encoded}&hl=en-US&gl=US&ceid=US:en"
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code != 200:
                continue

            root = ET.fromstring(r.content)
            for item in root.findall(".//item")[:max_results]:
                title = (item.findtext("title") or "").strip()
                link = (item.findtext("link") or "").strip()
                pub_date = (item.findtext("pubDate") or "").strip()
                description = re.sub(r"<[^>]+>", "", (item.findtext("description") or "")).strip()

                text = title
                if description and description != title:
                    text = f"{title}\n{description}"

                if len(text) < 15:
                    continue

                post_date = datetime.now().strftime("%Y-%m-%d")
                if pub_date:
                    try:
                        from email.utils import parsedate_to_datetime
                        post_date = parsedate_to_datetime(pub_date).strftime("%Y-%m-%d")
                    except Exception:
                        pass

                posts.append({
                    "text": text,
                    "title": title,
                    "source": "G2",
                    "url": link,
                    "username": "",
                    "post_date": post_date,
                    "_logged_date": datetime.now().isoformat(),
                    "search_term": product_name,
                    "score": 0,
                    "num_comments": 0,
                    "post_id": f"g2_{hash(link) % 10**8}",
                    "_g2_product": product_name,
                })
        except Exception as e:
            print(f"    [WARN] G2 search failed for '{product_name}': {e}")
        time.sleep(0.5)
    return posts


def _reddit_g2_reviews(product_name, limit=15):
    """Search Reddit for G2-style review discussions about a product."""
    posts = []
    queries = [
        f'"{product_name}" review',
        f'"{product_name}" pros cons',
    ]

    for query in queries:
        try:
            encoded = quote(query)
            url = f"https://www.reddit.com/search.json?q={encoded}&sort=relevance&limit={limit}&t=year"
            r = requests.get(url, headers={"User-Agent": "GeoPulse/1.0"}, timeout=15)
            if r.status_code != 200:
                continue

            data = r.json()
            for child in data.get("data", {}).get("children", []):
                p = child.get("data", {})
                title = p.get("title", "")
                body = p.get("selftext", "")
                text = f"{title}\n{body}".strip() if body else title

                if len(text) < 30:
                    continue

                # Must actually mention the product
                if product_name.lower() not in text.lower():
                    continue

                created = p.get("created_utc", 0)
                post_date = datetime.utcfromtimestamp(created).strftime("%Y-%m-%d") if created else ""

                posts.append({
                    "text": text,
                    "title": title,
                    "source": "G2 (Reddit)",
                    "url": f"https://reddit.com{p.get('permalink', '')}",
                    "username": p.get("author", ""),
                    "post_date": post_date,
                    "_logged_date": datetime.now().isoformat(),
                    "search_term": product_name,
                    "score": p.get("score", 0),
                    "num_comments": p.get("num_comments", 0),
                    "post_id": p.get("id", ""),
                    "_g2_product": product_name,
                })
        except Exception:
            pass
        time.sleep(1.0)
    return posts


def run_g2_scraper(since_date=None):
    """Main entry point."""
    config = _load_config()
    products = config.get("g2_products", [])

    all_posts = []

    print(f"  G2 reviews: {len(products)} products...")
    for product in products:
        g2_posts = _google_news_g2(product)
        reddit_posts = _reddit_g2_reviews(product)
        total = len(g2_posts) + len(reddit_posts)
        if total > 0:
            print(f"    {product}: {len(g2_posts)} G2, {len(reddit_posts)} Reddit reviews")
        all_posts.extend(g2_posts)
        all_posts.extend(reddit_posts)
        time.sleep(1.0)

    # Deduplicate
    seen = set()
    unique = []
    for p in all_posts:
        key = p.get("post_id", "") or p.get("url", "")
        if key and key not in seen:
            seen.add(key)
            unique.append(p)

    if since_date:
        unique = [p for p in unique if p.get("post_date", "") >= since_date]

    unique.sort(key=lambda x: x.get("post_date", ""), reverse=True)

    os.makedirs("data", exist_ok=True)
    with open(SAVE_PATH, "w", encoding="utf-8") as f:
        json.dump(unique, f, ensure_ascii=False, indent=2)

    print(f"  Total: {len(unique)} unique review posts -> {SAVE_PATH}")
    return unique


if __name__ == "__main__":
    run_g2_scraper()
