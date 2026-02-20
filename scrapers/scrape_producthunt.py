# scrape_producthunt.py — Product Hunt scraper via Google News RSS (PH blocks direct scraping)
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from urllib.parse import quote

import requests

SAVE_PATH = "data/scraped_producthunt.json"
CONFIG_PATH = "config/sources.json"

HEADERS = {
    "User-Agent": "GeoPulse/1.0 (market-intelligence)",
}


def _load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    return {}


def _load_company_names():
    path = "config/companies.json"
    if os.path.exists(path):
        with open(path, "r") as f:
            data = json.load(f)
        names = []
        for group in ("own_brands", "competitors"):
            for c in data.get(group, []):
                names.append(c["name"])
        return names
    return []


def _search_producthunt_via_gnews(query, max_results=30):
    """Search for Product Hunt pages indexed by Google News."""
    posts = []
    encoded = quote(f"site:producthunt.com {query}")
    url = f"https://news.google.com/rss/search?q={encoded}&hl=en-US&gl=US&ceid=US:en"

    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return posts

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
                "source": "Product Hunt",
                "url": link,
                "username": "",
                "post_date": post_date,
                "_logged_date": datetime.now().isoformat(),
                "search_term": query,
                "score": 0,
                "num_comments": 0,
                "post_id": f"ph_{hash(link) % 10**8}",
            })
    except Exception as e:
        print(f"    [WARN] PH search failed for '{query}': {e}")
    return posts


def _search_producthunt_reddit(query, limit=20):
    """Search Reddit for Product Hunt launches related to query."""
    posts = []
    encoded = quote(f"site:producthunt.com OR \"product hunt\" {query}")
    url = f"https://www.reddit.com/search.json?q={encoded}&sort=new&limit={limit}&t=year"

    try:
        r = requests.get(url, headers={"User-Agent": "GeoPulse/1.0"}, timeout=15)
        if r.status_code != 200:
            return posts

        data = r.json()
        for child in data.get("data", {}).get("children", []):
            p = child.get("data", {})
            title = p.get("title", "")
            body = p.get("selftext", "")
            text = f"{title}\n{body}".strip() if body else title

            if len(text) < 20:
                continue

            created = p.get("created_utc", 0)
            post_date = datetime.utcfromtimestamp(created).strftime("%Y-%m-%d") if created else ""

            posts.append({
                "text": text,
                "title": title,
                "source": "Product Hunt (Reddit)",
                "url": f"https://reddit.com{p.get('permalink', '')}",
                "username": p.get("author", ""),
                "post_date": post_date,
                "_logged_date": datetime.now().isoformat(),
                "search_term": query,
                "score": p.get("score", 0),
                "num_comments": p.get("num_comments", 0),
                "post_id": p.get("id", ""),
            })
    except Exception:
        pass
    return posts


def run_producthunt_scraper(since_date=None):
    """Main entry point."""
    config = _load_config()
    queries = config.get("producthunt_queries", [])
    company_names = _load_company_names()

    all_posts = []

    # Search Google News for PH pages
    print(f"  Product Hunt (Google News): {len(queries)} queries...")
    for q in queries:
        posts = _search_producthunt_via_gnews(q)
        print(f"    '{q}': {len(posts)} posts")
        all_posts.extend(posts)
        time.sleep(1.0)

    # Search for specific company launches on PH
    print(f"  Company launches on PH...")
    for name in company_names[:15]:
        posts = _search_producthunt_via_gnews(name)
        if posts:
            print(f"    '{name}': {len(posts)} posts")
        all_posts.extend(posts)
        time.sleep(1.0)

    # Reddit mentions of Product Hunt launches
    print(f"  Reddit PH mentions...")
    for q in queries[:5]:
        posts = _search_producthunt_reddit(q)
        if posts:
            print(f"    '{q}': {len(posts)} Reddit posts")
        all_posts.extend(posts)
        time.sleep(1.5)

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

    print(f"  Total: {len(unique)} unique PH posts -> {SAVE_PATH}")
    return unique


if __name__ == "__main__":
    run_producthunt_scraper()
