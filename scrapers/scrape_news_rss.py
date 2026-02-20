# scrape_news_rss.py — Google News RSS + trade press RSS feeds
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from email.utils import parsedate_to_datetime
from urllib.parse import quote

import requests

SAVE_PATH = "data/scraped_news_rss.json"
CONFIG_PATH = "config/sources.json"

HEADERS = {
    "User-Agent": "GeoPulse/1.0 (market-intelligence)",
    "Accept": "application/xml, text/xml, application/rss+xml, */*",
}


def _load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    return {}


def _parse_rss_date(date_str):
    try:
        return parsedate_to_datetime(date_str).strftime("%Y-%m-%d")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")


def _google_news_rss(query, max_results=50):
    """Fetch results from Google News RSS for a query."""
    posts = []
    try:
        encoded = quote(query)
        url = f"https://news.google.com/rss/search?q={encoded}&hl=en-US&gl=US&ceid=US:en"
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

            if len(text) < 20:
                continue

            posts.append({
                "text": text,
                "title": title,
                "source": "News",
                "url": link,
                "username": "",
                "post_date": _parse_rss_date(pub_date) if pub_date else datetime.now().strftime("%Y-%m-%d"),
                "_logged_date": datetime.now().isoformat(),
                "search_term": query,
                "score": 0,
                "num_comments": 0,
                "post_id": f"gnews_{hash(link) % 10**8}",
            })
    except Exception as e:
        print(f"    [WARN] Google News RSS failed for '{query}': {e}")
    return posts


def _fetch_rss_feed(feed_name, feed_url, max_items=30):
    """Fetch items from a direct RSS feed."""
    posts = []
    try:
        r = requests.get(feed_url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return posts

        root = ET.fromstring(r.content)
        for item in root.findall(".//item")[:max_items]:
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            pub_date = (item.findtext("pubDate") or "").strip()
            description = re.sub(r"<[^>]+>", "", (item.findtext("description") or "")).strip()

            text = title
            if description and description != title:
                text = f"{title}\n{description}"

            if len(text) < 20:
                continue

            posts.append({
                "text": text,
                "title": title,
                "source": f"News ({feed_name})",
                "url": link,
                "username": "",
                "post_date": _parse_rss_date(pub_date) if pub_date else datetime.now().strftime("%Y-%m-%d"),
                "_logged_date": datetime.now().isoformat(),
                "search_term": "",
                "score": 0,
                "num_comments": 0,
                "post_id": f"rss_{hash(link) % 10**8}",
                "_feed_name": feed_name,
            })
    except Exception as e:
        print(f"    [WARN] RSS feed '{feed_name}' failed: {e}")
    return posts


def run_news_rss_scraper(since_date=None):
    """Main entry point."""
    config = _load_config()
    queries = config.get("news_rss_queries", [])
    feeds = config.get("news_rss_feeds", [])

    all_posts = []

    # Google News queries
    print(f"  Google News: {len(queries)} queries...")
    for q in queries:
        posts = _google_news_rss(q)
        print(f"    '{q}': {len(posts)} articles")
        all_posts.extend(posts)
        time.sleep(1.0)

    # Direct RSS feeds
    print(f"  RSS feeds: {len(feeds)} feeds...")
    for feed in feeds:
        name = feed.get("name", "Unknown")
        url = feed.get("url", "")
        if not url:
            continue
        posts = _fetch_rss_feed(name, url)
        print(f"    {name}: {len(posts)} articles")
        all_posts.extend(posts)
        time.sleep(0.5)

    # Deduplicate by URL
    seen = set()
    unique = []
    for p in all_posts:
        url = p.get("url", "")
        if url and url in seen:
            continue
        if url:
            seen.add(url)
        unique.append(p)

    if since_date:
        unique = [p for p in unique if p.get("post_date", "") >= since_date]

    unique.sort(key=lambda x: x.get("post_date", ""), reverse=True)

    os.makedirs("data", exist_ok=True)
    with open(SAVE_PATH, "w", encoding="utf-8") as f:
        json.dump(unique, f, ensure_ascii=False, indent=2)

    print(f"  Total: {len(unique)} unique articles -> {SAVE_PATH}")
    return unique


if __name__ == "__main__":
    run_news_rss_scraper()
