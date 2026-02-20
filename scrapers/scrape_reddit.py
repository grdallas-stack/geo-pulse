# scrape_reddit.py — Reddit scraper for GEO Pulse (public JSON API, no auth)
import json
import os
import re
import time
from datetime import datetime
from urllib.parse import quote

import requests

SAVE_PATH = "data/scraped_reddit.json"
CONFIG_PATH = "config/sources.json"

HEADERS = {
    "User-Agent": "GeoPulse/1.0 (market-intelligence; +https://prorata.ai)",
}


def _load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    return {}


def _load_companies():
    path = "config/companies.json"
    if os.path.exists(path):
        with open(path, "r") as f:
            data = json.load(f)
        names = set()
        for group in ("own_brands", "competitors"):
            for c in data.get(group, []):
                names.add(c["name"].lower())
                for a in c.get("aliases", []):
                    names.add(a.lower())
        return names
    return set()


def _fetch_json(url, retries=2):
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 429:
                wait = 10 * (attempt + 1)
                print(f"    Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            if r.status_code == 200:
                return r.json()
            return None
        except Exception:
            time.sleep(3)
    return None


def _scrape_subreddit(sub, limit=100):
    """Scrape recent posts from a subreddit."""
    posts = []
    url = f"https://www.reddit.com/r/{sub}/new.json?limit={limit}"
    data = _fetch_json(url)
    if not data:
        return posts

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
            "source": "Reddit",
            "url": f"https://reddit.com{p.get('permalink', '')}",
            "username": p.get("author", ""),
            "post_date": post_date,
            "_logged_date": datetime.now().isoformat(),
            "subreddit": sub,
            "search_term": "",
            "score": p.get("score", 0),
            "num_comments": p.get("num_comments", 0),
            "post_id": p.get("id", ""),
        })
    return posts


def _search_reddit(query, limit=50):
    """Search Reddit for a query."""
    posts = []
    encoded = quote(query)
    url = f"https://www.reddit.com/search.json?q={encoded}&sort=new&limit={limit}&t=month"
    data = _fetch_json(url)
    if not data:
        return posts

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
            "source": "Reddit",
            "url": f"https://reddit.com{p.get('permalink', '')}",
            "username": p.get("author", ""),
            "post_date": post_date,
            "_logged_date": datetime.now().isoformat(),
            "subreddit": p.get("subreddit", ""),
            "search_term": query,
            "score": p.get("score", 0),
            "num_comments": p.get("num_comments", 0),
            "post_id": p.get("id", ""),
        })
    return posts


def _scrape_comments(permalink, limit=5):
    """Scrape top comments from a post."""
    posts = []
    url = f"https://www.reddit.com{permalink}.json?limit={limit}&sort=top"
    data = _fetch_json(url)
    if not data or not isinstance(data, list) or len(data) < 2:
        return posts

    for child in data[1].get("data", {}).get("children", []):
        c = child.get("data", {})
        body = c.get("body", "").strip()
        if len(body) < 30:
            continue

        created = c.get("created_utc", 0)
        post_date = datetime.utcfromtimestamp(created).strftime("%Y-%m-%d") if created else ""

        posts.append({
            "text": body,
            "title": "",
            "source": "Reddit",
            "url": f"https://reddit.com{permalink}",
            "username": c.get("author", ""),
            "post_date": post_date,
            "_logged_date": datetime.now().isoformat(),
            "subreddit": c.get("subreddit", ""),
            "search_term": "",
            "score": c.get("score", 0),
            "num_comments": 0,
            "post_id": c.get("id", ""),
        })
    return posts


def run_reddit_scraper(since_date=None):
    """Main entry point. If since_date provided, only return posts after that date."""
    config = _load_config()
    company_names = _load_companies()
    subreddits = config.get("reddit_subreddits", [])
    queries = config.get("reddit_queries", [])

    print(f"  Scraping {len(subreddits)} subreddits...")
    all_posts = []

    for sub in subreddits:
        posts = _scrape_subreddit(sub)
        print(f"    r/{sub}: {len(posts)} posts")
        all_posts.extend(posts)
        time.sleep(1.5)

    print(f"  Searching {len(queries)} queries...")
    for q in queries:
        posts = _search_reddit(q)
        print(f"    '{q}': {len(posts)} posts")
        all_posts.extend(posts)
        time.sleep(1.5)

    # Also search for company names
    for name in sorted(company_names)[:15]:
        if len(name) >= 5:  # skip short aliases
            posts = _search_reddit(name)
            if posts:
                print(f"    '{name}': {len(posts)} posts")
                all_posts.extend(posts)
            time.sleep(1.5)

    # Scrape comments from high-score posts that mention companies
    high_signal = [p for p in all_posts if p.get("score", 0) >= 10 and p.get("num_comments", 0) >= 3]
    company_posts = [p for p in high_signal
                     if any(cn in p.get("text", "").lower() for cn in company_names)][:20]

    if company_posts:
        print(f"  Scraping comments from {len(company_posts)} high-signal posts...")
        for p in company_posts:
            permalink = p.get("url", "").replace("https://reddit.com", "")
            if permalink:
                comments = _scrape_comments(permalink)
                all_posts.extend(comments)
                time.sleep(1.0)

    # Deduplicate
    seen = set()
    unique = []
    for p in all_posts:
        key = p.get("post_id", "") or p.get("text", "")[:100]
        if key and key not in seen:
            seen.add(key)
            unique.append(p)

    # Filter by date if incremental
    if since_date:
        unique = [p for p in unique if p.get("post_date", "") >= since_date]

    unique.sort(key=lambda x: x.get("post_date", ""), reverse=True)

    os.makedirs("data", exist_ok=True)
    with open(SAVE_PATH, "w", encoding="utf-8") as f:
        json.dump(unique, f, ensure_ascii=False, indent=2)

    print(f"  Total: {len(unique)} unique Reddit posts -> {SAVE_PATH}")
    return unique


if __name__ == "__main__":
    run_reddit_scraper()
