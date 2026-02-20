# scrape_hackernews.py — Hacker News scraper via Algolia API
import json
import os
import time
from datetime import datetime
from urllib.parse import quote

import requests

SAVE_PATH = "data/scraped_hackernews.json"
CONFIG_PATH = "config/sources.json"
ALGOLIA_URL = "https://hn.algolia.com/api/v1"

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
        names = set()
        for group in ("own_brands", "competitors"):
            for c in data.get(group, []):
                names.add(c["name"].lower())
        return names
    return set()


def _search_hn(query, tags="story", hits_per_page=50):
    """Search HN via Algolia. tags: 'story', 'comment', '(story,comment)'."""
    posts = []
    encoded = quote(query)
    url = f"{ALGOLIA_URL}/search?query={encoded}&tags={tags}&hitsPerPage={hits_per_page}"

    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return posts
        data = r.json()
    except Exception:
        return posts

    for hit in data.get("hits", []):
        object_id = hit.get("objectID", "")
        title = hit.get("title", "") or ""
        comment_text = hit.get("comment_text", "") or ""
        story_title = hit.get("story_title", "") or ""

        # Build text: for stories use title, for comments use comment body
        if comment_text:
            text = comment_text
            display_title = story_title or title
        else:
            text = title
            display_title = title

        # Strip HTML from comments
        import re
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text).strip()

        if len(text) < 20:
            continue

        created = hit.get("created_at_i", 0)
        post_date = datetime.utcfromtimestamp(created).strftime("%Y-%m-%d") if created else ""

        hn_url = f"https://news.ycombinator.com/item?id={object_id}"
        story_url = hit.get("url", "") or hn_url

        posts.append({
            "text": text,
            "title": display_title,
            "source": "Hacker News",
            "url": story_url,
            "_hn_url": hn_url,
            "username": hit.get("author", ""),
            "post_date": post_date,
            "_logged_date": datetime.now().isoformat(),
            "search_term": query,
            "score": hit.get("points", 0) or 0,
            "num_comments": hit.get("num_comments", 0) or 0,
            "post_id": f"hn_{object_id}",
            "_hn_type": "comment" if comment_text else "story",
        })

    return posts


def _get_comments(story_id, limit=15):
    """Fetch top comments for a story."""
    posts = []
    url = f"{ALGOLIA_URL}/items/{story_id}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return posts
        data = r.json()
    except Exception:
        return posts

    import re

    def _walk_children(children, depth=0):
        if depth > 2:
            return
        for child in children[:limit]:
            text = child.get("text", "") or ""
            text = re.sub(r"<[^>]+>", " ", text)
            text = re.sub(r"\s+", " ", text).strip()
            if len(text) < 30:
                continue

            created = child.get("created_at_i", 0)
            post_date = datetime.utcfromtimestamp(created).strftime("%Y-%m-%d") if created else ""

            posts.append({
                "text": text,
                "title": data.get("title", ""),
                "source": "Hacker News",
                "url": f"https://news.ycombinator.com/item?id={child.get('id', '')}",
                "username": child.get("author", ""),
                "post_date": post_date,
                "_logged_date": datetime.now().isoformat(),
                "search_term": "",
                "score": child.get("points", 0) or 0,
                "num_comments": 0,
                "post_id": f"hn_{child.get('id', '')}",
                "_hn_type": "comment",
            })
            _walk_children(child.get("children", []), depth + 1)

    _walk_children(data.get("children", []))
    return posts


def run_hackernews_scraper(since_date=None):
    """Main entry point."""
    config = _load_config()
    company_names = _load_company_names()
    queries = config.get("hackernews_queries", [
        "generative engine optimization", "answer engine optimization",
        "AI search optimization", "GEO SEO", "AI visibility brand",
    ])

    all_posts = []

    # Search stories and comments
    print(f"  Searching {len(queries)} queries (stories + comments)...")
    for q in queries:
        stories = _search_hn(q, tags="story", hits_per_page=30)
        comments = _search_hn(q, tags="comment", hits_per_page=30)
        all_posts.extend(stories)
        all_posts.extend(comments)
        print(f"    '{q}': {len(stories)} stories, {len(comments)} comments")
        time.sleep(0.5)

    # Search company names (only longer names to avoid false positives)
    company_queries = [n for n in sorted(company_names) if len(n) >= 5][:20]
    print(f"  Searching {len(company_queries)} company names...")
    for name in company_queries:
        stories = _search_hn(name, tags="story", hits_per_page=10)
        comments = _search_hn(name, tags="comment", hits_per_page=15)
        total = len(stories) + len(comments)
        if total > 0:
            print(f"    '{name}': {len(stories)} stories, {len(comments)} comments")
        all_posts.extend(stories)
        all_posts.extend(comments)
        time.sleep(0.3)

    # Drill into comments on high-signal stories
    high_signal = [p for p in all_posts
                   if p.get("_hn_type") == "story" and p.get("num_comments", 0) >= 5]
    high_signal.sort(key=lambda x: x.get("score", 0), reverse=True)

    if high_signal[:10]:
        print(f"  Drilling into comments on {min(10, len(high_signal))} high-signal stories...")
        for story in high_signal[:10]:
            story_id = story.get("post_id", "").replace("hn_", "")
            if story_id:
                comments = _get_comments(story_id)
                all_posts.extend(comments)
            time.sleep(0.3)

    # Deduplicate
    seen = set()
    unique = []
    for p in all_posts:
        key = p.get("post_id", "")
        if key and key not in seen:
            seen.add(key)
            unique.append(p)

    if since_date:
        unique = [p for p in unique if p.get("post_date", "") >= since_date]

    unique.sort(key=lambda x: x.get("post_date", ""), reverse=True)

    os.makedirs("data", exist_ok=True)
    with open(SAVE_PATH, "w", encoding="utf-8") as f:
        json.dump(unique, f, ensure_ascii=False, indent=2)

    stories_count = sum(1 for p in unique if p.get("_hn_type") == "story")
    comments_count = sum(1 for p in unique if p.get("_hn_type") == "comment")
    print(f"  Total: {len(unique)} unique HN posts ({stories_count} stories, {comments_count} comments) -> {SAVE_PATH}")
    return unique


if __name__ == "__main__":
    run_hackernews_scraper()
