# scrape_slack.py — Ingest Slack channel export JSON
import json
import os
import re
from datetime import datetime

SAVE_PATH = "data/scraped_slack.json"
DEFAULT_EXPORT = "data/slack_export.json"


def _parse_ts(ts_str):
    try:
        return datetime.fromtimestamp(float(ts_str)).strftime("%Y-%m-%d")
    except (ValueError, TypeError, OSError):
        return datetime.now().strftime("%Y-%m-%d")


def _extract_urls(text):
    urls = []
    for match in re.finditer(r"<(https?://[^|>]+)(?:\|[^>]*)?>", text):
        urls.append(match.group(1))
    for match in re.finditer(r"(?<![<|])(https?://\S+)", text):
        urls.append(match.group(1))
    return list(dict.fromkeys(urls))


def _reaction_count(reactions):
    if not reactions:
        return 0
    return sum(r.get("count", 1) for r in reactions)


def _reaction_summary(reactions):
    if not reactions:
        return ""
    return " ".join(f":{r.get('name', '')}:x{r.get('count', 1)}" for r in reactions)


def run_slack_scraper(export_path=None, since_date=None):
    """Main entry point."""
    path = export_path or os.environ.get("SLACK_EXPORT_PATH", DEFAULT_EXPORT)

    if not os.path.exists(path):
        print(f"  Slack export not found at {path}, skipping.")
        return []

    print(f"  Loading Slack export from {path}...")

    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    # Handle flat list or directory export
    messages = []
    if isinstance(raw, list):
        messages = raw
    elif isinstance(raw, dict):
        for key, val in raw.items():
            if isinstance(val, list):
                messages.extend(val)

    posts = []
    skip_subtypes = {"channel_join", "channel_leave", "channel_purpose",
                     "channel_topic", "bot_message", "tombstone"}

    for msg in messages:
        if msg.get("subtype", "") in skip_subtypes:
            continue

        text = msg.get("text", "").strip()
        if len(text) < 20:
            continue

        ts = msg.get("ts", "")
        post_date = _parse_ts(ts)

        if since_date and post_date < since_date:
            continue

        author = msg.get("user", msg.get("username", "unknown"))
        reactions = msg.get("reactions", [])
        urls = _extract_urls(text)

        posts.append({
            "text": text,
            "title": text[:120],
            "source": "Slack",
            "url": urls[0] if urls else "",
            "username": author,
            "post_date": post_date,
            "_logged_date": datetime.now().isoformat(),
            "search_term": "",
            "score": _reaction_count(reactions),
            "num_comments": len(msg.get("replies", [])),
            "post_id": f"slack_{ts}",
            "_slack_author": author,
            "_slack_reactions": _reaction_summary(reactions),
            "_slack_urls": urls,
        })

    # Deduplicate
    seen = set()
    unique = []
    for p in posts:
        if p["post_id"] not in seen:
            seen.add(p["post_id"])
            unique.append(p)

    unique.sort(key=lambda x: x.get("post_date", ""), reverse=True)

    os.makedirs("data", exist_ok=True)
    with open(SAVE_PATH, "w", encoding="utf-8") as f:
        json.dump(unique, f, ensure_ascii=False, indent=2)

    print(f"  {len(unique)} messages ingested -> {SAVE_PATH}")
    return unique


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", default=None)
    args = parser.parse_args()
    run_slack_scraper(export_path=args.path)
