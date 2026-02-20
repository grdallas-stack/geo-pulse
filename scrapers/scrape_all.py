# scrape_all.py — Orchestrates all scrapers for GEO Pulse
import argparse
import json
import os
from datetime import datetime


def run_all_scrapers(since_date=None, skip=None):
    """Run all scrapers and return combined post list."""
    skip = skip or set()
    all_posts = []

    print(f"\n--- GEO Pulse Scraper Run ---")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if since_date:
        print(f"  Since: {since_date}")
    if skip:
        print(f"  Skipping: {', '.join(sorted(skip))}")
    print()

    # Reddit
    if "reddit" not in skip:
        try:
            from scrapers.scrape_reddit import run_reddit_scraper
            print("  [1/6] Reddit...")
            posts = run_reddit_scraper(since_date=since_date)
            print(f"         {len(posts)} posts")
            all_posts.extend(posts)
        except Exception as e:
            print(f"         FAILED: {e}")
    else:
        print("  [1/6] Reddit... SKIPPED")

    # Hacker News
    if "hackernews" not in skip:
        try:
            from scrapers.scrape_hackernews import run_hackernews_scraper
            print("  [2/6] Hacker News...")
            posts = run_hackernews_scraper(since_date=since_date)
            print(f"         {len(posts)} posts")
            all_posts.extend(posts)
        except Exception as e:
            print(f"         FAILED: {e}")
    else:
        print("  [2/6] Hacker News... SKIPPED")

    # Slack
    if "slack" not in skip:
        try:
            from scrapers.scrape_slack import run_slack_scraper
            print("  [3/6] Slack...")
            posts = run_slack_scraper(since_date=since_date)
            print(f"         {len(posts)} posts")
            all_posts.extend(posts)
        except Exception as e:
            print(f"         FAILED: {e}")
    else:
        print("  [3/6] Slack... SKIPPED")

    # News RSS
    if "news" not in skip:
        try:
            from scrapers.scrape_news_rss import run_news_rss_scraper
            print("  [4/6] News RSS...")
            posts = run_news_rss_scraper(since_date=since_date)
            print(f"         {len(posts)} posts")
            all_posts.extend(posts)
        except Exception as e:
            print(f"         FAILED: {e}")
    else:
        print("  [4/6] News RSS... SKIPPED")

    # Product Hunt
    if "producthunt" not in skip:
        try:
            from scrapers.scrape_producthunt import run_producthunt_scraper
            print("  [5/6] Product Hunt...")
            posts = run_producthunt_scraper(since_date=since_date)
            print(f"         {len(posts)} posts")
            all_posts.extend(posts)
        except Exception as e:
            print(f"         FAILED: {e}")
    else:
        print("  [5/6] Product Hunt... SKIPPED")

    # G2
    if "g2" not in skip:
        try:
            from scrapers.scrape_g2 import run_g2_scraper
            print("  [6/6] G2...")
            posts = run_g2_scraper(since_date=since_date)
            print(f"         {len(posts)} posts")
            all_posts.extend(posts)
        except Exception as e:
            print(f"         FAILED: {e}")
    else:
        print("  [6/6] G2... SKIPPED")

    # Deduplicate across all sources
    seen = set()
    unique = []
    for p in all_posts:
        key = p.get("post_id", "") or p.get("url", "")
        if key and key not in seen:
            seen.add(key)
            unique.append(p)

    print(f"\n  Total collected: {len(all_posts)} -> {len(unique)} unique")
    return unique


def main():
    parser = argparse.ArgumentParser(description="GEO Pulse Scrapers")
    parser.add_argument("--since", type=str, help="Only fetch posts since YYYY-MM-DD")
    parser.add_argument("--no-reddit", action="store_true")
    parser.add_argument("--no-hackernews", action="store_true")
    parser.add_argument("--no-slack", action="store_true")
    parser.add_argument("--no-news", action="store_true")
    parser.add_argument("--no-producthunt", action="store_true")
    parser.add_argument("--no-g2", action="store_true")
    parser.add_argument("--reddit-only", action="store_true", help="Only run Reddit scraper")
    args = parser.parse_args()

    skip = set()
    if args.reddit_only:
        skip = {"hackernews", "slack", "news", "producthunt", "g2"}
    else:
        if args.no_reddit:
            skip.add("reddit")
        if args.no_hackernews:
            skip.add("hackernews")
        if args.no_slack:
            skip.add("slack")
        if args.no_news:
            skip.add("news")
        if args.no_producthunt:
            skip.add("producthunt")
        if args.no_g2:
            skip.add("g2")

    run_all_scrapers(since_date=args.since, skip=skip)


if __name__ == "__main__":
    main()
