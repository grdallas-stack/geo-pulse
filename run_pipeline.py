# run_pipeline.py — Full pipeline runner for GEO Pulse
# Runs: scrape -> enrich -> discover -> trends -> cluster
import argparse
import json
import os
from datetime import datetime

DATA_DIR = "data"
RUN_LOG_PATH = os.path.join(DATA_DIR, "run_log.json")


def _load_run_log():
    if os.path.exists(RUN_LOG_PATH):
        with open(RUN_LOG_PATH, "r") as f:
            return json.load(f)
    return []


def _save_run_log(log):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(RUN_LOG_PATH, "w") as f:
        json.dump(log, f, ensure_ascii=False, indent=2)


def run_pipeline(since_date=None, skip_scrape=False, scrape_skip=None):
    """Run the full pipeline."""
    started_at = datetime.now().isoformat()
    mode = "incremental" if since_date else "full"

    print(f"\n{'=' * 60}")
    print(f"  GEO PULSE PIPELINE — {mode.upper()} RUN")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if since_date:
        print(f"  Since: {since_date}")
    print(f"{'=' * 60}\n")

    new_posts = 0
    status = "completed"
    error = None

    try:
        # Step 1: Scrape
        if not skip_scrape:
            print("--- STEP 1: SCRAPING ---")
            from scrapers.scrape_all import run_all_scrapers
            posts = run_all_scrapers(since_date=since_date, skip=scrape_skip)
            new_posts = len(posts)
        else:
            print("--- STEP 1: SCRAPING --- SKIPPED")

        # Step 2: Enrich
        print("\n--- STEP 2: ENRICHMENT ---")
        from pipeline.enrich import run_enrichment
        enriched = run_enrichment(since_date=since_date)
        enriched_count = len(enriched)
        if skip_scrape:
            new_posts = enriched_count
        print(f"  Enriched {enriched_count} insights")

        # Step 3: Source discovery
        print("\n--- STEP 3: SOURCE DISCOVERY ---")
        from pipeline.discover import run_discovery
        run_discovery()

        # Step 4: Trends
        print("\n--- STEP 4: TRENDS ---")
        from pipeline.trends import run_trends
        run_trends()

        # Step 5: Clustering
        print("\n--- STEP 5: CLUSTERING ---")
        from pipeline.cluster import run_clustering
        run_clustering()

    except Exception as e:
        status = "failed"
        error = str(e)
        print(f"\n  PIPELINE FAILED: {e}")
        import traceback
        traceback.print_exc()

    # Log the run
    log = _load_run_log()
    log.append({
        "run_type": mode,
        "started_at": started_at,
        "completed_at": datetime.now().isoformat(),
        "status": status,
        "new_posts": new_posts,
        "since_date": since_date,
        "error": error,
    })
    if len(log) > 100:
        log = log[-100:]
    _save_run_log(log)

    print(f"\n{'=' * 60}")
    print(f"  Pipeline {status}: {new_posts} posts scraped")
    print(f"{'=' * 60}\n")

    return status


def main():
    parser = argparse.ArgumentParser(description="GEO Pulse Pipeline")
    parser.add_argument("--since", type=str, help="Only process posts since YYYY-MM-DD")
    parser.add_argument("--skip-scrape", action="store_true", help="Skip scraping, run enrichment+ only")
    parser.add_argument("--reddit-only", action="store_true", help="Only scrape Reddit")
    args = parser.parse_args()

    scrape_skip = None
    if args.reddit_only:
        scrape_skip = {"hackernews", "slack", "news", "producthunt", "g2"}

    run_pipeline(
        since_date=args.since,
        skip_scrape=args.skip_scrape,
        scrape_skip=scrape_skip,
    )


if __name__ == "__main__":
    main()
