# scheduler.py — Background scrape orchestrator for GEO Pulse
# Runs incremental scrapes every N hours via APScheduler.
# Usage: python scheduler.py (runs as daemon)
# Or: python scheduler.py --once (single run, then exit)

import json
import os
import sys
from datetime import datetime, timedelta

from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = "data"
RUN_LOG_PATH = os.path.join(DATA_DIR, "run_log.json")
INTERVAL_HOURS = int(os.environ.get("SCRAPE_INTERVAL_HOURS", 6))


def _load_run_log():
    if os.path.exists(RUN_LOG_PATH):
        with open(RUN_LOG_PATH, "r") as f:
            return json.load(f)
    return []


def _save_run_log(log):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(RUN_LOG_PATH, "w") as f:
        json.dump(log, f, ensure_ascii=False, indent=2)


def _last_run_date():
    """Get the date of the last successful run for incremental scraping."""
    log = _load_run_log()
    for entry in reversed(log):
        if entry.get("status") == "completed":
            ts = entry.get("completed_at", "")
            try:
                dt = datetime.fromisoformat(ts)
                return (dt - timedelta(hours=1)).strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                pass
    return None


def run_pipeline_incremental():
    """Run a single incremental pipeline pass."""
    started_at = datetime.now().isoformat()
    since_date = _last_run_date()
    mode = "incremental" if since_date else "full"

    print(f"\n{'=' * 60}")
    print(f"  GEO PULSE — {mode.upper()} RUN")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if since_date:
        print(f"  Since: {since_date}")
    print(f"{'=' * 60}\n")

    new_posts = 0

    try:
        # Step 1: Scrape all sources
        from scrapers.scrape_all import run_all_scrapers
        posts = run_all_scrapers(since_date=since_date)
        new_posts = len(posts)

        # Step 2: Enrich
        from pipeline.enrich import run_enrichment
        enriched = run_enrichment(since_date=since_date)

        # Step 3: Source discovery
        from pipeline.discover import run_discovery
        run_discovery()

        # Step 4: Trends
        from pipeline.trends import run_trends
        run_trends()

        # Step 5: Cluster
        from pipeline.cluster import run_clustering
        run_clustering()

        status = "completed"
        error = None

    except Exception as e:
        status = "failed"
        error = str(e)
        print(f"  Pipeline failed: {e}")

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
    # Keep last 100 runs
    if len(log) > 100:
        log = log[-100:]
    _save_run_log(log)

    print(f"\n  Run {status}: {new_posts} posts processed")
    return status


def main():
    import argparse
    parser = argparse.ArgumentParser(description="GEO Pulse Scheduler")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    parser.add_argument("--interval", type=int, default=INTERVAL_HOURS,
                        help=f"Hours between runs (default {INTERVAL_HOURS})")
    args = parser.parse_args()

    if args.once:
        run_pipeline_incremental()
        return

    print(f"GEO Pulse Scheduler starting (every {args.interval} hours)")
    print(f"First run starting now...\n")

    scheduler = BlockingScheduler()

    # Run immediately on start
    run_pipeline_incremental()

    # Then schedule recurring
    scheduler.add_job(run_pipeline_incremental, "interval", hours=args.interval,
                      id="geo_pulse_pipeline", replace_existing=True)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("\nScheduler stopped.")


if __name__ == "__main__":
    main()
