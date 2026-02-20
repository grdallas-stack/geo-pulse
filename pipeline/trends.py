# trends.py — Week-over-week trend calculations for GEO Pulse
# Groups enriched insights by ISO week, calculates deltas,
# identifies rising/fading signals and companies.
import json
import os
from collections import defaultdict
from datetime import datetime

ENRICHED_PATH = "data/enriched_insights.json"
OUTPUT_PATH = "data/trends.json"


def _iso_week(date_str):
    """Convert YYYY-MM-DD to ISO week string like '2026-W08'."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        iso = dt.isocalendar()
        return f"{iso[0]}-W{iso[1]:02d}"
    except (ValueError, TypeError):
        return None


def _compute_deltas(current, previous):
    """Compute WoW delta and direction."""
    if previous == 0:
        if current == 0:
            return 0, "flat"
        return current, "new"
    delta_pct = ((current - previous) / previous) * 100
    if delta_pct > 20:
        return round(delta_pct, 1), "rising"
    if delta_pct < -20:
        return round(delta_pct, 1), "fading"
    return round(delta_pct, 1), "stable"


def run_trends():
    """Calculate week-over-week trends from enriched data."""
    if not os.path.exists(ENRICHED_PATH):
        print("  No enriched data for trend analysis.")
        return {}

    with open(ENRICHED_PATH, "r", encoding="utf-8") as f:
        insights = json.load(f)

    # Group by week
    by_week = defaultdict(list)
    for insight in insights:
        date = insight.get("post_date", "")
        week = _iso_week(date)
        if week:
            by_week[week].append(insight)

    weeks_sorted = sorted(by_week.keys())
    if not weeks_sorted:
        print("  No dated insights found.")
        return {}

    # --- Per-week metrics ---
    weekly_metrics = {}
    for week in weeks_sorted:
        posts = by_week[week]

        # Volume
        total = len(posts)

        # Sentiment breakdown
        sentiments = defaultdict(int)
        for p in posts:
            sentiments[p.get("sentiment", "neutral")] += 1

        # Company mentions
        company_counts = defaultdict(int)
        for p in posts:
            for c in p.get("companies_mentioned", []):
                company_counts[c] += 1

        # Entity tag counts
        tag_counts = defaultdict(int)
        for p in posts:
            for tag in p.get("entity_tags", []):
                tag_counts[tag] += 1

        # Signal counts
        signals = {
            "buyer_voice": sum(1 for p in posts if p.get("is_buyer_voice")),
            "founder_voice": sum(1 for p in posts if p.get("is_founder_voice")),
            "feature_request": sum(1 for p in posts if p.get("is_feature_request")),
            "competitive_intel": sum(1 for p in posts if p.get("is_competitive_intel")),
        }

        # Source breakdown
        source_counts = defaultdict(int)
        for p in posts:
            src = p.get("source", "Unknown")
            source_counts[src] += 1

        weekly_metrics[week] = {
            "total": total,
            "sentiments": dict(sentiments),
            "companies": dict(company_counts),
            "tags": dict(tag_counts),
            "signals": signals,
            "sources": dict(source_counts),
        }

    # --- Compute WoW deltas ---
    trend_data = {
        "weeks": [],
        "company_trends": {},
        "signal_trends": {},
        "tag_trends": {},
        "volume_trend": [],
        "rising": [],
        "fading": [],
        "generated_at": datetime.now().isoformat(),
    }

    # Volume trend
    for i, week in enumerate(weeks_sorted):
        current = weekly_metrics[week]["total"]
        previous = weekly_metrics[weeks_sorted[i - 1]]["total"] if i > 0 else 0
        delta, direction = _compute_deltas(current, previous)

        trend_data["weeks"].append(week)
        trend_data["volume_trend"].append({
            "week": week,
            "count": current,
            "delta_pct": delta,
            "direction": direction,
        })

    # Company trends (last 4 weeks focus)
    recent_weeks = weeks_sorted[-4:] if len(weeks_sorted) >= 4 else weeks_sorted
    all_companies = set()
    for week in recent_weeks:
        all_companies.update(weekly_metrics[week]["companies"].keys())

    for company in sorted(all_companies):
        history = []
        for week in weeks_sorted:
            count = weekly_metrics[week]["companies"].get(company, 0)
            history.append({"week": week, "count": count})

        # WoW delta for most recent period
        if len(recent_weeks) >= 2:
            curr = weekly_metrics[recent_weeks[-1]]["companies"].get(company, 0)
            prev = weekly_metrics[recent_weeks[-2]]["companies"].get(company, 0)
            delta, direction = _compute_deltas(curr, prev)
        else:
            delta, direction = 0, "flat"

        trend_data["company_trends"][company] = {
            "history": history[-12:],  # last 12 weeks
            "latest_count": history[-1]["count"] if history else 0,
            "delta_pct": delta,
            "direction": direction,
        }

        if direction == "rising":
            trend_data["rising"].append({"name": company, "type": "company", "delta": delta})
        elif direction == "fading":
            trend_data["fading"].append({"name": company, "type": "company", "delta": delta})

    # Tag trends
    all_tags = set()
    for week in recent_weeks:
        all_tags.update(weekly_metrics[week]["tags"].keys())

    for tag in sorted(all_tags):
        history = []
        for week in weeks_sorted:
            count = weekly_metrics[week]["tags"].get(tag, 0)
            history.append({"week": week, "count": count})

        if len(recent_weeks) >= 2:
            curr = weekly_metrics[recent_weeks[-1]]["tags"].get(tag, 0)
            prev = weekly_metrics[recent_weeks[-2]]["tags"].get(tag, 0)
            delta, direction = _compute_deltas(curr, prev)
        else:
            delta, direction = 0, "flat"

        trend_data["tag_trends"][tag] = {
            "history": history[-12:],
            "latest_count": history[-1]["count"] if history else 0,
            "delta_pct": delta,
            "direction": direction,
        }

        if direction == "rising":
            trend_data["rising"].append({"name": tag, "type": "tag", "delta": delta})
        elif direction == "fading":
            trend_data["fading"].append({"name": tag, "type": "tag", "delta": delta})

    # Signal trends
    signal_names = ["buyer_voice", "founder_voice", "feature_request", "competitive_intel"]
    for sig in signal_names:
        history = []
        for week in weeks_sorted:
            count = weekly_metrics[week]["signals"].get(sig, 0)
            history.append({"week": week, "count": count})

        if len(recent_weeks) >= 2:
            curr = weekly_metrics[recent_weeks[-1]]["signals"].get(sig, 0)
            prev = weekly_metrics[recent_weeks[-2]]["signals"].get(sig, 0)
            delta, direction = _compute_deltas(curr, prev)
        else:
            delta, direction = 0, "flat"

        trend_data["signal_trends"][sig] = {
            "history": history[-12:],
            "latest_count": history[-1]["count"] if history else 0,
            "delta_pct": delta,
            "direction": direction,
        }

    # Sort rising/fading by magnitude
    trend_data["rising"].sort(key=lambda x: abs(x["delta"]), reverse=True)
    trend_data["fading"].sort(key=lambda x: abs(x["delta"]), reverse=True)

    os.makedirs("data", exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(trend_data, f, ensure_ascii=False, indent=2)

    print(f"  Trends: {len(weeks_sorted)} weeks tracked")
    print(f"  Companies tracked: {len(trend_data['company_trends'])}")
    print(f"  Rising signals: {len(trend_data['rising'])}")
    print(f"  Fading signals: {len(trend_data['fading'])}")

    if trend_data["rising"][:3]:
        print(f"  Top rising:")
        for r in trend_data["rising"][:3]:
            print(f"    {r['name']} ({r['type']}): +{r['delta']}%")

    if trend_data["fading"][:3]:
        print(f"  Top fading:")
        for r in trend_data["fading"][:3]:
            print(f"    {r['name']} ({r['type']}): {r['delta']}%")

    return trend_data


if __name__ == "__main__":
    run_trends()
