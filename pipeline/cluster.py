# cluster.py — Theme clustering for GEO Pulse
# Groups enriched insights into thematic clusters based on
# entity tags, feature mentions, and company patterns.
import json
import os
from collections import defaultdict
from datetime import datetime

ENRICHED_PATH = "data/enriched_insights.json"
OUTPUT_PATH = "data/clusters.json"

# Theme definitions: name -> matching criteria
THEME_RULES = {
    "Visibility & Rankings": {
        "keywords": [
            "visibility", "ranking", "rank", "position", "serp",
            "ai overview", "ai overviews", "citation", "share of voice",
            "share of answer", "brand mention", "zero click", "featured snippet",
            "search result", "indexed", "indexing", "organic",
        ],
        "features": ["citation tracking", "share of voice"],
    },
    "Content & Optimization": {
        "keywords": [
            "content", "optimize", "optimization", "seo", "geo",
            "structured data", "schema", "markup", "keyword",
            "authority", "e-e-a-t", "eeat", "topical authority",
            "content strategy", "prompt", "answer engine",
        ],
        "features": ["content optimization", "recommendations"],
    },
    "Tools & Measurement": {
        "keywords": [
            "tool", "platform", "dashboard", "analytics", "measure",
            "metric", "report", "data", "track", "monitor",
            "benchmark", "api", "integration", "software",
        ],
        "features": ["dashboard", "reporting", "api", "integration", "benchmarking"],
        "tags": ["product_launch"],
    },
    "Competitive Landscape": {
        "keywords": [
            "competitor", "alternative", "vs", "versus", "compare",
            "switch", "market", "landscape", "leader", "player",
            "funding", "acquisition", "partnership", "raised",
        ],
        "tags": ["comparison", "funding_news", "competitive_intel"],
        "features": [],
    },
    "User Needs & Gaps": {
        "keywords": [
            "need", "want", "wish", "request", "missing",
            "frustrated", "problem", "issue", "gap", "pain",
            "pricing", "expensive", "cost", "value", "roi",
        ],
        "tags": ["complaint", "question"],
        "features": ["pricing", "accuracy", "workflow"],
    },
    "Industry & Market Shifts": {
        "keywords": [
            "industry", "market", "trend", "shift", "change",
            "google", "openai", "perplexity", "chatgpt",
            "regulation", "policy", "privacy", "cookie",
            "publisher", "media", "traffic", "revenue",
        ],
        "tags": [],
        "features": [],
    },
}


def _match_theme(insight):
    """Determine which theme an insight best fits."""
    text = (insight.get("text", "") + " " + insight.get("title", "")).lower()
    tags = set(insight.get("entity_tags", []))
    features = set(insight.get("features_mentioned", []))

    scores = {}

    for theme, rules in THEME_RULES.items():
        score = 0

        # Keyword matches
        for kw in rules.get("keywords", []):
            if kw in text:
                score += 1

        # Feature matches (weighted higher)
        for feat in rules.get("features", []):
            if feat in features:
                score += 3

        # Tag matches (weighted higher)
        for tag in rules.get("tags", []):
            if tag in tags:
                score += 2

        if score > 0:
            scores[theme] = score

    if not scores:
        return "Uncategorized"

    return max(scores, key=scores.get)


def run_clustering():
    """Cluster enriched insights into themes."""
    if not os.path.exists(ENRICHED_PATH):
        print("  No enriched data to cluster.")
        return {}

    with open(ENRICHED_PATH, "r", encoding="utf-8") as f:
        insights = json.load(f)

    clusters = defaultdict(lambda: {
        "insights": [],
        "count": 0,
        "sentiments": defaultdict(int),
        "top_companies": defaultdict(int),
        "top_tags": defaultdict(int),
        "sample_titles": [],
    })

    for insight in insights:
        theme = _match_theme(insight)
        c = clusters[theme]
        c["insights"].append(insight.get("post_id", ""))
        c["count"] += 1
        c["sentiments"][insight.get("sentiment", "neutral")] += 1

        for comp in insight.get("companies_mentioned", []):
            c["top_companies"][comp] += 1

        for tag in insight.get("entity_tags", []):
            c["top_tags"][tag] += 1

        if len(c["sample_titles"]) < 5:
            title = insight.get("title", "")
            if title and title not in c["sample_titles"]:
                c["sample_titles"].append(title)

    # Build output
    output = {
        "generated_at": datetime.now().isoformat(),
        "total_insights": len(insights),
        "clusters": {},
    }

    for theme in sorted(clusters.keys(), key=lambda t: clusters[t]["count"], reverse=True):
        c = clusters[theme]
        output["clusters"][theme] = {
            "count": c["count"],
            "pct": round(c["count"] / max(len(insights), 1) * 100, 1),
            "sentiments": dict(c["sentiments"]),
            "top_companies": dict(sorted(c["top_companies"].items(), key=lambda x: -x[1])[:10]),
            "top_tags": dict(sorted(c["top_tags"].items(), key=lambda x: -x[1])[:10]),
            "sample_titles": c["sample_titles"],
            "insight_ids": c["insights"],
        }

    os.makedirs("data", exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"  Clustering: {len(output['clusters'])} themes from {len(insights)} insights")
    for theme, data in output["clusters"].items():
        print(f"    {theme}: {data['count']} ({data['pct']}%)")

    return output


if __name__ == "__main__":
    run_clustering()
