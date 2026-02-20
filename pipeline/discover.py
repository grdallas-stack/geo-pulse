# discover.py — Source discovery for GEO Pulse
# Scans enriched insights for new sources (domains, newsletters, tools, communities)
# and suggests them for addition to the scrape config.
import json
import os
import re
from collections import defaultdict
from datetime import datetime
from urllib.parse import urlparse

ENRICHED_PATH = "data/enriched_insights.json"
OUTPUT_PATH = "data/discovered_sources.json"
CONFIG_PATH = "config/sources.json"

# Domains we already know about or want to skip
SKIP_DOMAINS = {
    "reddit.com", "old.reddit.com", "www.reddit.com",
    "news.ycombinator.com", "ycombinator.com",
    "twitter.com", "x.com",
    "youtube.com", "youtu.be",
    "google.com", "news.google.com",
    "facebook.com", "instagram.com", "tiktok.com",
    "linkedin.com", "medium.com", "substack.com",
    "github.com", "gitlab.com",
    "producthunt.com", "g2.com",
    "imgur.com", "i.redd.it", "v.redd.it",
    "bit.ly", "t.co", "goo.gl",
    "en.wikipedia.org", "wikipedia.org",
    "web.archive.org",
}

# Source type indicators
TOOL_PATTERNS = re.compile(
    r"\b(saas|platform|tool|software|app|dashboard|analytics|suite)\b", re.I
)

NEWSLETTER_PATTERNS = re.compile(
    r"\b(newsletter|weekly|digest|roundup|briefing|recap|subscribe|inbox)\b", re.I
)

COMMUNITY_PATTERNS = re.compile(
    r"\b(forum|community|slack|discord|group|subreddit|circle)\b", re.I
)

BLOG_PATTERNS = re.compile(
    r"\b(blog|article|post|guide|tutorial|how-?to|resource)\b", re.I
)

# GEO/AEO relevance terms
GEO_TERMS = [
    "geo", "aeo", "ai search", "ai visibility", "generative engine",
    "answer engine", "llm optimization", "brand visibility",
    "share of voice", "share of answer", "seo", "content optimization",
    "structured data", "schema markup", "ai overview",
]


def _extract_domains(text):
    """Extract domains from URLs in text."""
    domains = set()
    urls = re.findall(r"https?://[^\s\)\]\"'<>,]+", text)
    for url in urls:
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower().lstrip("www.")
            if domain and "." in domain and domain not in SKIP_DOMAINS:
                domains.add(domain)
        except Exception:
            pass
    return domains


def _classify_source(domain, context_text):
    """Classify what type of source a domain might be."""
    types = []
    combined = f"{domain} {context_text}"

    if TOOL_PATTERNS.search(combined):
        types.append("tool")
    if NEWSLETTER_PATTERNS.search(combined):
        types.append("newsletter")
    if COMMUNITY_PATTERNS.search(combined):
        types.append("community")
    if BLOG_PATTERNS.search(combined):
        types.append("blog")

    if not types:
        types.append("unknown")
    return types


def _is_geo_relevant(text):
    """Check if context text relates to GEO/AEO."""
    text_lower = text.lower()
    return any(term in text_lower for term in GEO_TERMS)


def _load_existing_sources():
    """Load already-known sources from config."""
    known = set()
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r") as f:
            config = json.load(f)
        for feed in config.get("news_rss_feeds", []):
            url = feed.get("url", "")
            try:
                domain = urlparse(url).netloc.lower().lstrip("www.")
                known.add(domain)
            except Exception:
                pass
    return known


def run_discovery():
    """Scan enriched insights for new sources."""
    if not os.path.exists(ENRICHED_PATH):
        print("  No enriched data to scan for sources.")
        return []

    with open(ENRICHED_PATH, "r", encoding="utf-8") as f:
        insights = json.load(f)

    known = _load_existing_sources()
    domain_data = defaultdict(lambda: {
        "count": 0,
        "first_seen": "",
        "last_seen": "",
        "types": set(),
        "sample_urls": [],
        "geo_relevant": False,
        "contexts": [],
    })

    for insight in insights:
        text = insight.get("text", "")
        url = insight.get("url", "")
        date = insight.get("post_date", "")

        # Extract domains from the post text
        domains_in_text = _extract_domains(text)

        # Also check the post URL itself
        if url:
            try:
                post_domain = urlparse(url).netloc.lower().lstrip("www.")
                if post_domain and "." in post_domain and post_domain not in SKIP_DOMAINS:
                    domains_in_text.add(post_domain)
            except Exception:
                pass

        for domain in domains_in_text:
            if domain in known:
                continue

            d = domain_data[domain]
            d["count"] += 1

            if not d["first_seen"] or (date and date < d["first_seen"]):
                d["first_seen"] = date
            if not d["last_seen"] or (date and date > d["last_seen"]):
                d["last_seen"] = date

            types = _classify_source(domain, text)
            d["types"].update(types)

            if _is_geo_relevant(text):
                d["geo_relevant"] = True

            if len(d["sample_urls"]) < 3 and url:
                d["sample_urls"].append(url)

            if len(d["contexts"]) < 2:
                snippet = text[:200].strip()
                if snippet:
                    d["contexts"].append(snippet)

    # Build output
    discovered = []
    for domain, data in domain_data.items():
        discovered.append({
            "domain": domain,
            "mention_count": data["count"],
            "first_seen": data["first_seen"],
            "last_seen": data["last_seen"],
            "source_types": sorted(data["types"]),
            "geo_relevant": data["geo_relevant"],
            "sample_urls": data["sample_urls"],
            "contexts": data["contexts"],
            "status": "suggested",
            "discovered_at": datetime.now().isoformat(),
        })

    # Sort by mention count (most mentioned first)
    discovered.sort(key=lambda x: x["mention_count"], reverse=True)

    # Merge with existing discovered sources
    existing = []
    if os.path.exists(OUTPUT_PATH):
        with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
            existing = json.load(f)

    # Keep approved/rejected status from existing
    existing_map = {s["domain"]: s for s in existing}
    merged = []
    seen_domains = set()

    for src in discovered:
        domain = src["domain"]
        if domain in existing_map:
            old = existing_map[domain]
            # Preserve user decisions
            if old.get("status") in ("approved", "rejected"):
                src["status"] = old["status"]
            src["mention_count"] = max(src["mention_count"], old.get("mention_count", 0))
        merged.append(src)
        seen_domains.add(domain)

    # Keep old entries not in new scan
    for old_src in existing:
        if old_src["domain"] not in seen_domains:
            merged.append(old_src)

    os.makedirs("data", exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    suggested = [s for s in merged if s["status"] == "suggested"]
    geo_relevant = [s for s in suggested if s.get("geo_relevant")]

    print(f"  Source discovery: {len(merged)} total, {len(suggested)} suggested, {len(geo_relevant)} GEO-relevant")
    if suggested[:5]:
        print(f"  Top suggested:")
        for s in suggested[:5]:
            geo_tag = " [GEO]" if s.get("geo_relevant") else ""
            print(f"    {s['domain']} ({s['mention_count']} mentions){geo_tag}")

    return merged


if __name__ == "__main__":
    run_discovery()
