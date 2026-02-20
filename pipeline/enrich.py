# enrich.py — Signal extraction, sentiment, entity tagging for GEO Pulse
import json
import os
import re
from datetime import datetime
from collections import defaultdict

SCRAPED_FILES = [
    "data/scraped_reddit.json",
    "data/scraped_hackernews.json",
    "data/scraped_slack.json",
    "data/scraped_producthunt.json",
    "data/scraped_news_rss.json",
    "data/scraped_g2.json",
]
OUTPUT_PATH = "data/enriched_insights.json"
COMPANIES_PATH = "config/companies.json"

# ---------------------------------------------------------------------------
# Load company data
# ---------------------------------------------------------------------------

def _load_companies():
    if not os.path.exists(COMPANIES_PATH):
        return [], {}, set(), set(), set()

    with open(COMPANIES_PATH, "r") as f:
        data = json.load(f)

    all_companies = []
    alias_map = {}  # alias -> canonical name
    own_brand_names = set()
    all_aliases = set()
    context_required_names = set()

    for c in data.get("own_brands", []):
        name = c["name"]
        all_companies.append(c)
        own_brand_names.add(name.lower())
        alias_map[name.lower()] = name
        for a in c.get("aliases", []):
            alias_map[a.lower()] = name
            own_brand_names.add(a.lower())
            all_aliases.add(a.lower())
        if c.get("context_required"):
            context_required_names.add(name)

    for c in data.get("competitors", []):
        name = c["name"]
        all_companies.append(c)
        alias_map[name.lower()] = name
        for a in c.get("aliases", []):
            alias_map[a.lower()] = name
            all_aliases.add(a.lower())
        if c.get("context_required"):
            context_required_names.add(name)

    return all_companies, alias_map, own_brand_names, all_aliases, context_required_names


# ---------------------------------------------------------------------------
# GEO/AEO context terms for relevance gating
# ---------------------------------------------------------------------------

GEO_CONTEXT = [
    "geo", "aeo", "ai search", "llm", "chatgpt", "gemini",
    "perplexity", "claude", "generative engine", "answer engine",
    "ai visibility", "ai overview", "seo", "search optimization",
    "brand visibility", "ai citation", "search generative",
    "content optimization", "structured data", "schema markup",
    "featured snippet", "knowledge panel", "ai recommend",
    "brand mention", "citation", "visibility score",
    "share of voice", "share of answer", "zero click",
    "ai overviews", "searchgpt",
]

# Short GEO terms that need word-boundary matching to avoid false positives
_GEO_SHORT_TERMS = {"geo", "aeo", "seo", "llm"}

# Sources inherently about GEO/AEO tools (company mention alone is valid)
GEO_SOURCES = {"G2", "Product Hunt"}

NOISE_PHRASES = [
    "i am a bot", "this action was performed automatically",
    "automoderator", "this post has been removed",
    "please read the rules", "megathread",
    "check out my channel", "subscribe to my",
    "use my affiliate", "promo code", "discount code",
]

# Hard exclusion: always reject these title patterns
TITLE_BLOCKLIST = re.compile(
    r"\[(dead|flagged|deleted)\]"
    r"|who is hiring"
    r"|who.s hiring"
    r"|ask hn:"
    r"|hiring thread"
    r"|freelancer.*seeking"
    r"|monthly.*job"
    r"|\btesla\b",
    re.I,
)

# Conditional exclusion: reject unless title also contains a GEO term
_CONDITIONAL_BLOCKLIST = re.compile(
    r"\bmicrosoft\b|\bapple\b|\bnetflix\b", re.I
)


def _has_geo_terms(text):
    """Check if text contains any GEO context term (word-boundary safe for short terms)."""
    t = text.lower()
    for term in GEO_CONTEXT:
        if term in _GEO_SHORT_TERMS:
            if re.search(r"\b" + re.escape(term) + r"\b", t):
                return True
        elif term in t:
            return True
    return False

# ---------------------------------------------------------------------------
# Sentiment detection
# ---------------------------------------------------------------------------

POSITIVE_PATTERNS = re.compile(
    r"\b(love|great|amazing|excellent|fantastic|impressed|perfect|best|recommend"
    r"|game.?changer|helpful|powerful|easy to use|well done|solid|smooth"
    r"|happy with|pleased|satisfied|works great|exactly what|finally a)\b", re.I
)

NEGATIVE_PATTERNS = re.compile(
    r"\b(terrible|horrible|awful|worst|hate|frustrated|annoying|broken|useless"
    r"|disappointed|waste|scam|misleading|overpriced|doesn.?t work|not working"
    r"|buggy|slow|inaccurate|unreliable|confusing|clunky|garbage|joke"
    r"|rip.?off|can.?t believe|ridiculous|unacceptable)\b", re.I
)


def _detect_sentiment(text):
    pos = len(POSITIVE_PATTERNS.findall(text))
    neg = len(NEGATIVE_PATTERNS.findall(text))

    if neg > pos and neg >= 2:
        return "negative", "Multiple negative expressions detected"
    if neg > pos:
        return "negative", "Negative language outweighs positive"
    if pos > neg and pos >= 2:
        return "positive", "Multiple positive expressions detected"
    if pos > neg:
        return "positive", "Positive language outweighs negative"
    if pos == neg and pos > 0:
        return "neutral", "Mixed positive and negative signals"
    return "neutral", "No strong sentiment indicators"


# ---------------------------------------------------------------------------
# Entity extraction
# ---------------------------------------------------------------------------

RE_COMPLAINT = re.compile(
    r"\b(problem|issue|bug|broken|frustrated|annoying|can.?t|won.?t|doesn.?t work"
    r"|not working|failed|error|waiting|slow|delay|missing|lack|no way to"
    r"|unable to|wish it|should have|terrible|awful|worst)\b", re.I
)

RE_PRAISE = re.compile(
    r"\b(love|great|amazing|excellent|perfect|best|recommend|impressed"
    r"|game.?changer|helpful|works great|exactly what|solid|smooth|finally)\b", re.I
)

RE_QUESTION = re.compile(
    r"(^|\n)\s*(how do|how can|how to|what is|what are|is there|anyone know"
    r"|has anyone|can someone|does anyone|which tool|what tool|best way to"
    r"|looking for|trying to find|need help|any recommendations)\b", re.I
)

RE_FUNDING = re.compile(
    r"\b(raised|funding|series [a-d]|seed round|venture|valuation|acquired"
    r"|acquisition|ipo|investment|investor|backed by|\$\d+[mk])\b", re.I
)

RE_LAUNCH = re.compile(
    r"\b(launched|launching|announcing|just released|new feature|now available"
    r"|introducing|beta|early access|product hunt|we built|just shipped"
    r"|v[12]\.\d|version \d)\b", re.I
)

RE_COMPARISON = re.compile(
    r"\b(vs\.?|versus|compared to|better than|worse than|alternative to"
    r"|switched from|moving from|replaced|instead of|or should i)\b", re.I
)

# ---------------------------------------------------------------------------
# Signal flags
# ---------------------------------------------------------------------------

RE_BUYER = re.compile(
    r"\b(looking for|evaluating|comparing|trialing|testing|considering"
    r"|signed up|started using|just bought|pricing|free trial|demo"
    r"|worth it|should i use|anyone using)\b", re.I
)

RE_FOUNDER = re.compile(
    r"\b(we built|i built|my startup|our product|founder|co-founder"
    r"|we.re building|we just launched|i.m the creator|our team"
    r"|bootstrapped|yc |y combinator)\b", re.I
)

RE_ANALYST = re.compile(
    r"\b(according to|report|research|study|analysis|survey|data shows"
    r"|market size|tam |gartner|forrester|g2 crowd|analyst"
    r"|venture|investor perspective)\b", re.I
)

RE_FEATURE_REQUEST = re.compile(
    r"\b(wish it|should have|would be nice|need a|looking for a tool that"
    r"|anyone know.*that can|feature request|missing feature|no way to"
    r"|i want|please add|when will|roadmap|planned)\b", re.I
)

# ---------------------------------------------------------------------------
# Source quality scoring
# ---------------------------------------------------------------------------

SOURCE_QUALITY = {
    "G2": 5,
    "Slack": 4,
    "Hacker News": 4,
    "Product Hunt": 3,
    "News": 3,
    "RSS": 3,
    "Reddit": 2,
}


def _source_quality(source):
    for key, score in SOURCE_QUALITY.items():
        if key.lower() in source.lower():
            return score
    return 2


# ---------------------------------------------------------------------------
# Main enrichment
# ---------------------------------------------------------------------------

def enrich_post(post, alias_map, own_brands, context_required_names=None):
    """Enrich a single post with all signals."""
    text = (post.get("text", "") + " " + post.get("title", "")).strip()
    text_lower = text.lower()

    # Pre-check GEO context for context_required validation
    has_geo_context = _has_geo_terms(text)

    # Company mentions
    companies_mentioned = set()
    is_own_brand = False
    for alias, canonical in alias_map.items():
        if len(alias) < 3:
            continue
        # Word boundary match for short names, substring for longer
        matched = False
        if len(alias) <= 4:
            if re.search(r"\b" + re.escape(alias) + r"\b", text_lower):
                matched = True
        else:
            if alias in text_lower:
                matched = True
        if matched:
            # Skip ambiguous names when post lacks GEO context
            if context_required_names and canonical in context_required_names and not has_geo_context:
                continue
            companies_mentioned.add(canonical)
            if alias in own_brands:
                is_own_brand = True

    # Sentiment
    sentiment, sentiment_reason = _detect_sentiment(text)

    # Entity tags
    tags = []
    if companies_mentioned:
        tags.append("company_mention")
    if RE_COMPLAINT.search(text):
        tags.append("complaint")
    if RE_PRAISE.search(text):
        tags.append("praise")
    if RE_QUESTION.search(text):
        tags.append("question")
    if RE_FUNDING.search(text):
        tags.append("funding_news")
    if RE_LAUNCH.search(text):
        tags.append("product_launch")
    if RE_COMPARISON.search(text):
        tags.append("comparison")

    # Signal flags
    is_buyer = bool(RE_BUYER.search(text))
    is_founder = bool(RE_FOUNDER.search(text))
    is_analyst = bool(RE_ANALYST.search(text))
    is_feature_request = bool(RE_FEATURE_REQUEST.search(text))
    is_competitive = bool(RE_COMPARISON.search(text)) and len(companies_mentioned) >= 2

    # Feature mentions (extract specific capabilities discussed)
    feature_keywords = [
        "dashboard", "reporting", "api", "integration", "pricing",
        "accuracy", "citation tracking", "share of voice", "recommendations",
        "workflow", "alerts", "real-time", "historical data", "export",
        "white label", "multi-brand", "custom prompts", "benchmarking",
    ]
    features_mentioned = [f for f in feature_keywords if f in text_lower]

    source = post.get("source", "")
    quality = _source_quality(source)

    return {
        **post,
        "sentiment": sentiment,
        "sentiment_reason": sentiment_reason,
        "companies_mentioned": sorted(companies_mentioned),
        "is_own_brand_mention": is_own_brand,
        "entity_tags": tags,
        "features_mentioned": features_mentioned,
        "is_buyer_voice": is_buyer,
        "is_founder_voice": is_founder,
        "is_analyst_voice": is_analyst,
        "is_feature_request": is_feature_request,
        "is_competitive_intel": is_competitive,
        "source_quality": quality,
    }


def run_enrichment(since_date=None):
    """Load all scraped data, filter for relevance, enrich, deduplicate, save."""
    _, alias_map, own_brands, all_aliases, context_required = _load_companies()
    company_terms = set(alias_map.keys())

    # Load all scraped files
    all_posts = []
    for path in SCRAPED_FILES:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                posts = json.load(f)
            print(f"  {path}: {len(posts)} posts")
            all_posts.extend(posts)

    if not all_posts:
        print("  No scraped data found.")
        return []

    print(f"  Total raw posts: {len(all_posts)}")

    # Pre-dedup by post_id / text prefix
    seen = set()
    unique = []
    for p in all_posts:
        key = p.get("post_id", "") or p.get("text", "")[:100]
        if key and key not in seen:
            seen.add(key)
            unique.append(p)

    print(f"  After pre-dedup: {len(unique)}")

    # ---------------------------------------------------------------
    # Relevance gate (Steps A-C)
    # ---------------------------------------------------------------
    relevant = []
    excluded_hard = 0
    excluded_cond = 0
    excluded_noise = 0
    excluded_no_geo = 0

    for post in unique:
        title = (post.get("title") or "").strip()
        text = (post.get("text", "") + " " + title)

        # (A) Hard exclusion: empty/short titles
        if len(title) < 10:
            excluded_hard += 1
            continue

        # (A) Hard exclusion: blocklisted title patterns (dead, hiring, tesla)
        if TITLE_BLOCKLIST.search(title):
            excluded_hard += 1
            continue

        # (A) Conditional exclusion: microsoft/apple/netflix — require GEO term in title
        if _CONDITIONAL_BLOCKLIST.search(title) and not _has_geo_terms(title):
            excluded_cond += 1
            continue

        # Skip noise phrases
        text_lower = text.lower()
        if any(n in text_lower for n in NOISE_PHRASES):
            excluded_noise += 1
            continue

        # (B) GEO relevance gate
        has_geo = _has_geo_terms(text)
        source = post.get("source", "")
        is_geo_source = source in GEO_SOURCES

        # Exception: G2/Product Hunt articles mentioning a tracked company pass automatically
        if is_geo_source:
            mentions_company = any(
                alias in text_lower for alias in company_terms if len(alias) >= 3
            )
            if mentions_company:
                relevant.append(post)
                continue

        # Otherwise require GEO context terms
        if has_geo:
            relevant.append(post)
        else:
            excluded_no_geo += 1

    print(f"  Excluded: {excluded_hard} hard, {excluded_cond} conditional, "
          f"{excluded_noise} noise, {excluded_no_geo} no GEO context")
    print(f"  Relevant: {len(relevant)} ({len(relevant)*100//max(len(unique),1)}%)")

    # Filter by date if incremental
    if since_date:
        relevant = [p for p in relevant if p.get("post_date", "") >= since_date]
        print(f"  After date filter ({since_date}): {len(relevant)}")

    # Enrich each post (C: context_required handled inside enrich_post)
    enriched = []
    for post in relevant:
        enriched.append(enrich_post(post, alias_map, own_brands, context_required))

    # ---------------------------------------------------------------
    # (D) Final dedup by URL then by title
    # ---------------------------------------------------------------
    by_url = {}
    no_url = []
    for e in enriched:
        url = (e.get("url") or "").strip().rstrip("/")
        if not url:
            no_url.append(e)
            continue
        if url not in by_url:
            by_url[url] = e
        else:
            # Keep the one with more enrichment signals
            new_score = len(e.get("companies_mentioned", [])) + len(e.get("entity_tags", []))
            old_score = len(by_url[url].get("companies_mentioned", [])) + len(by_url[url].get("entity_tags", []))
            if new_score > old_score:
                by_url[url] = e

    # Title dedup for remaining
    seen_titles = set()
    deduped = []
    for e in list(by_url.values()) + no_url:
        title_raw = (e.get("title") or "").strip().lower()
        title_key = re.sub(r"[^a-z0-9]", "", title_raw)[:60]
        if title_key and len(title_key) > 8 and title_key in seen_titles:
            continue
        if title_key and len(title_key) > 8:
            seen_titles.add(title_key)
        deduped.append(e)

    print(f"  After final dedup: {len(deduped)} (removed {len(enriched) - len(deduped)} dupes)")
    enriched = deduped

    # Sort by date
    enriched.sort(key=lambda x: x.get("post_date", ""), reverse=True)

    # Save
    os.makedirs("data", exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(enriched, f, ensure_ascii=False, indent=2)

    # Summary
    sentiments = defaultdict(int)
    tag_counts = defaultdict(int)
    company_counts = defaultdict(int)

    for e in enriched:
        sentiments[e.get("sentiment", "neutral")] += 1
        for tag in e.get("entity_tags", []):
            tag_counts[tag] += 1
        for comp in e.get("companies_mentioned", []):
            company_counts[comp] += 1

    print(f"\n  Enriched: {len(enriched)} insights -> {OUTPUT_PATH}")
    print(f"  Sentiment: {dict(sentiments)}")
    print(f"  Entity tags: {dict(sorted(tag_counts.items(), key=lambda x: -x[1]))}")
    print(f"  Top companies:")
    for comp, cnt in sorted(company_counts.items(), key=lambda x: -x[1])[:10]:
        print(f"    {comp}: {cnt}")

    signals = {
        "buyer_voice": sum(1 for e in enriched if e.get("is_buyer_voice")),
        "founder_voice": sum(1 for e in enriched if e.get("is_founder_voice")),
        "analyst_voice": sum(1 for e in enriched if e.get("is_analyst_voice")),
        "feature_request": sum(1 for e in enriched if e.get("is_feature_request")),
        "competitive_intel": sum(1 for e in enriched if e.get("is_competitive_intel")),
    }
    print(f"  Signal flags: {signals}")

    return enriched


if __name__ == "__main__":
    run_enrichment()
