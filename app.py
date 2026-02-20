# app.py — GEO Pulse: Bloomberg terminal for the GEO/AEO category
import json
import math
import os
from datetime import datetime, timedelta
from collections import defaultdict, Counter

import streamlit as st
import pandas as pd
import altair as alt

st.set_page_config(page_title="GEO Pulse", page_icon="📡", layout="wide")

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

DATA_DIR = "data"
INSIGHTS_PATH = os.path.join(DATA_DIR, "enriched_insights.json")
TRENDS_PATH = os.path.join(DATA_DIR, "trends.json")
SOURCES_PATH = os.path.join(DATA_DIR, "discovered_sources.json")
CLUSTERS_PATH = os.path.join(DATA_DIR, "clusters.json")
COMPANIES_PATH = "config/companies.json"
RUN_LOG_PATH = os.path.join(DATA_DIR, "run_log.json")


@st.cache_data(ttl=300)
def load_insights():
    if os.path.exists(INSIGHTS_PATH):
        with open(INSIGHTS_PATH, "r") as f:
            return json.load(f)
    return []


@st.cache_data(ttl=300)
def load_trends():
    if os.path.exists(TRENDS_PATH):
        with open(TRENDS_PATH, "r") as f:
            return json.load(f)
    return {}


@st.cache_data(ttl=300)
def load_discovered_sources():
    if os.path.exists(SOURCES_PATH):
        with open(SOURCES_PATH, "r") as f:
            return json.load(f)
    return []


@st.cache_data(ttl=600)
def load_companies():
    if os.path.exists(COMPANIES_PATH):
        with open(COMPANIES_PATH, "r") as f:
            return json.load(f)
    return {"own_brands": [], "competitors": []}


def load_run_log():
    if os.path.exists(RUN_LOG_PATH):
        with open(RUN_LOG_PATH, "r") as f:
            return json.load(f)
    return []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SOURCE_ICONS = {
    "Reddit": "💬", "Hacker News": "🟠", "Slack": "💼",
    "Product Hunt": "🚀", "G2": "⭐", "News": "📰", "RSS": "📰",
}

SOURCE_LIST = "Reddit, Hacker News, G2, Product Hunt, Google News, Search Engine Journal, Search Engine Land, Digiday, AdExchanger"


def _source_icon(source):
    for key, icon in SOURCE_ICONS.items():
        if key.lower() in source.lower():
            return icon
    return "📄"


def _sentiment_badge(sentiment):
    return {"positive": "🟢", "negative": "🔴", "neutral": "⚪"}.get(sentiment, "⚪")


def _time_ago(date_str):
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        days = (datetime.now() - dt).days
        if days == 0:
            return "today"
        if days == 1:
            return "1d ago"
        if days < 7:
            return f"{days}d ago"
        if days < 30:
            return f"{days // 7}w ago"
        return f"{days // 30}mo ago"
    except (ValueError, TypeError):
        return ""


def _relevance_score(insight):
    """Composite score: source quality x social signal x recency x enrichment signals."""
    quality = insight.get("source_quality", 2)
    score = insight.get("score", 0)
    comments = insight.get("num_comments", 0)
    social = 1 + math.log1p(min(score, 500) + comments)

    try:
        dt = datetime.strptime(insight.get("post_date", ""), "%Y-%m-%d")
        days_old = max(0, (datetime.now() - dt).days)
        recency = max(0.1, 1 - days_old / 60)
    except (ValueError, TypeError):
        recency = 0.3

    # Boost for enrichment signals
    enrichment = 1.0
    if insight.get("companies_mentioned"):
        enrichment += 0.5 * len(insight["companies_mentioned"])
    if insight.get("entity_tags"):
        enrichment += 0.3 * len(insight["entity_tags"])
    if insight.get("is_competitive_intel"):
        enrichment += 2.0
    if insight.get("is_feature_request"):
        enrichment += 1.0
    if insight.get("is_buyer_voice"):
        enrichment += 0.5

    return quality * social * recency * enrichment


def _relevance_sentence(insight):
    """One-sentence explanation of why this post matters."""
    parts = []
    tags = insight.get("entity_tags", [])
    companies = insight.get("companies_mentioned", [])
    features = insight.get("features_mentioned", [])
    source = insight.get("source", "")

    if insight.get("is_competitive_intel"):
        parts.append(f"Competitive comparison between {', '.join(companies[:3])}")
    elif insight.get("is_feature_request"):
        feat_str = f" ({', '.join(features[:2])})" if features else ""
        parts.append(f"Feature request{feat_str}")
    elif "funding_news" in tags:
        parts.append("Funding or investment signal")
    elif "product_launch" in tags:
        parts.append("New product or feature launch")
    elif "complaint" in tags and companies:
        parts.append(f"User complaint about {companies[0]}")
    elif "praise" in tags and companies:
        parts.append(f"Positive mention of {companies[0]}")
    elif insight.get("is_buyer_voice"):
        parts.append("Buyer evaluating tools")
    elif insight.get("is_founder_voice"):
        parts.append("Founder or builder perspective")
    elif insight.get("is_analyst_voice"):
        parts.append("Analyst or research signal")
    elif companies:
        parts.append(f"Mentions {', '.join(companies[:2])}")

    reason = insight.get("sentiment_reason", "")
    if reason and "No strong" not in reason:
        parts.append(reason.lower())

    return ". ".join(parts[:2]) + "." if parts else ""


def _keywords_for_card(insight):
    """Extract 3-5 display keywords from enrichment data."""
    kws = []
    for tag in insight.get("entity_tags", []):
        kws.append(tag.replace("_", " "))
    for feat in insight.get("features_mentioned", []):
        if feat not in kws:
            kws.append(feat)
    if insight.get("is_buyer_voice") and "buyer voice" not in kws:
        kws.append("buyer voice")
    if insight.get("is_founder_voice") and "founder voice" not in kws:
        kws.append("founder voice")
    return kws[:5]


@st.cache_data(ttl=300)
def _get_new_companies(_insights_json):
    """Pre-compute which companies were first seen in last 7 days."""
    insights_list = json.loads(_insights_json)
    company_oldest = {}
    now = datetime.now()
    for i in insights_list:
        date_str = i.get("post_date", "")
        for comp in i.get("companies_mentioned", []):
            if comp not in company_oldest or (date_str and date_str < company_oldest.get(comp, "")):
                company_oldest[comp] = date_str
    new_set = set()
    for comp, oldest in company_oldest.items():
        try:
            dt = datetime.strptime(oldest, "%Y-%m-%d")
            if (now - dt).days <= 7:
                new_set.add(comp)
        except (ValueError, TypeError):
            pass
    return new_set


CATEGORY_LABELS = {
    "geo_measurement": "GEO/AEO Measurement",
    "ai_attribution": "AI Attribution",
    "seo_suite": "SEO Suite",
    "content_optimization": "Content Optimization",
    "ai_content": "AI Content",
    "brand_monitoring": "Brand Monitoring",
    "competitive_intel": "Competitive Intel",
    "sales_intel": "Sales Intelligence",
}


def _company_positioning(comp_data):
    """One-line positioning from company metadata."""
    cat = comp_data.get("category", "")
    notes = comp_data.get("notes", "")
    if notes:
        return notes
    return CATEGORY_LABELS.get(cat, cat.replace("_", " ").title())


# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

insights = load_insights()
trends = load_trends()
companies_data = load_companies()
discovered = load_discovered_sources()

own_brands = {c["name"] for c in companies_data.get("own_brands", [])}
company_meta = {}
for group in ("own_brands", "competitors"):
    for c in companies_data.get(group, []):
        company_meta[c["name"]] = c

# Source stats for header
skip_domains = {"preview.redd.it", "i.redd.it", "v.redd.it", "sh.reddit.com", "imgur.com"}
approved_sources = [s for s in discovered
                    if s.get("status") == "approved" and s.get("domain", "") not in skip_domains]

# ---------------------------------------------------------------------------
# Sidebar: Ask GEO Pulse
# ---------------------------------------------------------------------------

def _build_data_summary():
    """Build a compact data summary for the LLM context."""
    from collections import Counter as _C
    comp_counts = _C()
    comp_sentiment = defaultdict(lambda: {"pos": 0, "neg": 0, "total": 0})
    tag_counts = _C()
    source_counts = _C()
    week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    this_week = []

    for i in insights:
        for c in i.get("companies_mentioned", []):
            comp_counts[c] += 1
            s = i.get("sentiment", "neutral")
            comp_sentiment[c]["total"] += 1
            if s == "positive":
                comp_sentiment[c]["pos"] += 1
            elif s == "negative":
                comp_sentiment[c]["neg"] += 1
        for t in i.get("entity_tags", []):
            tag_counts[t] += 1
        source_counts[i.get("source", "")] += 1
        if i.get("post_date", "") >= week_ago:
            this_week.append(i)

    # Top complaints this week
    complaints = [i for i in this_week if "complaint" in i.get("entity_tags", [])]
    requests = [i for i in this_week if i.get("is_feature_request")]

    # Rising companies from trends
    rising = []
    fading = []
    if trends:
        for r in trends.get("rising", [])[:5]:
            rising.append(f"{r['name']} (+{r['delta']}%)")
        for r in trends.get("fading", [])[:3]:
            fading.append(f"{r['name']} ({r['delta']}%)")

    summary = f"""DATA SUMMARY (as of {datetime.now().strftime('%Y-%m-%d %H:%M')}):
- Total signals: {len(insights):,} from {len(source_counts)} source types
- This week: {len(this_week)} signals, {len(complaints)} complaints, {len(requests)} feature requests
- Companies tracked: {len(comp_counts)}
- Top mentioned: {', '.join(f'{c} ({n})' for c, n in comp_counts.most_common(10))}
- Sources: {', '.join(f'{s}: {n}' for s, n in source_counts.most_common())}
- Rising: {', '.join(rising) if rising else 'None'}
- Fading: {', '.join(fading) if fading else 'None'}
- Top complaints: {dict(tag_counts)}

SENTIMENT BY COMPANY (top 15):
"""
    for c, _ in comp_counts.most_common(15):
        cs = comp_sentiment[c]
        pos_pct = round(cs["pos"] * 100 / max(cs["total"], 1))
        neg_pct = round(cs["neg"] * 100 / max(cs["total"], 1))
        summary += f"- {c}: {cs['total']} mentions, {pos_pct}% positive, {neg_pct}% negative\n"

    return summary


def _get_starter_questions(active_tab_idx=0):
    """Generate 5 starter questions based on current data and active tab."""
    # Base questions that always work
    base = [
        "What are buyers complaining about most this week?",
        "Which company is gaining the most momentum right now?",
        "What features should we prioritize building next?",
    ]

    # Data-driven additions
    if trends:
        rising = trends.get("rising", [])
        if rising:
            top = rising[0]
            base.append(f"Why is {top['name']} trending up?")

    comp_counts = Counter()
    for i in insights:
        for c in i.get("companies_mentioned", []):
            comp_counts[c] += 1
    top2 = [c for c, _ in comp_counts.most_common(2)]
    if len(top2) >= 2:
        base.append(f"What are people saying about {top2[0]} vs {top2[1]}?")

    if not any("gap" in q.lower() for q in base):
        base.append("What's the biggest gap no tool has solved yet?")

    # Tab-specific biasing
    if active_tab_idx == 1:  # Sentiment
        base.insert(0, "Which tools have the most negative sentiment and why?")
    elif active_tab_idx == 4:  # Roadmap
        base.insert(0, "What product opportunities have the strongest evidence?")
    elif active_tab_idx == 0:  # Live Feed
        base.insert(0, "What's the most important signal from the last 48 hours?")

    return base[:5]


def _get_relevant_posts(query, limit=15):
    """Find the most relevant posts for a given query."""
    query_lower = query.lower()
    scored = []
    for i in insights:
        text = (i.get("text", "") + " " + i.get("title", "")).lower()
        score = 0
        for word in query_lower.split():
            if len(word) > 3 and word in text:
                score += 1
        # Boost for company name matches
        for c in i.get("companies_mentioned", []):
            if c.lower() in query_lower:
                score += 3
        if score > 0:
            scored.append((score, i))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [s[1] for s in scored[:limit]]


with st.sidebar:
    st.markdown("### Ask GEO Pulse")
    st.caption("Ask questions grounded in the actual data.")

    # Initialize chat history
    if "chat_messages" not in st.session_state:
        st.session_state.chat_messages = []
    if "active_tab" not in st.session_state:
        st.session_state.active_tab = 0

    # Starter questions
    starters = _get_starter_questions(st.session_state.get("active_tab", 0))
    if not st.session_state.chat_messages:
        st.caption("Try asking:")
        for idx, q in enumerate(starters):
            if st.button(q, key=f"starter_{idx}", use_container_width=True):
                st.session_state.chat_messages.append({"role": "user", "content": q})
                st.rerun()

    # Display chat history
    for msg in st.session_state.chat_messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # Chat input
    user_input = st.chat_input("Ask about the GEO/AEO market...", key="chat_input")

    if user_input:
        st.session_state.chat_messages.append({"role": "user", "content": user_input})

    # Process the last user message if not yet answered
    if (st.session_state.chat_messages
            and st.session_state.chat_messages[-1]["role"] == "user"):

        from dotenv import load_dotenv
        load_dotenv()
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")

        if not api_key:
            st.session_state.chat_messages.append({
                "role": "assistant",
                "content": "Anthropic API key not configured. Add `ANTHROPIC_API_KEY` to your `.env` file to enable the AI assistant."
            })
            st.rerun()
        else:
            user_q = st.session_state.chat_messages[-1]["content"]

            # Build context
            data_summary = _build_data_summary()
            relevant_posts = _get_relevant_posts(user_q)
            posts_context = ""
            for p in relevant_posts[:10]:
                comps = ", ".join(p.get("companies_mentioned", []))
                posts_context += (
                    f"- [{p.get('source','')}] {p.get('title','')[:100]} "
                    f"| sentiment={p.get('sentiment','')} "
                    f"| companies={comps} "
                    f"| {p.get('post_date','')}\n"
                    f"  \"{p.get('text','')[:200]}\"\n"
                )

            system_prompt = f"""You are GEO Pulse, a market intelligence assistant for the GEO/AEO category.

RULES:
- 3-5 sentences max. Shorter is better.
- Lead with the direct answer. Evidence follows.
- Bullet points only when listing 4+ items.
- Cite numbers: "12 mentions this week" not "several mentions."
- If data is thin, say so: "Only 3 mentions, low confidence."
- No preamble. No "Great question." No "Based on the data I can see." Start with the answer.
- No hedging, no filler, no enthusiasm. Operator-clean.
- End with 2-3 follow-up questions on new lines prefixed with ">>".

{data_summary}

RELEVANT POSTS:
{posts_context}
"""
            try:
                import anthropic
                client = anthropic.Anthropic(api_key=api_key)

                # Build messages (Anthropic format: system is separate, not in messages list)
                messages = []
                for msg in st.session_state.chat_messages[-6:]:
                    messages.append({"role": msg["role"], "content": msg["content"]})

                with st.spinner("Analyzing..."):
                    response = client.messages.create(
                        model="claude-haiku-4-5-20251001",
                        system=system_prompt,
                        messages=messages,
                        max_tokens=800,
                        temperature=0.3,
                    )

                answer = response.content[0].text
                st.session_state.chat_messages.append({"role": "assistant", "content": answer})
                st.rerun()

            except Exception as e:
                st.session_state.chat_messages.append({
                    "role": "assistant",
                    "content": f"Error calling Anthropic: {str(e)}"
                })
                st.rerun()

    # Clear chat button
    if st.session_state.chat_messages:
        if st.button("Clear conversation", key="clear_chat"):
            st.session_state.chat_messages = []
            st.rerun()


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.markdown("# 📡 GEO Pulse — Market Intelligence for the GEO/AEO Category")
st.markdown("Everything happening in the GEO/AEO market, so your team always knows before the competition does.")

# Persistent header bar
runs = load_run_log()
last_run = runs[-1] if runs else {}
last_ts = last_run.get("completed_at", "")
try:
    last_dt = datetime.fromisoformat(last_ts)
    hours_ago = (datetime.now() - last_dt).total_seconds() / 3600
    if hours_ago < 1:
        freshness = "< 1 hour ago"
    elif hours_ago < 24:
        freshness = f"{hours_ago:.0f}h ago"
    else:
        freshness = f"{hours_ago / 24:.1f}d ago"
    fresh_icon = "🟢" if hours_ago < 6 else "🟡"
except (ValueError, TypeError):
    freshness = "unknown"
    fresh_icon = "⚪"

h1, h2, h3, h4 = st.columns(4)
h1.metric("Sources Monitored", f"{len(approved_sources) + 11}",
          help=f"Active scrapers: {SOURCE_LIST}. Plus {len(approved_sources)} auto-approved community sources.")
h2.metric("Signals Ingested", f"{len(insights):,}")
h3.metric("Companies Tracked", f"{len(company_meta)}")
h4.metric(f"{fresh_icon} Last Updated", freshness)

with st.expander("How to use this dashboard"):
    st.markdown("""
**Live Feed** — Real-time stream of every signal from the GEO/AEO market. Filter by company, source, sentiment, or signal type.

**Sentiment Map** — How the market talks about each tool. Sentiment shifts before market share shifts.

**Feature Gap Analysis** — Features the market wants that are missing or poorly executed. Your product research input.

**Company Tracker** — Every player ranked by conversation volume with week-over-week momentum.

**Roadmap Signals** — Strategic product opportunities ranked by market evidence. The competitive gap matrix shows exactly where to build.

Data refreshes every 6 hours from Reddit, Hacker News, G2 reviews, Product Hunt, Google News, and trade press.
""")

if not insights:
    st.info("New signals ingesting. Check back in minutes.")
    st.stop()


# ---------------------------------------------------------------------------
# Tabs (5 tabs — Source Radar removed, merged into header)
# ---------------------------------------------------------------------------

tabs = st.tabs([
    "🔴 Live Feed",
    "😀 Sentiment Map",
    "🔧 Feature Gap Analysis",
    "🏢 Company Tracker",
    "🎯 Roadmap Signals",
])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 1: LIVE FEED
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tabs[0]:
    st.markdown("### Live Feed")
    st.markdown(
        "This feed pulls from Reddit, Hacker News, G2 reviews, Product Hunt, and trade press. "
        "Ranked by relevance, recency, and social signal strength. Use it to stay current on what "
        "practitioners, buyers, and analysts are saying about every tool in the space."
    )

    # Filters
    fc1, fc2, fc3, fc4 = st.columns(4)

    all_companies_in_data = sorted(set(
        c for i in insights for c in i.get("companies_mentioned", [])
    ))
    with fc1:
        filter_company = st.selectbox("Company", ["All"] + all_companies_in_data, key="feed_company")
    with fc2:
        sources_in_data = sorted(set(i.get("source", "") for i in insights))
        filter_source = st.selectbox("Source", ["All"] + sources_in_data, key="feed_source")
    with fc3:
        filter_sentiment = st.selectbox("Sentiment", ["All", "positive", "negative", "neutral"], key="feed_sentiment")
    with fc4:
        signal_options = ["All", "buyer_voice", "founder_voice", "analyst_voice",
                          "feature_request", "competitive_intel", "complaint", "praise",
                          "funding_news", "product_launch"]
        filter_signal = st.selectbox("Signal", signal_options, key="feed_signal")

    # Add Competitor
    with st.expander("Add a new competitor to monitor"):
        new_comp_name = st.text_input("Company name", key="new_comp_name")
        new_comp_aliases = st.text_input("Aliases (comma-separated)", key="new_comp_aliases",
                                         help="e.g. 'acme, acme.ai, acme seo'")
        if st.button("Start Monitoring", key="add_comp_btn"):
            if new_comp_name.strip():
                name = new_comp_name.strip()
                aliases = [a.strip().lower() for a in new_comp_aliases.split(",") if a.strip()]
                if not aliases:
                    aliases = [name.lower()]
                new_entry = {"name": name, "aliases": aliases, "category": "unknown", "url": ""}
                cd = load_companies()
                cd.setdefault("competitors", []).append(new_entry)
                with open(COMPANIES_PATH, "w") as f:
                    json.dump(cd, f, ensure_ascii=False, indent=2)
                st.cache_data.clear()
                st.success(f"Now monitoring **{name}**. Will appear in next pipeline run.")

    # Apply filters
    filtered = insights
    if filter_company != "All":
        filtered = [i for i in filtered if filter_company in i.get("companies_mentioned", [])]
    if filter_source != "All":
        filtered = [i for i in filtered if i.get("source", "") == filter_source]
    if filter_sentiment != "All":
        filtered = [i for i in filtered if i.get("sentiment", "") == filter_sentiment]
    if filter_signal != "All":
        if filter_signal in ("complaint", "praise", "funding_news", "product_launch"):
            filtered = [i for i in filtered if filter_signal in i.get("entity_tags", [])]
        else:
            filtered = [i for i in filtered if i.get(f"is_{filter_signal}")]

    # Sort by relevance score
    filtered.sort(key=lambda x: _relevance_score(x), reverse=True)

    st.caption(f"Showing {min(25, len(filtered))} of {len(filtered)} signals (sorted by relevance)")

    # Render cards
    new_companies = _get_new_companies(json.dumps(insights))
    page_size = 25
    for idx, insight in enumerate(filtered[:page_size]):
        source = insight.get("source", "")
        icon = _source_icon(source)
        sentiment = insight.get("sentiment", "neutral")
        badge = _sentiment_badge(sentiment)
        title = insight.get("title", "")[:120] or insight.get("text", "")[:120]
        companies = insight.get("companies_mentioned", [])
        url = insight.get("url", "")
        date = insight.get("post_date", "")
        time_label = _time_ago(date)

        # Relevance sentence
        rel_sentence = _relevance_sentence(insight)
        # Keywords
        kws = _keywords_for_card(insight)

        # Company badges
        company_badges = ""
        for comp in companies[:4]:
            is_own = comp in own_brands
            new = comp in new_companies
            label = f"**{comp}**" if is_own else comp
            new_badge = " 🆕" if new else ""
            company_badges += f" `{label}{new_badge}`"

        # Keyword tags
        kw_str = " ".join(f"`{k}`" for k in kws)

        # Card
        with st.container(border=True):
            st.markdown(f"{icon} {badge} **{title}**")
            if rel_sentence:
                st.caption(f"_{rel_sentence}_")
            meta_parts = [time_label, source]
            if url:
                meta_parts.append(f"[Source]({url})")
            meta = " · ".join(p for p in meta_parts if p)
            if company_badges:
                meta += f" ·{company_badges}"
            st.caption(meta)
            if kw_str:
                st.markdown(kw_str)

    if len(filtered) > page_size:
        st.caption(f"+ {len(filtered) - page_size} more signals")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 2: SENTIMENT MAP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tabs[1]:
    st.markdown("### Sentiment Map")
    st.markdown(
        "How the market talks about each tool, based on what real users, buyers, and analysts are actually saying. "
        "Positive means people recommend it or praise specific features. Negative means complaints, switching signals, "
        "or warnings to others."
    )
    st.info("Sentiment shifts before market share shifts. A tool going negative is an opportunity.")

    # Build company sentiment data
    company_sentiment = defaultdict(lambda: {"positive": 0, "negative": 0, "neutral": 0,
                                              "total": 0, "quotes": []})
    for i in insights:
        for comp in i.get("companies_mentioned", []):
            s = i.get("sentiment", "neutral")
            company_sentiment[comp][s] += 1
            company_sentiment[comp]["total"] += 1
            if len(company_sentiment[comp]["quotes"]) < 6:
                company_sentiment[comp]["quotes"].append({
                    "text": i.get("text", "")[:250],
                    "sentiment": s,
                    "sentiment_reason": i.get("sentiment_reason", ""),
                    "source": i.get("source", ""),
                    "date": i.get("post_date", ""),
                    "url": i.get("url", ""),
                })

    if not company_sentiment:
        st.info("No company mentions found in data.")
    else:
        sort_by = st.radio("Sort by", ["Most mentioned", "Most negative", "Most positive"],
                           horizontal=True, key="sentiment_sort")

        sorted_companies = sorted(company_sentiment.items(), key=lambda x: x[1]["total"], reverse=True)
        if sort_by == "Most negative":
            # Sort by negative PERCENTAGE, not count
            sorted_companies.sort(
                key=lambda x: x[1]["negative"] / max(x[1]["total"], 1),
                reverse=True
            )
        elif sort_by == "Most positive":
            sorted_companies.sort(
                key=lambda x: x[1]["positive"] / max(x[1]["total"], 1),
                reverse=True
            )

        for comp, data in sorted_companies[:20]:
            total = data["total"]
            pos_pct = round(data["positive"] * 100 / max(total, 1))
            neg_pct = round(data["negative"] * 100 / max(total, 1))
            neu_pct = 100 - pos_pct - neg_pct

            is_own = comp in own_brands
            name_display = f"**{comp}** (own brand)" if is_own else f"**{comp}**"

            with st.expander(f"{name_display} — {total} mentions · 🟢{pos_pct}% 🔴{neg_pct}% ⚪{neu_pct}%"):
                # Sentiment bar
                bar_data = pd.DataFrame([
                    {"Sentiment": "Positive", "Count": data["positive"]},
                    {"Sentiment": "Negative", "Count": data["negative"]},
                    {"Sentiment": "Neutral", "Count": data["neutral"]},
                ])
                chart = alt.Chart(bar_data).mark_bar().encode(
                    x=alt.X("Count:Q"),
                    y=alt.Y("Sentiment:N", sort=["Positive", "Neutral", "Negative"]),
                    color=alt.Color("Sentiment:N", scale=alt.Scale(
                        domain=["Positive", "Negative", "Neutral"],
                        range=["#4CAF50", "#F44336", "#9E9E9E"]
                    )),
                ).properties(height=100)
                st.altair_chart(chart, use_container_width=True)

                # Quotes in card format
                for q in data["quotes"]:
                    badge = _sentiment_badge(q["sentiment"])
                    link = f" · [Source]({q['url']})" if q.get("url") else ""
                    reason = f" · _{q['sentiment_reason']}_" if q.get("sentiment_reason") and "No strong" not in q.get("sentiment_reason", "") else ""
                    st.markdown(f"> {badge} {q['text']}")
                    st.caption(f"{q['source']} · {q['date']}{reason}{link}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 3: FEATURE GAP ANALYSIS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tabs[2]:
    st.markdown("### Feature Gap Analysis")
    st.markdown(
        "Features the market is actively requesting or complaining about across all GEO/AEO tools. "
        "This is your product research, ranked by how often a gap is mentioned and how frustrated "
        "people are about it."
    )

    feature_requests = [i for i in insights if i.get("is_feature_request") or "complaint" in i.get("entity_tags", [])]

    GAP_THEMES = {
        "Measurement & Analytics": ["dashboard", "reporting", "analytics", "metrics", "kpi", "roi",
                                    "tracking", "measurement", "benchmark", "share of voice"],
        "Reporting & Export": ["report", "export", "csv", "pdf", "presentation", "visualization"],
        "Integrations & API": ["api", "integration", "plugin", "connect", "sync", "webhook",
                               "zapier", "slack", "google analytics", "hubspot"],
        "Pricing & Value": ["pricing", "price", "cost", "expensive", "affordable", "free tier",
                            "free plan", "trial", "subscription", "enterprise"],
        "Accuracy & Reliability": ["accuracy", "accurate", "inaccurate", "wrong", "hallucinate",
                                   "false", "misleading", "reliable", "unreliable"],
        "Actionable Guidance": ["recommendation", "suggest", "actionable", "insights",
                                "next steps", "what to do", "optimize", "improve", "specific advice"],
        "Workflow & Automation": ["workflow", "automation", "alert", "notification", "schedule",
                                 "bulk", "team", "collaboration", "permission"],
    }

    # Build gap data with company-level detail
    gap_data = defaultdict(lambda: {
        "mentions": 0, "sentiment": defaultdict(int),
        "companies_with_gap": set(),  # complained about having this feature poorly
        "companies_requested": set(),  # requested for this company
        "quotes": [],
    })

    for i in feature_requests:
        text_lower = (i.get("text", "") + " " + i.get("title", "")).lower()
        companies = i.get("companies_mentioned", [])
        is_complaint = "complaint" in i.get("entity_tags", [])
        sent = i.get("sentiment", "neutral")

        for theme, keywords in GAP_THEMES.items():
            if any(kw in text_lower for kw in keywords):
                gd = gap_data[theme]
                gd["mentions"] += 1
                gd["sentiment"][sent] += 1

                for comp in companies:
                    if is_complaint:
                        gd["companies_with_gap"].add(comp)
                    else:
                        gd["companies_requested"].add(comp)

                if len(gd["quotes"]) < 4:
                    gd["quotes"].append({
                        "text": i.get("text", "")[:250],
                        "sentiment": sent,
                        "companies": companies,
                        "source": i.get("source", ""),
                    })

    if not gap_data:
        st.info("No feature gaps found yet.")
    else:
        # Summary table
        table_rows = []
        sorted_gaps = sorted(gap_data.items(), key=lambda x: x[1]["mentions"], reverse=True)

        for theme, gd in sorted_gaps:
            neg = gd["sentiment"].get("negative", 0)
            pos = gd["sentiment"].get("positive", 0)
            neu = gd["sentiment"].get("neutral", 0)
            total = gd["mentions"]
            neg_pct = round(neg * 100 / max(total, 1))
            gap_companies = sorted(gd["companies_with_gap"])
            requested_by = sorted(gd["companies_requested"])

            table_rows.append({
                "Feature Gap": theme,
                "Mentions": total,
                "Negative %": f"{neg_pct}%",
                "Requested By": ", ".join(requested_by[:5]) or "General",
                "Poorly Executed At": ", ".join(gap_companies[:5]) or "None cited",
            })

        st.dataframe(pd.DataFrame(table_rows), use_container_width=True, hide_index=True)

        # Detail expanders
        st.markdown("#### Detail")
        for theme, gd in sorted_gaps:
            if gd["mentions"] < 2:
                continue
            neg = gd["sentiment"].get("negative", 0)
            pos = gd["sentiment"].get("positive", 0)

            with st.expander(f"**{theme}** — {gd['mentions']} mentions · 🔴{neg} negative · 🟢{pos} positive"):
                gap_comps = sorted(gd["companies_with_gap"])
                req_comps = sorted(gd["companies_requested"])
                if gap_comps:
                    st.markdown(f"**Poorly executed at:** {', '.join(gap_comps)}")
                if req_comps:
                    st.markdown(f"**Requested by users of:** {', '.join(req_comps)}")

                for q in gd["quotes"]:
                    badge = _sentiment_badge(q["sentiment"])
                    comp_str = ", ".join(q["companies"]) if q["companies"] else ""
                    st.markdown(f"> {badge} {q['text']}")
                    meta = q["source"]
                    if comp_str:
                        meta += f" · {comp_str}"
                    st.caption(meta)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 4: COMPANY TRACKER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tabs[3]:
    st.markdown("### Company Tracker")
    st.markdown(
        "Every company in the GEO/AEO space ranked by how much the market is talking about them. "
        "Velocity shows momentum. Rising means gaining attention, falling means losing it."
    )

    # Build company stats
    company_stats = defaultdict(lambda: {
        "mentions_total": 0, "mentions_this_week": 0,
        "pos": 0, "neg": 0, "neu": 0,
        "latest_date": "", "latest_title": "", "latest_url": "",
        "is_own_brand": False,
    })

    now = datetime.now()
    week_ago = (now - timedelta(days=7)).strftime("%Y-%m-%d")

    for i in insights:
        for comp in i.get("companies_mentioned", []):
            cs = company_stats[comp]
            cs["mentions_total"] += 1
            date = i.get("post_date", "")
            if date >= week_ago:
                cs["mentions_this_week"] += 1
            if date > cs["latest_date"]:
                cs["latest_date"] = date
                cs["latest_title"] = i.get("title", "")[:70] or i.get("text", "")[:70]
                cs["latest_url"] = i.get("url", "")

            s = i.get("sentiment", "neutral")
            cs[{"positive": "pos", "negative": "neg"}.get(s, "neu")] += 1
            cs["is_own_brand"] = comp in own_brands

    # WoW delta from trends
    wow_deltas = {}
    if trends:
        for name, d in trends.get("company_trends", {}).items():
            wow_deltas[name] = d.get("delta_pct", 0)

    if not company_stats:
        st.info("No company mentions found.")
    else:
        rows = []
        for comp, cs in sorted(company_stats.items(), key=lambda x: x[1]["mentions_total"], reverse=True):
            total = cs["mentions_total"]
            pos_pct = round(cs["pos"] * 100 / max(total, 1))
            neg_pct = round(cs["neg"] * 100 / max(total, 1))
            delta = wow_deltas.get(comp, 0)

            # Color indicator for delta
            if delta > 20:
                delta_display = f"🟢 +{delta:.0f}%"
            elif delta < -20:
                delta_display = f"🔴 {delta:.0f}%"
            elif delta != 0:
                delta_display = f"⚪ {delta:+.0f}%"
            else:
                delta_display = "⚪ —"

            is_new = cs["mentions_this_week"] == cs["mentions_total"] and cs["mentions_total"] <= 3
            new_badge = " 🆕" if is_new else ""
            own_badge = " ⭐" if cs["is_own_brand"] else ""

            # Positioning from company metadata
            meta = company_meta.get(comp, {})
            positioning = _company_positioning(meta) if meta else ""

            rows.append({
                "Company": f"{comp}{own_badge}{new_badge}",
                "Category": positioning,
                "This Week": cs["mentions_this_week"],
                "Total": total,
                "Velocity": delta_display,
                "Sentiment": f"🟢{pos_pct}% 🔴{neg_pct}%",
                "Latest": cs["latest_title"],
                "Link": cs["latest_url"],
            })

        df = pd.DataFrame(rows)
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            height=600,
            column_config={
                "Link": st.column_config.LinkColumn("Latest Source", display_text="Open"),
            },
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 5: ROADMAP SIGNALS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tabs[4]:
    st.markdown("### Roadmap Signals")
    st.markdown(
        "Strategic product opportunities ranked by market evidence. Each signal represents a gap, "
        "something buyers are asking for that no current tool does well. The higher the confidence "
        "score, the more evidence exists across multiple independent sources."
    )

    st.info("These are the features ProRata should evaluate for Gist, ranked by market demand and competitive gap.")

    # Opportunity themes
    OPPORTUNITY_THEMES = {
        "Real-time Tracking": ["real-time", "real time", "live tracking", "live monitoring",
                                        "instant", "continuous"],
        "Multi-LLM Coverage": ["multiple llm", "all llm", "perplexity and chatgpt", "cross-platform",
                               "every ai", "all ai", "multi-model"],
        "Actionable Recs": ["actionable", "what to do", "next steps", "recommendations",
                                       "how to improve", "specific advice"],
        "ROI Measurement": ["roi", "return on investment", "revenue impact", "attribution",
                           "prove value", "business impact", "conversion"],
        "Historical Trends": ["historical", "trend", "over time", "change over", "compare week",
                             "month over month", "trajectory"],
        "Comp. Benchmarking": ["benchmark", "compare to competitor", "competitive",
                                    "industry average", "how do we compare", "vs competitor"],
        "Content Guidance": ["what to write", "content recommendations", "topic suggestion",
                                         "content gap", "optimization guide"],
        "Brand Safety": ["brand safety", "misinformation", "hallucination about",
                              "wrong information", "incorrect", "ai says wrong"],
        "Integrations": ["integrate", "integration", "connect to", "plugin",
                                           "works with", "api", "google analytics", "hubspot"],
        "Prompt Influence": ["influence prompt", "shape answer", "control what ai says",
                           "optimize prompt", "prompt engineering", "answer shaping"],
    }

    # Build opportunity data with per-company tracking
    opportunity_data = defaultdict(lambda: {
        "complaints": 0, "requests": 0, "praise": 0,
        "evidence": 0, "companies_tried": set(),
        "companies_praised": set(), "companies_complained": set(),
        "quotes": [], "confidence": 0,
    })

    for i in insights:
        text_lower = (i.get("text", "") + " " + i.get("title", "")).lower()
        tags = i.get("entity_tags", [])
        sentiment = i.get("sentiment", "")
        companies = i.get("companies_mentioned", [])

        for opp, keywords in OPPORTUNITY_THEMES.items():
            if any(kw in text_lower for kw in keywords):
                od = opportunity_data[opp]
                od["evidence"] += 1
                if "complaint" in tags:
                    od["complaints"] += 1
                    for c in companies:
                        od["companies_complained"].add(c)
                if i.get("is_feature_request"):
                    od["requests"] += 1
                if "praise" in tags and sentiment == "positive":
                    od["praise"] += 1
                    for c in companies:
                        od["companies_praised"].add(c)
                for c in companies:
                    od["companies_tried"].add(c)
                if len(od["quotes"]) < 4:
                    od["quotes"].append({
                        "text": i.get("text", "")[:250],
                        "sentiment": sentiment,
                        "source": i.get("source", ""),
                        "companies": companies,
                    })

    # Score
    for opp, od in opportunity_data.items():
        pain = od["complaints"] + od["requests"]
        satisfaction = od["praise"]
        od["confidence"] = round(min(pain / max(pain + satisfaction, 1) * 100, 100))

    # --- COMPETITIVE GAP MATRIX ---
    st.markdown("#### Competitive Gap Matrix")
    st.caption("Which tools address which market needs. Green = has it (praised). Yellow = attempted (complaints exist). Red = not addressed.")

    # Top companies by mentions for matrix columns
    comp_mention_counts = Counter()
    for i in insights:
        for c in i.get("companies_mentioned", []):
            comp_mention_counts[c] += 1
    top_companies = [c for c, _ in comp_mention_counts.most_common(10)]

    # Features with enough evidence
    active_opps = {opp: od for opp, od in opportunity_data.items() if od["evidence"] >= 2}
    sorted_opp_names = sorted(active_opps.keys(),
                               key=lambda x: active_opps[x]["evidence"], reverse=True)

    if sorted_opp_names and top_companies:
        matrix_rows = []
        for opp in sorted_opp_names:
            od = active_opps[opp]
            row = {"Feature": opp}
            for comp in top_companies:
                if comp in od["companies_praised"]:
                    row[comp] = "🟢"
                elif comp in od["companies_complained"]:
                    row[comp] = "🟡"
                elif comp in od["companies_tried"]:
                    row[comp] = "🟡"
                else:
                    row[comp] = "🔴"
            matrix_rows.append(row)

        matrix_df = pd.DataFrame(matrix_rows)
        st.dataframe(matrix_df, use_container_width=True, hide_index=True)
    else:
        st.caption("Not enough data to build the matrix yet.")

    # --- RANKED SIGNAL CARDS ---
    st.markdown("#### Opportunity Signals")
    if not opportunity_data:
        st.info("Not enough data to identify opportunities. Run more scrapes.")
    else:
        sorted_opps = sorted(opportunity_data.items(),
                             key=lambda x: (x[1]["confidence"], x[1]["evidence"]), reverse=True)

        for opp, od in sorted_opps:
            if od["evidence"] < 2:
                continue

            companies = sorted(od["companies_tried"])
            conf = od["confidence"]
            conf_color = "🔴" if conf >= 70 else ("🟡" if conf >= 40 else "🟢")

            asked = od["complaints"] + od["requests"]
            tried = len(od["companies_tried"])
            neg_impl = od["complaints"]

            with st.expander(
                f"{conf_color} **{opp}** — {asked} people asked · "
                f"{tried} tools tried · {neg_impl} negative reviews · "
                f"Confidence: {conf}%"
            ):
                praised = sorted(od["companies_praised"])
                complained = sorted(od["companies_complained"])
                if praised:
                    st.markdown(f"**Praised at:** {', '.join(praised)}")
                if complained:
                    st.markdown(f"**Complaints about:** {', '.join(complained)}")
                st.markdown(f"**Praise for existing solutions:** {od['praise']} (low = bigger gap)")

                for q in od["quotes"]:
                    badge = _sentiment_badge(q["sentiment"])
                    comp_str = ", ".join(q["companies"]) if q["companies"] else ""
                    st.markdown(f"> {badge} {q['text']}")
                    meta = q["source"]
                    if comp_str:
                        meta += f" · {comp_str}"
                    st.caption(meta)


# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------

st.divider()
st.caption("Updated every 6 hours from Reddit, Hacker News, G2, Product Hunt, trade press, and internal sources.")
