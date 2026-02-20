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

SOURCE_LIST = "Reddit, Hacker News, G2, Product Hunt, Google News, Search Engine Journal, Search Engine Land, Digiday, AdExchanger"


def _source_badge(source):
    """Plain text source label for pill display."""
    for key in ("Reddit", "Hacker News", "Slack", "Product Hunt", "G2", "News", "RSS"):
        if key.lower() in source.lower():
            return key
    return source.strip() or "Source"


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
# Display-level relevance and age filters
# ---------------------------------------------------------------------------

_GEO_DISPLAY_TERMS = [
    "geo", "aeo", "generative engine", "answer engine",
    "ai search", "ai visibility", "ai answer", "ai citation", "ai overview",
    "brand visibility", "share of voice", "share of answer",
    "llm optimization", "llm brand", "llm monitoring",
    "perplexity", "chatgpt", "searchgpt", "gemini",
    "seo", "content optimization", "structured data", "schema markup",
    "zero click", "ai overviews",
]

_MAX_AGE_DAYS = 730  # 24 months


def _is_display_relevant(insight):
    """Require GEO context for display. Company mention alone is insufficient."""
    text = (insight.get("text", "") + " " + insight.get("title", "")).lower()
    has_context = any(term in text for term in _GEO_DISPLAY_TERMS)
    if has_context:
        return True
    has_companies = bool(insight.get("companies_mentioned"))
    source = insight.get("source", "")
    if has_companies and source in ("G2", "Product Hunt"):
        return True
    return False


def _within_age_limit(insight):
    """Exclude articles older than 24 months."""
    try:
        dt = datetime.strptime(insight.get("post_date", ""), "%Y-%m-%d")
        cutoff = datetime.now() - timedelta(days=_MAX_AGE_DAYS)
        return dt >= cutoff
    except (ValueError, TypeError):
        return False


# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

insights = [i for i in load_insights()
            if _is_display_relevant(i) and _within_age_limit(i)]
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


_ASK_AI_RENDERED_ABOVE_TABS = True  # Ask GEO Pulse lives above the tab bar


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

**Competitors** — One card per competitor with momentum, sentiment, and top signals this week. Expand for more.

**Roadmap** — Competitive gap matrix and ranked product opportunities with supporting evidence.

Data refreshes every 6 hours from Reddit, Hacker News, G2 reviews, Product Hunt, Google News, and trade press.
""")

def _data_missing():
    """Check if core data files are missing or empty."""
    if not os.path.exists(INSIGHTS_PATH):
        return True
    try:
        size = os.path.getsize(INSIGHTS_PATH)
        return size < 10  # empty json "[]" is 2 bytes
    except OSError:
        return True


def _run_pipeline_subprocess():
    """Run the pipeline as a subprocess and reload data on completion."""
    import subprocess
    import sys
    env = os.environ.copy()
    env["PYTHONPATH"] = os.path.dirname(os.path.abspath(__file__))
    result = subprocess.run(
        [sys.executable, "run_pipeline.py"],
        cwd=os.path.dirname(os.path.abspath(__file__)),
        env=env,
        capture_output=True, text=True, timeout=300,
    )
    return result.returncode == 0, result.stderr


# Auto-initialize on first load if data is missing
if _data_missing():
    if "pipeline_ran" not in st.session_state:
        with st.status("First run detected. Initializing data pipeline...", expanded=True) as status:
            st.write("Scraping Reddit, Hacker News, G2, Product Hunt, News RSS...")
            ok, err = _run_pipeline_subprocess()
            if ok:
                st.session_state.pipeline_ran = True
                status.update(label="Pipeline complete. Reloading...", state="complete")
                st.cache_data.clear()
                st.rerun()
            else:
                st.session_state.pipeline_ran = "failed"
                status.update(label="Pipeline failed.", state="error")
                st.code(err[-500:] if err else "No error output captured.")

    # Manual fallback button
    if not insights:
        st.warning("No data available. The pipeline may have failed or is still running.")
        if st.button("Initialize Data", type="primary"):
            st.session_state.pop("pipeline_ran", None)
            st.rerun()
        st.stop()
elif not insights:
    st.info("New signals ingesting. Check back in minutes.")
    st.stop()


# ---------------------------------------------------------------------------
# Ask GEO Pulse (above tabs, matching SignalSynth-GEO layout)
# ---------------------------------------------------------------------------

st.markdown("### Ask GEO Pulse")
st.caption("Ask any question about GEO/AEO market signals and get a data-grounded intelligence brief.")

from dotenv import load_dotenv
load_dotenv()
_anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")

if not _anthropic_key:
    st.warning("Anthropic API key not configured. Add `ANTHROPIC_API_KEY` to your `.env` file to enable AI Q&A.")
else:
    import re as _re

    if "qa_messages" not in st.session_state:
        st.session_state["qa_messages"] = []
    if "qa_submit" not in st.session_state:
        st.session_state["qa_submit"] = ""
    if "qa_sources_map" not in st.session_state:
        st.session_state["qa_sources_map"] = {}

    def _parse_response(raw, sources_map):
        """Split body, build clickable sources from map, extract follow-ups."""
        lines = raw.split("\n")
        body_lines = []
        followups = []
        in_sources = False
        for line in lines:
            stripped = line.strip()
            # Follow-up lines: >> question text
            if stripped.startswith(">>"):
                q = _re.sub(r'^>+\s*', '', stripped).strip()
                if q:
                    followups.append(q)
                continue
            # Detect start of Sources section and skip LLM-generated source lines
            if stripped.lower().startswith("**sources**") or stripped.lower() == "sources":
                in_sources = True
                continue
            if in_sources and _re.match(r'^\[S\d+\]', stripped):
                continue
            if in_sources and stripped == "":
                continue
            if in_sources and stripped:
                in_sources = False
            body_lines.append(line)

        body = "\n".join(body_lines).rstrip()

        # Build our own clickable sources block from the stored map
        cited = sorted(set(_re.findall(r'\[S(\d+)\]', body)), key=int)
        source_lines = []
        for num in cited:
            sid = f"S{num}"
            s = sources_map.get(sid)
            if s and s.get("url"):
                source_lines.append(f"\\[{sid}\\] [{s['title']}]({s['url']}) ({s['source']})")
            elif s:
                source_lines.append(f"\\[{sid}\\] {s['title']} ({s['source']})")

        if source_lines:
            body += "\n\n**Sources**\n\n" + "\n\n".join(source_lines)

        return body, followups

    def _do_submit(question):
        """Set the question to be processed on next rerun."""
        st.session_state["qa_submit"] = question

    # --- Input form (Enter key submits via form) ---
    with st.form("qa_form", clear_on_submit=True):
        user_question = st.text_input(
            "Ask a question",
            placeholder="e.g., What are buyers complaining about most this week?",
        )
        submitted = st.form_submit_button("Ask AI", type="primary")
    if submitted and user_question.strip():
        st.session_state["qa_submit"] = user_question.strip()

    # --- Starter question pills (below input, only when no chat history) ---
    if not st.session_state["qa_messages"] and not st.session_state["qa_submit"]:
        starters = _get_starter_questions()
        st.caption("Try asking:")
        cols = st.columns(len(starters))
        for idx, q in enumerate(starters):
            with cols[idx]:
                st.button(q, key=f"starter_{idx}", on_click=_do_submit, args=(q,), use_container_width=True)

    # --- Process pending question ---
    question_to_ask = st.session_state.get("qa_submit", "")
    if question_to_ask:
        st.session_state["qa_submit"] = ""
        st.session_state["qa_messages"].append({"role": "user", "content": question_to_ask})

        # Build context with source IDs
        data_summary = _build_data_summary()
        relevant_posts = _get_relevant_posts(question_to_ask)
        posts_context = ""
        sources_ref = ""
        sources_map = {}
        for idx, p in enumerate(relevant_posts[:10], 1):
            sid = f"S{idx}"
            comps = ", ".join(p.get("companies_mentioned", []))
            title = p.get("title", "")[:100] or p.get("text", "")[:60]
            url = p.get("url", "")
            source = p.get("source", "")
            posts_context += (
                f"- [{sid}] [{source}] {title} "
                f"| sentiment={p.get('sentiment','')} "
                f"| companies={comps} "
                f"| {p.get('post_date','')}\n"
                f"  \"{p.get('text','')[:200]}\"\n"
            )
            sources_ref += f"[{sid}] {title} ({source}) {url}\n"
            sources_map[sid] = {"title": title, "source": source, "url": url}

        st.session_state["qa_sources_map"] = sources_map

        system_prompt = f"""You are GEO Pulse, a market intelligence assistant for the GEO/AEO category. ProRata/Gist is the user's own product.

Your response must be boardroom-ready: concise, specific, and grounded only in provided data.
If evidence is weak, explicitly say so.

Format your answer exactly with these headings:
1) **Executive answer** (2-3 sentences, direct answer first)
2) **What the signals show** (3-6 bullets with inline citations [S1], [S2] referencing sources below)
3) **Implications for ProRata/Gist** (2-3 bullets, what this means for the product)
4) **Recommended actions** (2-4 numbered actions with owner: Product, Engineering, GTM, or Leadership)
5) **Confidence & gaps** (1-2 bullets on evidence strength and what's missing)

Do NOT include a Sources section. Sources will be rendered separately.

Use [S1], [S2] etc. inline to cite the signals below.

Rules:
- Never invent facts not present in the provided signals.
- Cite numbers: "12 mentions" not "several."
- No preamble. No "Great question." Start with the executive answer.
- No hedging, no filler, no enthusiasm. Operator-clean.
- Keep total response under ~350 words unless explicitly asked for more.
- End with 2-3 follow-up questions on new lines prefixed with ">>".

DATASET SUMMARY:
{data_summary}

RELEVANT SIGNALS:
{posts_context}

SOURCE REFERENCE (for [S1] etc. citations):
{sources_ref}
"""
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=_anthropic_key)

            messages = []
            for msg in st.session_state["qa_messages"][-6:]:
                messages.append({"role": msg["role"], "content": msg["content"]})

            with st.spinner("Searching signals and generating brief..."):
                response = client.messages.create(
                    model="claude-haiku-4-5-20251001",
                    system=system_prompt,
                    messages=messages,
                    max_tokens=1200,
                    temperature=0.3,
                )

            answer = response.content[0].text
            st.session_state["qa_messages"].append({"role": "assistant", "content": answer})
        except Exception as e:
            st.session_state["qa_messages"].append({"role": "assistant", "content": f"Error: {e}"})
        st.rerun()

    # --- Render chat history ---
    if st.session_state.get("qa_messages"):
        with st.expander("AI Q&A responses", expanded=True):
            for msg in st.session_state["qa_messages"]:
                with st.chat_message(msg["role"]):
                    if msg["role"] == "assistant":
                        body, _ = _parse_response(msg["content"], st.session_state.get("qa_sources_map", {}))
                        st.markdown(body)
                    else:
                        st.markdown(msg["content"])

            # Follow-up question buttons from the last assistant message
            last_assistant = None
            for msg in reversed(st.session_state["qa_messages"]):
                if msg["role"] == "assistant":
                    last_assistant = msg["content"]
                    break
            if last_assistant:
                _, followups = _parse_response(last_assistant, {})
                if followups:
                    st.markdown("**Follow-up questions:**")
                    for fidx, fq in enumerate(followups):
                        st.button(fq, key=f"followup_{fidx}", on_click=_do_submit, args=(fq,))

            if st.button("Clear chat", key="clear_qa"):
                st.session_state["qa_messages"] = []
                st.session_state["qa_sources_map"] = {}
                st.rerun()


# ---------------------------------------------------------------------------
# Tabs (3 tabs)
# ---------------------------------------------------------------------------

tabs = st.tabs([
    "🔴 Live Feed",
    "🏢 Competitors",
    "🎯 Roadmap",
])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 1: LIVE FEED
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tabs[0]:
    st.markdown("### Live Feed")
    st.markdown(
        "Real-time stream from Reddit, Hacker News, G2, Product Hunt, and trade press. "
        "Ranked by relevance, recency, and social signal strength."
    )

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

    filtered = [i for i in filtered if _relevance_sentence(i)]
    filtered.sort(key=lambda x: _relevance_score(x), reverse=True)
    st.caption(f"Showing {min(25, len(filtered))} of {len(filtered)} signals (sorted by relevance)")

    new_companies = _get_new_companies(json.dumps(insights))
    page_size = 25
    for idx, insight in enumerate(filtered[:page_size]):
        source = insight.get("source", "")
        source_label = _source_badge(source)
        title = insight.get("title", "")[:120] or insight.get("text", "")[:120]
        companies = insight.get("companies_mentioned", [])
        url = insight.get("url", "")
        date = insight.get("post_date", "")
        time_label = _time_ago(date)
        rel_sentence = _relevance_sentence(insight)
        kws = _keywords_for_card(insight)

        company_badges = ""
        for comp in companies[:4]:
            is_own = comp in own_brands
            new = comp in new_companies
            label = f"**{comp}**" if is_own else comp
            new_badge = " 🆕" if new else ""
            company_badges += f" `{label}{new_badge}`"
        kw_str = " ".join(f"`{k}`" for k in kws)

        with st.container(border=True):
            st.markdown(f"`{source_label}` **{title}**")
            st.caption(f"_{rel_sentence}_")
            meta_parts = [time_label]
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
# TAB 2: COMPETITORS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tabs[1]:
    st.markdown("### Competitors")
    st.markdown(
        "One card per competitor. Momentum, sentiment, and the most relevant signals this week."
    )

    # Build per-company data
    now = datetime.now()
    week_ago_str = (now - timedelta(days=7)).strftime("%Y-%m-%d")

    comp_data_map = defaultdict(lambda: {
        "total": 0, "this_week": 0,
        "pos": 0, "neg": 0, "neu": 0,
        "signals": [],  # all signals for this company, scored
    })

    for i in insights:
        for comp in i.get("companies_mentioned", []):
            cd = comp_data_map[comp]
            cd["total"] += 1
            s = i.get("sentiment", "neutral")
            cd[{"positive": "pos", "negative": "neg"}.get(s, "neu")] += 1
            date = i.get("post_date", "")
            if date >= week_ago_str:
                cd["this_week"] += 1
            cd["signals"].append(i)

    # WoW deltas
    wow_deltas = {}
    if trends:
        for name, d in trends.get("company_trends", {}).items():
            wow_deltas[name] = d.get("delta_pct", 0)

    # Sort by total mentions
    sorted_comps = sorted(comp_data_map.items(), key=lambda x: x[1]["total"], reverse=True)

    for comp, cd in sorted_comps:
        total = cd["total"]
        if total < 2:
            continue

        pos_pct = round(cd["pos"] * 100 / max(total, 1))
        neg_pct = round(cd["neg"] * 100 / max(total, 1))
        delta = wow_deltas.get(comp, 0)

        # Momentum indicator
        if delta > 20:
            momentum = f"🟢 +{delta:.0f}% WoW"
        elif delta < -20:
            momentum = f"🔴 {delta:.0f}% WoW"
        elif delta != 0:
            momentum = f"⚪ {delta:+.0f}% WoW"
        else:
            momentum = "⚪ Stable"

        is_own = comp in own_brands
        meta = company_meta.get(comp, {})
        positioning = _company_positioning(meta) if meta else ""
        own_tag = " ⭐ own brand" if is_own else ""

        with st.container(border=True):
            # Header row
            hc1, hc2, hc3 = st.columns([3, 2, 2])
            with hc1:
                comp_url = meta.get("url", "") if meta else ""
                site_link = f" · [Visit site]({comp_url})" if comp_url else ""
                st.markdown(f"**{comp}**{own_tag}{site_link}")
                if positioning:
                    st.caption(positioning)
            with hc2:
                st.markdown(f"{momentum}")
            with hc3:
                neu_pct = 100 - pos_pct - neg_pct
                st.markdown(f"{pos_pct}% positive · {neg_pct}% negative · {neu_pct}% neutral · {total} mentions")

            # Top 3 most relevant signals this week
            week_signals = [s for s in cd["signals"] if s.get("post_date", "") >= week_ago_str]
            if not week_signals:
                week_signals = cd["signals"]
            week_signals.sort(key=lambda x: _relevance_score(x), reverse=True)

            top_signals = [s for s in week_signals if _relevance_sentence(s)][:3]
            for sig in top_signals:
                sig_title = sig.get("title", "")[:100] or sig.get("text", "")[:100]
                sig_url = sig.get("url", "")
                sig_source = sig.get("source", "")
                sig_source_label = _source_badge(sig_source)
                sig_reason = _relevance_sentence(sig)

                headline = f"[{sig_title}]({sig_url})" if sig_url else sig_title
                st.markdown(f"  `{sig_source_label}` {headline}")
                st.caption(f"  _{sig_reason}_ · {_time_ago(sig.get('post_date', ''))}")

            # Show more expander
            remaining = [s for s in week_signals[3:] if _relevance_sentence(s)][:12]
            if remaining:
                with st.expander(f"Show {len(remaining)} more signals"):
                    for sig in remaining:
                        sig_title = sig.get("title", "")[:100] or sig.get("text", "")[:100]
                        sig_url = sig.get("url", "")
                        sig_source = sig.get("source", "")
                        sig_source_label = _source_badge(sig_source)
                        sig_reason = _relevance_sentence(sig)

                        headline = f"[{sig_title}]({sig_url})" if sig_url else sig_title
                        st.markdown(f"`{sig_source_label}` {headline}")
                        st.caption(f"_{sig_reason}_ · {_time_ago(sig.get('post_date', ''))}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 3: ROADMAP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tabs[2]:
    st.markdown("### Roadmap")
    st.markdown(
        "Product opportunities ranked by market evidence. The gap matrix shows who has what. "
        "Below it: what to build, why, and the signals backing each recommendation."
    )

    FEATURE_TOOLTIPS = {
        "Real-time Tracking": "Monitoring brand mentions in AI answers as they happen, not batch-processed hours later.",
        "Multi-LLM Coverage": "Tracking visibility across ChatGPT, Perplexity, Gemini, Claude, and other AI platforms simultaneously.",
        "Actionable Recs": "Specific, concrete suggestions for what to change (content, schema, links) to improve AI visibility.",
        "ROI Measurement": "Connecting AI visibility metrics to business outcomes like traffic, leads, and revenue.",
        "Historical Trends": "Tracking how brand visibility in AI answers changes over weeks and months.",
        "Comp. Benchmarking": "Comparing your AI visibility against specific competitors on the same queries.",
        "Content Guidance": "AI-driven recommendations for what topics to write about and how to structure content for AI citations.",
        "Brand Safety": "Detecting when AI platforms give incorrect, outdated, or harmful information about your brand.",
        "Integrations": "Connecting GEO data to existing tools like Google Analytics, HubSpot, Slack, or BI dashboards.",
        "Prompt Influence": "Techniques and tools for shaping how AI models reference and describe your brand.",
    }

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

    # Build opportunity data with per-company tracking and full signal refs
    opportunity_data = defaultdict(lambda: {
        "complaints": 0, "requests": 0, "praise": 0,
        "evidence": 0, "companies_tried": set(),
        "companies_praised": set(), "companies_complained": set(),
        "signals": [], "confidence": 0,
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
                od["signals"].append(i)

    for opp, od in opportunity_data.items():
        pain = od["complaints"] + od["requests"]
        satisfaction = od["praise"]
        od["confidence"] = round(min(pain / max(pain + satisfaction, 1) * 100, 100))

    # --- COMPETITIVE GAP MATRIX ---
    st.markdown("#### Competitive Gap Matrix")
    st.caption("🟢 Has it (praised) · 🟡 Attempted (complaints exist) · 🔴 Not addressed")

    comp_mention_counts = Counter()
    for i in insights:
        for c in i.get("companies_mentioned", []):
            comp_mention_counts[c] += 1
    top_companies = [c for c, _ in comp_mention_counts.most_common(10)]

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

        st.dataframe(pd.DataFrame(matrix_rows), use_container_width=True, hide_index=True)
    else:
        st.caption("Not enough data to build the matrix yet.")

    # --- RANKED: WHAT TO BUILD ---
    st.markdown("#### What to Build")
    sorted_opps = sorted(opportunity_data.items(),
                         key=lambda x: (x[1]["confidence"], x[1]["evidence"]), reverse=True)

    for opp, od in sorted_opps:
        if od["evidence"] < 2:
            continue

        conf = od["confidence"]
        conf_color = "🔴" if conf >= 70 else ("🟡" if conf >= 40 else "🟢")
        asked = od["complaints"] + od["requests"]
        praised_comps = sorted(od["companies_praised"])
        complained_comps = sorted(od["companies_complained"])
        no_coverage = [c for c in top_companies
                       if c not in od["companies_praised"]
                       and c not in od["companies_complained"]
                       and c not in od["companies_tried"]]

        # Build rationale from data
        rationale_parts = []
        if asked:
            rationale_parts.append(f"{asked} market signals asking for this")
        if complained_comps:
            rationale_parts.append(f"negative reviews at {', '.join(complained_comps[:3])}")
        if no_coverage:
            rationale_parts.append(f"no coverage from {', '.join(no_coverage[:3])}")
        elif od["praise"] == 0:
            rationale_parts.append("no tool praised for this yet")
        rationale = ". ".join(p[0].upper() + p[1:] for p in rationale_parts) + "." if rationale_parts else ""

        with st.container(border=True):
            tooltip = FEATURE_TOOLTIPS.get(opp, "")
            if tooltip:
                st.markdown(f"{conf_color} **{opp}** — Confidence: {conf}%", help=tooltip)
            else:
                st.markdown(f"{conf_color} **{opp}** — Confidence: {conf}%")
            if rationale:
                st.markdown(f"**Why build it:** {rationale}")

            # Has it / Doesn't
            has_it = ", ".join(praised_comps[:5]) if praised_comps else "None"
            doesnt = ", ".join(no_coverage[:5]) if no_coverage else "All have attempted"
            st.caption(f"Has it (praised): {has_it} · Not addressed: {doesnt}")

            # Top 3 supporting signals
            scored_signals = sorted(od["signals"], key=lambda x: _relevance_score(x), reverse=True)
            top3 = [s for s in scored_signals if _relevance_sentence(s)][:3]
            for sig in top3:
                sig_title = sig.get("title", "")[:100] or sig.get("text", "")[:100]
                sig_url = sig.get("url", "")
                sig_source = sig.get("source", "")
                sig_source_label = _source_badge(sig_source)
                sig_reason = _relevance_sentence(sig)

                headline = f"[{sig_title}]({sig_url})" if sig_url else sig_title
                st.markdown(f"  `{sig_source_label}` {headline}")
                st.caption(f"  _{sig_reason}_")

            # Show more evidence
            remaining = [s for s in scored_signals[3:] if _relevance_sentence(s)][:12]
            if remaining:
                with st.expander(f"Show {len(remaining)} more evidence"):
                    for sig in remaining:
                        sig_title = sig.get("title", "")[:100] or sig.get("text", "")[:100]
                        sig_url = sig.get("url", "")
                        sig_source = sig.get("source", "")
                        sig_source_label = _source_badge(sig_source)
                        sig_reason = _relevance_sentence(sig)

                        headline = f"[{sig_title}]({sig_url})" if sig_url else sig_title
                        st.markdown(f"`{sig_source_label}` {headline}")
                        st.caption(f"_{sig_reason}_")


# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------

st.divider()
st.caption("Updated every 6 hours from Reddit, Hacker News, G2, Product Hunt, trade press, and internal sources.")
