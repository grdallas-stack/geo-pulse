# GEO Pulse QA Review Checklist

## Persona: Time-Pressed Executive

You are a VP of Product or CMO who just opened GEO Pulse for the first time today. You have 60 seconds to get a genuine insight. Score every element against that standard.

---

## Review Dimensions

### 1. Signal Quality (Weight: 30%)

- [ ] **Relevance**: Every signal in the Live Feed relates to GEO/AEO/AI search/brand visibility. Zero off-topic articles visible.
- [ ] **Descriptions**: Every signal card has a 1-2 sentence brief explaining WHY this article matters, not a classifier label.
- [ ] **Keyword tags**: Each card shows 2-3 keyword pills explaining why it scored high.
- [ ] **Competitor tagging**: ALL competitors mentioned in an article appear as pills, not just the primary.
- [ ] **Signal of the Week**: Always present, always has a clickable title, always the highest-relevance signal from the last 48 hours.
- [ ] **No dead links**: Every title that should be clickable is clickable.

### 2. Competitor Intelligence (Weight: 25%)

- [ ] **Activity labels**: "Last signal: X ago" for companies with signals. "No recent activity" only if zero signals in 30 days.
- [ ] **No raw dates**: All dates are human-readable relative time ("4 months ago", not "2025-10-28").
- [ ] **Sentiment accuracy**: If sentiment is 100% neutral across all competitors, the row is hidden and replaced with signal counts.
- [ ] **Visual hierarchy**: Cards sorted by signal volume descending. Top 3 have rank badges. Low-signal cards are de-emphasized.
- [ ] **No misleading disclaimers**: "Fewer than 3 signals tracked" instead of "Limited data -- results may not be representative."

### 3. Roadmap Quality (Weight: 25%)

- [ ] **"What the Market Wants"**: Ranked list of unmet buyer needs with confidence scores, signal counts, briefs, and supporting links.
- [ ] **"Competitive White Space"**: Clean text table showing feature, demand signal count, competitor coverage, opportunity score.
- [ ] **"Competitor Momentum"**: Simple ranked table with signals this week, last 30 days, trend arrow, last signal date.
- [ ] **"Download Roadmap Brief"**: Working .docx export button.
- [ ] **Minimum signal threshold**: Items with fewer than 3 signals are in a collapsed "Weak signals to watch" section.
- [ ] **Filter buttons**: Ghost style for inactive, filled navy for active.

### 4. Design System Compliance (Weight: 10%)

- [ ] **Fonts**: DM Sans for body, DM Mono for labels/pills/metadata.
- [ ] **Colors**: Navy (#0E3B7E), Orange (#FF9D1C), Cream (#F8F4EB), White (#FFFFFF), Dark (#0A0A0A).
- [ ] **Ghost pills**: Transparent background, 1px solid #0E3B7E border, #0E3B7E text, DM Mono 11px uppercase.
- [ ] **Filled buttons**: #0E3B7E background, #F8F4EB text.
- [ ] **Consistent capitalization**: All section headers use Title Case.
- [ ] **No em dashes in visible text** (use commas or restructure).

### 5. Performance and Polish (Weight: 10%)

- [ ] **60-second insight test**: Can an exec open the app and get a genuine, actionable insight within 60 seconds?
- [ ] **No broken UI**: No empty sections, no placeholder text visible, no "undefined" or "NaN" values.
- [ ] **Load more works**: "Load 25 more signals" button works without page reload.
- [ ] **Share buttons**: All share/copy-link buttons are inside their respective cards, styled as ghost pills.
- [ ] **Downloads work**: All .docx export buttons produce valid, formatted documents.

---

## Severity Levels

- **BLOCKER**: Breaks the 60-second insight test or shows clearly wrong data. Must fix before push.
- **HIGH**: Visible design inconsistency or misleading information. Fix if possible.
- **POLISH**: Minor styling or copy issue. Note for next run.

---

## Output Format

```
## QA Review Results

### Score: X/10

### Blockers (must fix)
- [description] | [file:line or section]

### High Priority
- [description] | [file:line or section]

### Polish
- [description] | [file:line or section]

### Passes
- [checklist item that passes cleanly]
```
