"""
FLINTEL CRM DASHBOARD — index.py
=================================
Real-time, READ-ONLY monitoring dashboard for the FLINTEL signal
intelligence system. 

Connects to the EXACT SAME MongoDB database + collections the background
service (flintel.py) already writes to:

    signals                  — every scored signal, all 5 platforms
    flintel_pending_batch     — current in-progress batch per platform
    flintel_queue_messages    — persisted raw-queue backlog per platform
    flintel_rescore_messages  — manual rescore queue

This service NEVER writes to Mongo and NEVER touches flintel.py's own
state — it only reads. It does not poll the whole `signals` collection on
a timer either: it opens ONE MongoDB Change Stream on `signals` at
startup and keeps an in-memory running tally (count / high / medium / low
/ average) per platform, updated incrementally as new documents arrive.
That tally is pushed to every connected browser over a WebSocket the
instant it changes — real "+1 the moment a message lands," with a single
long-lived Mongo connection instead of repeated find()/aggregate() calls.

Only the small batch/queue/rescore documents (a handful of tiny docs) are
lightly polled every 5s, since those aren't insert-only streams worth a
change-stream subscription.

────────────────────────────────────────────────────────────────────────
NEW: INDUSTRY CLASSIFICATION (added — everything above/below this note
is otherwise untouched from the original script)
────────────────────────────────────────────────────────────────────────
Every signal is additionally bucketed into one of the industries shown
in the client's category picker (Fintech & Payments, Cybersecurity,
CRM & Sales Tools, Logistics, Recruitment & HR, Accounting Software),
falling back to "Other" if nothing matches.

Classification logic, per document:
  1. If the doc has a `search_keyword` (or `keyword` / `matched_keyword`)
     field AND it is available (present, non-null, non-empty), that
     value ALONE decides the industry — matched against the industry
     keyword lists. Post text is NOT consulted in this case, even if it
     would otherwise have matched something.
  2. Only if that field is unavailable (missing/null/empty) does the
     dashboard fall back to matching industry keywords against the
     post's own text (`text` / `post_text` / `message` / `content` /
     `body` — whichever field exists on the doc).
  3. If nothing matches, the signal is bucketed as "Other".

This does not change the existing per-platform tally logic at all — it
runs alongside it, using the same seed (startup aggregation replaced by
a scan for industry purposes) and the same change-stream events.

Run:
    pip install fastapi uvicorn "motor" python-dotenv jinja2 websockets --break-system-packages
    python index.py
    → http://localhost:8100
"""

import os
import re
import asyncio
import logging
from datetime import datetime, timezone

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.templating import Jinja2Templates
import uvicorn

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | flintel-crm | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("flintel-crm")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG — same env vars as flintel.py, so this points at the SAME cluster/DB
# ─────────────────────────────────────────────────────────────────────────────

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB  = os.getenv("MONGODB_DB", "fx_signals")

PLATFORMS = ["reddit", "twitter", "telegram", "facebook", "linkedin"]

PLATFORM_LABELS = {
    "reddit":   "Reddit",
    "twitter":  "Twitter / X",
    "telegram": "Telegram",
    "facebook": "Facebook",
    "linkedin": "LinkedIn",
}

# Display-only — mirrors flintel.py's batch env vars so the dashboard can
# show "3/10 items, 45s of 120s elapsed" without needing write access.
BATCH_CONFIG = {
    "reddit":   {"batch_size": int(os.getenv("REDDIT_BATCH_SIZE",   "10")), "timeout": int(os.getenv("REDDIT_BATCH_TIMEOUT_SECONDS",   "120"))},
    "twitter":  {"batch_size": int(os.getenv("TWITTER_BATCH_SIZE",  "50")), "timeout": int(os.getenv("TWITTER_BATCH_TIMEOUT_SECONDS",  "120"))},
    "telegram": {"batch_size": int(os.getenv("TELEGRAM_BATCH_SIZE", "10")), "timeout": int(os.getenv("TELEGRAM_BATCH_TIMEOUT_SECONDS", "120"))},
    "facebook": {"batch_size": int(os.getenv("FACEBOOK_BATCH_SIZE", "10")), "timeout": int(os.getenv("FACEBOOK_BATCH_TIMEOUT_SECONDS", "120"))},
    "linkedin": {"batch_size": int(os.getenv("LINKEDIN_BATCH_SIZE", "10")), "timeout": int(os.getenv("LINKEDIN_BATCH_TIMEOUT_SECONDS", "1200"))},
}

MIN_SCORE_MEDIUM = int(os.getenv("MIN_SCORE_MEDIUM", "4"))
MIN_SCORE_HIGH   = int(os.getenv("MIN_SCORE_HIGH",   "8"))

# ─────────────────────────────────────────────────────────────────────────────
# NEW: INDUSTRY CLASSIFICATION CONFIG
# ─────────────────────────────────────────────────────────────────────────────

# Which fields might hold the "this post was scraped for keyword X" value.
# Checked in order; first one present+non-empty on the doc wins as the
# "available" source. Override via env if your schema uses a different name.
KEYWORD_FIELD_CANDIDATES = [
    f.strip() for f in os.getenv(
        "INDUSTRY_KEYWORD_FIELDS", "search_keyword,keyword,matched_keyword"
    ).split(",") if f.strip()
]

# Which fields might hold the raw post/message text. Checked in order;
# first one present+non-empty on the doc is used as the fallback source.
TEXT_FIELD_CANDIDATES = [
    f.strip() for f in os.getenv(
        "INDUSTRY_TEXT_FIELDS", "text,post_text,message,content,body,raw_text"
    ).split(",") if f.strip()
]

INDUSTRIES = {
    "fintech_payments": {
        "label": "Fintech & Payments",
        "keywords": [
            "cross-border", "cross border", "cross border payment", "cross-border payments",
            "payment", "payments", "payment gateway", "payment processor", "payment processing",
            "payment infrastructure", "payment platform", "payment provider", "payment rails",
            "remittance", "remittances", "remit money", "forex", "fx trading", "fx rate",
            "fx hedging", "wire transfer", "swift transfer", "ach transfer", "sepa transfer",
            "iban", "swift", "stripe", "paypal", "wise transfer", "revolut", "adyen", "plaid",
            "square payments", "venmo", "cash app", "zelle", "banking", "bank transfer",
            "online banking", "digital bank", "neobank", "challenger bank", "open banking",
            "embedded finance", "currency exchange", "money transfer", "international payments",
            "b2b payments", "merchant account", "card processing", "checkout flow",
            "billing platform", "subscription billing", "recurring billing", "invoice payments",
            "buy now pay later", "bnpl", "digital wallet", "e-wallet", "mobile wallet",
            "kyc", "aml compliance", "correspondent banking", "treasury management",
            "currency conversion", "exchange rate", "money transmitter", "payfac",
            "acquiring bank", "issuing bank", "virtual card", "prepaid card", "settlement",
            "reconciliation", "chargeback", "payment fraud", "fintech", "fintech startup",
            "fintech platform",
        ],
    },
    "cybersecurity": {
        "label": "Cybersecurity",
        "keywords": [
            "breach", "data breach", "security breach", "cyberattack", "cyber attack",
            "hacked", "hacking", "vulnerability", "vulnerable", "malware", "ransomware",
            "ransomware attack", "phishing", "phishing attack", "firewall", "pentest",
            "penetration test", "penetration testing", "soc2", "soc 2", "soc analyst",
            "compliance audit", "security audit", "security compliance", "data leak",
            "leaked credentials", "credential stuffing", "zero-day", "zero day",
            "endpoint security", "endpoint detection", "edr", "xdr", "siem",
            "incident response", "threat intel", "threat intelligence", "intrusion detection",
            "ids", "ips", "waf", "web application firewall", "vpn", "mfa",
            "two factor authentication", "2fa", "encryption", "ddos", "denial of service",
            "patch management", "vulnerability scan", "vulnerability management",
            "red team", "blue team", "iso 27001", "gdpr compliance", "hipaa compliance",
            "pci dss", "cyber insurance", "security vendor", "cloud security",
            "network security", "zero trust", "identity access management", "iam",
            "privileged access", "security operations center", "malware analysis",
            "supply chain attack", "insider threat", "cybersecurity", "infosec",
        ],
    },
    "crm_sales": {
        "label": "CRM & Sales Tools",
        "keywords": [
            "salesforce", "salesforce alternative", "crm", "crm software", "crm platform",
            "crm alternative", "crm migration", "sales pipeline", "hubspot", "zoho crm",
            "pipedrive", "sales tool", "sales tools", "lead gen", "lead generation",
            "lead scoring", "lead management", "deal pipeline", "sales funnel",
            "outbound sales", "cold outreach", "email sequences", "sales cadence",
            "account based marketing", "abm", "sales enablement", "sales analytics",
            "sales forecasting", "contact management", "customer database", "sales dialer",
            "sales automation", "sales engagement", "gong", "outreach.io", "apollo.io",
            "close crm", "monday sales crm", "copper crm", "nutshell crm", "insightly",
            "sugarcrm", "microsoft dynamics", "sales navigator", "prospecting tool",
            "pipeline management", "sales stack", "customer relationship management",
        ],
    },
    "logistics": {
        "label": "Logistics",
        "keywords": [
            "shipping", "freight", "freight forwarding", "freight broker", "ltl shipping",
            "ftl shipping", "carrier switch", "carrier", "carriers", "supply chain",
            "supply chain software", "supply chain disruption", "logistics", "logistics provider",
            "fulfillment", "order fulfillment", "warehouse", "warehouse management", "wms",
            "inventory management", "fleet", "fleet management", "last mile", "last-mile",
            "last mile delivery", "3pl", "third party logistics", "trucking", "trucking company",
            "dispatch", "cargo shipping", "container shipping", "customs clearance",
            "import export", "port congestion", "delivery tracking", "shipment tracking",
            "carrier rates", "shipping rates", "parcel shipping", "courier service",
            "cold chain logistics", "drop shipping", "dropshipping", "route optimization",
            "distribution center", "freight rates", "freight quote",
        ],
    },
    "recruitment_hr": {
        "label": "Recruitment & HR",
        "keywords": [
            "recruiter", "recruiters", "recruitment", "recruiting", "recruiting software",
            "recruiting tool", "ats", "applicant tracking system", "applicant tracking",
            "hiring", "hiring manager", "hiring pipeline", "hr software", "hr platform",
            "hr tool", "hris", "workforce management", "workforce planning", "onboarding",
            "employee onboarding", "payroll", "payroll software", "payroll provider",
            "talent acquisition", "talent management", "candidate sourcing",
            "candidate experience", "job board", "job posting", "employer branding",
            "performance management", "performance review", "employee engagement",
            "benefits administration", "compensation management", "staffing agency",
            "interview scheduling", "background check", "career site", "linkedin recruiter",
            "indeed job", "ziprecruiter", "bamboohr", "gusto payroll", "rippling",
            "ashby ats", "smartrecruiters", "greenhouse", "lever ats", "workday hr",
        ],
    },
    "accounting": {
        "label": "Accounting Software",
        "keywords": [
            "bookkeeping", "bookkeeper", "bookkeeping service", "quickbooks", "xero",
            "accounting", "accounting software", "accounting platform", "accounting tool",
            "accounting firm", "cpa firm", "tax software", "tax filing", "tax preparation",
            "tax compliance", "invoice", "invoicing", "invoicing software", "billing software",
            "ledger", "general ledger", "accounts payable", "accounts receivable",
            "financial management", "financial software", "financial reporting",
            "financial statements", "cash flow management", "expense management",
            "expense tracking", "expense report", "budgeting software", "audit software",
            "erp accounting", "netsuite", "sage accounting", "zoho books", "wave accounting",
            "wave app", "bill.com", "expensify", "receipt scanning", "reconciliation software",
            "cfo tools", "payroll accounting", "freshbooks",
            # job-title / role signals — someone hiring for or working as one of these is
            # an accounting-software-relevant signal even without the software name itself
            "accountant", "staff accountant", "senior accountant", "junior accountant",
            "finance manager", "finance director", "controller role", "financial controller",
            "accounting clerk", "ap clerk", "ar clerk", "payroll clerk", "tax accountant",
            # Sage products/brand — bare "sage" catches "alternative to Sage", "leaving Sage",
            # etc. where the phrase doesn't literally say "sage accounting"
            "sage", "sage intacct", "sage 50", "sage business cloud", "sage one",
        ],
    },
}

OTHER_INDUSTRY_KEY = "other"
OTHER_INDUSTRY_LABEL = "Other"

INDUSTRY_KEYS_IN_ORDER = list(INDUSTRIES.keys()) + [OTHER_INDUSTRY_KEY]
INDUSTRY_LABELS = {k: v["label"] for k, v in INDUSTRIES.items()}
INDUSTRY_LABELS[OTHER_INDUSTRY_KEY] = OTHER_INDUSTRY_LABEL

# NEW — precompiled, case-insensitive, WORD-BOUNDARY regex per keyword
# (instead of raw substring search). This avoids false positives like a
# bare "crm" keyword matching inside an unrelated word, and works fine
# for multi-word phrases too since \b anchors on the phrase's own edges.
_INDUSTRY_PATTERNS = {
    key: [
        (kw, re.compile(r"(?<![A-Za-z0-9])" + re.escape(kw) + r"(?![A-Za-z0-9])", re.IGNORECASE))
        for kw in cfg["keywords"]
    ]
    for key, cfg in INDUSTRIES.items()
}


def _first_available(doc: dict, field_candidates) -> str:
    """Return the first non-empty string value found among field_candidates
    on doc, or '' if none of them are available."""
    for field in field_candidates:
        value = doc.get(field)
        if value is None:
            continue
        value = str(value).strip()
        if value:
            return value
    return ""


def _match_industry_from_string(s: str):
    """
    Match s against every industry's keyword list using precompiled,
    case-insensitive, word-boundary regex patterns (not raw substring
    search — avoids false hits like "crm" matching inside an unrelated
    word). Each industry's score is the sum of the *character length* of
    every keyword that matched, not just a hit count — so one specific,
    multi-word match (e.g. "applicant tracking system") outweighs a
    handful of generic single-word coincidences, which is what you want
    when classifying real post text at scale.

    Returns the industry key with the highest score, or None if nothing
    matched anywhere.
    """
    if not s:
        return None
    best_key, best_score = None, 0
    for key, patterns in _INDUSTRY_PATTERNS.items():
        score = sum(len(kw) for kw, pattern in patterns if pattern.search(s))
        if score > best_score:
            best_key, best_score = key, score
    return best_key


def _classify_industry_with_source(doc: dict):
    """
    Same rule as classify_industry(), but also returns the raw value that
    decided the bucket when a doc lands in "Other" — this is what powers
    the "what's actually inside Other" breakdown on the dashboard:
      1. If the search_keyword-style field is AVAILABLE, use it and ONLY
         it. If it doesn't match any industry -> "Other", and the raw
         keyword value itself is returned as the source.
      2. If that field is UNAVAILABLE, fall back to the post's own text.
         If that doesn't match any industry either -> "Other", and the
         (truncated) text is returned as the source.
      3. If a match IS found (either source), no source is returned —
         the breakdown only needs to explain *unmatched* signals.
    Returns: (industry_key, other_source_value_or_None)
    """
    keyword_value = _first_available(doc, KEYWORD_FIELD_CANDIDATES)
    if keyword_value:
        matched = _match_industry_from_string(keyword_value)
        if matched:
            return matched, None
        return OTHER_INDUSTRY_KEY, keyword_value

    text_value = _first_available(doc, TEXT_FIELD_CANDIDATES)
    if text_value:
        matched = _match_industry_from_string(text_value)
        if matched:
            return matched, None
        return OTHER_INDUSTRY_KEY, text_value

    return OTHER_INDUSTRY_KEY, None


def classify_industry(doc: dict) -> str:
    """
    Industry classification for one signal doc:
      1. If the search_keyword-style field is AVAILABLE on the doc, use it
         and ONLY it — match it against the industry keyword lists, and
         if it doesn't match anything, bucket as "Other". Post text is
         NOT consulted in this case, even if it would have matched.
      2. If the search_keyword-style field is UNAVAILABLE (missing/null/
         empty) on the doc, fall back to matching against the post's own
         text field instead.
      3. If neither source is available/matches, bucket as "Other".
    """
    industry, _source = _classify_industry_with_source(doc)
    return industry


# NEW — tally of raw values (search_keyword, or post text when the keyword
# field is unavailable) that landed in "Other", so the dashboard can show
# *what* is actually inside that bucket instead of just a count. Capped so
# a very long-tail of distinct one-off values can't grow this unbounded —
# once the cap is hit, only counts for values already being tracked keep
# incrementing; brand-new distinct values are folded into an "(others)"
# catch-all entry instead of being dropped silently.
OTHER_BREAKDOWN_MAX_DISTINCT_VALUES = int(os.getenv("OTHER_BREAKDOWN_MAX_DISTINCT_VALUES", "300"))
OTHER_BREAKDOWN_MAX_VALUE_LENGTH    = int(os.getenv("OTHER_BREAKDOWN_MAX_VALUE_LENGTH", "80"))
OTHER_BREAKDOWN_TOP_N               = int(os.getenv("OTHER_BREAKDOWN_TOP_N", "10"))
OTHER_BREAKDOWN_OVERFLOW_LABEL      = "(other distinct values)"

other_breakdown: dict = {}


def _normalize_other_value(raw_value: str) -> str:
    value = " ".join(raw_value.split())  # collapse whitespace/newlines
    if len(value) > OTHER_BREAKDOWN_MAX_VALUE_LENGTH:
        value = value[:OTHER_BREAKDOWN_MAX_VALUE_LENGTH].rstrip() + "…"
    return value


def _record_other_breakdown(raw_value: str):
    if not raw_value:
        return
    value = _normalize_other_value(raw_value)
    if value in other_breakdown:
        other_breakdown[value] += 1
        return
    if len(other_breakdown) >= OTHER_BREAKDOWN_MAX_DISTINCT_VALUES:
        other_breakdown[OTHER_BREAKDOWN_OVERFLOW_LABEL] = other_breakdown.get(OTHER_BREAKDOWN_OVERFLOW_LABEL, 0) + 1
        return
    other_breakdown[value] = 1


def _top_other_breakdown() -> list:
    return [
        {"value": value, "count": count}
        for value, count in sorted(other_breakdown.items(), key=lambda kv: kv[1], reverse=True)[:OTHER_BREAKDOWN_TOP_N]
    ]


# ─────────────────────────────────────────────────────────────────────────────
# APP + DB
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(title="FLINTEL CRM Dashboard", version="1.0.0")
templates = Jinja2Templates(directory="templates")

mongo_client = AsyncIOMotorClient(MONGODB_URI)
db = mongo_client[MONGODB_DB]

# ─────────────────────────────────────────────────────────────────────────────
# IN-MEMORY LIVE STATE — seeded once from Mongo, then updated incrementally.
# This is the ENTIRE reason the dashboard doesn't need to hammer Atlas.
# ─────────────────────────────────────────────────────────────────────────────

live_stats: dict = {
    p: {"count": 0, "high": 0, "medium": 0, "low": 0, "sum_score": 0, "avg_score": 0.0}
    for p in PLATFORMS
}

# NEW: industry_stats — running tally of message counts per industry,
# updated the same way live_stats is (seed once, then incrementally via
# the same change stream on `signals`).
industry_stats: dict = {key: 0 for key in INDUSTRY_KEYS_IN_ORDER}

connected_sockets: set = set()
started_at = datetime.now(timezone.utc)


def _bucket(score: int) -> str:
    if score >= MIN_SCORE_HIGH:
        return "high"
    if score >= MIN_SCORE_MEDIUM:
        return "medium"
    return "low"


def _recalc_avg(platform: str):
    cnt = live_stats[platform]["count"]
    live_stats[platform]["avg_score"] = round(live_stats[platform]["sum_score"] / cnt, 2) if cnt else 0.0


def _busiest_industry() -> dict:
    """NEW — which industry currently has the most messages."""
    if not any(industry_stats.values()):
        return {"key": None, "label": None, "count": 0}
    best_key = max(industry_stats, key=lambda k: industry_stats[k])
    return {
        "key": best_key,
        "label": INDUSTRY_LABELS[best_key],
        "count": industry_stats[best_key],
    }


async def _aggregate_platform(platform: str = None) -> dict:
    """Single aggregation, only run at startup (all platforms) or after an
    UPDATE event on one platform (rescoring can change a score after
    insert, so that one platform gets cheaply re-summed — never the whole
    collection on a timer)."""
    match_stage = {"$match": {"platform": platform}} if platform else {"$match": {}}
    pipeline = [
        match_stage,
        {"$group": {
            "_id": "$platform",
            "count": {"$sum": 1},
            "sum_score": {"$sum": "$intent_score"},
            "high":   {"$sum": {"$cond": [{"$gte": ["$intent_score", MIN_SCORE_HIGH]}, 1, 0]}},
            "medium": {"$sum": {"$cond": [{"$and": [
                {"$gte": ["$intent_score", MIN_SCORE_MEDIUM]},
                {"$lt":  ["$intent_score", MIN_SCORE_HIGH]},
            ]}, 1, 0]}},
            "low":    {"$sum": {"$cond": [{"$lt": ["$intent_score", MIN_SCORE_MEDIUM]}, 1, 0]}},
        }},
    ]
    results = {}
    async for row in db.signals.aggregate(pipeline):
        results[row["_id"]] = row
    return results


async def seed_live_stats():
    results = await _aggregate_platform(None)
    for p in PLATFORMS:
        row = results.get(p)
        if not row:
            continue
        live_stats[p]["count"]     = row["count"]
        live_stats[p]["sum_score"] = row["sum_score"]
        live_stats[p]["high"]      = row["high"]
        live_stats[p]["medium"]    = row["medium"]
        live_stats[p]["low"]       = row["low"]
        _recalc_avg(p)
    log.info(f"Seeded live stats from MongoDB | { {k: v['count'] for k, v in live_stats.items()} }")


async def seed_industry_stats():
    """
    NEW — seed industry_stats once at startup.

    Unlike live_stats (which is a pure numeric aggregation MongoDB can do
    server-side), industry classification depends on free-text content, so
    this pulls only the small set of relevant fields per doc (platform +
    the keyword/text candidate fields) and classifies in Python. This is a
    one-time full scan at startup — not run on a timer — after which the
    change stream keeps industry_stats current incrementally, exactly like
    live_stats.
    """
    projection = {"platform": 1}
    for f in KEYWORD_FIELD_CANDIDATES + TEXT_FIELD_CANDIDATES:
        projection[f] = 1

    counts = {key: 0 for key in INDUSTRY_KEYS_IN_ORDER}
    other_breakdown.clear()
    cursor = db.signals.find({}, projection)
    async for doc in cursor:
        industry, other_source = _classify_industry_with_source(doc)
        counts[industry] = counts.get(industry, 0) + 1
        if industry == OTHER_INDUSTRY_KEY:
            _record_other_breakdown(other_source)

    industry_stats.update(counts)
    log.info(f"Seeded industry stats from MongoDB | {industry_stats}")


async def broadcast(payload: dict):
    if not connected_sockets:
        return
    dead = set()
    for ws in connected_sockets:
        try:
            await ws.send_json(payload)
        except Exception:
            dead.add(ws)
    connected_sockets.difference_update(dead)


# ─────────────────────────────────────────────────────────────────────────────
# CHANGE STREAM — the real-time heart of the dashboard.
# Watches ONLY the `signals` collection, exactly as flintel.py writes it.
# ─────────────────────────────────────────────────────────────────────────────

async def watch_signals():
    while True:
        try:
            async with db.signals.watch(
                [{"$match": {"operationType": {"$in": ["insert", "update", "replace"]}}}],
                full_document="updateLookup",
            ) as stream:
                log.info("Change stream on `signals` opened — live updates active.")
                async for change in stream:
                    doc = change.get("fullDocument")
                    if not doc:
                        continue
                    platform = doc.get("platform")
                    if platform not in live_stats:
                        continue
                    score = doc.get("intent_score", 1)
                    op = change["operationType"]

                    if op == "insert":
                        live_stats[platform]["count"]     += 1
                        live_stats[platform]["sum_score"]  += score
                        live_stats[platform][_bucket(score)] += 1
                        _recalc_avg(platform)

                        # NEW — classify the new signal into an industry and
                        # bump the running tally the same way live_stats is.
                        industry, other_source = _classify_industry_with_source(doc)
                        industry_stats[industry] = industry_stats.get(industry, 0) + 1
                        if industry == OTHER_INDUSTRY_KEY:
                            _record_other_breakdown(other_source)

                        event = {
                            "platform": platform,
                            "kind": "new_signal",
                            "score": score,
                            "industry": industry,               # NEW
                            "industry_label": INDUSTRY_LABELS[industry],  # NEW
                        }
                    else:
                        # rescore changed a score after the fact — re-sum
                        # just this one platform (cheap, indexed) rather
                        # than guessing which bucket to decrement.
                        results = await _aggregate_platform(platform)
                        row = results.get(platform)
                        if row:
                            live_stats[platform]["count"]     = row["count"]
                            live_stats[platform]["sum_score"] = row["sum_score"]
                            live_stats[platform]["high"]      = row["high"]
                            live_stats[platform]["medium"]    = row["medium"]
                            live_stats[platform]["low"]       = row["low"]
                            _recalc_avg(platform)
                        # NOTE: industry is not recomputed here — a rescore
                        # changes intent_score, not the post's text/keyword,
                        # so the industry bucket a signal already landed in
                        # doesn't change.
                        event = {"platform": platform, "kind": "rescored", "score": score}

                    await broadcast({
                        "type": "stats",
                        "data": live_stats,
                        "event": event,
                        "industries": industry_stats,                 # NEW
                        "busiest_industry": _busiest_industry(),       # NEW
                        "other_breakdown": _top_other_breakdown(),     # NEW
                    })
        except PyMongoError as exc:
            log.error(f"Change stream error: {exc} — reconnecting in 5s...")
            await asyncio.sleep(5)
        except Exception as exc:
            log.error(f"watch_signals unexpected error: {exc} — reconnecting in 5s...")
            await asyncio.sleep(5)


# ─────────────────────────────────────────────────────────────────────────────
# LIGHT POLL — batch/queue/rescore state (tiny docs, not insert-only, so a
# 5s poll is cheaper and simpler than a change stream here).
# ─────────────────────────────────────────────────────────────────────────────

async def get_queue_snapshot() -> dict:
    snapshot = {}
    for p in PLATFORMS:
        pending_doc = await db.flintel_pending_batch.find_one({"platform": p})
        pending_items = len(pending_doc.get("items", [])) if pending_doc else 0
        batch_start   = pending_doc.get("batch_start_time") if pending_doc else None

        backlog = await db.flintel_queue_messages.count_documents({"_platform_key": p})

        elapsed = None
        if batch_start:
            bs = batch_start if batch_start.tzinfo else batch_start.replace(tzinfo=timezone.utc)
            elapsed = round((datetime.now(timezone.utc) - bs).total_seconds(), 1)

        cfg = BATCH_CONFIG[p]
        snapshot[p] = {
            "pending_in_batch": pending_items,
            "batch_size":       cfg["batch_size"],
            "timeout_seconds":  cfg["timeout"],
            "elapsed_seconds":  elapsed,
            "backlog_queue":    backlog,
        }
    return snapshot


async def get_rescore_snapshot() -> dict:
    pending    = await db.flintel_rescore_messages.count_documents({"status": "pending"})
    processing = await db.flintel_rescore_messages.count_documents({"status": "processing"})
    return {"pending": pending, "processing": processing}


async def queue_poll_loop():
    while True:
        try:
            queues  = await get_queue_snapshot()
            rescore = await get_rescore_snapshot()
            await broadcast({"type": "queues", "data": queues, "rescore": rescore})
        except Exception as exc:
            log.error(f"queue_poll_loop error: {exc}")
        await asyncio.sleep(5)


# ─────────────────────────────────────────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def on_startup():
    await seed_live_stats()
    await seed_industry_stats()  # NEW
    asyncio.create_task(watch_signals())
    asyncio.create_task(queue_poll_loop())
    log.info("FLINTEL CRM Dashboard ready — http://0.0.0.0:8100")


# ─────────────────────────────────────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/")
async def dashboard(request: Request):
    queues  = await get_queue_snapshot()
    rescore = await get_rescore_snapshot()
    return templates.TemplateResponse("crm.html", {
        "request":          request,
        "platforms":        PLATFORMS,
        "platform_labels":  PLATFORM_LABELS,
        "stats":            live_stats,
        "queues":           queues,
        "rescore":          rescore,
        "mongodb_db":       MONGODB_DB,
        "min_score_medium": MIN_SCORE_MEDIUM,
        "min_score_high":   MIN_SCORE_HIGH,
        # NEW — industry breakdown, passed through for the template to render.
        "industries":       industry_stats,
        "industry_labels":  INDUSTRY_LABELS,
        "busiest_industry": _busiest_industry(),
        "other_breakdown":  _top_other_breakdown(),
    })


@app.websocket("/ws/live")
async def ws_live(websocket: WebSocket):
    await websocket.accept()
    connected_sockets.add(websocket)
    log.info(f"Dashboard client connected | total:{len(connected_sockets)}")
    try:
        await websocket.send_json({
            "type": "stats",
            "data": live_stats,
            "industries": industry_stats,               # NEW
            "busiest_industry": _busiest_industry(),     # NEW
            "other_breakdown": _top_other_breakdown(),   # NEW
        })
        queues  = await get_queue_snapshot()
        rescore = await get_rescore_snapshot()
        await websocket.send_json({"type": "queues", "data": queues, "rescore": rescore})
        while True:
            await websocket.receive_text()  # client keep-alive ping only
    except WebSocketDisconnect:
        pass
    finally:
        connected_sockets.discard(websocket)
        log.info(f"Dashboard client disconnected | total:{len(connected_sockets)}")


@app.get("/api/health")
async def health():
    try:
        await db.command("ping")
        mongo_ok = True
    except Exception:
        mongo_ok = False
    return {
        "status":         "ok" if mongo_ok else "degraded",
        "mongodb":        "connected" if mongo_ok else "disconnected",
        "mongodb_db":     MONGODB_DB,
        "clients_live":   len(connected_sockets),
        "uptime_seconds": round((datetime.now(timezone.utc) - started_at).total_seconds()),
    }


@app.get("/api/debug-industry-fields")
async def debug_industry_fields():
    """
    NEW — diagnostic endpoint. Everything is landing in "Other" almost
    always means the KEYWORD_FIELD_CANDIDATES / TEXT_FIELD_CANDIDATES
    field names don't actually match your `signals` schema. This endpoint
    returns:
      - every top-level field name found on a sample document
      - out of a 500-doc sample, how many docs actually have a non-empty
        value for each candidate field we're currently checking

    Hit this, look at `sample_document_fields` for the real field name
    that holds your post text (or the search keyword), then set
    INDUSTRY_TEXT_FIELDS / INDUSTRY_KEYWORD_FIELDS in your .env to match.
    """
    sample_doc = await db.signals.find_one({})
    sample_fields = sorted(sample_doc.keys()) if sample_doc else []

    sample_cursor = db.signals.find({}).limit(500)
    checked = 0
    field_presence = {f: 0 for f in (KEYWORD_FIELD_CANDIDATES + TEXT_FIELD_CANDIDATES)}
    example_values = {}

    async for doc in sample_cursor:
        checked += 1
        for f in field_presence:
            val = doc.get(f)
            if val is not None and str(val).strip():
                field_presence[f] += 1
                if f not in example_values:
                    example_values[f] = str(val)[:120]

    return {
        "sample_document_fields": sample_fields,
        "sample_document_preview": {
            k: (str(v)[:120] if not isinstance(v, (int, float, bool)) else v)
            for k, v in (sample_doc or {}).items()
        },
        "checked_docs": checked,
        "keyword_field_candidates": KEYWORD_FIELD_CANDIDATES,
        "text_field_candidates": TEXT_FIELD_CANDIDATES,
        "field_presence_count": field_presence,
        "example_values_found": example_values,
        "hint": (
            "If field_presence_count is 0 for every candidate, none of "
            "our guessed field names exist on your docs. Check "
            "sample_document_fields for the real name of the field that "
            "holds post text (and/or the search keyword), then set "
            "INDUSTRY_TEXT_FIELDS / INDUSTRY_KEYWORD_FIELDS in .env, e.g. "
            "INDUSTRY_TEXT_FIELDS=raw_message,body_text"
        ),
    }


@app.get("/api/industries")
async def api_industries():
    """
    NEW — standalone endpoint for the industry breakdown, in case the
    dashboard template isn't updated yet to render it. Returns per-industry
    counts plus which industry currently has the most messages.
    """
    return {
        "industries": [
            {"key": key, "label": INDUSTRY_LABELS[key], "count": industry_stats.get(key, 0)}
            for key in INDUSTRY_KEYS_IN_ORDER
        ],
        "busiest_industry": _busiest_industry(),
        "other_breakdown": _top_other_breakdown(),
        "classification": {
            "keyword_field_candidates": KEYWORD_FIELD_CANDIDATES,
            "text_field_candidates": TEXT_FIELD_CANDIDATES,
        },
    }


if __name__ == "__main__":
    log.info("=" * 70)
    log.info("  FLINTEL CRM DASHBOARD — read-only, real-time, same MongoDB")
    log.info(f"  DB: {MONGODB_DB} | Platforms: {', '.join(PLATFORMS)}")
    log.info("=" * 70)
    uvicorn.run(app, host="0.0.0.0", port=8100, log_level="warning")
