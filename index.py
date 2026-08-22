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
CRM & Sales Tools, Logistics, Recruitment & HR, Accounting Software,
AI Agents), falling back to "Other" if nothing matches.

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

  NEW — matching isn't required to be a 100% exact/substring hit anymore.
  If nothing matches exactly, a fuzzy fallback checks whether the value
  is at least ~70% similar (configurable via FUZZY_MATCH_THRESHOLD) to
  one of an industry's keywords (e.g. "wise block my account" ~ "wise
  block account") and counts it toward that industry if so. This applies
  the same way across every industry's keyword list.

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
import difflib
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
             "stripe froze my funds", "stripe held my funds", "stripe holding my funds",
    "stripe banned my account", "stripe suspended my account",
    "stripe account closed", "stripe account frozen", "stripe rejected my business",
    "stripe withheld my payout", "stripe payout delayed",
    "stripe high fees", "stripe hidden fees", "why is stripe so expensive",
    "stripe terrible support", "stripe chargeback problems",
    "stripe too many chargebacks", "stripe withdrawal issues",
    "stripe funds stuck", "stripe down", "stripe problems",
    "alternative to stripe", "switching from stripe", "leaving stripe",
    "moving off stripe", "migrating from stripe", "replacing stripe",
    "ditching stripe", "fed up with stripe", "done with stripe",
    "better than stripe", "cheaper than stripe", "instead of stripe",

    # ── OTHER PROCESSOR/PSP PAIN (generalized, not per-platform spam) ───────
    "PayPal froze my funds", "PayPal held my funds", "PayPal banned my account",
    "PayPal high fees", "PayPal chargeback problems", "PayPal terrible support",
    "Square froze my funds", "Square held my funds", "Square account closed",
    "Adyen froze my funds", "Checkout.com froze my funds",
    "Wise froze my funds", "Wise account closed", "Revolut froze my funds",
    "Revolut business account blocked", "Mollie froze my funds",
    "payment processor froze my funds", "payment processor held my funds",
    "payment processor banned my account", "payment processor rejected my business",
    "payment processor withheld my payout", "payment processor payout delayed",
    "payment processor high fees", "payment processor hidden fees",
    "payment processor terrible support", "payment processor keeps failing",
    "payment processor api keeps failing", "payment processor integration broke",
    "payment processor no chargebacks", "payment processor too many chargebacks",
    "payment processor won't approve my business", "payment processor limits too low",
    "payment processor settlement delays", "payment processor evm compatible",

    # ── CRYPTO PAYMENT GATEWAY: SETUP / HOW-TO ────────────────────────────────
    "how to accept crypto payments", "how to accept bitcoin payments",
    "how to accept USDT payments", "how to accept USDC payments",
    "how to accept stablecoin payments", "how to accept ethereum payments",
    "how to accept solana payments", "how to accept web3 payments",
    "how to accept crypto for my SaaS", "how to accept crypto for my store",
    "how to accept crypto for my ecommerce store", "how to accept crypto on my site",
    "how to accept crypto at checkout", "best way to accept crypto payments",
    "start accepting crypto payments", "add crypto payments to my site",
    "integrate crypto payments", "implement crypto payments",
    "enable crypto payments", "set up crypto payments",
    "accept crypto payments for SaaS", "accept crypto payments for ecommerce",

    # ── CRYPTO GATEWAY: RECOMMENDATION / COMPARISON ──────────────────────────
    "looking for crypto payment gateway", "looking for crypto payment processor",
    "looking for crypto payments provider", "recommend a crypto payment gateway",
    "anyone using a crypto payment gateway", "what's the best crypto payment gateway",
    "best crypto payment gateway for SaaS", "crypto payment gateway comparison",
    "crypto payment gateway low fees", "crypto payment gateway no chargebacks",
    "crypto payment gateway non-custodial", "crypto payment gateway self-hosted",
    "crypto payment gateway no kyc", "crypto payment gateway instant settlement",
    "crypto payment gateway multi-chain", "crypto payment gateway evm compatible",
    "crypto payment gateway developer friendly", "crypto payment gateway with api",
    "crypto payment gateway no rolling reserve", "crypto payment gateway fast payouts",
    "crypto payment gateway global", "which crypto payment gateway is best",
    "experience with crypto payment gateway", "thoughts on crypto payment gateway",
    "who offers a crypto payment gateway",

    # ── SPECIFIC CRYPTO PROCESSOR COMPLAINTS ─────────────────────────────────
    "bitpay froze my funds", "bitpay banned my account", "bitpay high fees",
    "bitpay problems", "bitpay support terrible", "alternative to bitpay",
    "coinbase commerce froze my funds", "coinbase commerce down",
    "coinbase commerce banned my account", "coinbase commerce high fees",
    "alternative to coinbase commerce",
    "nowpayments froze my funds", "nowpayments problems", "nowpayments high fees",
    "moonpay froze my funds", "moonpay banned my account", "moonpay problems",
    "transak froze my funds", "transak problems", "transak high fees",
    "ramp froze my funds", "ramp problems", "ramp banned my account",
    "banxa froze my funds", "banxa problems",
    "triple-a froze my funds", "triple-a problems",
    "bvnk froze my funds", "bvnk problems",
    "0xprocessing froze my funds", "0xprocessing problems",
    "opennode froze my funds", "opennode problems",
    "btcpay server froze my funds", "btcpay server problems",
    "cryptomus froze my funds", "cryptomus problems",
    "plisio froze my funds", "plisio problems",
    "coingate froze my funds", "coingate problems",
    "coinpayments froze my funds", "coinpayments problems",
    "utrust froze my funds", "utrust problems",
    "circle froze my funds", "circle problems",
    "paxos froze my funds", "paxos problems",
    "fireblocks froze my funds",
    "wyre froze my funds", "wyre shut down",
    "sardine froze my funds",
    "alchemy pay froze my funds",

    # ── HIGH RISK / RESTRICTED INDUSTRY MERCHANT ACCOUNTS ────────────────────
    "high risk merchant account", "need a high risk merchant account",
    "looking for a high risk merchant account", "high risk merchant account rejected",
    "high risk merchant account declined", "high risk payment processor",
    "high risk crypto processor", "offshore merchant account",
    "offshore crypto processor", "forex payment processor",
    "gambling payment processor", "igaming payment processor",
    "casino crypto payments", "adult payment processor",
    "cbd payment processor", "nutra payment processor",
    "crypto processor for high risk business", "merchant account for crypto business",
    "payment processor for restricted industries",

    # ── STABLECOIN / SPECIFIC ASSET RAILS ─────────────────────────────────────
    "USDT payment gateway", "USDC payment gateway", "stablecoin payment gateway",
    "accept USDT payments for business", "accept USDC payments for business",
    "onchain payments for business", "crypto on-ramp for business",
    "crypto off-ramp for business", "fiat on-ramp integration",
    "crypto acquiring solution", "crypto checkout solution",
    "crypto invoicing tool", "crypto billing platform",
    "agent payments for AI", "AI agent payments infrastructure",
    "programmatic crypto payments", "machine-to-machine crypto payments",

    # ── COMPLIANCE / KYC / KYB PAIN ────────────────────────────────────────────
    "KYB rejected", "KYB verification failed", "KYC rejected crypto",
    "AML compliance crypto payments", "compliance issue crypto payments",
    "regulatory issue accepting crypto", "licensing requirements crypto payments",
    "MSB license crypto payments", "crypto payment compliance nightmare",

    # ── BUSINESS CONTEXT ────────────────────────────────────────────────────────
    "SaaS founder payment processing", "ecommerce store payment processing",
    "marketplace payment infrastructure", "startup needs payment processor",
    "cross-border payments crypto", "international payments crypto business",
    "web3 startup payments", "DAO payment infrastructure",
    "on-chain business payments", "crypto native business payments",

    # ── URGENCY / SWITCHING SIGNALS ──────────────────────────────────────────
    "need a new payment processor urgently", "payment processor shut down my business",
    "processor terminated my account", "need a backup payment processor",
    "diversifying payment processors", "risk of losing payment processor",
    "worried about getting cut off by stripe", "worried about account termination",
    
        "deal at risk", "relationship at risk",
    "can't wait any longer", "running out of time", "no more time",

    # ── BUSINESS EXPANSION ───────────────────────────────────────────────────
    "just signed a supplier", "signed a new supplier", "found a supplier",
    "new supplier in", "signed a contract with", "new contract with",
    "starting to import", "starting an import", "starting to export",
    "starting an export", "launching in", "expanding to",
    "entering the market", "new market", "setting up payments",
    "need to set up payments", "need to transfer money",
    "will need to send", "will need to transfer", "going to need",
    "starting a business", "new business", "import business",
    "export business", "trading company", "sourcing products from",
    "sourcing goods from", "buying products from", "buying goods from",
    "manufacturing in", "producing in",

    # ── TREASURY & FX ────────────────────────────────────────────────────────
    "treasury management", "cash management", "liquidity management",
    "FX management", "FX exposure", "FX risk", "FX hedging",
    "currency hedging", "currency risk", "currency exposure",
    "FX solution", "FX platform", "FX tool",
    "treasury solution", "treasury platform", "cash flow management",
    "multi currency", "multi-currency", "multicurrency",
    "currency account", "foreign currency account",
    "international banking", "international bank account",
    "global banking", "global bank account", "correspondent banking",
    "banking relationship", "banking partner",
    "payment infrastructure", "payment rails", "payment solution",
    "payment platform", "payment provider", "payment partner",
    "fintech payment", "embedded payment", "embedded finance",
    "cross border banking", "international banking solution",
    "FX banking", "FX banking relationship", "FX liquidity",
    "cash pooling", "cash concentration",
    "intercompany payment", "intercompany transfer",

    # ── JOB SIGNALS ──────────────────────────────────────────────────────────
    "treasury manager", "treasury analyst", "FX manager", "FX analyst",
    "FX trader", "treasury director", "head of treasury", "VP treasury",
    "international payments manager", "global payments manager",
    "cross border payments", "payments operations manager",
    "payments specialist", "treasury specialist", "FX specialist",
    "international finance manager", "global finance manager",
    "head of payments", "director of payments", "VP payments",
    "chief financial officer", "head of finance", "finance director",
    "controller international", "global controller",

        "Wise blocked my account",
    "bank blocked my transfer",
    "Wise Business restricted",
    "Payoneer account blocked",
    "cross border payment problem",
    "CRM is a nightmare",
    "our CRM is a mess",
    "recommend a CRM for small business",
    "we got hacked",
    "ransomware attack",
    "need incident response",
    "Salesforce alternative",
    "switching from HubSpot",
    # ── BUSINESS CONTEXT ───────────────────────────────────────────────────────
    "my bookkeeper", "our bookkeeper", "my accountant", "our accountant",
    "small business accounting", "startup accounting", "solo founder accounting",
    "freelancer accounting", "self employed accounting", "DIY bookkeeping",
    "doing my own books", "founder doing the books", "wearing the finance hat",
    "no dedicated finance person", "growing business need better accounting",
    "scaling finance operations", "outsourced bookkeeping", "outsourced accounting",
    "virtual CFO", "fractional CFO", "need a fractional CFO",
    "part time bookkeeper", "part time accountant", "CFO"

      "urgently need a bookkeeper", "need books cleaned up ASAP",
    "tax deadline approaching", "need this done before tax season",
    "investors asking for financials", "due diligence deadline",
    "board wants updated financials", "need financials for loan application",
    "need financials for a loan", "applying for a business loan financials",

      "head of talent", "head of HR", "head of people",
    "VP of people", "VP of talent", "chief people officer",
    "talent acquisition manager", "recruiting manager",
    "HR manager", "HR business partner", "people operations manager",
    "HRIS manager", "compensation and benefits manager",
    "director of talent acquisition", "director of people operations",
    "technical recruiter", "corporate recruiter", "recruiting coordinator",

      "send money to", "sending money to", "transfer money to",
    "transferring money to", "wire money to", "wiring money to",
    "move money to", "moving money to", "remit money to",
    "remitting money to", "pay my supplier", "paying my supplier",
    "pay a supplier", "paying a supplier", "pay my vendor",
    "paying my vendor", "pay my manufacturer", "pay my factory",
    "pay my partner", "pay my contractor", "pay an invoice",
    "paying an invoice", "settle an invoice", "settling an invoice",
    "pay a business", "business payment to", "supplier payment to",
    "vendor payment to", "invoice payment to", "international payment to",
    "overseas payment to", "cross border payment", "cross-border payment",
    "cross border transfer", "cross-border transfer",
    "international transfer", "international wire",
    "international wire transfer", "foreign wire transfer",
    "overseas wire transfer", "overseas transfer", "global payment",
    "global transfer", "b2b payment", "b2b transfer",
    "business to business payment",
    
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
            
             # ── INCIDENT / BREACH SIGNALS ────────────────────────────────────────────
    "we got hacked", "we got breached", "company got hacked",
    "company got breached", "data breach", "we had a breach",
    "security breach", "breach happened", "just got ransomwared",
    "ransomware attack", "ransomware hit us", "hit by ransomware",
    "encrypted our files", "files got encrypted", "systems encrypted",
    "locked out of our systems", "locked out of our servers",
    "attacker got in", "attackers got in", "unauthorized access",
    "someone accessed our", "someone breached our", "network compromised",
    "systems compromised", "account compromised", "accounts compromised",
    "email compromised", "credentials leaked", "credentials stolen",
    "password leaked", "passwords leaked", "data leaked", "data exposed",
    "customer data exposed", "customer data leaked", "PII exposed",
    "PII leaked", "source code leaked", "database leaked",
    "database exposed", "exfiltrated data", "data exfiltration",
    "phishing attack", "phishing email", "spear phishing",
    "business email compromise", "BEC attack", "CEO fraud",
    "invoice fraud", "wire fraud attack", "supply chain attack",
    "zero day exploit", "zero-day exploit", "actively exploited",
    "malware infection", "infected with malware", "trojan detected",
    "backdoor found", "backdoor discovered", "rootkit found",
    "DDoS attack", "under DDoS", "site went down attack",
    "insider threat", "insider attack", "third party breach",
    "vendor breach", "supplier breach", "MSP breach",

    # ── INCIDENT RESPONSE URGENCY ────────────────────────────────────────────
    "need incident response", "need an IR firm", "need a forensics team",
    "who do I call after a breach", "who to call after hack",
    "emergency incident response", "24/7 incident response",
    "need help now hacked", "actively being attacked",
    "attack in progress", "attacker still in our network",
    "containment help", "need containment", "need remediation",
    "recovering from ransomware", "ransomware recovery",
    "should we pay the ransom", "pay the ransom or not",
    "ransom demand", "ransom note", "threat actor demanding",
    "need to notify customers breach", "breach notification requirements",
    "legally required to disclose breach", "disclose the breach",

    # ── TOOLING / PLATFORM FRUSTRATION ───────────────────────────────────────
    "our SIEM missed it", "SIEM didn't catch it", "SIEM false positives",
    "too many false positives", "alert fatigue", "drowning in alerts",
    "no visibility into our network", "no visibility into endpoints",
    "can't see what's happening on our network",
    "our EDR didn't catch it", "EDR missed", "antivirus didn't catch it",
    "firewall got bypassed", "firewall wasn't enough",
    "our current tool isn't working", "outgrown our current tool",
    "outgrown our security stack", "current vendor isn't cutting it",
    "switching security vendors", "replacing our SIEM",
    "replacing our EDR", "need a new MDR", "need a new SOC",
    "understaffed security team", "no security team",
    "one person security team", "no dedicated security staff",
    "can't afford a full SOC", "need outsourced SOC",
    "need a virtual CISO", "need a fractional CISO", "need vCISO",

    # ── FEE / COST FRUSTRATION ───────────────────────────────────────────────
    "security tools too expensive", "cybersecurity budget too small",
    "can't justify the cost", "pricing is outrageous",
    "licensing costs killing us", "per-endpoint pricing too high",
    "hidden costs security vendor", "surprise renewal fees",
    "renewal price increase", "price hike renewal",
    "cheaper alternative to CrowdStrike", "cheaper alternative to SentinelOne",
    "cheaper EDR", "cheaper SIEM", "cheaper MDR",
    "affordable cybersecurity for small business",
    "budget-friendly security tools", "best value security platform",

    # ── COMPETITOR MENTIONS ───────────────────────────────────────────────────
    "CrowdStrike outage", "CrowdStrike issue", "CrowdStrike problem",
    "CrowdStrike blocked", "CrowdStrike alternative",
    "SentinelOne problem", "SentinelOne issue", "SentinelOne alternative",
    "switching from CrowdStrike", "switching from SentinelOne",
    "leaving Microsoft Defender", "Defender missed", "Defender didn't catch",
    "Palo Alto issue", "Palo Alto problem", "Fortinet vulnerability",
    "Fortinet issue", "Fortinet exploit", "Cisco vulnerability",
    "Cisco exploit", "Sophos problem", "Sophos issue",
    "Trend Micro problem", "McAfee problem", "Norton problem",
    "Rapid7 issue", "Qualys issue", "Tenable issue", "Splunk too expensive",
    "Splunk alternative", "Datadog security alternative",
    "leaving our MSSP", "switching MSSPs", "MSSP isn't responsive",
    "our MSP dropped the ball", "MSP missed the breach",
    "alternative to Norton", "alternative to McAfee",
    "alternative to Splunk", "alternative to Rapid7",
    "better than CrowdStrike", "better than SentinelOne",

    # ── RECOMMENDATION REQUESTS ──────────────────────────────────────────────
    "recommend a SIEM", "recommend an EDR", "recommend an MDR",
    "recommend a firewall", "recommend a security vendor",
    "recommend a pentest firm", "recommend a security consultant",
    "anyone used", "has anyone used", "does anyone recommend",
    "what EDR do you use", "what SIEM do you use",
    "which security tool is best", "best EDR for small business",
    "best SIEM for startups", "best MDR provider",
    "best pentest company", "looking for a security vendor",
    "looking for a pentest firm", "looking for an MSSP",
    "need a security assessment", "need a vulnerability assessment",
    "need a penetration test", "need a pen test", "need a red team",
    "who should we hire for security", "who do you use for security",

    # ── COMPLIANCE PAIN ───────────────────────────────────────────────────────
    "SOC 2 audit failed", "failed SOC 2", "SOC 2 readiness",
    "need SOC 2 compliance", "preparing for SOC 2",
    "ISO 27001 certification", "need ISO 27001", "ISO 27001 audit",
    "PCI DSS compliance", "failed PCI audit", "PCI compliance issue",
    "HIPAA violation", "HIPAA compliance issue", "HIPAA audit",
    "GDPR fine", "GDPR violation", "GDPR compliance issue",
    "CMMC compliance", "CMMC certification", "NIST compliance",
    "NIST framework", "failed audit", "audit findings",
    "compliance deadline", "compliance gap", "compliance nightmare",
    "regulators are asking", "auditor flagged", "auditors flagged",

    # ── URGENCY SIGNALS ──────────────────────────────────────────────────────
    "urgently need", "critical vulnerability", "emergency patch",
    "patch immediately", "exploit in the wild", "actively exploited",
    "ASAP security", "need help immediately", "time sensitive breach",
    "board is asking questions", "customers are asking questions",
    "losing customers over breach", "losing the contract over security",
    "insurance requires", "cyber insurance requirement",
    "cyber insurance denied claim", "can't get cyber insurance",
    "insurance premium went up after breach",

    # ── BUSINESS EXPANSION / GROWTH ──────────────────────────────────────────
    "building our security program", "starting a security program",
    "hiring our first security hire", "hiring a CISO",
    "scaling our security team", "growing security team",
    "new compliance requirement", "new client requires SOC 2",
    "client requiring security review", "vendor security questionnaire",
    "security questionnaire from client", "need to pass security review",

    # ── JOB SIGNALS ───────────────────────────────────────────────────────────
    "CISO", "chief information security officer", "security engineer",
    "security analyst", "SOC analyst", "SOC manager",
    "head of security", "director of security", "VP security",
    "security operations manager", "threat intel analyst",
    "incident response manager", "GRC manager", "GRC analyst",
    "penetration tester", "red team lead", "blue team lead",
    "application security engineer", "cloud security engineer",
    "detection engineer", "security architect",
    
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
            "our CRM is a mess", "CRM is too complicated", "CRM too complex",
    "CRM is clunky", "clunky CRM", "outdated CRM", "CRM feels outdated",
    "hate our CRM", "CRM is a nightmare", "CRM nightmare",
    "CRM isn't working for us", "CRM not working for our team",
    "outgrown our CRM", "outgrown our current CRM",
    "CRM doesn't scale", "CRM can't handle our volume",
    "CRM is too slow", "CRM keeps crashing", "CRM keeps freezing",
    "CRM data is a mess", "messy CRM data", "duplicate contacts CRM",
    "duplicate leads CRM", "CRM data quality issues",
    "no one updates the CRM", "reps don't update the CRM",
    "sales team hates the CRM", "sales team won't use the CRM",
    "low CRM adoption", "poor CRM adoption", "CRM adoption problem",
    "manual data entry CRM", "too much manual data entry",
    "spreadsheets instead of CRM", "still using spreadsheets for sales",
    "tracking leads in spreadsheets", "tracking deals in spreadsheets",
    "no visibility into pipeline", "no pipeline visibility",
    "can't see our pipeline", "pipeline is a black box",
    "forecasting is a guess", "sales forecasting is inaccurate",
    "inaccurate sales forecast", "forecast doesn't match reality",
    "reports take forever", "building reports manually",
    "CRM reporting is limited", "CRM reporting is weak",

    # ── SETUP / IMPLEMENTATION FRUSTRATION ───────────────────────────────────
    "CRM implementation nightmare", "CRM implementation failed",
    "CRM setup took months", "CRM migration nightmare",
    "migrating off our CRM", "migrating from our CRM",
    "CRM onboarding took forever", "took too long to set up CRM",
    "CRM customization is hard", "hard to customize our CRM",
    "need a developer to change anything", "too technical for our team",
    "CRM requires an admin", "need a dedicated CRM admin",
    "consultants to set up CRM", "paying consultants for CRM",

    # ── FEE / COST FRUSTRATION ───────────────────────────────────────────────
    "CRM is too expensive", "CRM pricing too high",
    "CRM cost too much", "per seat pricing CRM", "per user pricing CRM",
    "CRM licensing costs", "CRM renewal price increase",
    "CRM price hike", "surprise CRM fees", "hidden fees CRM",
    "add-ons cost extra CRM", "everything is an add-on",
    "paying for features we don't use", "paying for unused seats",
    "cheaper alternative to Salesforce", "cheaper than Salesforce",
    "cheaper CRM", "affordable CRM for small business",
    "budget-friendly CRM", "CRM on a budget", "best value CRM",
    "CRM ROI", "not seeing ROI from our CRM",

    # ── COMPETITOR / SALESFORCE MENTIONS ─────────────────────────────────────
    "Salesforce is too complex", "Salesforce too complicated",
    "Salesforce is overkill", "Salesforce overkill for small business",
    "Salesforce too expensive", "Salesforce pricing",
    "Salesforce alternative", "alternative to Salesforce",
    "leaving Salesforce", "switching from Salesforce",
    "migrating from Salesforce", "migrating off Salesforce",
    "moving away from Salesforce", "Salesforce is a pain",
    "Salesforce admin nightmare", "need a Salesforce admin",
    "HubSpot alternative", "alternative to HubSpot",
    "switching from HubSpot", "leaving HubSpot", "HubSpot too expensive",
    "HubSpot pricing", "HubSpot limitations",
    "Zoho CRM problem", "Zoho CRM issue", "switching from Zoho",
    "Pipedrive limitations", "Pipedrive alternative", "switching from Pipedrive",
    "Monday CRM problem", "Monday sales CRM issue",
    "Copper CRM problem", "Close CRM alternative",
    "Freshsales problem", "Freshsales alternative",
    "Insightly problem", "Insightly alternative",
    "Nimble CRM problem", "SugarCRM problem",
    "Microsoft Dynamics alternative", "Dynamics 365 too complex",
    "alternative to HubSpot", "alternative to Pipedrive",
    "alternative to Zoho", "alternative to Monday CRM",
    "better than Salesforce", "better than HubSpot",
    "better than Pipedrive", "competitors to Salesforce",
    "Salesforce competitors", "HubSpot competitors",

    # ── RECOMMENDATION REQUESTS ──────────────────────────────────────────────
    "recommend a CRM", "recommend a sales tool", "recommend a pipeline tool",
    "recommend a sales platform", "anyone recommend a CRM",
    "can anyone recommend a CRM", "does anyone recommend a CRM",
    "what CRM do you use", "what CRM should I use",
    "which CRM is best", "which CRM should we use",
    "best CRM for small business", "best CRM for startups",
    "best CRM for sales teams", "best CRM for agencies",
    "best CRM for real estate", "best CRM for solo founders",
    "best sales pipeline tool", "best pipeline management tool",
    "best sales tracking tool", "best lead tracking tool",
    "looking for a CRM", "looking for a sales tool",
    "looking for a pipeline tool", "searching for a CRM",
    "need a CRM", "need a sales tool", "need a pipeline tool",
    "need a better CRM", "need a simple CRM", "need an easy CRM",
    "anyone using a CRM", "does anyone use", "has anyone used",
    "who uses", "what do you use for sales", "what are you using for CRM",
    "tried several CRMs", "tried multiple CRMs", "tried everything CRM",
    "still looking for a CRM", "still haven't found a CRM",

    # ── SALES TOOLS / PIPELINE ────────────────────────────────────────────────
    "sales pipeline management", "pipeline management tool",
    "sales pipeline tracking", "deal tracking tool",
    "lead tracking software", "lead management tool",
    "lead scoring tool", "sales automation tool",
    "sales engagement platform", "sales enablement tool",
    "outbound sales tool", "cold outreach tool", "cold email tool",
    "sales prospecting tool", "prospecting software",
    "sales sequence tool", "email sequencing tool",
    "sales dialer", "auto dialer sales", "call tracking sales",
    "quote to cash", "proposal software sales", "contract management sales",
    "sales forecasting tool", "revenue operations tool",
    "RevOps tool", "sales analytics tool", "sales dashboard tool",
    "deal desk tool", "sales stack", "sales tech stack",
    "building our sales stack", "sales tools we use",

    # ── BUSINESS CONTEXT ──────────────────────────────────────────────────────
    "my sales team", "our sales team", "small sales team",
    "growing sales team", "scaling our sales team", "sales reps need",
    "sales manager needs", "head of sales needs",
    "startup sales process", "our sales process", "no sales process",
    "informal sales process", "need a sales process",
    "founder-led sales", "solo founder sales", "one-person sales team",
    "agency CRM needs", "real estate CRM needs",
    "B2B sales pipeline", "B2B sales tool", "B2B sales software",
    "SaaS sales tool", "SaaS CRM", "startup CRM",

    # ── COMPLIANCE / DATA ─────────────────────────────────────────────────────
    "CRM data security", "CRM GDPR compliance", "CRM data privacy",
    "CRM permissions issue", "CRM access control",
    "data silos sales marketing", "sales and marketing not aligned",
    "CRM integration issue", "CRM doesn't integrate with",
    "CRM integration with email", "CRM integration with marketing",
    "CRM API limitations", "CRM lacks integrations",

    # ── URGENCY / EXPANSION SIGNALS ───────────────────────────────────────────
    "urgently need a CRM", "need a CRM ASAP", "need this set up quickly",
    "launching soon need CRM", "onboarding new sales hires",
    "just hired our first salesperson", "scaling our sales operations",
    "new sales hire needs a CRM", "board wants better reporting",
    "investors asking about pipeline", "need better reporting for investors",

    # ── JOB SIGNALS ────────────────────────────────────────────────────────────
    "VP of sales", "head of sales", "sales operations manager",
    "RevOps manager", "revenue operations manager", "CRM administrator",
    "Salesforce administrator", "Salesforce admin", "Salesforce developer",
    "sales enablement manager", "director of sales operations",
    "chief revenue officer", "CRO", "sales operations analyst",
    
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
            # ── SHIPPING / DELIVERY PROBLEMS ──────────────────────────────────────────
    "shipment delayed", "shipment stuck", "shipment lost",
    "package delayed", "package stuck", "package lost",
    "order delayed", "order stuck in transit", "stuck in customs",
    "held at customs", "customs delay", "customs clearance issue",
    "customs holding my shipment", "customs rejected my shipment",
    "freight delayed", "freight stuck", "container delayed",
    "container stuck", "container held", "cargo delayed", "cargo stuck",
    "shipment damaged", "damaged in transit", "damaged freight",
    "damaged goods arrived", "arrived damaged", "goods lost in transit",
    "missing shipment", "missing package", "missing inventory",
    "tracking not updating", "no tracking updates",
    "can't track my shipment", "no visibility into shipment",
    "no visibility into freight", "no supply chain visibility",
    "shipment taking forever", "delivery taking forever",
    "late delivery", "missed delivery window", "missed delivery deadline",
    "delivery delayed again", "delayed again", "shipment delayed again",

    # ── CARRIER PROBLEMS / SWAPS ─────────────────────────────────────────────
    "carrier dropped my shipment", "carrier cancelled", "carrier failed",
    "carrier issue", "carrier problem", "carrier keeps delaying",
    "carrier unreliable", "unreliable carrier", "bad carrier experience",
    "switching carriers", "switched carriers", "need a new carrier",
    "looking for a new carrier", "carrier alternative",
    "alternative carrier", "carrier keeps raising rates",
    "carrier rate increase", "carrier surcharge", "unexpected surcharge",
    "fuel surcharge too high", "accessorial fees", "hidden carrier fees",
    "carrier capacity issues", "no capacity available",
    "can't get capacity", "capacity crunch", "space unavailable",
    "booking rejected carrier", "carrier booking cancelled",
    "carrier overbooked", "rolled shipment", "shipment rolled",
    "carrier keeps rolling my cargo", "blank sailing",
    "vessel delayed", "vessel skipped port", "port congestion",
    "port delays", "port backlog", "warehouse delays",
    "3PL problem", "3PL issue", "3PL dropped the ball",
    "switching 3PL", "leaving our 3PL", "need a new 3PL",
    "freight forwarder problem", "freight forwarder issue",
    "switching freight forwarders", "need a new freight forwarder",
    "trucking company problem", "trucking company unreliable",
    "LTL carrier issue", "FTL carrier issue", "drayage delay",
    "drayage problem", "last mile delivery problem",
    "last mile delivery issues", "final mile delivery problem",

    # ── FEE / COST FRUSTRATION ────────────────────────────────────────────────
    "shipping costs too high", "freight costs too high",
    "shipping rates increased", "freight rates increased",
    "shipping fees killing margins", "freight fees eating margins",
    "logistics costs too high", "cheaper shipping option",
    "cheaper freight option", "cheaper carrier", "cheaper 3PL",
    "affordable freight forwarding", "reduce shipping costs",
    "lower our freight costs", "cut shipping costs",
    "shipping cost comparison", "compare freight rates",
    "best freight rates", "best shipping rates",

    # ── COMPETITOR / PLATFORM MENTIONS ───────────────────────────────────────
    "FedEx delayed", "FedEx lost my package", "FedEx problem",
    "FedEx issue", "UPS delayed", "UPS lost my package", "UPS problem",
    "USPS lost my package", "USPS delayed", "USPS problem",
    "DHL delayed", "DHL lost my package", "DHL problem",
    "Maersk delayed", "Maersk booking issue", "MSC delayed",
    "MSC booking issue", "CMA CGM delayed", "COSCO delayed",
    "Flexport problem", "Flexport issue", "Flexport alternative",
    "leaving Flexport", "switching from Flexport",
    "project44 alternative", "FourKites alternative",
    "ShipBob problem", "ShipBob issue", "ShipBob alternative",
    "leaving ShipBob", "ShipStation problem", "ShipStation alternative",
    "Shippo problem", "Shippo alternative", "EasyPost alternative",
    "Freightos alternative", "uShip problem", "Convoy shut down",
    "alternative to FedEx", "alternative to UPS", "alternative to DHL",
    "alternative to Flexport", "alternative to ShipBob",
    "better than Flexport", "better than ShipBob",
    "competitors to Flexport", "Flexport competitors",

    # ── RECOMMENDATION REQUESTS ───────────────────────────────────────────────
    "recommend a freight forwarder", "recommend a carrier",
    "recommend a 3PL", "recommend a logistics provider",
    "recommend a shipping company", "recommend a fulfillment company",
    "anyone recommend a carrier", "can anyone recommend a 3PL",
    "does anyone recommend a freight forwarder",
    "what carrier do you use", "what 3PL do you use",
    "which carrier is best", "which 3PL is best",
    "best freight forwarder for", "best 3PL for small business",
    "best fulfillment company", "best shipping carrier for ecommerce",
    "looking for a freight forwarder", "looking for a 3PL",
    "looking for a carrier", "looking for a fulfillment partner",
    "need a logistics partner", "need a shipping partner",
    "need a new supplier for shipping", "anyone using",
    "has anyone used", "who do you use for shipping",
    "what are you using for fulfillment", "tried several carriers",
    "tried multiple 3PLs", "still looking for a carrier",

    # ── SUPPLY CHAIN / SOURCING ───────────────────────────────────────────────
    "supply chain disruption", "supply chain issue", "supply chain problem",
    "supply chain delay", "supply chain risk", "supply chain visibility",
    "diversifying our supply chain", "diversify suppliers",
    "reduce supply chain risk", "supply chain resilience",
    "reshoring manufacturing", "nearshoring supply chain",
    "friend-shoring", "supplier diversification",
    "single source supplier risk", "backup supplier needed",
    "need a backup supplier", "supplier reliability issues",
    "supplier missed deadline", "supplier delay", "manufacturer delay",
    "factory delay", "production delay", "inventory shortage",
    "stock shortage", "out of stock supplier issue",
    "inventory management problem", "warehouse management issue",
    "demand planning problem", "forecasting supply chain",
    "procurement issue", "procurement delay", "sourcing new supplier",
    "sourcing new manufacturer", "vetting new supplier",
    "supplier audit", "supplier quality issue", "quality control issue factory",

    # ── BUSINESS CONTEXT ──────────────────────────────────────────────────────
    "our warehouse", "our fulfillment center", "our distribution center",
    "ecommerce fulfillment", "ecommerce shipping", "dropshipping supplier",
    "dropshipping issue", "import/export logistics", "cross-border shipping",
    "international shipping problem", "international freight",
    "B2B shipping", "wholesale shipping", "bulk shipping",
    "small business shipping", "startup logistics", "scaling logistics",
    "growing ecommerce brand shipping", "DTC brand fulfillment",
    "manufacturing overseas", "shipping from China", "shipping from Asia",
    "container shipping from China", "freight from China",

    # ── COMPLIANCE / DOCUMENTATION ────────────────────────────────────────────
    "bill of lading issue", "customs documentation error",
    "incoterms confusion", "wrong incoterms", "tariff increase",
    "tariff impact supply chain", "duties and tariffs issue",
    "import duties too high", "export documentation problem",
    "compliance issue shipping", "trade compliance", "HS code error",
    "denied party screening", "customs broker issue",
    "customs broker problem", "need a customs broker",

    # ── URGENCY SIGNALS ────────────────────────────────────────────────────────
    "urgently need shipping", "need this shipped ASAP",
    "customer waiting on shipment", "customers are angry about shipping",
    "losing customers over shipping delays", "losing the contract shipping",
    "peak season shipping", "holiday shipping delays",
    "need a solution before peak season", "running out of inventory",
    "can't fulfill orders", "backordered", "backlog of orders",

    # ── JOB SIGNALS ────────────────────────────────────────────────────────────
    "supply chain manager", "logistics manager", "logistics coordinator",
    "procurement manager", "sourcing manager", "fulfillment manager",
    "warehouse manager", "operations manager logistics",
    "VP supply chain", "head of logistics", "head of supply chain",
    "director of logistics", "director of supply chain",
    "chief supply chain officer", "freight broker", "logistics analyst",
    
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
            
            # ── HIRING / RECRUITING PAIN POINTS ──────────────────────────────────────
    "hiring is a nightmare", "recruiting is a nightmare",
    "can't find good candidates", "can't find qualified candidates",
    "struggling to hire", "struggling to find talent",
    "talent shortage", "hard to find good talent",
    "too many unqualified applicants", "flooded with applications",
    "drowning in resumes", "too many resumes to screen",
    "resume screening taking forever", "screening candidates manually",
    "no time to screen candidates", "hiring process too slow",
    "recruiting process too slow", "hiring taking too long",
    "time to hire too long", "losing candidates to slow process",
    "losing candidates to other offers", "candidate ghosted us",
    "candidates ghosting", "no shows for interviews",
    "interview no shows", "offer rejected", "candidate declined offer",
    "high candidate drop off", "candidates dropping out of process",
    "bad candidate experience", "poor candidate experience",
    "our hiring process is broken", "broken hiring process",
    "no structured interview process", "inconsistent interview process",
    "hiring manager not responsive", "hiring managers slow to respond",
    "interview feedback delayed", "no feedback loop hiring",

    # ── ATS / TOOLING FRUSTRATION ─────────────────────────────────────────────
    "our ATS is a mess", "ATS is clunky", "clunky ATS",
    "outdated ATS", "ATS feels outdated", "hate our ATS",
    "ATS is too complicated", "ATS too complex", "ATS doesn't work for us",
    "outgrown our ATS", "outgrown our applicant tracking system",
    "ATS doesn't scale", "ATS can't handle our volume",
    "ATS is too slow", "ATS keeps crashing", "ATS keeps glitching",
    "ATS data is a mess", "duplicate candidates ATS",
    "no one updates the ATS", "recruiters don't use the ATS",
    "low ATS adoption", "manual tracking candidates",
    "tracking candidates in spreadsheets", "still using spreadsheets to hire",
    "no visibility into hiring pipeline", "can't see our hiring pipeline",
    "hiring pipeline is a black box", "reporting on hiring is hard",
    "ATS reporting is limited", "ATS doesn't integrate with",
    "ATS lacks integrations", "ATS integration issue",

    # ── FEE / COST FRUSTRATION ────────────────────────────────────────────────
    "recruiting costs too high", "cost per hire too high",
    "ATS pricing too high", "HR software too expensive",
    "per seat pricing ATS", "per employee pricing HR software",
    "recruiter agency fees too high", "staffing agency fees too high",
    "hidden fees ATS", "surprise renewal fees HR software",
    "HR software renewal price increase", "job board costs too high",
    "job posting fees too expensive", "cheaper alternative to Greenhouse",
    "cheaper alternative to Workday", "cheaper ATS",
    "affordable HR software for small business", "budget-friendly ATS",
    "best value ATS", "not seeing ROI from our ATS",

    # ── COMPETITOR MENTIONS ───────────────────────────────────────────────────
    "Greenhouse too complex", "Greenhouse too expensive",
    "Greenhouse alternative", "alternative to Greenhouse",
    "switching from Greenhouse", "leaving Greenhouse",
    "Workday is a nightmare", "Workday too complicated",
    "Workday alternative", "alternative to Workday",
    "switching from Workday", "leaving Workday",
    "Lever alternative", "switching from Lever", "leaving Lever",
    "BambooHR problem", "BambooHR alternative", "switching from BambooHR",
    "ADP problem", "ADP alternative", "switching from ADP",
    "Gusto problem", "Gusto alternative", "switching from Gusto",
    "Rippling problem", "Rippling alternative",
    "iCIMS problem", "iCIMS alternative", "switching from iCIMS",
    "JazzHR alternative", "Breezy HR alternative", "Recruitee alternative",
    "SmartRecruiters alternative", "Workable alternative",
    "Zoho Recruit alternative", "Indeed hiring platform problem",
    "LinkedIn Recruiter too expensive", "LinkedIn Recruiter alternative",
    "ZipRecruiter problem", "ZipRecruiter alternative",
    "alternative to Greenhouse", "alternative to Lever",
    "alternative to BambooHR", "alternative to Workable",
    "better than Greenhouse", "better than Workday",
    "competitors to Greenhouse", "Greenhouse competitors",
    "Workday competitors",

    # ── RECOMMENDATION REQUESTS ───────────────────────────────────────────────
    "recommend an ATS", "recommend a hiring tool", "recommend an HR platform",
    "recommend a recruiting tool", "recommend a payroll provider",
    "anyone recommend an ATS", "can anyone recommend a hiring tool",
    "does anyone recommend an HR platform", "what ATS do you use",
    "what HR software do you use", "which ATS is best",
    "which HR platform is best", "best ATS for small business",
    "best ATS for startups", "best HR software for small teams",
    "best recruiting software", "best payroll software",
    "best workforce management tool", "looking for an ATS",
    "looking for an HR platform", "looking for a recruiting tool",
    "looking for a payroll provider", "need an ATS",
    "need an HR platform", "need a recruiting tool",
    "need a simple ATS", "need an easy HR system",
    "anyone using an ATS", "does anyone use", "has anyone used",
    "who uses", "what are you using for hiring",
    "tried several ATS platforms", "tried multiple HR tools",
    "still looking for an ATS", "still haven't found the right HR software",

    # ── RECRUITING / HR TOOLS & CATEGORIES ────────────────────────────────────
    "applicant tracking system", "candidate relationship management",
    "recruiting CRM", "talent acquisition software",
    "employer branding tool", "job posting software",
    "interview scheduling tool", "automated interview scheduling",
    "background check software", "reference checking tool",
    "onboarding software", "employee onboarding tool",
    "payroll software", "benefits administration software",
    "performance management software", "employee engagement tool",
    "workforce management software", "HRIS platform",
    "time tracking software HR", "PTO tracking tool",
    "compensation management tool", "org chart software",
    "employee scheduling software", "shift scheduling tool",
    "recruitment marketing tool", "candidate sourcing tool",
    "sourcing candidates tool", "resume parsing tool",
    "skills assessment tool", "pre-employment testing",
    "video interview platform", "async video interview tool",

    # ── BUSINESS CONTEXT ───────────────────────────────────────────────────────
    "my HR team", "our HR team", "small HR team", "one person HR team",
    "no dedicated HR person", "wearing the HR hat", "founder doing HR",
    "growing our team", "scaling our hiring", "scaling headcount",
    "hiring our first employee", "hiring first HR hire",
    "startup hiring process", "no formal hiring process",
    "need a hiring process", "remote hiring challenges",
    "hiring remote employees", "distributed team hiring",
    "hiring across multiple countries", "international hiring",
    "hiring contractors vs employees", "EOR provider", "employer of record",
    "PEO provider", "professional employer organization",

    # ── COMPLIANCE / HR RISK ───────────────────────────────────────────────────
    "compliance issue HR", "employment law compliance",
    "wrongful termination risk", "HR compliance nightmare",
    "I-9 compliance", "E-Verify issue", "labor law compliance",
    "wage and hour compliance", "overtime compliance issue",
    "worker classification issue", "1099 vs W2 issue",
    "background check compliance", "EEOC complaint", "HR audit",
    "failed HR audit", "employee handbook outdated",

    # ── URGENCY SIGNALS ────────────────────────────────────────────────────────
    "urgently need to hire", "need to hire ASAP", "critical role open",
    "position open for months", "role has been open too long",
    "losing revenue because understaffed", "understaffed team",
    "burning out the team hiring slow", "board wants headcount plan",
    "investors asking about headcount", "need to scale hiring fast",

    # ── JOB SIGNALS ────────────────────────────────────────────────────────────
    "head of talent", "head of HR", "head of people",
    "VP of people", "VP of talent", "chief people officer",
    "talent acquisition manager", "recruiting manager",
    "HR manager", "HR business partner", "people operations manager",
    "HRIS manager", "compensation and benefits manager",
    "director of talent acquisition", "director of people operations",
    "technical recruiter", "corporate recruiter", "recruiting coordinator",
    
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
             # ── ACCOUNTING / BOOKKEEPING PAIN POINTS ──────────────────────────────────
    "our books are a mess", "bookkeeping is a mess", "behind on bookkeeping",
    "months behind on bookkeeping", "need to catch up on books",
    "accounting is a nightmare", "accounting software is a nightmare",
    "hate our accounting software", "accounting software too complicated",
    "accounting software too complex", "outdated accounting software",
    "accounting software feels outdated", "outgrown our accounting software",
    "accounting software doesn't scale", "accounting software is too slow",
    "accounting software keeps crashing", "accounting software keeps glitching",
    "reconciliation is a nightmare", "bank reconciliation taking forever",
    "reconciling accounts manually", "manual data entry accounting",
    "too much manual data entry bookkeeping", "still using spreadsheets for accounting",
    "tracking expenses in spreadsheets", "invoicing manually",
    "sending invoices manually", "chasing invoices", "chasing late payments",
    "chasing unpaid invoices", "clients not paying invoices",
    "cash flow visibility problem", "no visibility into cash flow",
    "can't see our cash flow", "cash flow is a black box",
    "financial reporting takes forever", "building reports manually finance",
    "accounting reporting is limited", "accounting reporting is weak",
    "closing the books takes forever", "month end close takes too long",
    "month end close nightmare", "year end accounting nightmare",
    "tax season nightmare", "not ready for tax season",
    "no idea what we owe in taxes", "surprised by tax bill",
    "underestimated taxes owed", "quarterly taxes nightmare",

    # ── SETUP / MIGRATION FRUSTRATION ────────────────────────────────────────
    "accounting software migration nightmare", "migrating off our accounting software",
    "migrating from QuickBooks", "switching accounting software",
    "accounting software setup took months", "accounting software onboarding nightmare",
    "hard to customize our accounting software", "need an accountant to set this up",
    "need a bookkeeper to fix this", "paying a bookkeeper to clean up our books",
    "cleanup project bookkeeping", "books need a cleanup",

    # ── FEE / COST FRUSTRATION ────────────────────────────────────────────────
    "accounting software too expensive", "accounting software pricing too high",
    "bookkeeping fees too high", "accountant fees too high",
    "per user pricing accounting software", "accounting software renewal price increase",
    "accounting software price hike", "hidden fees accounting software",
    "add-ons cost extra accounting software", "paying for features we don't use accounting",
    "cheaper alternative to QuickBooks", "cheaper than QuickBooks",
    "cheaper accounting software", "affordable accounting software for small business",
    "budget-friendly accounting software", "best value accounting software",
    "not seeing ROI from accounting software",

    # ── COMPETITOR MENTIONS ───────────────────────────────────────────────────
    "QuickBooks is a nightmare", "QuickBooks too complicated",
    "QuickBooks too expensive", "QuickBooks pricing increase",
    "QuickBooks alternative", "alternative to QuickBooks",
    "leaving QuickBooks", "switching from QuickBooks",
    "migrating from QuickBooks", "QuickBooks customer support terrible",
    "Xero alternative", "alternative to Xero", "switching from Xero",
    "leaving Xero", "Xero problem", "Xero issue",
    "FreshBooks alternative", "switching from FreshBooks",
    "Wave accounting problem", "Wave accounting alternative",
    "Sage alternative", "Sage accounting problem", "switching from Sage",
    "NetSuite too complex", "NetSuite too expensive", "NetSuite alternative",
    "Zoho Books alternative", "switching from Zoho Books",
    "Bill.com problem", "Bill.com alternative",
    "Gusto payroll problem", "Gusto accounting integration issue",
    "Expensify problem", "Expensify alternative",
    "alternative to Xero", "alternative to FreshBooks",
    "alternative to NetSuite", "alternative to Sage",
    "better than QuickBooks", "better than Xero",
    "competitors to QuickBooks", "QuickBooks competitors",
    "Xero competitors",

    # ── RECOMMENDATION REQUESTS ───────────────────────────────────────────────
    "recommend an accounting software", "recommend a bookkeeping tool",
    "recommend an accountant", "recommend a bookkeeper",
    "anyone recommend an accounting software", "can anyone recommend a bookkeeper",
    "does anyone recommend an accountant", "what accounting software do you use",
    "which accounting software is best", "best accounting software for small business",
    "best accounting software for startups", "best accounting software for freelancers",
    "best invoicing software", "best expense tracking software",
    "best bookkeeping software", "best payroll and accounting software",
    "looking for an accounting software", "looking for a bookkeeper",
    "looking for an accountant", "need an accounting software",
    "need a bookkeeper", "need an accountant", "need a simple accounting tool",
    "anyone using", "does anyone use", "has anyone used",
    "who uses", "what are you using for accounting",
    "tried several accounting tools", "still looking for accounting software",
    "still haven't found the right accounting software",

    # ── ACCOUNTING TOOLS & CATEGORIES ────────────────────────────────────────
    "invoicing software", "expense tracking software", "expense management tool",
    "receipt scanning app", "mileage tracking app", "payroll software",
    "tax filing software", "tax preparation software", "sales tax software",
    "sales tax compliance tool", "1099 filing software", "W2 filing software",
    "accounts payable software", "accounts receivable software",
    "AP automation", "AR automation", "cash flow forecasting tool",
    "financial planning software", "FP&A tool", "budgeting software business",
    "multi-entity accounting software", "multi-currency accounting software",
    "inventory accounting software", "job costing software",
    "project accounting software", "nonprofit accounting software",
    "e-commerce accounting software", "Shopify accounting integration",
    "Amazon seller accounting software",

    # ── BUSINESS CONTEXT ───────────────────────────────────────────────────────
    "my bookkeeper", "our bookkeeper", "my accountant", "our accountant",
    "small business accounting", "startup accounting", "solo founder accounting",
    "freelancer accounting", "self employed accounting", "DIY bookkeeping",
    "doing my own books", "founder doing the books", "wearing the finance hat",
    "no dedicated finance person", "growing business need better accounting",
    "scaling finance operations", "outsourced bookkeeping", "outsourced accounting",
    "virtual CFO", "fractional CFO", "need a fractional CFO",
    "part time bookkeeper", "part time accountant",

    # ── COMPLIANCE / TAX RISK ─────────────────────────────────────────────────
    "IRS audit", "audit risk small business", "tax compliance issue",
    "missed tax deadline", "late filing penalty", "sales tax nexus issue",
    "multi-state tax compliance", "1099 compliance issue",
    "payroll tax compliance", "bookkeeping compliance issue",
    "financial statements for investors", "need clean books for investors",
    "due diligence financials", "GAAP compliance issue",

    # ── URGENCY SIGNALS ────────────────────────────────────────────────────────
    "urgently need a bookkeeper", "need books cleaned up ASAP",
    "tax deadline approaching", "need this done before tax season",
    "investors asking for financials", "due diligence deadline",
    "board wants updated financials", "need financials for loan application",
    "need financials for a loan", "applying for a business loan financials",

    # ── JOB SIGNALS ────────────────────────────────────────────────────────────
    "controller", "VP of finance", "head of finance", "director of finance",
    "chief financial officer", "CFO", "finance manager", "accounting manager",
    "bookkeeper", "staff accountant", "senior accountant",
    "accounts payable manager", "accounts receivable manager",
    "financial analyst", "FP&A manager",
    
        ],
    },
    "ai_agents": {
        "label": "AI Agents",
        "keywords": [],
    },
    "community_software": {
        "label": "Community Software / Online Community Platform",
        "keywords": [],
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
    if best_key:
        return best_key

    # NEW — fuzzy fallback. If nothing matched exactly (word-boundary),
    # the value may still be a close variant of a keyword — e.g. a
    # search_keyword of "wise block my account" isn't a substring match
    # for "wise block account", but it's clearly the same intent. Only
    # runs when the exact pass above found nothing, so it never overrides
    # an exact match; applies uniformly across every industry's keyword
    # list, same as the exact pass.
    return _fuzzy_match_industry_from_string(s)


FUZZY_MATCH_THRESHOLD = float(os.getenv("FUZZY_MATCH_THRESHOLD", "0.70"))


def _fuzzy_match_industry_from_string(s: str):
    """
    NEW — fuzzy fallback used only when the exact word-boundary pass in
    _match_industry_from_string finds no match. Compares s against every
    keyword across every industry using difflib's SequenceMatcher ratio
    (a 0.0–1.0 similarity score). If a keyword clears
    FUZZY_MATCH_THRESHOLD (default 0.70, i.e. 70% similar), that industry
    is counted as a match. Returns the industry key belonging to the
    single highest-ratio keyword at/above the threshold, or None if
    nothing clears it.
    """
    if not s:
        return None
    s_norm = " ".join(s.strip().lower().split())
    best_key, best_ratio = None, 0.0
    for key, cfg in INDUSTRIES.items():
        for kw in cfg["keywords"]:
            ratio = difflib.SequenceMatcher(None, s_norm, kw.lower()).ratio()
            if ratio >= FUZZY_MATCH_THRESHOLD and ratio > best_ratio:
                best_key, best_ratio = key, ratio
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
