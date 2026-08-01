"""
Flintel Shopping Assistant
-----------------------------
FastAPI backend + Claude Haiku 4.5 powered e-commerce chatbot.

IMPORTANT: there is NO fixed local product database here. Claude itself
comes up with realistic product recommendations based on what the user
asks and the context of the conversation. For example:

    User: "I'm going to Dubai, recommend me some t-shirts"
    -> Claude understands Dubai = hot climate, and generates lightweight,
       breathable t-shirts itself (name, price, rating, color).
    -> Claude ALSO suggests complementary items on its own initiative
       (shorts/trousers, sandals, sunglasses, etc.) as a
       "You might also like" cross-sell list — exactly like a real
       shopping assistant would upsell related items.

Claude replies in strict JSON (reply + products + recommended). The
backend does not invent product data itself — it only turns Claude's
JSON into image URLs (deterministic placeholder images keyed off the
product name, since Claude can't fetch real photos) and serves it to
the frontend.

Run:
    pip install fastapi uvicorn anthropic jinja2 python-multipart
    export ANTHROPIC_API_KEY=your_key_here
    uvicorn index:app --reload
"""

import os
import re
import json
import requests

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

import anthropic

try:
    from dotenv import load_dotenv
    load_dotenv()  # reads a .env file in the same folder, if present
except ImportError:
    pass  # python-dotenv is optional; env var can still be set manually

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

MODEL = "claude-haiku-4-5-20251001"
API_KEY = os.environ.get("ANTHROPIC_API_KEY")
UNSPLASH_ACCESS_KEY = os.environ.get("UNSPLASH_ACCESS_KEY")  # optional but recommended

if not API_KEY:
    raise RuntimeError(
        "\n\n"
        "ANTHROPIC_API_KEY is not set.\n"
        "Fix one of these ways before running the server:\n"
        "  1) Create a file named .env next to index.py containing:\n"
        "     ANTHROPIC_API_KEY=sk-ant-your-key-here\n"
        "  2) Or set it in the SAME terminal you run uvicorn from:\n"
        "     Windows PowerShell:  $env:ANTHROPIC_API_KEY = \"sk-ant-your-key-here\"\n"
        "     Windows cmd.exe:     set ANTHROPIC_API_KEY=sk-ant-your-key-here\n"
        "     macOS/Linux:         export ANTHROPIC_API_KEY=sk-ant-your-key-here\n"
        "Get a key at https://console.anthropic.com/settings/keys\n"
    )

if not UNSPLASH_ACCESS_KEY:
    print(
        "[warning] UNSPLASH_ACCESS_KEY is not set — product images will fall back "
        "to generic placeholders instead of real photos. Get a free key at "
        "https://unsplash.com/developers and add UNSPLASH_ACCESS_KEY=... to your .env file."
    )

client = anthropic.Anthropic(api_key=API_KEY)

app = FastAPI(title="Flintel Shopping Assistant")
templates = Jinja2Templates(directory="templates")

# ---------------------------------------------------------------------------
# System prompt: Claude generates the products itself. No DB, no tool.
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are Flintel, the shopping assistant for an online clothing
store called Flintel. There is NO product database behind you — you
yourself come up with realistic, well-matched product recommendations
based on the user's request and the context of the conversation
(destination, weather/climate, season, occasion, budget, style, etc).

For example, if the user says "I'm going to Dubai, suggest some
t-shirts", think about the fact that Dubai is hot, and recommend
lightweight, breathable, light-colored t-shirts rather than generic
ones. That is just an example of how to reason about context — do the
same kind of thinking for whatever the user actually says, don't reuse
that specific example.

For every user request that involves products, respond with:
1. "products" — ONLY 2 to 3 items that most directly and best match
   what the user asked for. Do not return more than 3. Pick your best,
   highest-quality picks rather than padding the list.
2. "recommended" — complementary items, but ONLY include this when the
   conversation actually gives you a real reason to (e.g. the user
   mentioned a trip, an occasion, a specific need, or their message
   clearly implies a full outfit/use-case). Base it on what THIS user
   said, not a generic default pairing. If there's no real contextual
   basis, return an empty list — do not force a cross-sell every time.
3. Give each item in "recommended" a short "reason" tied to what the
   user actually said (not a generic reason).
4. Realistic price in USD, a rating between 3.5 and 5.0, a sensible
   color, and a category (t-shirt, hoodie, jeans, trousers, shorts,
   shoes, sandals, jacket, accessory, etc).

If the user is just chatting and not asking about products, return
empty lists for "products" and "recommended".

Handle advanced requests naturally: multiple constraints at once
("red hoodie under $40 with high ratings"), comparisons, follow-ups
referring to earlier turns, and vague context clues (destination,
weather, event) that imply what kind of products fit.

Also include "suggestions": 2 to 4 short quick-reply button labels
(2-5 words each) for what the user might naturally want to say or ask
next, based on THIS specific turn — not generic, not static. They
should update every message. Examples of the kind of thing to produce
(don't reuse these verbatim, generate your own fitting the actual
conversation): if you just showed t-shirts, suggestions could be
"Show me hoodies instead", "Under $20 only", "In blue", "Track my
order". If the user hasn't asked about products yet, suggestions
should help them get started (e.g. "Show me t-shirts", "Find a
hoodie", "Best rated jeans", "Track my order").

You MUST reply with ONLY valid JSON — no markdown fences, no text
before or after — matching exactly this shape:

{
  "reply": "short, warm, conversational reply (1-3 sentences, no product details listed out since cards render separately)",
  "products": [
    {"name": "string", "category": "string", "color": "string", "price": 0.0, "rating": 0.0}
  ],
  "recommended": [
    {"name": "string", "category": "string", "color": "string", "price": 0.0, "rating": 0.0, "reason": "short reason tied to what the user said"}
  ],
  "suggestions": ["string", "string"]
}
"""


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    message: str
    history: list[ChatMessage] = []


class ChatResponse(BaseModel):
    reply: str
    products: list
    recommended: list
    suggestions: list[str] = []


_image_cache: dict[str, str] = {}
_FALLBACK_BASE = "https://picsum.photos/seed"


def slugify(*parts: str) -> str:
    text = "-".join(parts).lower()
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text or "product"


def fetch_real_image(query: str, seed: str) -> str:
    """Look up a real photo on Unsplash matching the product description.
    Falls back to a generic placeholder if no API key is set or the
    request fails for any reason (network error, rate limit, etc)."""
    if query in _image_cache:
        return _image_cache[query]

    if UNSPLASH_ACCESS_KEY:
        try:
            resp = requests.get(
                "https://api.unsplash.com/search/photos",
                params={
                    "query": query,
                    "per_page": 1,
                    "orientation": "squarish",
                    "content_filter": "high",
                },
                headers={"Authorization": f"Client-ID {UNSPLASH_ACCESS_KEY}"},
                timeout=6,
            )
            if resp.ok:
                results = resp.json().get("results") or []
                if results:
                    url = results[0]["urls"]["small"]
                    _image_cache[query] = url
                    return url
        except requests.RequestException:
            pass  # fall through to placeholder

    fallback = f"{_FALLBACK_BASE}/{seed}/400/400"
    _image_cache[query] = fallback
    return fallback


def attach_image(item: dict) -> dict:
    """Real product photos matching name/category/color, via Unsplash."""
    seed = slugify(item.get("name", ""), item.get("color", ""), item.get("category", ""))
    query = f"{item.get('color', '')} {item.get('category', '')} clothing".strip()
    item["image"] = fetch_real_image(query, seed)
    return item


def extract_json(text: str) -> dict:
    """Claude is instructed to return raw JSON, but strip code fences
    defensively in case it adds them anyway."""
    cleaned = text.strip()
    cleaned = re.sub(r"^```(json)?", "", cleaned).strip()
    cleaned = re.sub(r"```$", "", cleaned).strip()
    return json.loads(cleaned)


def ask_claude(messages: list) -> dict:
    response = client.messages.create(
        model=MODEL,
        max_tokens=1200,
        system=SYSTEM_PROMPT,
        messages=messages,
    )
    text = "".join(block.text for block in response.content if block.type == "text")

    try:
        data = extract_json(text)
    except (json.JSONDecodeError, ValueError):
        # Fallback: Claude didn't return clean JSON, surface its raw text
        return {"reply": text.strip() or "Sorry, could you rephrase that?",
                "products": [], "recommended": []}

    data.setdefault("reply", "")
    data.setdefault("products", [])
    data.setdefault("recommended", [])
    data.setdefault("suggestions", [])

    data["products"] = [attach_image(p) for p in data["products"]][:3]
    data["recommended"] = [attach_image(p) for p in data["recommended"]][:3]
    data["suggestions"] = [str(s) for s in data["suggestions"]][:4]
    return data


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("bot.html", {"request": request})


@app.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    messages = [{"role": m.role, "content": m.content} for m in req.history]
    messages.append({"role": "user", "content": req.message})

    try:
        result = ask_claude(messages)
    except anthropic.AuthenticationError:
        return JSONResponse(
            status_code=500,
            content={
                "reply": "Server config error: ANTHROPIC_API_KEY is missing or invalid. "
                         "Check the .env file / environment variable and restart the server.",
                "products": [],
                "recommended": [],
                "suggestions": [],
            },
        )
    except anthropic.APIError as e:
        return JSONResponse(
            status_code=500,
            content={"reply": f"Sorry, the assistant is unavailable right now ({e.__class__.__name__}).",
                     "products": [], "recommended": [], "suggestions": []},
        )

    return ChatResponse(
        reply=result["reply"],
        products=result["products"],
        recommended=result["recommended"],
        suggestions=result["suggestions"],
    )
 

if __name__ == "__main__":
    import uvicorn 

    uvicorn.run("index:app", host="0.0.0.0", port=8000, reload=True) 