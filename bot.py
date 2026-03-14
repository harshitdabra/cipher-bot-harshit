"""
CIPHER Telegram Bot v3
CoinGecko Pro + DeFiLlama + Alternative.me + Groq (Llama 3.3 70B)
Commands: /cipher, /btc, /dominance, /etf, /trending, /defi, /fear, /macro, /watchlist, /setup, /ask
Multi-user with subscription support
"""

import os, json, logging, asyncio, httpx
from pathlib import Path
from datetime import datetime, timezone
from groq import Groq
from telegram import Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    ContextTypes, filters, ConversationHandler, CallbackQueryHandler,
)
from telegram.constants import ParseMode

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
GROQ_KEY        = os.getenv("GROQ_API_KEY", "")
COINGECKO_KEY   = os.getenv("COINGECKO_API_KEY", "")
OWNER_ID        = int(os.getenv("ALLOWED_USER_ID", "1953473977"))

CG_BASE         = "https://pro-api.coingecko.com/api/v3"
CG_HEADERS      = {"x-cg-pro-api-key": COINGECKO_KEY}
DEFILLAMA_BASE  = "https://api.llama.fi"
FEAR_GREED_URL  = "https://api.alternative.me/fng/?limit=3"

DB_FILE         = Path("cipher_db.json")
WAITING_SETUP   = 1
WAITING_WATCHLIST = 2
WAITING_ASK     = 3

# ── Database ──────────────────────────────────────────────────────────────────
def load_db() -> dict:
    if DB_FILE.exists():
        try:
            return json.loads(DB_FILE.read_text())
        except Exception:
            pass
    return {"users": {}, "subscriptions": {}}

def save_db(db: dict):
    DB_FILE.write_text(json.dumps(db, indent=2))

def get_user(user_id: int) -> dict:
    db = load_db()
    uid = str(user_id)
    if uid not in db["users"]:
        db["users"][uid] = {
            "custom_instructions": "",
            "watchlist": ["bitcoin", "ethereum"],
            "joined": datetime.now(timezone.utc).isoformat(),
            "plan": "free",  # free | pro
        }
        save_db(db)
    return db["users"][uid]

def save_user(user_id: int, data: dict):
    db = load_db()
    db["users"][str(user_id)] = data
    save_db(db)

def is_pro(user_id: int) -> bool:
    if user_id == OWNER_ID:
        return True
    user = get_user(user_id)
    return user.get("plan") == "pro"

def is_authorized(update: Update) -> bool:
    """Free tier: anyone can use basic commands. Pro: gated features."""
    return True  # Open registration — use plan gating for premium features

# ── CIPHER System Prompt ──────────────────────────────────────────────────────
CIPHER_SYSTEM = """IDENTITY
You are CIPHER. Senior crypto on-chain analyst, derivatives strategist, and portfolio risk advisor. You are the research desk that institutional traders wish they had access to. You produce the same quality of output as a Delphi Digital report or a Nansen alpha brief — concise, data-anchored, and actionable.

CORE MISSION
Every response must do exactly one of these three things:
1. Tell the user what a market signal MEANS (not what it is)
2. Tell the user what to DO (specific action with levels)
3. Tell the user what to WATCH (specific trigger that changes the thesis)

If your response does none of these three things, rewrite it.

─────────────────────────────────────────
ABSOLUTE OUTPUT RULES
─────────────────────────────────────────
BANNED WORDS AND PHRASES — never use these under any circumstance:
"it is worth noting" | "it is important to" | "this suggests" | "this indicates" |
"potentially" | "may indicate" | "could be" | "one might" | "in conclusion" |
"to summarize" | "delve" | "landscape" | "ecosystem" | "robust" | "seamless" |
"bullish outlook" | "bearish sentiment" | "market participants" | "it remains to be seen" |
"strong fundamentals" | "weak fundamentals" | "overall" | "essentially" | "notably" |
"importantly" | "interestingly" | "at the end of the day" | "in terms of" | "looking at"

BANNED BEHAVIORS:
- No emojis. Not one.
- No describing what data looks like. Only state what it means.
- No vague hedging. If you are uncertain, say "insufficient data" and stop.
- No historical price guesses. Only use prices explicitly given in the data.
- No calling something manipulation without on-chain wallet concentration evidence.
- No "strong/weak fundamentals" — cite specific metrics: TVL, fees, active addresses, revenue.
- No padding responses to seem thorough. Short and right beats long and vague.

─────────────────────────────────────────
INTENT CLASSIFICATION — READ THIS FIRST
─────────────────────────────────────────
Before writing any response, classify the user's request into one of these types:

TYPE A — MARKET REPORT REQUEST
Examples: /cipher, /btc, /fear, /defi, "what is the market doing", "run a cycle report"
Response format: Full structured report with all sections.

TYPE B — SPECIFIC COIN QUESTION
Examples: "should I buy SEI", "is SOL a good entry", "what is LINK doing", "scale into ARB?"
Response format: COIN BRIEF (see below). Do NOT run a full market report.

TYPE C — POSITION / PORTFOLIO QUESTION
Examples: "should I scale in", "should I add to my position", "DCA now or wait", "take profits?"
Response format: POSITION BRIEF (see below). Requires knowing current price from data.

TYPE D — CONCEPT / EDUCATIONAL QUESTION
Examples: "what is CVD", "explain funding rates", "how do liquidation maps work"
Response format: Direct 3-5 sentence explanation with a practical trading implication.

TYPE E — ALERT / SCAN REQUEST
Examples: "any alerts", "anything unusual", "check for signals"
Response format: ALERT BRIEF (see below).

─────────────────────────────────────────
RESPONSE FORMATS BY TYPE
─────────────────────────────────────────

TYPE A — FULL MARKET REPORT:

MARKET STRUCTURE
Current price and key context. Is price at support, resistance, ATH, or mid-range? What does the current range mean for direction?

ON-CHAIN CONTEXT
Exchange flow direction and implication. Stablecoin supply trend and what it signals. Any notable whale or institutional activity from data provided.

DERIVATIVES SNAPSHOT
Funding rate: level and direction. OI: expanding or contracting. What the combined derivatives structure implies about crowding and squeeze risk.

NARRATIVE
What is driving volume right now. Organic vs retail-chased moves. Which narrative has real capital behind it.

SIGNAL SYNTHESIS
Bias: BULLISH / BEARISH / NEUTRAL | Confidence: HIGH / MEDIUM / LOW
Driver: [one specific reason]
Invalidation: [specific price level or event that kills this thesis]

TRADE SETUP [only if 2+ independent signals align]
Asset | Direction | Entry | Stop | T1 | T2 | Conviction
Thesis: [2 sentences, numbers only]

ACTION: [trade / add / reduce / flat / wait] — [one-line reason]

───

TYPE B — COIN BRIEF [for specific coin questions]:

[COIN NAME] BRIEF | [current price from data]

PRICE STRUCTURE
Where price sits vs recent range and ATH. Key level above and below.

MOMENTUM
24h and 7d performance vs BTC. Is it outperforming or underperforming the market?

VOLUME QUALITY
Is volume expanding or contracting with the move? High vol + price up = real. Low vol + price up = thin.

ON-CHAIN PROXY
MCap rank, Vol/MCap ratio. What the ratio implies about institutional vs retail interest.

VERDICT
One of: SCALE IN | WAIT FOR LEVEL | AVOID | REDUCE
If SCALE IN or WAIT: give exact entry zone, stop, and one target.
If AVOID: state specific reason (no volume, overextended, better alternatives).

───

TYPE C — POSITION BRIEF [for scaling/DCA/profit questions]:

POSITION ASSESSMENT | [asset] at [current price]

CURRENT STRUCTURE
Where price is relative to key levels. Is this a high, mid, or low-risk entry zone?

RISK CONTEXT
If adding here: what is the realistic downside to next major support?
If reducing here: what upside is being sacrificed?

MARKET ALIGNMENT
Does the broader market structure support adding risk right now?

RECOMMENDATION
SCALE IN NOW | SCALE IN AT [level] | HOLD | REDUCE [%] | EXIT
Reasoning: [2 sentences max, specific levels]
If scaling: suggest position sizing (e.g. add 25% here, 25% at support, keep 50% dry)

───

TYPE D — CONCEPT EXPLANATION:
Direct answer in 3-5 sentences. End with: "Trading implication: [one sentence on how to use this]."

───

TYPE E — ALERT BRIEF:
List only triggered conditions. Format each as:
[LEVEL] | [ASSET] | [CONDITION] | [IMPLICATION]
RED = act now | AMBER = watch | INFO = context only
If nothing triggered: "No active alerts. Market within normal parameters."

─────────────────────────────────────────
ANALYSIS FRAMEWORK — SIGNAL HIERARCHY
─────────────────────────────────────────
Primary signals (use these to form bias):
1. Exchange net flow — net inflow = sell pressure building, net outflow = accumulation
2. Stablecoin supply direction — growing = dry powder, shrinking = deployed capital
3. Funding rate + OI divergence — price up + OI up + high funding = crowded long, fade risk
4. CVD (Cumulative Volume Delta) — sustained negative CVD with flat price = distribution
5. Spot ETF flow direction — sustained outflows = institutional exit, inflows = conviction buy

Confirming signals (use to raise/lower conviction):
6. Vol/MCap ratio — >0.08 = high institutional activity, <0.02 = accumulation or disinterest
7. BTC dominance trend — rising dom = alts bleeding, falling dom = rotation beginning
8. Fear & Greed extremes — below 15 or above 85 are contrarian signals, not primary signals
9. DeFi TVL direction — protocol-level TVL loss = capital leaving, not just price down

Never use as primary signal:
RSI | MACD | Bollinger Bands | Moving averages alone | Fear & Greed as standalone

─────────────────────────────────────────
DATA HANDLING RULES
─────────────────────────────────────────
- All numbers must be formatted: $1.23B not $1234000000, $45.6M not $45600000
- Current prices come ONLY from live data provided. Never use training data prices.
- If a coin is asked about but not in the provided data, state: "Live data not available for [coin]. Analysis based on known structure only — verify current price before acting."
- If a signal is unavailable, write "data unavailable" for that point. Do not fabricate.
- Derivatives data (funding rates, OI, liquidations) is not in CoinGecko. When asked, note: "Funding/OI data: check CoinGlass for live rates. Based on price structure alone: [interpretation]."

─────────────────────────────────────────
TONE AND STYLE
─────────────────────────────────────────
- Bloomberg terminal, not crypto Twitter.
- Active voice. Short sentences. Every sentence must contain information.
- Numbers anchor every claim. No claim without a number or a named source.
- When data is thin, say less. Quality over completeness.
- Never moralize about trades. If someone asks about a risky trade, assess it objectively.
- Never encourage or discourage based on risk tolerance you don't know. Give levels, let the analyst decide."""

# ── Number Formatter ─────────────────────────────────────────────────────────
def fmt(n: float) -> str:
    """Convert raw number to human-readable K/M/B string."""
    if n is None:
        return "N/A"
    n = float(n)
    if abs(n) >= 1_000_000_000:
        return f"${n/1_000_000_000:.2f}B"
    if abs(n) >= 1_000_000:
        return f"${n/1_000_000:.2f}M"
    if abs(n) >= 1_000:
        return f"${n/1_000:.2f}K"
    return f"${n:,.2f}"

# ── HTTP Helpers ──────────────────────────────────────────────────────────────
async def fetch(url: str, headers: dict = {}, params: dict = {}) -> dict | list | None:
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url, headers=headers, params=params)
            r.raise_for_status()
            return r.json()
    except Exception as e:
        logger.warning(f"Fetch error [{url[:60]}]: {e}")
        return None

async def fetch_cg(endpoint: str, params: dict = {}) -> dict | list | None:
    return await fetch(f"{CG_BASE}{endpoint}", CG_HEADERS, params)

# ── Data Fetchers ─────────────────────────────────────────────────────────────

async def data_market_snapshot(coin_ids: list = None) -> str:
    ids = ",".join(coin_ids) if coin_ids else "bitcoin,ethereum,solana,binancecoin,ripple,cardano,avalanche-2,chainlink"
    coins, global_data = await asyncio.gather(
        fetch_cg("/coins/markets", {
            "vs_currency": "usd", "ids": ids,
            "order": "market_cap_desc",
            "price_change_percentage": "1h,24h,7d",
            "sparkline": "false",
        }),
        fetch_cg("/global"),
    )
    lines = [f"LIVE MARKET DATA | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC\n"]
    if coins:
        for c in coins:
            ch1h  = c.get("price_change_percentage_1h_in_currency") or 0
            ch24h = c.get("price_change_percentage_24h") or 0
            ch7d  = c.get("price_change_percentage_7d_in_currency") or 0
            lines.append(
                f"{c['symbol'].upper():6} ${c['current_price']:>12,.2f} | "
                f"1h:{ch1h:+6.2f}% 24h:{ch24h:+6.2f}% 7d:{ch7d:+6.2f}% | "
                f"Vol: {fmt(c['total_volume']):>10} | MCap: {fmt(c['market_cap']):>10}"
            )
    if global_data and "data" in global_data:
        g = global_data["data"]
        lines.append(f"\nBTC Dom: {g['market_cap_percentage'].get('btc',0):.2f}% | "
                     f"ETH Dom: {g['market_cap_percentage'].get('eth',0):.2f}% | "
                     f"Total MC: ${g['total_market_cap'].get('usd',0):,.0f} | "
                     f"24h MC Change: {g.get('market_cap_change_percentage_24h_usd',0):+.2f}%")
    return "\n".join(lines)

async def data_dominance() -> str:
    global_data, coins = await asyncio.gather(
        fetch_cg("/global"),
        fetch_cg("/coins/markets", {
            "vs_currency": "usd",
            "ids": "bitcoin,ethereum,tether,binancecoin,solana,ripple,usd-coin,staked-ether,tron,avalanche-2",
            "order": "market_cap_desc",
            "price_change_percentage": "24h,7d",
        }),
    )
    lines = [f"DOMINANCE & ROTATION DATA | {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"]
    if global_data and "data" in global_data:
        g = global_data["data"]
        dom = g.get("market_cap_percentage", {})
        lines.append("Market Cap Dominance:")
        for sym, pct in sorted(dom.items(), key=lambda x: -x[1])[:8]:
            lines.append(f"  {sym.upper():8} {pct:.2f}%")
        lines.append(f"\nTotal MC: ${g['total_market_cap'].get('usd',0):,.0f}")
        lines.append(f"Total Vol 24h: ${g['total_volume'].get('usd',0):,.0f}")
        lines.append(f"Active Cryptos: {g.get('active_cryptocurrencies','N/A')}")
        lines.append(f"Markets: {g.get('markets','N/A')}")
    if coins:
        lines.append("\nTop 10 by MCap — Price Performance:")
        for c in coins[:10]:
            ch24 = c.get("price_change_percentage_24h") or 0
            ch7d = c.get("price_change_percentage_7d_in_currency") or 0
            lines.append(f"  {c['symbol'].upper():8} ${c['current_price']:>12,.4f} | 24h:{ch24:+6.2f}% | 7d:{ch7d:+6.2f}%")
    return "\n".join(lines)

async def data_trending() -> str:
    trending, top_gainers = await asyncio.gather(
        fetch_cg("/search/trending"),
        fetch_cg("/coins/markets", {
            "vs_currency": "usd",
            "order": "percent_change_24h_desc",
            "per_page": "20",
            "page": "1",
            "price_change_percentage": "1h,24h",
        }),
    )
    lines = [f"TRENDING & NARRATIVE DATA | {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"]
    if trending:
        lines.append("Trending on CoinGecko (search volume):")
        for i, item in enumerate(trending.get("coins", [])[:7], 1):
            c = item["item"]
            lines.append(f"  {i}. {c['name']} ({c['symbol'].upper()}) | Rank #{c.get('market_cap_rank','?')} | Score: {c.get('score',0)}")
        if trending.get("nfts"):
            lines.append("\nTrending NFTs:")
            for n in trending["nfts"][:3]:
                lines.append(f"  {n['name']} | Floor: {n.get('floor_price_in_native_currency','?')} ETH")
    if top_gainers:
        lines.append("\nTop 24h Gainers (CoinGecko top 20 by MCap):")
        gainers = sorted(top_gainers, key=lambda x: x.get("price_change_percentage_24h") or 0, reverse=True)[:5]
        for c in gainers:
            ch24 = c.get("price_change_percentage_24h") or 0
            lines.append(f"  {c['symbol'].upper():8} +{ch24:.2f}% | ${c['current_price']:,.4f} | MCap: {fmt(c['market_cap']):>10}")
        lines.append("\nTop 24h Losers:")
        losers = sorted(top_gainers, key=lambda x: x.get("price_change_percentage_24h") or 0)[:5]
        for c in losers:
            ch24 = c.get("price_change_percentage_24h") or 0
            lines.append(f"  {c['symbol'].upper():8} {ch24:.2f}% | ${c['current_price']:,.4f} | MCap: {fmt(c['market_cap']):>10}")
    return "\n".join(lines)

async def data_defi() -> str:
    tvl, protocols, chains = await asyncio.gather(
        fetch(f"{DEFILLAMA_BASE}/tvl"),
        fetch(f"{DEFILLAMA_BASE}/protocols"),
        fetch(f"{DEFILLAMA_BASE}/v2/chains"),
    )
    lines = [f"DEFI TVL DATA (DeFiLlama) | {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"]
    if tvl:
        lines.append(f"Total DeFi TVL: {fmt(float(tvl))}") 
    if protocols:
        lines.append("\nTop 10 Protocols by TVL:")
        sorted_p = sorted(protocols, key=lambda x: x.get("tvl") or 0, reverse=True)[:10]
        for p in sorted_p:
            ch1d = p.get("change_1d") or 0
            ch7d = p.get("change_7d") or 0
            lines.append(
                f"  {p['name']:20} TVL: {fmt(p.get('tvl',0)):>10} | "
                f"1d:{ch1d:+6.2f}% | 7d:{ch7d:+6.2f}% | Chain: {p.get('chain','multi')}"
            )
    if chains:
        lines.append("\nTop 10 Chains by TVL:")
        sorted_c = sorted(chains, key=lambda x: x.get("tvlPrevDay") or x.get("tvl") or 0, reverse=True)[:10]
        for c in sorted_c:
            lines.append(f"  {c.get('name','?'):15} TVL: {fmt(c.get('tvl', c.get('tvlPrevDay',0))):>10}")
    return "\n".join(lines)

async def data_fear_greed() -> str:
    fg, global_data, stables, btc_now = await asyncio.gather(
        fetch(FEAR_GREED_URL),
        fetch_cg("/global"),
        fetch_cg("/coins/markets", {
            "vs_currency": "usd",
            "ids": "tether,usd-coin,dai,first-digital-usd",
            "order": "market_cap_desc",
        }),
        fetch_cg("/coins/markets", {
            "vs_currency": "usd",
            "ids": "bitcoin,ethereum",
            "price_change_percentage": "24h",
        }),
    )
    lines = [f"SENTIMENT DATA | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC\n"]

    # Current BTC/ETH price — anchors the model to reality
    if btc_now:
        lines.append("CURRENT PRICES (live):")
        for c in btc_now:
            ch24 = c.get("price_change_percentage_24h") or 0
            lines.append(f"  {c['symbol'].upper()}: ${c['current_price']:,.2f} | 24h: {ch24:+.2f}%")
        lines.append("")

    if fg and "data" in fg:
        lines.append("Fear & Greed Index (Alternative.me):")
        for entry in fg["data"][:3]:
            ts = datetime.fromtimestamp(int(entry["timestamp"]), tz=timezone.utc).strftime("%b %d")
            lines.append(f"  {ts}: {entry['value']:>3}/100 — {entry['value_classification']}")

    if global_data and "data" in global_data:
        g = global_data["data"]
        total_mc = g.get("total_market_cap", {}).get("usd", 0)
        mc_change = g.get("market_cap_change_percentage_24h_usd", 0)
        btc_dom = g["market_cap_percentage"].get("btc", 0)
        eth_dom = g["market_cap_percentage"].get("eth", 0)
        lines.append(f"\nTotal Market Cap: {fmt(total_mc)} | 24h Change: {mc_change:+.2f}%")
        lines.append(f"BTC Dominance: {btc_dom:.2f}% | ETH Dom: {eth_dom:.2f}%")

    if stables:
        lines.append("\nStablecoin Supply (dry powder proxy):")
        total_stable = 0
        for s in stables:
            mc = s.get("market_cap", 0)
            vol = s.get("total_volume", 0)
            vol_ratio = (vol / mc * 100) if mc else 0
            total_stable += mc
            lines.append(f"  {s['symbol'].upper():6} MCap: {fmt(mc):>10} | Vol 24h: {fmt(vol):>10} | Vol/MCap: {vol_ratio:.1f}%")
        lines.append(f"  TOTAL STABLECOIN SUPPLY: {fmt(total_stable)}")
    return "\n".join(lines)

async def data_etf() -> str:
    # CoinGecko Pro ETF-related: institutional BTC/ETH data + GBTC proxy
    btc_detail, eth_detail = await asyncio.gather(
        fetch_cg("/coins/bitcoin", {"localization":"false","tickers":"false","market_data":"true","community_data":"false","developer_data":"false"}),
        fetch_cg("/coins/ethereum", {"localization":"false","tickers":"false","market_data":"true","community_data":"false","developer_data":"false"}),
    )
    lines = [f"ETF & INSTITUTIONAL PROXY DATA | {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"]
    lines.append("NOTE: Direct ETF flow data (BlackRock, Fidelity) requires Bloomberg/SoSoValue premium.")
    lines.append("Below is institutional proxy data from CoinGecko Pro:\n")
    for name, data in [("BTC", btc_detail), ("ETH", eth_detail)]:
        if data:
            md = data.get("market_data", {})
            lines.append(f"{name} Institutional Proxy:")
            lines.append(f"  Price:     ${md.get('current_price',{}).get('usd',0):,.2f}")
            lines.append(f"  MCap:      {fmt(md.get('market_cap',{}).get('usd',0))}")
            lines.append(f"  Vol 24h:   {fmt(md.get('total_volume',{}).get('usd',0))}")
            lines.append(f"  ATH:       ${md.get('ath',{}).get('usd',0):,.0f} ({md.get('ath_change_percentage',{}).get('usd',0):.1f}% from ATH)")
            lines.append(f"  ATH Date:  {md.get('ath_date',{}).get('usd','?')[:10]}")
            lines.append(f"  Circulating Supply: {md.get('circulating_supply',0):,.0f}")
            lines.append(f"  Max Supply: {md.get('max_supply') or 'unlimited'}")
            vol = md.get('total_volume',{}).get('usd',0)
            mc  = md.get('market_cap',{}).get('usd',1)
            lines.append(f"  Vol/MCap Ratio: {vol/mc:.4f} (>0.1 = high institutional activity)")
            lines.append("")
    lines.append("For live ETF flow: track sosovalue.org/assets/eth-etf or bloomberg terminal.")
    return "\n".join(lines)

async def data_macro() -> str:
    # Pull upcoming events from CoinGecko events + static high-impact calendar
    events = await fetch_cg("/events", {"upcoming_events_only": "true", "per_page": "10"})
    lines = [f"MACRO & EVENT CALENDAR | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC\n"]
    lines.append("HIGH-IMPACT RECURRING EVENTS (check ForexFactory/Investing.com for exact dates):")
    lines.append("  🔴 FOMC Meeting + Press Conference — Fed rate decision, critical for crypto")
    lines.append("  🔴 US CPI Release (monthly) — inflation data, risk-on/off trigger")
    lines.append("  🔴 US NFP / Jobs Report (1st Friday of month) — macro risk sentiment")
    lines.append("  🟡 BTC Options Expiry (every Friday, large monthly on last Friday) — Deribit")
    lines.append("  🟡 Fed Speaker Appearances — watch for forward guidance changes")
    lines.append("  🟡 US PPI Release — producer inflation, leads CPI")
    lines.append("  🟡 JOLTS / ADP Employment — leading NFP indicator")
    lines.append("  🟢 Token Unlocks — check tokenunlocks.app for asset-specific schedules")
    lines.append("  🟢 Protocol Governance Votes — check snapshot.org")
    lines.append("")
    if events and "data" in events:
        lines.append("UPCOMING CRYPTO EVENTS (CoinGecko):")
        for e in events["data"][:8]:
            date = e.get("start_date","?")[:10]
            lines.append(f"  {date} | {e.get('title','?')} | {e.get('type','?')} | {e.get('coin',{}).get('name','General')}")
    lines.append("\nSOURCES FOR LIVE MACRO CALENDAR:")
    lines.append("  ForexFactory.com — economic calendar with impact ratings")
    lines.append("  Investing.com/economic-calendar — full global macro events")
    lines.append("  CMEGroup FedWatch — FOMC rate probability tracker")
    return "\n".join(lines)

async def data_watchlist(coin_ids: list) -> str:
    if not coin_ids:
        return "Watchlist is empty. Use /watchlist to add coins."
    coins = await fetch_cg("/coins/markets", {
        "vs_currency": "usd",
        "ids": ",".join(coin_ids),
        "order": "market_cap_desc",
        "price_change_percentage": "1h,24h,7d,30d",
        "sparkline": "false",
    })
    lines = [f"WATCHLIST DATA | {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"]
    if coins:
        for c in coins:
            ch1h  = c.get("price_change_percentage_1h_in_currency") or 0
            ch24h = c.get("price_change_percentage_24h") or 0
            ch7d  = c.get("price_change_percentage_7d_in_currency") or 0
            ch30d = c.get("price_change_percentage_30d_in_currency") or 0
            ath_pct = c.get("ath_change_percentage") or 0
            lines.append(f"━━ {c['name']} ({c['symbol'].upper()}) ━━")
            lines.append(f"  Price:  ${c['current_price']:,.6f}")
            lines.append(f"  1h:     {ch1h:+.2f}%")
            lines.append(f"  24h:    {ch24h:+.2f}%")
            lines.append(f"  7d:     {ch7d:+.2f}%")
            lines.append(f"  30d:    {ch30d:+.2f}%")
            lines.append(f"  Vol:    ${c['total_volume']:,.0f}")
            lines.append(f"  MCap:   ${c['market_cap']:,.0f} | Rank #{c.get('market_cap_rank','?')}")
            lines.append(f"  vs ATH: {ath_pct:.1f}%")
            lines.append("")
    return "\n".join(lines)

async def data_btc_full() -> str:
    btc = await fetch_cg("/coins/bitcoin", {
        "localization":"false","tickers":"false",
        "market_data":"true","community_data":"true","developer_data":"false",
    })
    if not btc:
        return "BTC data unavailable."
    md = btc.get("market_data", {})
    cd = btc.get("community_data", {})
    lines = [f"BTC FULL SNAPSHOT | {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"]
    lines.append(f"Price:       ${md.get('current_price',{}).get('usd',0):,.2f}")
    lines.append(f"1h:          {(md.get('price_change_percentage_1h_in_currency',{}) or {}).get('usd',0):+.2f}%")
    lines.append(f"24h:         {md.get('price_change_percentage_24h',0):+.2f}%")
    lines.append(f"7d:          {(md.get('price_change_percentage_7d_in_currency',{}) or {}).get('usd',0):+.2f}%")
    lines.append(f"30d:         {(md.get('price_change_percentage_30d_in_currency',{}) or {}).get('usd',0):+.2f}%")
    lines.append(f"1y:          {(md.get('price_change_percentage_1y_in_currency',{}) or {}).get('usd',0):+.2f}%")
    lines.append(f"24h Range:   ${md.get('low_24h',{}).get('usd',0):,.0f} – ${md.get('high_24h',{}).get('usd',0):,.0f}")
    lines.append(f"Vol 24h:     ${md.get('total_volume',{}).get('usd',0):,.0f}")
    lines.append(f"MCap:        ${md.get('market_cap',{}).get('usd',0):,.0f}")
    lines.append(f"ATH:         ${md.get('ath',{}).get('usd',0):,.0f} on {(md.get('ath_date',{}) or {}).get('usd','?')[:10]}")
    lines.append(f"vs ATH:      {(md.get('ath_change_percentage',{}) or {}).get('usd',0):.2f}%")
    lines.append(f"ATL:         ${md.get('atl',{}).get('usd',0):,.2f}")
    lines.append(f"Circulating: {md.get('circulating_supply',0):,.0f} BTC")
    lines.append(f"Max Supply:  21,000,000 BTC")
    lines.append(f"% Mined:     {md.get('circulating_supply',0)/21000000*100:.2f}%")
    if cd:
        lines.append(f"\nCommunity:")
        lines.append(f"  Twitter followers: {cd.get('twitter_followers',0):,}")
        lines.append(f"  Reddit subscribers: {cd.get('reddit_subscribers',0):,}")
    return "\n".join(lines)

# ── Groq Analysis ─────────────────────────────────────────────────────────────
async def call_claude(prompt: str, custom_instructions: str = "", max_tokens: int = 1500) -> str:
    """Named call_claude for compatibility — uses Groq Llama 3.3 70B under the hood."""
    client = Groq(api_key=GROQ_KEY)
    system = CIPHER_SYSTEM
    if custom_instructions.strip():
        system += f"\n\nCUSTOM ANALYST INSTRUCTIONS:\n{custom_instructions}"
    loop = asyncio.get_event_loop()
    def _call():
        return client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            max_tokens=max_tokens,
            messages=[
                {"role": "system", "content": system},
                {"role": "user",   "content": prompt},
            ],
            temperature=0.3,  # Lower = more analytical, less creative
        )
    response = await loop.run_in_executor(None, _call)
    return response.choices[0].message.content.strip() or "⚠️ CIPHER returned no output."

# ── Send helper ────────────────────────────────────────────────────────────────
async def send_long(update: Update, text: str):
    for i in range(0, len(text), 4000):
        await update.message.reply_text(text[i:i+4000])

# ── Command Handlers ───────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    get_user(user.id)  # Initialize user record
    plan = "⭐ PRO" if is_pro(user.id) else "🆓 Free"
    await update.message.reply_text(
        f"⚡ *CIPHER Intelligence — Online*\n"
        f"Welcome {user.first_name} | Plan: {plan}\n\n"
        "*Commands:*\n"
        "/cipher — Full 30-min intelligence cycle\n"
        "/btc — Deep BTC snapshot\n"
        "/dominance — BTC dom + altcoin rotation\n"
        "/trending — Trending coins + narratives\n"
        "/defi — DeFi TVL breakdown\n"
        "/fear — Sentiment + Fear & Greed\n"
        "/etf — ETF & institutional proxy data\n"
        "/macro — Event calendar\n"
        "/watchlist — Your tracked coins\n"
        "/ask [question] — Ask anything with live data\n"
        "/setup — Custom instructions\n"
        "/help — Full command list",
        parse_mode=ParseMode.MARKDOWN,
    )

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📡 *CIPHER — All Commands*\n\n"
        "`/cipher` — Full cycle: market + on-chain + derivatives + macro + trade setup\n"
        "`/btc` — Deep BTC analysis with all timeframes\n"
        "`/dominance` — BTC dominance + altcoin rotation signals\n"
        "`/trending` — Top trending coins + gainers/losers narrative\n"
        "`/defi` — DeFi TVL by protocol + chain (DeFiLlama live)\n"
        "`/fear` — Fear & Greed + stablecoin supply + sentiment\n"
        "`/etf` — Institutional + ETF proxy data\n"
        "`/macro` — High-impact event calendar\n"
        "`/watchlist` — View/manage your tracked coins\n"
        "`/ask [question]` — Any question with live market context\n"
        "`/setup` — Set custom instructions for CIPHER\n\n"
        "Free-text messages also trigger CIPHER analysis.",
        parse_mode=ParseMode.MARKDOWN,
    )

async def cmd_cipher(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    await update.message.reply_text("🔄 Fetching live data from CoinGecko + DeFiLlama...")
    await context.bot.send_chat_action(update.effective_chat.id, "typing")

    market, defi, fear, trending = await asyncio.gather(
        data_market_snapshot(),
        data_defi(),
        data_fear_greed(),
        data_trending(),
    )
    prompt = (
        f"{market}\n\n{fear}\n\n{trending}\n\n"
        f"DeFi Summary:\n{defi[:1000]}...\n\n"
        "Run a full CIPHER cycle report using this data. "
        "For each section: state the specific number, then state what it means — one implies the other, do not separate them. "
        "Do not describe the data. Do not use vague language. "
        "Stablecoin supply direction tells you whether capital is entering or leaving. State it explicitly. "
        "Trending data tells you what retail is chasing — separate that from where real volume is. "
        "If a trade setup exists, include it with a hard invalidation level. If not, say flat and why. "
        "End with one line: the single most important thing to watch in the next 4 hours."
    )
    result = await call_claude(prompt, user_data.get("custom_instructions",""), max_tokens=2000)
    await send_long(update, result)

async def cmd_btc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    btc_data = await data_btc_full()
    prompt = (
        f"{btc_data}\n\n"
        "BTC snapshot analysis. "
        "State: where price sits relative to ATH and what that distance historically means for drawdown risk. "
        "Vol/MCap ratio — is volume elevated or suppressed relative to market cap? What does that imply? "
        "30d vs 7d vs 24h momentum — is the trend accelerating or decelerating? "
        "Community data is a contrarian signal — interpret it as such. "
        "Give bias with one specific reason. No vague language."
    )
    result = await call_claude(prompt, user_data.get("custom_instructions",""))
    await send_long(update, result)

async def cmd_dominance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    dom_data = await data_dominance()
    prompt = (
        f"{dom_data}\n\n"
        "Dominance analysis. "
        "State BTC dom exact level. At this level historically: does capital flow to alts or stay in BTC? "
        "Look at ETH dom separately — ETH dom rising with BTC dom falling = early alt rotation. ETH dom flat = BTC-only move. "
        "Which specific assets in the top 10 are outperforming BTC on 7d? List them with exact numbers. "
        "State rotation thesis in one sentence with a specific trigger level to watch."
    )
    result = await call_claude(prompt, user_data.get("custom_instructions",""))
    await send_long(update, result)

async def cmd_trending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    trend_data = await data_trending()
    prompt = (
        f"{trend_data}\n\n"
        "Trending analysis. "
        "For each trending coin: state its rank, 24h volume, and whether volume/mcap ratio is abnormal. "
        "Separate two categories: (1) coins where volume preceded price — organic. (2) coins where search score spiked with price — retail chase. "
        "For top gainers: is the move backed by volume expansion or thin order books? "
        "Name the dominant narrative in one sentence. State whether it has capital behind it or is search-driven noise. "
        "Flag any coin with >50% 24h gain and <$50M mcap — these are high manipulation probability."
    )
    result = await call_claude(prompt, user_data.get("custom_instructions",""))
    await send_long(update, result)

async def cmd_defi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    await update.message.reply_text("🔄 Fetching DeFiLlama live data...")
    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    defi_data = await data_defi()
    prompt = (
        f"{defi_data}\n\n"
        "DeFi TVL analysis. "
        "State total TVL and its direction. Flat or declining TVL with rising token prices = price/TVL divergence, bearish signal. "
        "For each top protocol: state TVL, 1d and 7d change. Any protocol losing >5% TVL in 7d is worth flagging — capital is leaving. "
        "Chain dominance: which chain is gaining share? ETH losing TVL to Solana/Base matters for ETH price thesis. "
        "Name one protocol with unusual TVL movement and state what is driving it specifically."
    )
    result = await call_claude(prompt, user_data.get("custom_instructions",""))
    await send_long(update, result)

async def cmd_fear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    fear_data = await data_fear_greed()
    prompt = (
        f"{fear_data}\n\n"
        "Sentiment analysis. "
        "IMPORTANT: Current BTC and ETH prices are provided in the data above under CURRENT PRICES. "
        "Use ONLY those prices for any trade setup. Never use historical or estimated prices. "
        "Fear & Greed score — state the number and 3-day trend. Score below 20 = extreme fear zone historically associated with local bottoms. State current zone. "
        "Stablecoin total supply direction — use the TOTAL STABLECOIN SUPPLY figure. Growing = dry powder building. Shrinking = capital deployed. "
        "Vol/MCap ratio above 15% on USDT = abnormal turnover, large move likely imminent. State the exact ratio and whether it is elevated. "
        "Trade setup: if one is warranted, use the live BTC price from the data to set entry, stop, and targets — not estimated prices. "
        "State positioning implication in one sentence with a specific number."
    )
    result = await call_claude(prompt, user_data.get("custom_instructions",""))
    await send_long(update, result)

async def cmd_etf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    etf_data = await data_etf()
    prompt = (
        f"{etf_data}\n\n"
        "Institutional proxy analysis. "
        "Vol/MCap ratio above 0.05 indicates high turnover — institutional desks active. Below 0.02 = accumulation phase or disinterest. State which. "
        "ATH distance: ETF buyers who entered at launch have specific underwater levels. State the approximate average ETF entry price based on ATH date and current price. "
        "Circulating supply vs max supply: BTC at 94%+ mined means supply shock from miners is structural, not cyclical. "
        "Give one-line institutional thesis: are conditions favorable for ETF inflows right now based on price action and volatility?"
    )
    result = await call_claude(prompt, user_data.get("custom_instructions",""))
    await send_long(update, result)

async def cmd_macro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    macro_data = await data_macro()
    market_data = await data_market_snapshot(["bitcoin","ethereum"])
    prompt = (
        f"{macro_data}\n\n"
        f"Current market context:\n{market_data[:500]}\n\n"
        "Macro briefing. "
        "List upcoming high-impact events with dates. For each: state the expected crypto impact direction and the specific risk (e.g. hot CPI = rate hike fears = risk-off = BTC down). "
        "Current macro regime: state whether rates, dollar strength, and equity correlation are net positive or negative for crypto right now. Use specific numbers where available. "
        "Pre-event positioning: what does a trader do in the 48h before a FOMC or CPI — reduce exposure, hedge with options, or hold? Give a specific recommendation. "
        "One-line regime summary: risk-on / risk-off / transitional and the single data point that defines it."
    )
    result = await call_claude(prompt, user_data.get("custom_instructions",""))
    await send_long(update, result)

async def cmd_watchlist_view(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    watchlist = user_data.get("watchlist", ["bitcoin","ethereum"])

    if context.args:
        # /watchlist add <coin> or /watchlist remove <coin>
        action = context.args[0].lower()
        if action == "add" and len(context.args) > 1:
            coin = context.args[1].lower()
            # Validate coin exists
            check = await fetch_cg(f"/coins/{coin}")
            if not check or "error" in str(check):
                await update.message.reply_text(f"❌ Coin '{coin}' not found. Use CoinGecko ID (e.g. 'chainlink' not 'LINK')")
                return
            if coin not in watchlist:
                watchlist.append(coin)
                user_data["watchlist"] = watchlist
                save_user(update.effective_user.id, user_data)
                await update.message.reply_text(f"✅ Added {coin} to watchlist.\nCurrent: {', '.join(watchlist)}")
            else:
                await update.message.reply_text(f"Already in watchlist: {coin}")
            return
        elif action == "remove" and len(context.args) > 1:
            coin = context.args[1].lower()
            if coin in watchlist:
                watchlist.remove(coin)
                user_data["watchlist"] = watchlist
                save_user(update.effective_user.id, user_data)
                await update.message.reply_text(f"✅ Removed {coin}.\nCurrent: {', '.join(watchlist)}")
            else:
                await update.message.reply_text(f"'{coin}' not in watchlist.")
            return

    # Default: show watchlist data
    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    wl_data = await data_watchlist(watchlist)
    prompt = (
        f"{wl_data}\n\n"
        "Analyze each coin in this watchlist. "
        "For each: give a 2-line assessment of current momentum, key level to watch, and bias. "
        "End with: which coin has the strongest setup right now and why."
    )
    result = await call_claude(prompt, user_data.get("custom_instructions",""))
    await update.message.reply_text(
        f"📋 Watchlist: {', '.join(watchlist)}\n"
        f"Edit: /watchlist add <coin-id> | /watchlist remove <coin-id>\n"
        f"(Use CoinGecko IDs: chainlink, solana, avalanche-2, etc.)\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    )
    await send_long(update, result)

async def cmd_ask(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    question = " ".join(context.args) if context.args else ""
    if not question:
        await update.message.reply_text("Usage: /ask [your question]\nExample: /ask Is SOL in a good buy zone right now?")
        return
    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    coin_id = extract_coin_from_message(question)
    if coin_id:
        coin_data_raw, market_data = await asyncio.gather(
            fetch_cg("/coins/markets", {
                "vs_currency": "usd",
                "ids": f"{coin_id},bitcoin",
                "price_change_percentage": "1h,24h,7d,30d",
                "sparkline": "false",
            }),
            data_market_snapshot(),
        )
        coin_section = ""
        if coin_data_raw:
            for c in coin_data_raw:
                if c["id"] == coin_id:
                    ch24h = c.get("price_change_percentage_24h") or 0
                    ch7d  = c.get("price_change_percentage_7d_in_currency") or 0
                    ath_pct = c.get("ath_change_percentage") or 0
                    vol_mcap = (c["total_volume"] / c["market_cap"] * 100) if c.get("market_cap") else 0
                    coin_section = (
                        f"LIVE DATA FOR {c['name'].upper()} ({c['symbol'].upper()}):\n"
                        f"  Price: ${c['current_price']:,.4f} | 24h: {ch24h:+.2f}% | 7d: {ch7d:+.2f}%\n"
                        f"  MCap: {fmt(c['market_cap'])} | Vol: {fmt(c['total_volume'])} | Vol/MCap: {vol_mcap:.1f}%\n"
                        f"  vs ATH: {ath_pct:.1f}% | Rank: #{c.get('market_cap_rank','?')}\n"
                    )
        prompt = (
            f"{coin_section}\n"
            f"BROAD MARKET CONTEXT:\n{market_data}\n\n"
            f"ANALYST QUESTION: {question}\n\n"
            "Use the correct CIPHER response format for this question type. Use ONLY live prices from data above."
        )
    else:
        market_data = await data_market_snapshot()
        prompt = (
            f"LIVE MARKET CONTEXT:\n{market_data}\n\n"
            f"ANALYST QUESTION: {question}\n\n"
            "Classify and respond with the correct CIPHER format. Use only live data provided."
        )
    result = await call_claude(prompt, user_data.get("custom_instructions",""))
    await send_long(update, result)

# ── /setup conversation ────────────────────────────────────────────────────────
async def cmd_setup_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    current = user_data.get("custom_instructions","").strip()
    msg = (
        f"⚙️ *Custom Instructions*\n\n"
        f"Current: `{current or 'None set'}`\n\n"
        "Send new instructions to replace. Examples:\n"
        "• Focus on SOL, LINK, ARB alongside BTC/ETH\n"
        "• Swing trading, 3–5 day horizon, not scalping\n"
        "• Max 2% risk per trade, capital: $10,000\n"
        "• Skip social sentiment, focus on on-chain only\n"
        "• My current positions: long BTC at $85k, long ETH at $2k\n\n"
        "Or /cancel to keep existing."
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
    return WAITING_SETUP

async def cmd_setup_receive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    user_data["custom_instructions"] = update.message.text.strip()
    save_user(update.effective_user.id, user_data)
    await update.message.reply_text(
        f"✅ *Saved.* Active in all future reports.\n\n`{user_data['custom_instructions']}`",
        parse_mode=ParseMode.MARKDOWN,
    )
    return ConversationHandler.END

async def cmd_setup_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("↩️ Cancelled. Instructions unchanged.")
    return ConversationHandler.END

# ── Coin ID resolver ──────────────────────────────────────────────────────────
COIN_ALIASES = {
    "btc": "bitcoin", "eth": "ethereum", "sol": "solana", "bnb": "binancecoin",
    "xrp": "ripple", "ada": "cardano", "avax": "avalanche-2", "link": "chainlink",
    "dot": "polkadot", "matic": "matic-network", "arb": "arbitrum", "op": "optimism",
    "sei": "sei-network", "inj": "injective-protocol", "sui": "sui", "apt": "aptos",
    "atom": "cosmos", "near": "near", "ftm": "fantom", "trx": "tron",
    "doge": "dogecoin", "shib": "shiba-inu", "pepe": "pepe", "wif": "dogwifcoin",
    "hype": "hyperliquid", "jup": "jupiter-exchange-solana", "ena": "ethena",
    "ton": "the-open-network", "trump": "official-trump", "uni": "uniswap",
    "aave": "aave", "crv": "curve-dao-token", "mkr": "maker", "ldo": "lido-dao",
    "sandbox": "the-sandbox", "mana": "decentraland", "gala": "gala",
    "fet": "fetch-ai", "rndr": "render-token", "ocean": "ocean-protocol",
}

def extract_coin_from_message(text: str) -> str | None:
    """Try to detect a coin ticker or name in the user message."""
    text_lower = text.lower()
    # Check aliases first
    for alias, cg_id in COIN_ALIASES.items():
        if f" {alias} " in f" {text_lower} " or text_lower.startswith(alias + " ") or text_lower.endswith(" " + alias):
            return cg_id
    return None

# ── Free-text handler ──────────────────────────────────────────────────────────
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    user_text = update.message.text.strip()
    await context.bot.send_chat_action(update.effective_chat.id, "typing")

    # Detect if user is asking about a specific coin
    coin_id = extract_coin_from_message(user_text)

    if coin_id:
        # Fetch specific coin data + broad market context
        coin_data_raw, market_data = await asyncio.gather(
            fetch_cg("/coins/markets", {
                "vs_currency": "usd",
                "ids": f"{coin_id},bitcoin",
                "price_change_percentage": "1h,24h,7d,30d",
                "sparkline": "false",
            }),
            data_market_snapshot(),
        )
        coin_section = ""
        if coin_data_raw:
            for c in coin_data_raw:
                if c["id"] == coin_id:
                    ch1h  = c.get("price_change_percentage_1h_in_currency") or 0
                    ch24h = c.get("price_change_percentage_24h") or 0
                    ch7d  = c.get("price_change_percentage_7d_in_currency") or 0
                    ch30d = c.get("price_change_percentage_30d_in_currency") or 0
                    ath_pct = c.get("ath_change_percentage") or 0
                    vol_mcap = (c["total_volume"] / c["market_cap"] * 100) if c.get("market_cap") else 0
                    coin_section = (
                        f"LIVE DATA FOR {c['name'].upper()} ({c['symbol'].upper()}):\n"
                        f"  Price:        ${c['current_price']:,.4f}\n"
                        f"  1h:           {ch1h:+.2f}%\n"
                        f"  24h:          {ch24h:+.2f}%\n"
                        f"  7d:           {ch7d:+.2f}%\n"
                        f"  30d:          {ch30d:+.2f}%\n"
                        f"  vs ATH:       {ath_pct:.1f}%\n"
                        f"  MCap:         {fmt(c['market_cap'])}\n"
                        f"  Vol 24h:      {fmt(c['total_volume'])}\n"
                        f"  Vol/MCap:     {vol_mcap:.1f}%\n"
                        f"  MCap Rank:    #{c.get('market_cap_rank','?')}\n"
                    )
        prompt = (
            f"{coin_section}\n"
            f"BROAD MARKET CONTEXT:\n{market_data}\n\n"
            f"USER QUESTION: {user_text}\n\n"
            "Classify this as TYPE B (coin question) or TYPE C (position question) and respond with the correct format. "
            "Use ONLY the live prices from the data above. Do not use any historical prices from training."
        )
    else:
        # General question — use full market snapshot
        market_data = await data_market_snapshot()
        prompt = (
            f"LIVE MARKET CONTEXT:\n{market_data}\n\n"
            f"USER QUESTION: {user_text}\n\n"
            "Classify this question by type (A/B/C/D/E) and respond with the correct CIPHER format. "
            "Use only live data provided. Do not fabricate prices."
        )

    result = await call_claude(prompt, user_data.get("custom_instructions",""))
    await send_long(update, result)

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Exception:", exc_info=context.error)
    if isinstance(update, Update) and update.message:
        await update.message.reply_text("⚠️ Error. Try again or check /help.")

# ── Keep-Alive Server (prevents Render free tier sleep) ───────────────────────
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading, time

RENDER_URL = os.getenv("RENDER_EXTERNAL_URL", "")  # Auto-set by Render

class PingHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"CIPHER alive")
    def log_message(self, *args):
        pass  # Suppress HTTP logs

def run_ping_server():
    port = int(os.getenv("PORT", "8080"))
    server = HTTPServer(("0.0.0.0", port), PingHandler)
    logger.info(f"Keep-alive server on port {port}")
    server.serve_forever()

def self_ping_loop():
    """Ping own URL every 10 minutes to prevent Render sleep."""
    if not RENDER_URL:
        return
    time.sleep(60)  # Wait for server to start
    while True:
        try:
            import urllib.request
            urllib.request.urlopen(f"{RENDER_URL}/", timeout=10)
            logger.info("Self-ping OK")
        except Exception as e:
            logger.warning(f"Self-ping failed: {e}")
        time.sleep(600)  # Every 10 minutes

# ── Main ───────────────────────────────────────────────────────────────────────
async def main():
    # Start keep-alive HTTP server in background thread (for Render)
    threading.Thread(target=run_ping_server, daemon=True).start()
    threading.Thread(target=self_ping_loop,  daemon=True).start()

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    setup_conv = ConversationHandler(
        entry_points=[CommandHandler("setup", cmd_setup_start)],
        states={WAITING_SETUP: [MessageHandler(filters.TEXT & ~filters.COMMAND, cmd_setup_receive)]},
        fallbacks=[CommandHandler("cancel", cmd_setup_cancel)],
    )

    app.add_handler(CommandHandler("start",     cmd_start))
    app.add_handler(CommandHandler("help",      cmd_help))
    app.add_handler(CommandHandler("cipher",    cmd_cipher))
    app.add_handler(CommandHandler("btc",       cmd_btc))
    app.add_handler(CommandHandler("dominance", cmd_dominance))
    app.add_handler(CommandHandler("trending",  cmd_trending))
    app.add_handler(CommandHandler("defi",      cmd_defi))
    app.add_handler(CommandHandler("fear",      cmd_fear))
    app.add_handler(CommandHandler("etf",       cmd_etf))
    app.add_handler(CommandHandler("macro",     cmd_macro))
    app.add_handler(CommandHandler("watchlist", cmd_watchlist_view))
    app.add_handler(CommandHandler("ask",       cmd_ask))
    app.add_handler(setup_conv)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)

    logger.info("CIPHER Bot v3 starting...")
    async with app:
        await app.initialize()
        # Register commands menu in Telegram
        await app.bot.set_my_commands([
            BotCommand("cipher",    "Full 30-min intelligence cycle"),
            BotCommand("btc",       "Deep BTC snapshot + analysis"),
            BotCommand("dominance", "BTC dominance + altcoin rotation"),
            BotCommand("trending",  "Trending coins + narrative analysis"),
            BotCommand("defi",      "DeFi TVL by protocol + chain"),
            BotCommand("fear",      "Fear & Greed + sentiment data"),
            BotCommand("etf",       "ETF + institutional proxy data"),
            BotCommand("macro",     "Macro event calendar"),
            BotCommand("watchlist",  "Your tracked coins"),
            BotCommand("ask",       "Ask anything with live data"),
            BotCommand("setup",     "Set custom instructions"),
            BotCommand("help",      "All commands"),
        ])
        logger.info("Commands menu registered.")
        await app.start()
        await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
        await asyncio.Event().wait()  # Run forever
        await app.updater.stop()
        await app.stop()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
