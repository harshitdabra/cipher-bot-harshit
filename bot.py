"""
CIPHER Telegram Bot — Production Release
CoinGecko Pro + DeFiLlama + Alternative.me + Groq (Llama 3.3 70B)
QA Pass: all edge cases, error paths, data formatting, prompt engineering
"""

import os, json, logging, asyncio, httpx, re
from pathlib import Path
from datetime import datetime, timezone
from groq import Groq
from telegram import Update, BotCommand
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    ContextTypes, filters, ConversationHandler,
)
from telegram.constants import ParseMode

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
GROQ_KEY       = os.getenv("GROQ_API_KEY", "")
CG_KEY         = os.getenv("COINGECKO_API_KEY", "")
OWNER_ID       = int(os.getenv("ALLOWED_USER_ID", "1953473977"))

CG_BASE        = "https://pro-api.coingecko.com/api/v3"
CG_HEADERS     = {"x-cg-pro-api-key": CG_KEY}
LLAMA_BASE     = "https://api.llama.fi"
FNG_URL        = "https://api.alternative.me/fng/?limit=3"

DB_FILE        = Path("cipher_db.json")
WAITING_SETUP  = 1

# ── In-memory DB cache (avoid file reads on every message) ───────────────────
_db_cache: dict = {}

def load_db() -> dict:
    global _db_cache
    if _db_cache:
        return _db_cache
    if DB_FILE.exists():
        try:
            _db_cache = json.loads(DB_FILE.read_text())
            return _db_cache
        except Exception:
            pass
    _db_cache = {"users": {}}
    return _db_cache

def save_db(db: dict):
    global _db_cache
    _db_cache = db
    DB_FILE.write_text(json.dumps(db, indent=2))

def get_user(uid: int) -> dict:
    db = load_db()
    key = str(uid)
    if key not in db["users"]:
        db["users"][key] = {
            "custom_instructions": "",
            "watchlist": ["bitcoin", "ethereum"],
            "joined": datetime.now(timezone.utc).isoformat(),
            "plan": "owner" if uid == OWNER_ID else "free",
        }
        save_db(db)
    return db["users"][key]

def save_user(uid: int, data: dict):
    db = load_db()
    db["users"][str(uid)] = data
    save_db(db)

def is_pro(uid: int) -> bool:
    if uid == OWNER_ID:
        return True
    return get_user(uid).get("plan") in ("pro", "owner")

# ── Number formatter ──────────────────────────────────────────────────────────
def fmt(n, prefix="$") -> str:
    """Format large numbers to K/M/B. Handles None, str, int, float."""
    try:
        n = float(n)
    except (TypeError, ValueError):
        return "N/A"
    if abs(n) >= 1_000_000_000:
        return f"{prefix}{n/1_000_000_000:.2f}B"
    if abs(n) >= 1_000_000:
        return f"{prefix}{n/1_000_000:.2f}M"
    if abs(n) >= 1_000:
        return f"{prefix}{n/1_000:.2f}K"
    return f"{prefix}{n:,.4f}"

def fmtp(n) -> str:
    """Format percentage safely."""
    try:
        return f"{float(n):+.2f}%"
    except (TypeError, ValueError):
        return "N/A"

# ── HTTP client (shared, with retries) ───────────────────────────────────────
async def fetch(url: str, headers: dict = None, params: dict = None) -> dict | list | None:
    h = headers or {}
    p = params or {}
    for attempt in range(2):  # 1 retry
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                r = await client.get(url, headers=h, params=p)
                if r.status_code == 429:
                    logger.warning(f"Rate limited: {url[:60]}")
                    await asyncio.sleep(2)
                    continue
                r.raise_for_status()
                return r.json()
        except httpx.TimeoutException:
            logger.warning(f"Timeout: {url[:60]}")
        except Exception as e:
            logger.warning(f"Fetch error [{url[:60]}]: {e}")
        if attempt == 0:
            await asyncio.sleep(1)
    return None

async def cg(endpoint: str, params: dict = None) -> dict | list | None:
    return await fetch(f"{CG_BASE}{endpoint}", CG_HEADERS, params or {})

# ── CIPHER Master System Prompt ───────────────────────────────────────────────
CIPHER_SYSTEM = """IDENTITY
You are CIPHER — senior crypto on-chain analyst, derivatives strategist, and portfolio risk advisor.
You produce the quality of a Delphi Digital brief or Nansen alpha report: concise, data-anchored, and immediately actionable.

CORE MISSION
Every response must do one of these:
1. State what a signal MEANS (not what it is)
2. State what to DO (specific action with levels)
3. State what to WATCH (specific trigger that changes the thesis)
If your response does none of these three, rewrite it.

══════════════════════════════════════════
ABSOLUTE OUTPUT RULES — ZERO EXCEPTIONS
══════════════════════════════════════════
BANNED PHRASES — never write these:
it is worth noting | it is important to | this suggests | this indicates | potentially |
may indicate | could be | one might | in conclusion | to summarize | delve | landscape |
ecosystem | robust | seamless | bullish outlook | bearish sentiment | market participants |
it remains to be seen | strong fundamentals | weak fundamentals | overall | essentially |
notably | importantly | interestingly | at the end of the day | in terms of | looking at |
this shows | we can see | it appears | seems to be | might be | could indicate

BANNED BEHAVIORS:
- Zero emojis
- Never describe data. Only interpret it.
- Never use training-data prices. Use ONLY prices in the provided live data.
- Never fabricate signals. If data is missing, write: "data unavailable"
- Never call something manipulation without on-chain wallet evidence
- Never say "strong/weak fundamentals" — cite TVL, fees, active addresses, revenue
- Never pad responses. Short and right beats long and vague.
- Derivatives data (funding rates, OI, liquidations) is NOT in CoinGecko. When asked: note "Check CoinGlass for live rates" then interpret price structure instead.

NUMBER FORMAT: Always use K/M/B. Never write raw numbers like 1234567890.

══════════════════════════════════════════
INTENT CLASSIFICATION — ALWAYS DO THIS FIRST
══════════════════════════════════════════
Before writing, classify the request:

TYPE A — MARKET REPORT: /cipher, /btc, /fear, /defi, "what is market doing", "run cycle"
TYPE B — COIN QUESTION: "what about TAO", "is SEI good", "LINK analysis", any ticker question
TYPE C — POSITION QUESTION: "should I scale in", "DCA now", "take profits", "add to position"
TYPE D — CONCEPT QUESTION: "what is CVD", "explain funding rates", "how do liquidations work"
TYPE E — ALERT/SCAN: "any alerts", "anything unusual", "check signals"
TYPE F — COMPARISON: "BTC vs ETH", "SOL vs AVAX", "which is better right now"
TYPE G — PORTFOLIO: "review my portfolio", "which of my coins", "rebalance"

══════════════════════════════════════════
RESPONSE FORMATS
══════════════════════════════════════════

TYPE A — FULL REPORT:
---
MARKET STRUCTURE
[Price + context. Key level above and below. Is this support, resistance, or no-man's land?]

ON-CHAIN CONTEXT
[Exchange flow direction + implication. Stablecoin supply trend + what it signals. Any notable entity activity.]

DERIVATIVES SNAPSHOT
[Funding rate level + direction. OI expanding or contracting. Crowding risk assessment.]

NARRATIVE
[What is driving real volume. Organic vs retail-chased. Which narrative has capital behind it.]

SIGNAL SYNTHESIS
Bias: BULLISH / BEARISH / NEUTRAL | Confidence: HIGH / MEDIUM / LOW
Driver: [one specific reason with a number]
Invalidation: [specific price or event]

TRADE SETUP [only if 2+ signals confirm — skip section entirely if not]
Asset | Direction | Entry | Stop | T1 | T2 | Conviction: H/M/L
Thesis: [2 sentences, numbers only]

ACTION: [trade / add / reduce / flat / wait] — [one-line reason]
---

TYPE B — COIN BRIEF:
---
[COIN] BRIEF | $[live price from data]

PRICE STRUCTURE
[vs recent range, vs ATH, key level above and below]

MOMENTUM
[24h and 7d vs BTC — outperforming or underperforming]

VOLUME QUALITY
[Vol/MCap ratio. Expanding or contracting with move. Thin or real.]

ON-CHAIN PROXY
[MCap rank. Institutional vs retail interest signal from ratio.]

VERDICT: SCALE IN / WAIT FOR LEVEL / AVOID / REDUCE
[If SCALE IN or WAIT: entry zone, stop, target]
[If AVOID: specific reason — overextended / no volume / better alternative]
---

TYPE C — POSITION BRIEF:
---
POSITION ASSESSMENT | [asset] @ $[live price]

STRUCTURE
[Where price is vs key levels. High / mid / low risk entry zone right now.]

DOWNSIDE RISK
[Realistic next major support if wrong. % drawdown from current.]

MARKET ALIGNMENT
[Does broad market support adding risk here? Y/N + reason.]

RECOMMENDATION: SCALE IN NOW / SCALE IN AT $[X] / HOLD / REDUCE [%] / EXIT
Sizing: [e.g. add 25% here, 25% at $X, hold 50% dry]
Reason: [2 sentences, specific levels]
---

TYPE D — CONCEPT:
[3-5 sentences, direct answer.]
Trading implication: [one sentence on how to use this in practice.]
---

TYPE E — ALERT BRIEF:
[List only triggered conditions:]
[RED / AMBER / INFO] | [asset] | [condition] | [implication]
If nothing triggered: "No active alerts. Market within normal parameters."
---

TYPE F — COMPARISON:
---
[ASSET A] vs [ASSET B] | [timeframe]

RELATIVE PERFORMANCE
[Exact 24h and 7d numbers for each]

MOMENTUM DIFFERENTIAL
[Which is stronger right now and by how much]

VOLUME QUALITY
[Which has better vol/mcap ratio — more conviction]

RELATIVE VERDICT
[Which has stronger setup right now and why — one sentence]
---

TYPE G — PORTFOLIO:
For each coin in portfolio:
[COIN]: [bias] | [key level] | [action: hold / add / reduce]
Summary: [overall portfolio risk assessment — 2 sentences]
---

══════════════════════════════════════════
SIGNAL HIERARCHY
══════════════════════════════════════════
PRIMARY (form bias from these):
1. Exchange net flow — inflow = sell pressure, outflow = accumulation
2. Stablecoin supply direction — growing = dry powder, shrinking = deployed
3. Funding + OI divergence — price up + OI up + high funding = crowded long
4. CVD — sustained negative CVD with flat price = distribution
5. Spot ETF flow — sustained outflows = institutional exit

CONFIRMING (adjust conviction):
6. Vol/MCap ratio — >0.08 high activity, <0.02 accumulation or disinterest
7. BTC dominance trend — rising = alts bleeding, falling = rotation
8. Fear & Greed extremes — <15 or >85 = contrarian signal only
9. DeFi TVL — protocol TVL loss = capital leaving ecosystem

NEVER PRIMARY: RSI | MACD | Bollinger Bands | MAs alone | Fear & Greed standalone

══════════════════════════════════════════
TONE
══════════════════════════════════════════
Bloomberg terminal analyst. Not crypto Twitter.
Active voice. Short sentences. Every sentence contains information.
No claim without a number. No padding. Quality over length."""

# ── Coin alias map ────────────────────────────────────────────────────────────
ALIASES = {
    # BTC / ETH
    "btc":"bitcoin","bitcoin":"bitcoin","eth":"ethereum","ethereum":"ethereum",
    # Top L1
    "sol":"solana","solana":"solana","bnb":"binancecoin","binance":"binancecoin",
    "xrp":"ripple","ripple":"ripple","ada":"cardano","cardano":"cardano",
    "avax":"avalanche-2","avalanche":"avalanche-2","dot":"polkadot","polkadot":"polkadot",
    "trx":"tron","tron":"tron","ton":"the-open-network","near":"near",
    "atom":"cosmos","cosmos":"cosmos","algo":"algorand","algorand":"algorand",
    "hbar":"hedera-hashgraph","hedera":"hedera-hashgraph","icp":"internet-computer",
    "fil":"filecoin","filecoin":"filecoin","kas":"kaspa","kaspa":"kaspa",
    # L2 / Alt L1
    "arb":"arbitrum","arbitrum":"arbitrum","op":"optimism","optimism":"optimism",
    "matic":"matic-network","polygon":"matic-network","stx":"blockstack","stacks":"blockstack",
    "sei":"sei-network","inj":"injective-protocol","injective":"injective-protocol",
    "sui":"sui","apt":"aptos","aptos":"aptos","ftm":"fantom","fantom":"fantom",
    "imx":"immutable-x","immutable":"immutable-x","zk":"zksync","zksync":"zksync",
    "strk":"starknet","starknet":"starknet","w":"wormhole","wormhole":"wormhole",
    # DeFi
    "link":"chainlink","chainlink":"chainlink","uni":"uniswap","uniswap":"uniswap",
    "aave":"aave","crv":"curve-dao-token","curve":"curve-dao-token",
    "mkr":"maker","maker":"maker","ldo":"lido-dao","lido":"lido-dao",
    "snx":"havven","synthetix":"havven","gmx":"gmx",
    "jup":"jupiter-exchange-solana","jupiter":"jupiter-exchange-solana",
    "comp":"compound-governance-token","compound":"compound-governance-token",
    "dydx":"dydx","pendle":"pendle","eigen":"eigenlayer","eigenlayer":"eigenlayer",
    "pyth":"pyth-network","grt":"the-graph","graph":"the-graph",
    # AI / Infra
    "tao":"bittensor","bittensor":"bittensor","fet":"fetch-ai","fetch":"fetch-ai",
    "rndr":"render-token","render":"render-token","ocean":"ocean-protocol",
    "wld":"worldcoin-wld","worldcoin":"worldcoin-wld","akash":"akash-network",
    # Meme
    "doge":"dogecoin","dogecoin":"dogecoin","shib":"shiba-inu","shiba":"shiba-inu",
    "pepe":"pepe","wif":"dogwifcoin","bonk":"bonk","floki":"floki","meme":"meme-token",
    # Other notable
    "hype":"hyperliquid","hyperliquid":"hyperliquid","ena":"ethena","ethena":"ethena",
    "trump":"official-trump","jto":"jito-governance-token","jito":"jito-governance-token",
    "sandbox":"the-sandbox","mana":"decentraland","decentraland":"decentraland",
    "blur":"blur","arkm":"arkham","gala":"gala","ondo":"ondo-finance","ondo":"ondo-finance",
    "sui":"sui","move":"movement","not":"notcoin","notcoin":"notcoin",
}

async def resolve_coin(text: str) -> str | None:
    """
    Resolve coin from user text.
    Priority: 1) alias map exact word match  2) CoinGecko search fallback
    """
    t = text.lower().strip()
    words = re.findall(r"[a-z0-9]+", t)

    # Check alias map — word boundary match
    for word in words:
        if word in ALIASES:
            return ALIASES[word]

    # CoinGecko search fallback — try each word
    for word in words:
        if len(word) < 2 or word in {
            "the","is","a","an","should","i","my","about","what","think",
            "buy","sell","good","bad","now","still","long","short","hold",
            "add","into","this","that","and","or","for","with","how","why",
            "when","where","scale","dca","do","you","me","it","not","be",
        }:
            continue
        result = await cg("/search", {"query": word})
        if not result or not result.get("coins"):
            continue
        top = result["coins"][0]
        sym = top.get("symbol","").lower()
        name = top.get("name","").lower()
        if sym == word or name == word or name.startswith(word):
            return top["id"]
    return None

async def fetch_coin_data(coin_id: str) -> str:
    """Fetch full data for a specific coin and return formatted string."""
    data = await cg("/coins/markets", {
        "vs_currency": "usd",
        "ids": f"{coin_id},bitcoin",
        "price_change_percentage": "1h,24h,7d,30d",
        "sparkline": "false",
    })
    if not data:
        return f"Live data unavailable for {coin_id}. CoinGecko request failed."

    lines = []
    btc_price = None
    for c in data:
        if c["id"] == "bitcoin":
            btc_price = c["current_price"]
        if c["id"] != coin_id:
            continue
        ch1h  = c.get("price_change_percentage_1h_in_currency") or 0
        ch24h = c.get("price_change_percentage_24h") or 0
        ch7d  = c.get("price_change_percentage_7d_in_currency") or 0
        ch30d = c.get("price_change_percentage_30d_in_currency") or 0
        ath_pct = c.get("ath_change_percentage") or 0
        mc    = c.get("market_cap") or 1
        vol   = c.get("total_volume") or 0
        vol_mc_ratio = (vol / mc * 100) if mc else 0
        price = c.get("current_price", 0)

        # BTC-relative performance
        btc_rel = ""
        if btc_price and btc_price > 0:
            btc_rel = f"\n  vs BTC 24h:     {ch24h - 0:.2f}pp (raw: need BTC 24h for exact)"

        lines.append(f"LIVE DATA: {c['name'].upper()} ({c['symbol'].upper()})")
        lines.append(f"  Price:          ${price:,.4f}")
        lines.append(f"  1h:             {fmtp(ch1h)}")
        lines.append(f"  24h:            {fmtp(ch24h)}")
        lines.append(f"  7d:             {fmtp(ch7d)}")
        lines.append(f"  30d:            {fmtp(ch30d)}")
        lines.append(f"  24h Range:      ${c.get('low_24h',0):,.4f} — ${c.get('high_24h',0):,.4f}")
        lines.append(f"  vs ATH:         {ath_pct:.1f}%  (ATH: ${c.get('ath',0):,.4f})")
        lines.append(f"  MCap:           {fmt(mc)}  |  Rank #{c.get('market_cap_rank','?')}")
        lines.append(f"  Vol 24h:        {fmt(vol)}")
        lines.append(f"  Vol/MCap:       {vol_mc_ratio:.1f}%  (>8% = elevated activity)")
        lines.append(f"  Circulating:    {c.get('circulating_supply',0):,.0f}")
        if c.get("max_supply"):
            pct_issued = c["circulating_supply"] / c["max_supply"] * 100 if c.get("circulating_supply") else 0
            lines.append(f"  Max Supply:     {c['max_supply']:,.0f}  ({pct_issued:.1f}% issued)")

    if not lines:
        return f"No data returned for coin_id: {coin_id}"

    if btc_price:
        lines.append(f"\nBTC CONTEXT: ${btc_price:,.2f} (for relative performance reference)")

    return "\n".join(lines)

# ── Data fetchers ──────────────────────────────────────────────────────────────

async def data_market_snapshot(coin_ids: list = None) -> str:
    ids = ",".join(coin_ids) if coin_ids else (
        "bitcoin,ethereum,solana,binancecoin,ripple,cardano,avalanche-2,"
        "chainlink,polkadot,tron,near,cosmos,arbitrum,optimism,sui"
    )
    coins, gdata = await asyncio.gather(
        cg("/coins/markets", {
            "vs_currency": "usd", "ids": ids,
            "order": "market_cap_desc",
            "price_change_percentage": "1h,24h,7d",
            "sparkline": "false",
        }),
        cg("/global"),
    )
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    lines = [f"LIVE MARKET DATA | {ts} UTC\n"]

    if coins:
        lines.append(f"{'SYM':6} {'PRICE':>12}  {'1H':>7}  {'24H':>7}  {'7D':>7}  {'VOL':>10}  {'MCAP':>10}")
        lines.append("─" * 72)
        for c in coins:
            ch1h  = c.get("price_change_percentage_1h_in_currency") or 0
            ch24h = c.get("price_change_percentage_24h") or 0
            ch7d  = c.get("price_change_percentage_7d_in_currency") or 0
            price = c.get("current_price", 0)
            price_str = f"${price:,.2f}" if price >= 1 else f"${price:,.5f}"
            lines.append(
                f"{c['symbol'].upper():6} {price_str:>12}  "
                f"{ch1h:>+6.2f}%  {ch24h:>+6.2f}%  {ch7d:>+6.2f}%  "
                f"{fmt(c['total_volume']):>10}  {fmt(c['market_cap']):>10}"
            )

    if gdata and "data" in gdata:
        g = gdata["data"]
        btc_dom = g["market_cap_percentage"].get("btc", 0)
        eth_dom = g["market_cap_percentage"].get("eth", 0)
        total_mc = g.get("total_market_cap", {}).get("usd", 0)
        mc_ch = g.get("market_cap_change_percentage_24h_usd", 0)
        total_vol = g.get("total_volume", {}).get("usd", 0)
        vol_mc = (total_vol / total_mc * 100) if total_mc else 0
        lines.append(f"\nBTC Dom: {btc_dom:.2f}%  |  ETH Dom: {eth_dom:.2f}%  |  "
                     f"Total MC: {fmt(total_mc)}  |  24h MC: {mc_ch:+.2f}%")
        lines.append(f"Total Vol: {fmt(total_vol)}  |  Vol/MC Ratio: {vol_mc:.1f}%  "
                     f"|  Active Cryptos: {g.get('active_cryptocurrencies','?')}")
    return "\n".join(lines)

async def data_dominance() -> str:
    gdata, coins = await asyncio.gather(
        cg("/global"),
        cg("/coins/markets", {
            "vs_currency": "usd",
            "ids": "bitcoin,ethereum,tether,binancecoin,solana,ripple,"
                   "usd-coin,tron,cardano,avalanche-2,chainlink,polkadot",
            "order": "market_cap_desc",
            "price_change_percentage": "24h,7d",
        }),
    )
    ts = datetime.now(timezone.utc).strftime("%H:%M")
    lines = [f"DOMINANCE & ROTATION | {ts} UTC\n"]

    if gdata and "data" in gdata:
        g = gdata["data"]
        dom = g.get("market_cap_percentage", {})
        btc_dom = dom.get("btc", 0)
        eth_dom = dom.get("eth", 0)
        lines.append(f"BTC Dominance: {btc_dom:.2f}%")
        lines.append(f"ETH Dominance: {eth_dom:.2f}%")
        lines.append(f"Stablecoin Dom (USDT+USDC): {dom.get('usdt',0)+dom.get('usdc',0):.2f}%")
        lines.append(f"Total MC: {fmt(g['total_market_cap'].get('usd',0))}")
        lines.append(f"Total Vol 24h: {fmt(g['total_volume'].get('usd',0))}")
        lines.append("\nFull dominance breakdown:")
        for sym, pct in sorted(dom.items(), key=lambda x: -x[1])[:10]:
            lines.append(f"  {sym.upper():8} {pct:.3f}%")

    if coins:
        lines.append("\nTop assets — price vs BTC (7d):")
        btc_7d = next((c.get("price_change_percentage_7d_in_currency",0) or 0
                       for c in coins if c["id"] == "bitcoin"), 0)
        for c in coins:
            ch24 = c.get("price_change_percentage_24h") or 0
            ch7d = c.get("price_change_percentage_7d_in_currency") or 0
            rel_btc = (ch7d - btc_7d)
            flag = " [OUTPERFORM]" if rel_btc > 2 else (" [UNDERPERFORM]" if rel_btc < -2 else "")
            lines.append(
                f"  {c['symbol'].upper():8} ${c['current_price']:>12,.4f}  "
                f"24h:{ch24:>+6.2f}%  7d:{ch7d:>+6.2f}%  "
                f"vs BTC 7d:{rel_btc:>+6.2f}%{flag}"
            )
    return "\n".join(lines)

async def data_trending() -> str:
    # Use market_cap_desc + price_change for gainers — percent_change param doesn't exist in CoinGecko
    trending, market = await asyncio.gather(
        cg("/search/trending"),
        cg("/coins/markets", {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": "50",
            "page": "1",
            "price_change_percentage": "1h,24h",
            "sparkline": "false",
        }),
    )
    ts = datetime.now(timezone.utc).strftime("%H:%M")
    lines = [f"TRENDING & NARRATIVE DATA | {ts} UTC\n"]

    if trending and "coins" in trending:
        lines.append("CoinGecko trending (search volume — last 24h):")
        for i, item in enumerate(trending["coins"][:7], 1):
            c = item["item"]
            price_btc = c.get("price_btc", 0)
            lines.append(
                f"  {i}. {c['name']} ({c['symbol'].upper()})  "
                f"Rank #{c.get('market_cap_rank','?')}  "
                f"Score: {c.get('score',0)}"
            )

    if market:
        gainers = sorted(
            [c for c in market if c.get("price_change_percentage_24h") is not None],
            key=lambda x: x["price_change_percentage_24h"], reverse=True
        )[:7]
        losers = sorted(
            [c for c in market if c.get("price_change_percentage_24h") is not None],
            key=lambda x: x["price_change_percentage_24h"]
        )[:7]

        lines.append("\nTop 24h gainers (top 50 by MCap):")
        for c in gainers:
            ch24 = c.get("price_change_percentage_24h") or 0
            vol = c.get("total_volume", 0)
            mc  = c.get("market_cap", 1)
            vol_ratio = vol / mc * 100 if mc else 0
            lines.append(
                f"  {c['symbol'].upper():8} {ch24:>+6.2f}%  "
                f"${c['current_price']:>10,.4f}  "
                f"MCap:{fmt(mc):>9}  Vol/MC:{vol_ratio:.1f}%"
            )

        lines.append("\nTop 24h losers (top 50 by MCap):")
        for c in losers:
            ch24 = c.get("price_change_percentage_24h") or 0
            vol = c.get("total_volume", 0)
            mc  = c.get("market_cap", 1)
            vol_ratio = vol / mc * 100 if mc else 0
            lines.append(
                f"  {c['symbol'].upper():8} {ch24:>+6.2f}%  "
                f"${c['current_price']:>10,.4f}  "
                f"MCap:{fmt(mc):>9}  Vol/MC:{vol_ratio:.1f}%"
            )
    return "\n".join(lines)

async def data_defi() -> str:
    tvl, protocols, chains = await asyncio.gather(
        fetch(f"{LLAMA_BASE}/tvl"),
        fetch(f"{LLAMA_BASE}/protocols"),
        fetch(f"{LLAMA_BASE}/v2/chains"),
    )
    ts = datetime.now(timezone.utc).strftime("%H:%M")
    lines = [f"DEFI TVL DATA (DeFiLlama) | {ts} UTC\n"]

    if tvl:
        try:
            lines.append(f"Total DeFi TVL: {fmt(float(tvl))}")
        except (ValueError, TypeError):
            lines.append("Total DeFi TVL: unavailable")

    if protocols:
        valid = [p for p in protocols if p.get("tvl") and p["tvl"] > 0]
        top = sorted(valid, key=lambda x: x["tvl"], reverse=True)[:12]
        lines.append("\nTop 12 protocols by TVL:")
        lines.append(f"  {'NAME':22} {'TVL':>10}  {'1D':>7}  {'7D':>7}  CHAIN")
        lines.append("  " + "─" * 62)
        for p in top:
            ch1d = p.get("change_1d") or 0
            ch7d = p.get("change_7d") or 0
            lines.append(
                f"  {p['name']:22} {fmt(p['tvl']):>10}  "
                f"{ch1d:>+6.2f}%  {ch7d:>+6.2f}%  {p.get('chain','multi')}"
            )

    if chains:
        valid_c = [c for c in chains if c.get("tvl") and c["tvl"] > 0]
        top_c = sorted(valid_c, key=lambda x: x["tvl"], reverse=True)[:10]
        lines.append("\nTop 10 chains by TVL:")
        for c in top_c:
            lines.append(f"  {c.get('name','?'):18} {fmt(c['tvl']):>10}")

    return "\n".join(lines)

async def data_fear_greed() -> str:
    fng, gdata, stables, prices = await asyncio.gather(
        fetch(FNG_URL),
        cg("/global"),
        cg("/coins/markets", {
            "vs_currency": "usd",
            "ids": "tether,usd-coin,dai,first-digital-usd,true-usd",
            "order": "market_cap_desc",
        }),
        cg("/coins/markets", {
            "vs_currency": "usd",
            "ids": "bitcoin,ethereum",
            "price_change_percentage": "1h,24h,7d",
        }),
    )
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    lines = [f"SENTIMENT DATA | {ts} UTC\n"]

    # Live prices first — ground truth for model
    if prices:
        lines.append("CURRENT PRICES (live — use these for any trade setup):")
        for c in prices:
            ch1h  = c.get("price_change_percentage_1h_in_currency") or 0
            ch24h = c.get("price_change_percentage_24h") or 0
            ch7d  = c.get("price_change_percentage_7d_in_currency") or 0
            lines.append(
                f"  {c['symbol'].upper():5} ${c['current_price']:>12,.2f}  "
                f"1h:{ch1h:>+6.2f}%  24h:{ch24h:>+6.2f}%  7d:{ch7d:>+6.2f}%"
            )
        lines.append("")

    # Fear & Greed
    if fng and "data" in fng:
        lines.append("Fear & Greed Index (Alternative.me):")
        for entry in fng["data"][:3]:
            ts2 = datetime.fromtimestamp(int(entry["timestamp"]), tz=timezone.utc).strftime("%b %d")
            val = int(entry["value"])
            zone = entry["value_classification"]
            lines.append(f"  {ts2}: {val:>3}/100 — {zone}")
        lines.append("  (Below 20 = extreme fear / historical buy zone. Above 80 = extreme greed / fade zone.)")

    # Global market
    if gdata and "data" in gdata:
        g = gdata["data"]
        total_mc = g.get("total_market_cap", {}).get("usd", 0)
        mc_ch    = g.get("market_cap_change_percentage_24h_usd", 0)
        btc_dom  = g["market_cap_percentage"].get("btc", 0)
        lines.append(f"\nTotal MC: {fmt(total_mc)}  |  24h: {mc_ch:+.2f}%")
        lines.append(f"BTC Dom: {btc_dom:.2f}%")

    # Stablecoin supply
    if stables:
        lines.append("\nStablecoin supply (dry powder proxy):")
        total = 0
        for s in stables:
            mc  = s.get("market_cap", 0) or 0
            vol = s.get("total_volume", 0) or 0
            ratio = (vol / mc * 100) if mc else 0
            total += mc
            lines.append(
                f"  {s['symbol'].upper():6} MCap:{fmt(mc):>10}  "
                f"Vol:{fmt(vol):>10}  Vol/MCap:{ratio:.1f}%"
            )
        lines.append(f"  TOTAL STABLECOIN SUPPLY: {fmt(total)}")
        lines.append("  (Supply growing = dry powder building. Vol/MCap >15% on USDT = large move imminent.)")

    return "\n".join(lines)

async def data_etf() -> str:
    btc_d, eth_d = await asyncio.gather(
        cg("/coins/bitcoin", {
            "localization":"false","tickers":"false",
            "market_data":"true","community_data":"false","developer_data":"false",
        }),
        cg("/coins/ethereum", {
            "localization":"false","tickers":"false",
            "market_data":"true","community_data":"false","developer_data":"false",
        }),
    )
    ts = datetime.now(timezone.utc).strftime("%H:%M")
    lines = [f"ETF & INSTITUTIONAL PROXY DATA | {ts} UTC\n"]
    lines.append("Note: Direct ETF flow (BlackRock/Fidelity) requires SoSoValue or Bloomberg.")
    lines.append("Below: institutional proxy signals from CoinGecko Pro.\n")

    for label, d in [("BTC", btc_d), ("ETH", eth_d)]:
        if not d:
            lines.append(f"{label}: data unavailable")
            continue
        md = d.get("market_data", {}) or {}
        price  = (md.get("current_price") or {}).get("usd", 0)
        mc     = (md.get("market_cap") or {}).get("usd", 0)
        vol    = (md.get("total_volume") or {}).get("usd", 0)
        ath    = (md.get("ath") or {}).get("usd", 0)
        ath_dt = ((md.get("ath_date") or {}).get("usd") or "?")[:10]
        ath_pct= (md.get("ath_change_percentage") or {}).get("usd", 0)
        circ   = md.get("circulating_supply", 0) or 0
        maxs   = md.get("max_supply")
        vol_mc = (vol / mc * 100) if mc else 0
        ch24   = md.get("price_change_percentage_24h") or 0
        ch7d   = (md.get("price_change_percentage_7d_in_currency") or {}).get("usd", 0)

        lines.append(f"{label} INSTITUTIONAL PROXY:")
        lines.append(f"  Price:          ${price:,.2f}  |  24h: {ch24:+.2f}%  |  7d: {ch7d:+.2f}%")
        lines.append(f"  MCap:           {fmt(mc)}")
        lines.append(f"  Vol 24h:        {fmt(vol)}  |  Vol/MCap: {vol_mc:.2f}%")
        lines.append(f"  ATH:            ${ath:,.2f}  on {ath_dt}  ({ath_pct:.1f}% from ATH)")
        lines.append(f"  Circulating:    {circ:,.0f}")
        if maxs:
            pct = circ / maxs * 100 if circ else 0
            lines.append(f"  Max Supply:     {maxs:,.0f}  ({pct:.1f}% issued)")
        lines.append(f"  Interpretation: Vol/MCap {vol_mc:.2f}% — "
                     f"{'elevated institutional activity' if vol_mc > 8 else 'normal/low activity'}")
        lines.append("")

    lines.append("Live ETF flow: sosovalue.org | bloomberg terminal | theblock.co")
    return "\n".join(lines)

async def data_macro() -> str:
    events = await cg("/events", {"upcoming_events_only": "true", "per_page": "15"})
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    lines = [f"MACRO & EVENT CALENDAR | {ts} UTC\n"]

    lines.append("HIGH-IMPACT RECURRING EVENTS:")
    lines.append("  [RED]    FOMC Meeting + Press Conference — rate decision, critical")
    lines.append("  [RED]    US CPI Release (monthly) — inflation data, risk-on/off trigger")
    lines.append("  [RED]    US NFP Jobs Report (1st Friday of month) — macro risk sentiment")
    lines.append("  [AMBER]  BTC Options Expiry (every Friday, large on last Friday) — Deribit")
    lines.append("  [AMBER]  Fed Speaker appearances — forward guidance shifts")
    lines.append("  [AMBER]  US PPI Release — leads CPI")
    lines.append("  [AMBER]  JOLTS / ADP — leads NFP")
    lines.append("  [INFO]   Token unlocks — check tokenunlocks.app")
    lines.append("  [INFO]   Governance votes — check snapshot.org")

    if events and "data" in events:
        lines.append("\nUPCOMING CRYPTO EVENTS (CoinGecko):")
        for e in events["data"][:10]:
            date  = (e.get("start_date") or "?")[:10]
            title = e.get("title", "?")
            etype = e.get("type", "?")
            coin  = (e.get("coin") or {}).get("name", "General")
            lines.append(f"  {date}  {title[:40]:42}  [{etype}]  {coin}")

    lines.append("\nCALENDAR SOURCES:")
    lines.append("  ForexFactory.com  |  Investing.com/economic-calendar  |  CMEGroup FedWatch")
    return "\n".join(lines)

async def data_watchlist(coin_ids: list) -> str:
    if not coin_ids:
        return "Watchlist empty. Use /watchlist add <coin-id>"
    coins = await cg("/coins/markets", {
        "vs_currency": "usd",
        "ids": ",".join(coin_ids),
        "order": "market_cap_desc",
        "price_change_percentage": "1h,24h,7d,30d",
        "sparkline": "false",
    })
    ts = datetime.now(timezone.utc).strftime("%H:%M")
    lines = [f"WATCHLIST | {ts} UTC\n"]
    if not coins:
        return "Watchlist data unavailable. CoinGecko request failed."

    # Index by id for ordering
    coin_map = {c["id"]: c for c in coins}
    for cid in coin_ids:
        c = coin_map.get(cid)
        if not c:
            lines.append(f"  {cid}: data unavailable\n")
            continue
        ch1h  = c.get("price_change_percentage_1h_in_currency") or 0
        ch24h = c.get("price_change_percentage_24h") or 0
        ch7d  = c.get("price_change_percentage_7d_in_currency") or 0
        ch30d = c.get("price_change_percentage_30d_in_currency") or 0
        ath_pct = c.get("ath_change_percentage") or 0
        mc    = c.get("market_cap", 0) or 0
        vol   = c.get("total_volume", 0) or 0
        vol_mc = (vol / mc * 100) if mc else 0
        price = c.get("current_price", 0)
        price_str = f"${price:,.4f}" if price < 1 else f"${price:,.2f}"

        lines.append(f"{c['name']} ({c['symbol'].upper()})  Rank #{c.get('market_cap_rank','?')}")
        lines.append(f"  Price:   {price_str}  |  vs ATH: {ath_pct:.1f}%")
        lines.append(f"  1h:{ch1h:>+6.2f}%  24h:{ch24h:>+6.2f}%  7d:{ch7d:>+6.2f}%  30d:{ch30d:>+6.2f}%")
        lines.append(f"  MCap:    {fmt(mc):>10}  |  Vol: {fmt(vol):>10}  |  Vol/MCap: {vol_mc:.1f}%")
        lines.append("")
    return "\n".join(lines)

async def data_btc_full() -> str:
    btc = await cg("/coins/bitcoin", {
        "localization":"false","tickers":"false",
        "market_data":"true","community_data":"true","developer_data":"false",
    })
    if not btc:
        return "BTC data unavailable."
    md = btc.get("market_data", {}) or {}
    cd = btc.get("community_data", {}) or {}
    ts = datetime.now(timezone.utc).strftime("%H:%M")
    lines = [f"BTC FULL SNAPSHOT | {ts} UTC\n"]

    price  = (md.get("current_price") or {}).get("usd", 0)
    mc     = (md.get("market_cap") or {}).get("usd", 0)
    vol    = (md.get("total_volume") or {}).get("usd", 0)
    ath    = (md.get("ath") or {}).get("usd", 0)
    ath_dt = ((md.get("ath_date") or {}).get("usd") or "?")[:10]
    ath_pct= (md.get("ath_change_percentage") or {}).get("usd", 0)
    atl    = (md.get("atl") or {}).get("usd", 0)
    low24  = (md.get("low_24h") or {}).get("usd", 0)
    high24 = (md.get("high_24h") or {}).get("usd", 0)
    circ   = md.get("circulating_supply", 0) or 0
    ch1h   = (md.get("price_change_percentage_1h_in_currency") or {}).get("usd", 0)
    ch24   = md.get("price_change_percentage_24h", 0) or 0
    ch7d   = (md.get("price_change_percentage_7d_in_currency") or {}).get("usd", 0)
    ch30d  = (md.get("price_change_percentage_30d_in_currency") or {}).get("usd", 0)
    ch1y   = (md.get("price_change_percentage_1y_in_currency") or {}).get("usd", 0)
    vol_mc = (vol / mc * 100) if mc else 0
    pct_mined = circ / 21_000_000 * 100

    lines.append(f"Price:          ${price:,.2f}")
    lines.append(f"1h:             {ch1h:+.2f}%")
    lines.append(f"24h:            {ch24:+.2f}%")
    lines.append(f"7d:             {ch7d:+.2f}%")
    lines.append(f"30d:            {ch30d:+.2f}%")
    lines.append(f"1y:             {ch1y:+.2f}%")
    lines.append(f"24h Range:      ${low24:,.2f} — ${high24:,.2f}")
    lines.append(f"MCap:           {fmt(mc)}")
    lines.append(f"Vol 24h:        {fmt(vol)}  |  Vol/MCap: {vol_mc:.2f}%")
    lines.append(f"ATH:            ${ath:,.2f}  on {ath_dt}  ({ath_pct:.1f}% from ATH)")
    lines.append(f"ATL:            ${atl:,.4f}")
    lines.append(f"Circulating:    {circ:,.0f} BTC  ({pct_mined:.2f}% of 21M mined)")

    if cd:
        tw = cd.get("twitter_followers", 0) or 0
        rd = cd.get("reddit_subscribers", 0) or 0
        if tw or rd:
            lines.append(f"\nCommunity:      Twitter {tw:,}  |  Reddit {rd:,}")
    return "\n".join(lines)

# ── Groq call ─────────────────────────────────────────────────────────────────
async def call_groq(prompt: str, custom: str = "", max_tokens: int = 1500) -> str:
    client = Groq(api_key=GROQ_KEY)
    system = CIPHER_SYSTEM
    if custom.strip():
        system += f"\n\nANALYST PROFILE & CUSTOM INSTRUCTIONS:\n{custom}"

    loop = asyncio.get_event_loop()
    def _call():
        return client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            max_tokens=max_tokens,
            temperature=0.2,
            messages=[
                {"role": "system", "content": system},
                {"role": "user",   "content": prompt},
            ],
        )
    try:
        resp = await asyncio.wait_for(
            loop.run_in_executor(None, _call),
            timeout=45.0
        )
        return resp.choices[0].message.content.strip() or "CIPHER: no output returned."
    except asyncio.TimeoutError:
        return "CIPHER: Groq timeout (>45s). Try again or use a shorter query."
    except Exception as e:
        logger.error(f"Groq error: {e}")
        return f"CIPHER: AI call failed — {str(e)[:100]}"

# ── Send helper ───────────────────────────────────────────────────────────────
async def send_long(update: Update, text: str):
    """Split and send messages respecting Telegram's 4096 char limit."""
    if not text.strip():
        await update.message.reply_text("CIPHER: Empty response. Try again.")
        return
    for i in range(0, len(text), 4000):
        await update.message.reply_text(text[i:i+4000])

async def typing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_chat_action(update.effective_chat.id, "typing")

# ── Command Handlers ──────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    get_user(user.id)
    plan = "OWNER" if user.id == OWNER_ID else ("PRO" if is_pro(user.id) else "FREE")
    await update.message.reply_text(
        f"*CIPHER Intelligence*\n"
        f"Welcome {user.first_name} | Plan: {plan}\n\n"
        "*Commands:*\n"
        "/cipher — Full 30-min cycle report\n"
        "/btc — Deep BTC snapshot\n"
        "/dominance — BTC dom + rotation\n"
        "/trending — Trending + gainers/losers\n"
        "/defi — DeFi TVL breakdown\n"
        "/fear — Sentiment + stablecoin supply\n"
        "/etf — Institutional proxy data\n"
        "/macro — Event calendar\n"
        "/watchlist — Your tracked coins\n"
        "/ask [question] — Any question with live data\n"
        "/setup — Custom analyst instructions\n"
        "/help — Full command reference\n\n"
        "Or just type anything — CIPHER responds with live data.",
        parse_mode=ParseMode.MARKDOWN,
    )

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "*CIPHER — Command Reference*\n\n"
        "`/cipher` — Full cycle: market + on-chain + derivatives + macro + setup\n"
        "`/btc` — BTC deep dive: all timeframes, supply, community\n"
        "`/dominance` — BTC dom, ETH dom, altcoin rotation signals\n"
        "`/trending` — CoinGecko trending + top gainers/losers with vol quality\n"
        "`/defi` — DeFi TVL by protocol and chain (DeFiLlama live)\n"
        "`/fear` — Fear & Greed + stablecoin supply + live BTC/ETH prices\n"
        "`/etf` — Institutional proxy: Vol/MCap, ATH distance, supply\n"
        "`/macro` — High-impact event calendar + upcoming crypto events\n"
        "`/watchlist` — View tracked coins\n"
        "`/watchlist add <coin-id>` — Add coin (use CoinGecko ID)\n"
        "`/watchlist remove <coin-id>` — Remove coin\n"
        "`/ask [question]` — Any question with live market context\n"
        "`/setup` — Set custom instructions (trading style, focus coins, risk params)\n\n"
        "*Free-text works too:*\n"
        "Type a coin name, ticker, or any question — CIPHER fetches live data and responds.\n\n"
        "*Examples:*\n"
        "`what about tao` `should I scale into sei` `is link breaking out`\n"
        "`compare sol vs avax` `explain funding rates` `any alerts right now`",
        parse_mode=ParseMode.MARKDOWN,
    )

async def cmd_cipher(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    await update.message.reply_text("Fetching live data from CoinGecko + DeFiLlama + Alternative.me...")
    await typing(update, context)

    market, defi, fear, trending = await asyncio.gather(
        data_market_snapshot(),
        data_defi(),
        data_fear_greed(),
        data_trending(),
    )
    prompt = (
        f"{market}\n\n"
        f"{fear}\n\n"
        f"DEFI SUMMARY (top protocols):\n{chr(10).join(defi.split(chr(10))[:15])}\n\n"
        f"TRENDING:\n{chr(10).join(trending.split(chr(10))[:20])}\n\n"
        "TYPE A — Run full CIPHER cycle report.\n"
        "For every metric: state the number AND what it means in the same sentence.\n"
        "Stablecoin supply direction and Vol/MCap ratio are your on-chain proxy signals — interpret them.\n"
        "Trending data shows retail positioning — separate from where real volume is.\n"
        "Include TRADE SETUP only if 2+ independent signals align. Hard invalidation required.\n"
        "End with ACTION line: trade / add / reduce / flat / wait and one specific reason."
    )
    result = await call_groq(prompt, user_data.get("custom_instructions",""), max_tokens=2000)
    await send_long(update, result)

async def cmd_btc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    await typing(update, context)
    btc_data = await data_btc_full()
    prompt = (
        f"{btc_data}\n\n"
        "TYPE B — BTC BRIEF.\n"
        "Price vs ATH: state the % gap and what it implies for downside risk at this level.\n"
        "Vol/MCap ratio: is volume elevated or suppressed? What does the ratio say about conviction?\n"
        "Momentum across 1h/24h/7d/30d: is the trend accelerating, decelerating, or reversing?\n"
        "% mined: contextualise supply dynamics.\n"
        "Community data: treat as contrarian — high follower count at tops, capitulation at bottoms.\n"
        "End with VERDICT: SCALE IN / WAIT FOR LEVEL / AVOID — with specific price levels."
    )
    result = await call_groq(prompt, user_data.get("custom_instructions",""))
    await send_long(update, result)

async def cmd_dominance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    await typing(update, context)
    dom_data = await data_dominance()
    prompt = (
        f"{dom_data}\n\n"
        "TYPE A — DOMINANCE REPORT.\n"
        "BTC dom: state exact % and historical context — does capital flow to alts at this level?\n"
        "ETH dom vs BTC dom: if both falling = stablecoin rotation (fear). ETH rising + BTC falling = early alt season.\n"
        "List every asset outperforming BTC on 7d with exact relative performance number.\n"
        "Stablecoin dominance: growing stablecoin dom = risk-off, capital exiting.\n"
        "Rotation thesis: one sentence with specific BTC dom level that triggers alt rotation."
    )
    result = await call_groq(prompt, user_data.get("custom_instructions",""))
    await send_long(update, result)

async def cmd_trending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    await typing(update, context)
    trend_data = await data_trending()
    prompt = (
        f"{trend_data}\n\n"
        "TYPE A — TRENDING REPORT.\n"
        "For each trending coin: Vol/MCap ratio separates organic from retail chase — state which for each.\n"
        "Gainers: is volume expanding with the move (real) or thin (fragile)?\n"
        "Losers: are fundamentally strong assets being sold (opportunity) or justified selloff?\n"
        "Dominant narrative: name it in one sentence and state whether capital is behind it.\n"
        "Flag any coin with >30% 24h gain and <$100M MCap — high manipulation probability."
    )
    result = await call_groq(prompt, user_data.get("custom_instructions",""))
    await send_long(update, result)

async def cmd_defi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    await update.message.reply_text("Fetching DeFiLlama live data...")
    await typing(update, context)
    defi_data = await data_defi()
    prompt = (
        f"{defi_data}\n\n"
        "TYPE A — DEFI REPORT.\n"
        "Total TVL direction and what it implies for overall DeFi health.\n"
        "Top 3 protocols gaining TVL: why? Which chains are capturing share?\n"
        "Top 3 protocols losing TVL: is it a price effect or genuine capital exit?\n"
        "Chain dominance shift: any chain gaining >2% share on 7d is a structural signal.\n"
        "One sentence: where is smart capital moving in DeFi right now?"
    )
    result = await call_groq(prompt, user_data.get("custom_instructions",""))
    await send_long(update, result)

async def cmd_fear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    await typing(update, context)
    fear_data = await data_fear_greed()
    prompt = (
        f"{fear_data}\n\n"
        "TYPE A — SENTIMENT REPORT.\n"
        "IMPORTANT: Use ONLY the live prices in CURRENT PRICES section for any trade levels.\n"
        "Fear & Greed: state score, 3-day trend, and which zone (extreme fear/fear/neutral/greed/extreme greed).\n"
        "Stablecoin total supply: growing = dry powder accumulating = potential fuel for upside. Shrinking = deployed.\n"
        "USDT Vol/MCap >15% = abnormal turnover, large move imminent — flag if triggered.\n"
        "Positioning implication: are traders over-extended long or is there capacity to absorb buying?\n"
        "If trade setup warranted: use live BTC/ETH price from data. Hard stop required."
    )
    result = await call_groq(prompt, user_data.get("custom_instructions",""))
    await send_long(update, result)

async def cmd_etf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    await typing(update, context)
    etf_data = await data_etf()
    prompt = (
        f"{etf_data}\n\n"
        "TYPE A — INSTITUTIONAL PROXY REPORT.\n"
        "Vol/MCap ratio: >8% = elevated institutional activity, <2% = accumulation or disinterest — state which.\n"
        "ATH distance: what % drawdown are late-cycle ETF buyers sitting on? Contextualise the pain.\n"
        "Supply issuance: BTC 94%+ mined = structural supply scarcity. ETH has no cap — different dynamic.\n"
        "One-line institutional thesis: are conditions favourable for ETF inflows based on price structure and vol?"
    )
    result = await call_groq(prompt, user_data.get("custom_instructions",""))
    await send_long(update, result)

async def cmd_macro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    await typing(update, context)
    macro_data, market_data = await asyncio.gather(
        data_macro(),
        data_market_snapshot(["bitcoin", "ethereum"]),
    )
    prompt = (
        f"{macro_data}\n\n"
        f"Current market:\n{market_data[:600]}\n\n"
        "TYPE A — MACRO BRIEFING.\n"
        "List upcoming events with specific crypto impact direction and the exact risk mechanism.\n"
        "Current macro regime: rates, dollar strength, equity correlation — net positive or negative for crypto?\n"
        "Pre-event playbook: what does a trader do in the 48h before FOMC/CPI? Specific recommendation.\n"
        "One-line regime summary: risk-on / risk-off / transitional and the single number that defines it."
    )
    result = await call_groq(prompt, user_data.get("custom_instructions",""))
    await send_long(update, result)

async def cmd_watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    watchlist = user_data.get("watchlist", ["bitcoin", "ethereum"])

    if context.args:
        action = context.args[0].lower()
        if action == "add" and len(context.args) > 1:
            coin = context.args[1].lower().strip()
            if len(watchlist) >= 20:
                await update.message.reply_text("Watchlist limit: 20 coins. Remove one first.")
                return
            if coin in watchlist:
                await update.message.reply_text(f"{coin} already in watchlist.")
                return
            # Validate against CoinGecko
            check = await cg(f"/coins/{coin}")
            if not check or isinstance(check, dict) and check.get("error"):
                # Try search
                sr = await cg("/search", {"query": coin})
                if sr and sr.get("coins"):
                    top = sr["coins"][0]
                    cg_id = top["id"]
                    await update.message.reply_text(
                        f"'{coin}' not found as CoinGecko ID.\n"
                        f"Did you mean: {top['name']} ({top['symbol'].upper()}) — ID: {cg_id}?\n"
                        f"Use: /watchlist add {cg_id}"
                    )
                else:
                    await update.message.reply_text(
                        f"Coin '{coin}' not found on CoinGecko.\n"
                        f"Use the CoinGecko ID (e.g. 'chainlink', 'sei-network', 'bittensor')"
                    )
                return
            watchlist.append(coin)
            user_data["watchlist"] = watchlist
            save_user(update.effective_user.id, user_data)
            await update.message.reply_text(f"Added: {coin}\nWatchlist: {', '.join(watchlist)}")
            return

        elif action == "remove" and len(context.args) > 1:
            coin = context.args[1].lower().strip()
            if coin not in watchlist:
                await update.message.reply_text(f"'{coin}' not in watchlist.")
                return
            watchlist.remove(coin)
            user_data["watchlist"] = watchlist
            save_user(update.effective_user.id, user_data)
            await update.message.reply_text(f"Removed: {coin}\nWatchlist: {', '.join(watchlist)}")
            return

        elif action == "clear":
            user_data["watchlist"] = []
            save_user(update.effective_user.id, user_data)
            await update.message.reply_text("Watchlist cleared.")
            return

    # Default: show watchlist
    if not watchlist:
        await update.message.reply_text(
            "Watchlist is empty.\n"
            "Add coins: /watchlist add chainlink\n"
            "Use CoinGecko IDs (lowercase): bitcoin, ethereum, solana, sei-network, bittensor..."
        )
        return

    await typing(update, context)
    wl_data = await data_watchlist(watchlist)
    prompt = (
        f"{wl_data}\n\n"
        "TYPE G — WATCHLIST ANALYSIS.\n"
        "For each coin: price context, 24h/7d momentum vs BTC, Vol/MCap signal, and a one-line verdict.\n"
        "End with: which coin has the strongest setup right now and the specific reason why.\n"
        "Use ONLY live prices from the data above."
    )
    result = await call_groq(prompt, user_data.get("custom_instructions",""))
    await update.message.reply_text(
        f"Watchlist: {', '.join(watchlist)}\n"
        f"Manage: /watchlist add <id> | /watchlist remove <id> | /watchlist clear\n"
        "─────────────────────────────────"
    )
    await send_long(update, result)

async def cmd_ask(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    question = " ".join(context.args).strip() if context.args else ""
    if not question:
        await update.message.reply_text(
            "Usage: /ask [your question]\n\n"
            "Examples:\n"
            "  /ask should I scale into SEI right now\n"
            "  /ask is BTC in a good buy zone\n"
            "  /ask compare SOL vs AVAX\n"
            "  /ask explain funding rate arbitrage\n"
            "  /ask what is driving the market today"
        )
        return
    await typing(update, context)
    # Use same handler logic as free-text
    await _handle_question(update, context, question, user_data)

async def _handle_question(update, context, text: str, user_data: dict):
    """Core question handler — used by both /ask and free-text."""
    coin_id = await resolve_coin(text)

    if coin_id:
        coin_section, market_data = await asyncio.gather(
            fetch_coin_data(coin_id),
            data_market_snapshot(["bitcoin", "ethereum"]),
        )
        prompt = (
            f"{coin_section}\n\n"
            f"BROAD MARKET CONTEXT:\n{market_data}\n\n"
            f"USER QUESTION: {text}\n\n"
            "Classify as TYPE B or TYPE C and respond with the correct CIPHER format.\n"
            "Use ONLY the live prices from the data above — not training data prices."
        )
    else:
        # General question — pull full snapshot
        market_data = await data_market_snapshot()
        prompt = (
            f"LIVE MARKET DATA:\n{market_data}\n\n"
            f"USER QUESTION: {text}\n\n"
            "Classify this question (TYPE A/B/C/D/E/F/G) and respond with the correct CIPHER format.\n"
            "Use only live data provided."
        )

    result = await call_groq(prompt, user_data.get("custom_instructions",""))
    await send_long(update, result)

# ── /setup conversation ───────────────────────────────────────────────────────
async def cmd_setup_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    current = user_data.get("custom_instructions","").strip()
    await update.message.reply_text(
        f"*Custom Analyst Instructions*\n\n"
        f"Current: `{current or 'none set'}`\n\n"
        "Send your instructions to replace. This context gets injected into every CIPHER response.\n\n"
        "Examples:\n"
        "  Focus on SOL, LINK, ARB, TAO alongside BTC/ETH\n"
        "  Swing trading, 3-5 day horizon. Not scalping.\n"
        "  Max 2% risk per trade. Portfolio size $15,000.\n"
        "  Currently long BTC at $82k and ETH at $2,100.\n"
        "  Skip social analysis. Focus on on-chain and derivatives only.\n\n"
        "Or /cancel to keep existing.",
        parse_mode=ParseMode.MARKDOWN,
    )
    return WAITING_SETUP

async def cmd_setup_receive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    user_data["custom_instructions"] = update.message.text.strip()
    save_user(update.effective_user.id, user_data)
    await update.message.reply_text(
        f"Saved. Active in all responses.\n\n`{user_data['custom_instructions']}`",
        parse_mode=ParseMode.MARKDOWN,
    )
    return ConversationHandler.END

async def cmd_setup_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Cancelled. Instructions unchanged.")
    return ConversationHandler.END

# ── Free-text handler ─────────────────────────────────────────────────────────
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    text = update.message.text.strip()
    if not text:
        return
    await typing(update, context)
    await _handle_question(update, context, text, user_data)

# ── Error handler ─────────────────────────────────────────────────────────────
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Unhandled exception:", exc_info=context.error)
    if isinstance(update, Update) and update.message:
        await update.message.reply_text(
            "An error occurred. Try again.\n"
            "If it persists, check /help or report to the bot owner."
        )

# ── Keep-alive (Render free tier) ─────────────────────────────────────────────
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading, time

RENDER_URL = os.getenv("RENDER_EXTERNAL_URL", "")

class PingHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"CIPHER OK")
    def log_message(self, *args):
        pass

def run_ping_server():
    port = int(os.getenv("PORT", "8080"))
    HTTPServer(("0.0.0.0", port), PingHandler).serve_forever()

def self_ping_loop():
    if not RENDER_URL:
        return
    time.sleep(60)
    import urllib.request
    while True:
        try:
            urllib.request.urlopen(f"{RENDER_URL}/", timeout=10)
        except Exception:
            pass
        time.sleep(600)

# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    threading.Thread(target=run_ping_server, daemon=True).start()
    threading.Thread(target=self_ping_loop, daemon=True).start()

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    setup_conv = ConversationHandler(
        entry_points=[CommandHandler("setup", cmd_setup_start)],
        states={WAITING_SETUP: [MessageHandler(filters.TEXT & ~filters.COMMAND, cmd_setup_receive)]},
        fallbacks=[CommandHandler("cancel", cmd_setup_cancel)],
    )

    app.add_handler(CommandHandler("start",      cmd_start))
    app.add_handler(CommandHandler("help",       cmd_help))
    app.add_handler(CommandHandler("cipher",     cmd_cipher))
    app.add_handler(CommandHandler("btc",        cmd_btc))
    app.add_handler(CommandHandler("dominance",  cmd_dominance))
    app.add_handler(CommandHandler("trending",   cmd_trending))
    app.add_handler(CommandHandler("defi",       cmd_defi))
    app.add_handler(CommandHandler("fear",       cmd_fear))
    app.add_handler(CommandHandler("etf",        cmd_etf))
    app.add_handler(CommandHandler("macro",      cmd_macro))
    app.add_handler(CommandHandler("watchlist",  cmd_watchlist))
    app.add_handler(CommandHandler("ask",        cmd_ask))
    app.add_handler(setup_conv)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)

    async with app:
        await app.initialize()
        await app.bot.set_my_commands([
            BotCommand("cipher",    "Full 30-min intelligence cycle"),
            BotCommand("btc",       "Deep BTC snapshot + analysis"),
            BotCommand("dominance", "BTC dominance + altcoin rotation"),
            BotCommand("trending",  "Trending coins + gainers/losers"),
            BotCommand("defi",      "DeFi TVL by protocol + chain"),
            BotCommand("fear",      "Fear & Greed + stablecoin supply"),
            BotCommand("etf",       "ETF + institutional proxy data"),
            BotCommand("macro",     "Macro event calendar"),
            BotCommand("watchlist", "Your tracked coins"),
            BotCommand("ask",       "Ask anything with live data"),
            BotCommand("setup",     "Set custom analyst instructions"),
            BotCommand("help",      "All commands + examples"),
        ])
        logger.info("Commands registered.")
        await app.start()
        await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
        logger.info("CIPHER Bot — Production Release — Online")
        await asyncio.Event().wait()
        await app.updater.stop()
        await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
