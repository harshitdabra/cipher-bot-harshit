"""
CIPHER Telegram Bot — Definitive Release
Data: CoinGecko Pro + CoinGlass Pro + DeFiLlama + Alternative.me
AI:   Groq Llama 3.3 70B

Architecture:
- CoinGecko Pro   → price, volume, market cap, dominance, trending
- CoinGlass Pro   → funding rates, open interest, liquidations, long/short ratio
- DeFiLlama       → TVL by protocol and chain
- Alternative.me  → Fear & Greed Index
- Groq            → analysis, synthesis, trade setup generation
"""

import os, json, logging, asyncio, httpx, re, time
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
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("CIPHER")

# ── Config ────────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
GROQ_KEY        = os.getenv("GROQ_API_KEY", "")
CG_KEY          = os.getenv("COINGECKO_API_KEY", "")
GLASS_KEY       = os.getenv("COINGLASS_API_KEY", "")
OWNER_ID        = int(os.getenv("ALLOWED_USER_ID", "1953473977"))

CG_BASE         = "https://pro-api.coingecko.com/api/v3"
GLASS_BASE      = "https://open-api.coinglass.com/public/v2"
LLAMA_BASE      = "https://api.llama.fi"
FNG_URL         = "https://api.alternative.me/fng/?limit=3"

DB_FILE         = Path("cipher_db.json")
WAITING_SETUP   = 1

# ── In-memory DB ──────────────────────────────────────────────────────────────
_DB: dict = {}

def load_db() -> dict:
    global _DB
    if _DB:
        return _DB
    if DB_FILE.exists():
        try:
            _DB = json.loads(DB_FILE.read_text())
            return _DB
        except Exception:
            pass
    _DB = {"users": {}}
    return _DB

def save_db(db: dict):
    global _DB
    _DB = db
    try:
        DB_FILE.write_text(json.dumps(db, indent=2))
    except Exception as e:
        logger.error(f"DB write failed: {e}")

def get_user(uid: int) -> dict:
    db = load_db()
    key = str(uid)
    if key not in db["users"]:
        db["users"][key] = {
            "custom_instructions": "",
            "watchlist": ["bitcoin", "ethereum"],
            "joined": datetime.now(timezone.utc).isoformat(),
            "plan": "owner" if uid == OWNER_ID else "free",
            "query_count": 0,
        }
        save_db(db)
    return db["users"][key]

def save_user(uid: int, data: dict):
    db = load_db()
    db["users"][str(uid)] = data
    save_db(db)

def is_pro(uid: int) -> bool:
    return uid == OWNER_ID or get_user(uid).get("plan") in ("pro", "owner")

# ── Formatters ────────────────────────────────────────────────────────────────
def fmt(n, dollar=True) -> str:
    """Human-readable K/M/B with null safety."""
    try:
        n = float(n)
    except (TypeError, ValueError):
        return "N/A"
    prefix = "$" if dollar else ""
    abs_n = abs(n)
    if abs_n >= 1_000_000_000:
        return f"{prefix}{n/1_000_000_000:.2f}B"
    if abs_n >= 1_000_000:
        return f"{prefix}{n/1_000_000:.2f}M"
    if abs_n >= 1_000:
        return f"{prefix}{n/1_000:.2f}K"
    return f"{prefix}{n:,.4f}"

def pct(n, show_plus=True) -> str:
    try:
        n = float(n)
        return f"{'+' if n > 0 and show_plus else ''}{n:.2f}%"
    except (TypeError, ValueError):
        return "N/A"

def price_str(n) -> str:
    """Smart price: 2 decimals for >=1, 5 for <1."""
    try:
        n = float(n)
        return f"${n:,.2f}" if n >= 1 else f"${n:,.5f}"
    except (TypeError, ValueError):
        return "N/A"

# ── HTTP client with retry + rate limit handling ───────────────────────────────
async def _fetch(url: str, headers: dict, params: dict) -> dict | list | None:
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=25) as client:
                r = await client.get(url, headers=headers, params=params)
                if r.status_code == 429:
                    wait = 2 ** attempt
                    logger.warning(f"Rate limited {url[:50]}, waiting {wait}s")
                    await asyncio.sleep(wait)
                    continue
                if r.status_code in (401, 403):
                    logger.error(f"Auth failed {url[:50]}: {r.status_code}")
                    return None
                r.raise_for_status()
                return r.json()
        except httpx.TimeoutException:
            logger.warning(f"Timeout attempt {attempt+1}: {url[:50]}")
            await asyncio.sleep(1)
        except Exception as e:
            logger.warning(f"Fetch error attempt {attempt+1} [{url[:50]}]: {e}")
            await asyncio.sleep(1)
    return None

async def cg(endpoint: str, params: dict = None) -> dict | list | None:
    return await _fetch(f"{CG_BASE}{endpoint}", {"x-cg-pro-api-key": CG_KEY}, params or {})

async def gl(endpoint: str, params: dict = None) -> dict | list | None:
    """CoinGlass Pro API."""
    return await _fetch(f"{GLASS_BASE}{endpoint}", {"coinglassSecret": GLASS_KEY}, params or {})

async def ll(endpoint: str) -> dict | list | None:
    """DeFiLlama — no auth needed."""
    return await _fetch(f"{LLAMA_BASE}{endpoint}", {}, {})

# ── Coin resolution ───────────────────────────────────────────────────────────
# Maps ticker/name → CoinGecko ID and CoinGlass symbol
COINS = {
    # symbol: (coingecko_id, coinglass_symbol)
    "btc":        ("bitcoin",                   "BTC"),
    "bitcoin":    ("bitcoin",                   "BTC"),
    "eth":        ("ethereum",                  "ETH"),
    "ethereum":   ("ethereum",                  "ETH"),
    "sol":        ("solana",                    "SOL"),
    "solana":     ("solana",                    "SOL"),
    "bnb":        ("binancecoin",               "BNB"),
    "xrp":        ("ripple",                    "XRP"),
    "ripple":     ("ripple",                    "XRP"),
    "ada":        ("cardano",                   "ADA"),
    "cardano":    ("cardano",                   "ADA"),
    "avax":       ("avalanche-2",               "AVAX"),
    "avalanche":  ("avalanche-2",               "AVAX"),
    "dot":        ("polkadot",                  "DOT"),
    "polkadot":   ("polkadot",                  "DOT"),
    "trx":        ("tron",                      "TRX"),
    "tron":       ("tron",                      "TRX"),
    "ton":        ("the-open-network",          "TON"),
    "near":       ("near",                      "NEAR"),
    "atom":       ("cosmos",                    "ATOM"),
    "cosmos":     ("cosmos",                    "ATOM"),
    "doge":       ("dogecoin",                  "DOGE"),
    "dogecoin":   ("dogecoin",                  "DOGE"),
    "link":       ("chainlink",                 "LINK"),
    "chainlink":  ("chainlink",                 "LINK"),
    "arb":        ("arbitrum",                  "ARB"),
    "arbitrum":   ("arbitrum",                  "ARB"),
    "op":         ("optimism",                  "OP"),
    "optimism":   ("optimism",                  "OP"),
    "matic":      ("matic-network",             "MATIC"),
    "polygon":    ("matic-network",             "MATIC"),
    "sui":        ("sui",                       "SUI"),
    "apt":        ("aptos",                     "APT"),
    "aptos":      ("aptos",                     "APT"),
    "sei":        ("sei-network",               "SEI"),
    "inj":        ("injective-protocol",        "INJ"),
    "injective":  ("injective-protocol",        "INJ"),
    "ftm":        ("fantom",                    "FTM"),
    "fantom":     ("fantom",                    "FTM"),
    "uni":        ("uniswap",                   "UNI"),
    "uniswap":    ("uniswap",                   "UNI"),
    "aave":       ("aave",                      "AAVE"),
    "mkr":        ("maker",                     "MKR"),
    "maker":      ("maker",                     "MKR"),
    "ldo":        ("lido-dao",                  "LDO"),
    "lido":       ("lido-dao",                  "LDO"),
    "crv":        ("curve-dao-token",           "CRV"),
    "curve":      ("curve-dao-token",           "CRV"),
    "gmx":        ("gmx",                       "GMX"),
    "jup":        ("jupiter-exchange-solana",   "JUP"),
    "jupiter":    ("jupiter-exchange-solana",   "JUP"),
    "tao":        ("bittensor",                 "TAO"),
    "bittensor":  ("bittensor",                 "TAO"),
    "fet":        ("fetch-ai",                  "FET"),
    "rndr":       ("render-token",              "RENDER"),
    "render":     ("render-token",              "RENDER"),
    "wld":        ("worldcoin-wld",             "WLD"),
    "worldcoin":  ("worldcoin-wld",             "WLD"),
    "grt":        ("the-graph",                 "GRT"),
    "shib":       ("shiba-inu",                 "SHIB"),
    "pepe":       ("pepe",                      "PEPE"),
    "wif":        ("dogwifcoin",                "WIF"),
    "bonk":       ("bonk",                      "BONK"),
    "hype":       ("hyperliquid",               "HYPE"),
    "hyperliquid":("hyperliquid",               "HYPE"),
    "ena":        ("ethena",                    "ENA"),
    "ethena":     ("ethena",                    "ENA"),
    "strk":       ("starknet",                  "STRK"),
    "starknet":   ("starknet",                  "STRK"),
    "zk":         ("zksync",                    "ZK"),
    "imx":        ("immutable-x",               "IMX"),
    "algo":       ("algorand",                  "ALGO"),
    "icp":        ("internet-computer",         "ICP"),
    "fil":        ("filecoin",                  "FIL"),
    "kas":        ("kaspa",                     "KAS"),
    "kaspa":      ("kaspa",                     "KAS"),
    "stx":        ("blockstack",                "STX"),
    "ondo":       ("ondo-finance",              "ONDO"),
    "pendle":     ("pendle",                    "PENDLE"),
    "dydx":       ("dydx",                      "DYDX"),
    "trump":      ("official-trump",            "TRUMP"),
    "hbar":       ("hedera-hashgraph",          "HBAR"),
    "hedera":     ("hedera-hashgraph",          "HBAR"),
    "floki":      ("floki",                     "FLOKI"),
    "not":        ("notcoin",                   "NOT"),
    "notcoin":    ("notcoin",                   "NOT"),
    "w":          ("wormhole",                  "W"),
    "wormhole":   ("wormhole",                  "W"),
    "eigen":      ("eigenlayer",                "EIGEN"),
    "pyth":       ("pyth-network",              "PYTH"),
    "jto":        ("jito-governance-token",     "JTO"),
    "blur":       ("blur",                      "BLUR"),
    "arkm":       ("arkham",                    "ARKM"),
}

SKIP = {
    "the","is","a","an","i","my","about","what","think","buy","sell",
    "good","bad","now","still","long","short","hold","add","into","this",
    "that","and","or","for","with","how","why","when","where","scale",
    "dca","do","you","me","it","not","be","at","in","on","of","to","up",
    "down","just","can","will","should","would","could","get","see","go",
    "give","tell","show","run","check","look","find","mean","need","want",
    "think","feel","know","take","make","has","have","had","was","were",
    "are","been","being","its","your","any","all","some","more","less",
    "very","too","also","than","then","so","if","but","by","from","after",
    "before","during","which","who","best","worst","high","low","price",
    "market","crypto","coin","token","trade","analysis","signal","entry",
    "exit","position","portfolio","chart","data","news","today","right",
    "now","here","there","ok","yes","no","hi","hey","hello","help","go",
}

async def resolve_coin(text: str) -> tuple[str, str] | None:
    """
    Returns (coingecko_id, coinglass_symbol) or None.
    Priority: 1) alias map  2) CoinGecko search fallback
    """
    words = re.findall(r"[a-zA-Z0-9]+", text.lower())

    # Pass 1: exact alias match
    for word in words:
        if word in COINS:
            return COINS[word]

    # Pass 2: CoinGecko search for unrecognised words
    candidates = [w for w in words if w not in SKIP and len(w) >= 2]
    for candidate in candidates:
        result = await cg("/search", {"query": candidate})
        if not result or not result.get("coins"):
            continue
        top = result["coins"][0]
        sym  = top.get("symbol", "").lower()
        name = top.get("name", "").lower()
        cg_id = top.get("id", "")
        if sym == candidate or name == candidate or name.startswith(candidate):
            gl_sym = top.get("symbol", "").upper()
            # Also check our map for CoinGlass symbol
            for k, (gid, gsym) in COINS.items():
                if gid == cg_id:
                    return (cg_id, gsym)
            return (cg_id, gl_sym)
    return None

# ── CoinGecko data ─────────────────────────────────────────────────────────────
async def cg_coin(cg_id: str) -> dict | None:
    """Full coin data from CoinGecko markets endpoint."""
    result = await cg("/coins/markets", {
        "vs_currency": "usd",
        "ids": f"{cg_id},bitcoin",
        "price_change_percentage": "1h,24h,7d,30d",
        "sparkline": "false",
    })
    return result

async def cg_global() -> dict | None:
    return await cg("/global")

async def cg_market(ids: str = None) -> list | None:
    default_ids = (
        "bitcoin,ethereum,solana,binancecoin,ripple,cardano,"
        "avalanche-2,polkadot,tron,near,cosmos,chainlink,"
        "arbitrum,optimism,sui,aptos,sei-network,injective-protocol"
    )
    return await cg("/coins/markets", {
        "vs_currency": "usd",
        "ids": ids or default_ids,
        "order": "market_cap_desc",
        "price_change_percentage": "1h,24h,7d",
        "sparkline": "false",
    })

async def cg_trending() -> dict | None:
    return await cg("/search/trending")

async def cg_top50() -> list | None:
    return await cg("/coins/markets", {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": "50",
        "page": "1",
        "price_change_percentage": "1h,24h,7d",
        "sparkline": "false",
    })

# ── CoinGlass data ─────────────────────────────────────────────────────────────
# CoinGlass Pro v2 endpoint reference:
# /funding_rates_symbol  -> funding per exchange
# /open_interest         -> OI per exchange (WORKING - confirmed via live response)
# /liquidation_chart     -> liquidation totals with time_type param
# /global_long_short_account_ratio -> L/S ratio with time_type param
# All use coinglassSecret header and ?symbol=BTC query param

async def gl_funding(symbol: str = "BTC") -> dict | None:
    """Funding rates per exchange. CoinGlass v2: /funding_rates_symbol"""
    # Try primary endpoint, fall back to alternate naming
    result = await gl("/funding_rates_symbol", {"symbol": symbol})
    if result and result.get("data"):
        logger.info(f"CoinGlass funding OK for {symbol}")
        return result
    # Alternate endpoint name used in some CoinGlass API versions
    result2 = await gl("/funding_rates", {"symbol": symbol})
    if result2 and result2.get("data"):
        logger.info(f"CoinGlass funding (alt) OK for {symbol}")
        return result2
    logger.warning(f"CoinGlass funding returned no data for {symbol}: {str(result)[:100]}")
    return None

async def gl_oi(symbol: str = "BTC") -> dict | None:
    """Open interest per exchange. CoinGlass v2: /open_interest"""
    result = await gl("/open_interest", {"symbol": symbol})
    if result and result.get("data"):
        return result
    logger.warning(f"CoinGlass OI returned no data for {symbol}")
    return None

async def gl_liquidations(symbol: str = "BTC") -> dict | None:
    """Liquidation chart data. CoinGlass v2: /liquidation_chart"""
    # time_type values: 1h, 4h, 12h, 24h
    result = await gl("/liquidation_chart", {"symbol": symbol, "time_type": "1h"})
    if result and result.get("data"):
        return result
    # Some versions use different param name
    result2 = await gl("/liquidation_chart", {"symbol": symbol, "timeType": "1h"})
    if result2 and result2.get("data"):
        return result2
    logger.warning(f"CoinGlass liquidation returned no data for {symbol}: {str(result)[:100]}")
    return None

async def gl_longshort(symbol: str = "BTC") -> dict | None:
    """Long/short account ratio. CoinGlass v2: /global_long_short_account_ratio"""
    result = await gl("/global_long_short_account_ratio", {"symbol": symbol, "time_type": "1h"})
    if result and result.get("data"):
        return result
    # Alternate param
    result2 = await gl("/global_long_short_account_ratio", {"symbol": symbol, "timeType": "1h"})
    if result2 and result2.get("data"):
        return result2
    logger.warning(f"CoinGlass L/S returned no data for {symbol}: {str(result)[:100]}")
    return None

async def gl_multi(symbol: str = "BTC") -> tuple:
    """Fetch all CoinGlass data for a symbol in parallel."""
    return await asyncio.gather(
        gl_funding(symbol),
        gl_oi(symbol),
        gl_liquidations(symbol),
        gl_longshort(symbol),
    )

async def gl_debug(symbol: str = "BTC") -> str:
    """Debug helper: probe CoinGlass endpoints and return status report."""
    endpoints = [
        ("/funding_rates_symbol",            {"symbol": symbol}),
        ("/funding_rates",                   {"symbol": symbol}),
        ("/open_interest",                   {"symbol": symbol}),
        ("/liquidation_chart",               {"symbol": symbol, "time_type": "1h"}),
        ("/global_long_short_account_ratio", {"symbol": symbol, "time_type": "1h"}),
    ]
    lines = [f"CoinGlass API Debug | {symbol} | {datetime.now(timezone.utc).strftime('%H:%M')} UTC"]
    lines.append(f"Base URL: {GLASS_BASE}")
    lines.append(f"Key configured: {'YES' if GLASS_KEY else 'NO — set COINGLASS_API_KEY'}")
    lines.append("")
    for ep, params in endpoints:
        result = await _fetch(f"{GLASS_BASE}{ep}", {"coinglassSecret": GLASS_KEY}, params)
        if result is None:
            status = "FAIL (None — auth error or wrong path)"
        elif not isinstance(result, dict):
            status = f"FAIL (unexpected type: {type(result).__name__})"
        elif result.get("data"):
            data = result["data"]
            if isinstance(data, list):
                status = f"OK — list with {len(data)} items"
            elif isinstance(data, dict):
                status = f"OK — dict keys: {list(data.keys())[:4]}"
            else:
                status = f"OK — data type: {type(data).__name__}"
        else:
            status = f"EMPTY — keys: {list(result.keys())[:5]}, msg: {result.get('msg','')}"
        lines.append(f"  {ep:45} {status}")
    return "\n".join(lines)

# ── Data formatters ───────────────────────────────────────────────────────────
def format_coin_section(c: dict, btc_24h: float = 0) -> str:
    """Format a single coin's market data into analysis-ready text."""
    ch1h  = c.get("price_change_percentage_1h_in_currency") or 0
    ch24h = c.get("price_change_percentage_24h") or 0
    ch7d  = c.get("price_change_percentage_7d_in_currency") or 0
    ch30d = c.get("price_change_percentage_30d_in_currency") or 0
    ath_p = c.get("ath_change_percentage") or 0
    mc    = c.get("market_cap") or 1
    vol   = c.get("total_volume") or 0
    vm    = (vol / mc * 100) if mc else 0
    lo    = c.get("low_24h") or 0
    hi    = c.get("high_24h") or 0
    circ  = c.get("circulating_supply") or 0
    maxs  = c.get("max_supply")
    ath   = c.get("ath") or 0
    p     = c.get("current_price") or 0
    rel   = ch24h - btc_24h

    lines = [
        f"=== {c['name'].upper()} ({c['symbol'].upper()}) ===",
        f"Price:       {price_str(p)}",
        f"1h:          {pct(ch1h)}  |  24h: {pct(ch24h)}  |  7d: {pct(ch7d)}  |  30d: {pct(ch30d)}",
        f"24h Range:   {price_str(lo)} — {price_str(hi)}",
        f"vs ATH:      {ath_p:.1f}%  (ATH: {price_str(ath)})",
        f"MCap:        {fmt(mc)}  |  Rank #{c.get('market_cap_rank','?')}",
        f"Vol 24h:     {fmt(vol)}  |  Vol/MCap: {vm:.1f}%",
    ]
    if circ:
        if maxs:
            issued = circ / maxs * 100
            lines.append(f"Supply:      {circ:,.0f} / {maxs:,.0f}  ({issued:.1f}% issued)")
        else:
            lines.append(f"Circulating: {circ:,.0f}")
    if btc_24h:
        lines.append(f"vs BTC 24h:  {rel:+.2f}pp  ({'OUTPERFORM' if rel > 0 else 'UNDERPERFORM'})")
    return "\n".join(lines)

def format_derivatives(funding_data, oi_data, liq_data, ls_data, symbol: str) -> str:
    lines = [f"=== {symbol} DERIVATIVES ==="]

    # Funding rates
    if funding_data and funding_data.get("data"):
        data = funding_data["data"]
        lines.append("\nFunding Rates (per 8h):")
        exchanges = data if isinstance(data, list) else data.get("uMarginList", [])
        total, count = 0, 0
        for ex in (exchanges[:8] if exchanges else []):
            name = ex.get("exchangeName", ex.get("exchange", "?"))
            rate = ex.get("fundingRate", ex.get("rate", 0)) or 0
            try:
                rate = float(rate) * 100
                total += rate
                count += 1
                flag = " [EXTREME]" if abs(rate) > 0.1 else ""
                lines.append(f"  {name:12} {rate:>+7.4f}%{flag}")
            except (TypeError, ValueError):
                pass
        if count:
            avg = total / count
            interp = "CROWDED LONG — fade risk" if avg > 0.08 else (
                      "CROWDED SHORT — squeeze risk" if avg < -0.03 else "NEUTRAL")
            lines.append(f"  Average:     {avg:>+7.4f}%  → {interp}")
    else:
        lines.append("\nFunding Rates: data unavailable")

    # Open Interest
    if oi_data and oi_data.get("data"):
        data = oi_data["data"]
        items = data if isinstance(data, list) else data.get("data", [])
        total_oi = sum(float(x.get("openInterest", x.get("oi", 0)) or 0) for x in items)
        lines.append(f"\nOpen Interest: {fmt(total_oi)}")
        for x in (items[:5] if items else []):
            ex = x.get("exchangeName", x.get("exchange", "?"))
            oi = float(x.get("openInterest", x.get("oi", 0)) or 0)
            lines.append(f"  {ex:12} OI: {fmt(oi)}")
    else:
        lines.append("\nOpen Interest: data unavailable")

    # Long/Short ratio
    if ls_data and ls_data.get("data"):
        data = ls_data["data"]
        items = data if isinstance(data, list) else []
        if items:
            latest = items[-1] if items else {}
            ls_ratio = latest.get("longRatio", latest.get("longAccount", 0)) or 0
            ss_ratio = latest.get("shortRatio", latest.get("shortAccount", 0)) or 0
            try:
                ls_ratio = float(ls_ratio) * 100
                ss_ratio = float(ss_ratio) * 100
                interp = ("Majority long — crowded, watch for squeeze down" if ls_ratio > 60
                          else "Majority short — potential squeeze up" if ls_ratio < 40
                          else "Balanced")
                lines.append(f"\nLong/Short Ratio:  Long {ls_ratio:.1f}%  /  Short {ss_ratio:.1f}%")
                lines.append(f"  Interpretation: {interp}")
            except (TypeError, ValueError):
                lines.append("\nLong/Short Ratio: parsing error")
    else:
        lines.append("\nLong/Short Ratio: data unavailable")

    # Liquidations
    if liq_data and liq_data.get("data"):
        data = liq_data["data"]
        buy_liq  = data.get("buyLiquidationSum",  data.get("buy",  0)) or 0
        sell_liq = data.get("sellLiquidationSum", data.get("sell", 0)) or 0
        try:
            buy_liq  = float(buy_liq)
            sell_liq = float(sell_liq)
            total    = buy_liq + sell_liq
            lines.append(f"\nLiquidations (1h):  Total {fmt(total)}")
            lines.append(f"  Longs liquidated:  {fmt(buy_liq)}")
            lines.append(f"  Shorts liquidated: {fmt(sell_liq)}")
            if total > 100_000_000:
                lines.append(f"  [ALERT] >$100M liquidated — cascade risk")
        except (TypeError, ValueError):
            lines.append("\nLiquidations: parsing error")
    else:
        lines.append("\nLiquidations: data unavailable")

    return "\n".join(lines)

# ── Master System Prompt ──────────────────────────────────────────────────────
SYSTEM = """IDENTITY
You are CIPHER — senior crypto on-chain analyst and derivatives strategist.
You write like a Delphi Digital research brief or a Nansen alpha report.
Direct. Data-anchored. Actionable. Nothing else.

MISSION
Every response must accomplish exactly one or more of:
1. State what a signal MEANS — not what it is
2. State what to DO — specific action with levels
3. State what to WATCH — trigger that changes the thesis

If your response does none of these, it is wrong. Rewrite it.

══════════════════════════════════════
BANNED — NEVER WRITE THESE
══════════════════════════════════════
PHRASES: "it is worth noting" | "this suggests" | "this indicates" | "potentially" |
"may indicate" | "could be" | "one might" | "in conclusion" | "to summarize" |
"it is important" | "it remains to be seen" | "overall" | "essentially" |
"notably" | "importantly" | "interestingly" | "looking at" | "in terms of" |
"delve" | "landscape" | "ecosystem" | "robust" | "seamless" |
"market participants" | "strong fundamentals" | "weak fundamentals" |
"bullish outlook" | "bearish sentiment" | "at the end of the day"

BEHAVIORS:
- No emojis
- No describing data. Only interpret it.
- No training-data prices. ONLY use prices in the provided live data.
- No fabricating signals. Missing data = write "data unavailable" and stop.
- No padding. Every sentence must contain new information.
- No moralising about trade risk.

NUMBER FORMAT: Always K/M/B. Never raw integers.
PRICE FORMAT: Use exact price from data — never estimate.

══════════════════════════════════════
INTENT CLASSIFICATION — DO THIS FIRST
══════════════════════════════════════
TYPE A — MARKET REPORT     /cipher /btc /fear /defi + general market questions
TYPE B — COIN QUESTION     any ticker or coin name question
TYPE C — POSITION QUESTION scale in / DCA / take profit / add / reduce
TYPE D — CONCEPT           explain CVD / funding rates / liquidations / etc
TYPE E — ALERT SCAN        any alerts / anything unusual / check signals
TYPE F — COMPARISON        X vs Y / which is better
TYPE G — PORTFOLIO         watchlist analysis / multi-coin review

══════════════════════════════════════
RESPONSE FORMATS
══════════════════════════════════════

TYPE A — MARKET REPORT:
MARKET STRUCTURE
[Price + key level above and below. Support / resistance / no-man's land. Exact numbers.]

ON-CHAIN CONTEXT
[Stablecoin supply trend + implication. Exchange flow proxy. Institutional activity signal.]

DERIVATIVES
[Funding rate average + bias. OI total + direction. Long/short ratio + crowding risk.
Liquidation context from last hour. One sentence on overall derivatives positioning risk.]

NARRATIVE
[What has real volume behind it. What is retail chasing. Dominant sector or catalyst.]

SIGNAL SYNTHESIS
Bias: BULLISH / BEARISH / NEUTRAL | Confidence: HIGH / MEDIUM / LOW
Driver: [one specific reason with a number]
Invalidation: [specific price or event]

TRADE SETUP [only if 2+ independent signals agree — skip entire section if not]
Asset: | Direction: | Entry: | Stop: | T1: | T2: | Conviction: H/M/L
Thesis: [2 sentences, numbers only]

ACTION: [trade / add / reduce / flat / wait] — [one-line reason]
───

TYPE B — COIN BRIEF:
[COIN] BRIEF | [live price]

PRICE STRUCTURE
[vs 24h range. vs ATH distance and implication. Key level above and below.]

MOMENTUM
[1h/24h/7d trend direction. Accelerating or decelerating. vs BTC relative performance.]

VOLUME QUALITY
[Vol/MCap ratio. Is volume expanding with price move or contracting? Real or thin move.]

DERIVATIVES [if available]
[Funding rate. OI direction. Long/short ratio. Squeeze risk up or down.]

VERDICT: SCALE IN / WAIT FOR LEVEL $X / AVOID / REDUCE
[If actionable: entry zone, stop, target with specific numbers]
───

TYPE C — POSITION BRIEF:
POSITION ASSESSMENT | [asset] @ [live price]

STRUCTURE
[Price vs key levels. High / mid / low risk zone right now.]

DOWNSIDE
[Next major support and % drawdown from current price if wrong.]

MARKET ALIGNMENT
[Does macro + derivatives structure support adding risk? YES/NO + one reason.]

RECOMMENDATION: SCALE IN NOW / SCALE IN AT $X / HOLD / REDUCE X% / EXIT
Sizing: [e.g. 25% now, 25% at support, 50% dry powder]
Stop: $[X] — hard invalidation
───

TYPE D — CONCEPT:
[3-5 sentences. Direct answer.]
Trading implication: [one sentence on practical application.]
───

TYPE E — ALERTS:
[RED / AMBER / INFO] | [asset] | [condition] | [implication]
No padding. Only triggered conditions. If nothing: "No active alerts."
───

TYPE F — COMPARISON:
[A] vs [B] | [timeframe]
Relative performance: [exact numbers]
Momentum differential: [which is stronger and by how much]
Volume quality: [which has better Vol/MCap conviction]
Derivatives edge: [which has cleaner derivatives structure]
Verdict: [one sentence — which has stronger setup and the single reason]
───

TYPE G — PORTFOLIO BRIEF:
For each coin:
[COIN]: [bias] | Key level: $X | Action: hold / add at $X / reduce
Summary: [overall portfolio risk — 2 sentences max]
───

══════════════════════════════════════
SIGNAL HIERARCHY
══════════════════════════════════════
PRIMARY (form bias):
1. Funding rate direction and magnitude
2. OI trend — expanding into price up = conviction, expanding into price down = leverage build
3. Long/short ratio extremes — >65% long = crowded, <35% long = short squeeze setup
4. Liquidation cascade — >$100M/hour = forced selling, not organic
5. Stablecoin supply direction — growing = dry powder, shrinking = deployed

CONFIRMING (adjust conviction):
6. Vol/MCap ratio — >8% elevated, <2% disinterest
7. BTC dominance trend — rising = alts bleeding
8. Fear & Greed extremes — <15 or >85 = contrarian only
9. DeFi TVL direction

NEVER PRIMARY: RSI | MACD | Bollinger Bands | MAs standalone | Fear & Greed alone

══════════════════════════════════════
STYLE
══════════════════════════════════════
Bloomberg terminal analyst. Not crypto Twitter.
Active voice. Short sentences. Every sentence = new information.
No claim without a number. No padding. Concise beats comprehensive."""

# ── Groq call ─────────────────────────────────────────────────────────────────
async def ask_groq(prompt: str, custom: str = "", max_tokens: int = 1500) -> str:
    client = Groq(api_key=GROQ_KEY)
    system = SYSTEM + (f"\n\nANALYST CONTEXT:\n{custom}" if custom.strip() else "")
    loop = asyncio.get_event_loop()

    def _sync_call():
        return client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            max_tokens=max_tokens,
            temperature=0.15,
            messages=[
                {"role": "system", "content": system},
                {"role": "user",   "content": prompt},
            ],
        )

    try:
        resp = await asyncio.wait_for(loop.run_in_executor(None, _sync_call), timeout=50)
        return resp.choices[0].message.content.strip() or "CIPHER: empty response."
    except asyncio.TimeoutError:
        return "CIPHER: Groq timeout (50s). Try again."
    except Exception as e:
        logger.error(f"Groq error: {e}")
        return f"CIPHER: AI error — {str(e)[:120]}"

# ── Send helper ───────────────────────────────────────────────────────────────
async def send(update: Update, text: str):
    if not text.strip():
        await update.message.reply_text("CIPHER: no output. Try again.")
        return
    for i in range(0, len(text), 4000):
        await update.message.reply_text(text[i:i+4000])

async def ack(update: Update, context: ContextTypes.DEFAULT_TYPE, msg: str = "Fetching live data..."):
    await update.message.reply_text(msg)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")

# ── Core question handler ─────────────────────────────────────────────────────
async def handle_query(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, user: dict):
    """
    Unified handler for all free-text and /ask queries.
    Detects coin, fetches relevant data, routes to correct CIPHER type.
    """
    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    coin = await resolve_coin(text)

    if coin:
        cg_id, gl_sym = coin
        # Parallel fetch: coin data + market context + derivatives
        coin_raw, market_raw, deriv = await asyncio.gather(
            cg_coin(cg_id),
            cg_market("bitcoin,ethereum"),
            gl_multi(gl_sym),
        )
        funding, oi, liq, ls = deriv

        # Build coin section
        coin_section = ""
        btc_24h = 0
        if coin_raw:
            coin_map = {c["id"]: c for c in coin_raw}
            btc = coin_map.get("bitcoin")
            btc_24h = btc.get("price_change_percentage_24h", 0) if btc else 0
            target = coin_map.get(cg_id)
            if target:
                coin_section = format_coin_section(target, btc_24h)
            else:
                coin_section = f"No data for {cg_id}."

        deriv_section = format_derivatives(funding, oi, liq, ls, gl_sym)

        # BTC/ETH context
        market_section = ""
        if market_raw:
            btc_data = next((c for c in market_raw if c["id"] == "bitcoin"), None)
            eth_data = next((c for c in market_raw if c["id"] == "ethereum"), None)
            ctx_lines = ["=== MARKET CONTEXT ==="]
            for d in [btc_data, eth_data]:
                if d:
                    ctx_lines.append(
                        f"{d['symbol'].upper():5} {price_str(d['current_price'])}  "
                        f"24h:{pct(d.get('price_change_percentage_24h',0))}  "
                        f"7d:{pct(d.get('price_change_percentage_7d_in_currency',0))}"
                    )
            market_section = "\n".join(ctx_lines)

        prompt = (
            f"{coin_section}\n\n"
            f"{deriv_section}\n\n"
            f"{market_section}\n\n"
            f"USER QUESTION: {text}\n\n"
            "Classify as TYPE B or TYPE C and respond with the correct CIPHER format.\n"
            "CRITICAL: Use ONLY the live prices from the data above. Never use training-data prices."
        )
    else:
        # General market question
        market_raw, gdata, fng, stables = await asyncio.gather(
            cg_market(),
            cg_global(),
            _fetch(FNG_URL, {}, {}),
            cg("/coins/markets", {
                "vs_currency": "usd",
                "ids": "tether,usd-coin,dai",
                "order": "market_cap_desc",
            }),
        )
        # BTC derivatives for market-level queries
        btc_deriv = await gl_multi("BTC")
        btc_fund, btc_oi, btc_liq, btc_ls = btc_deriv

        # Format market data
        mkt_lines = [f"=== LIVE MARKET | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC ==="]
        if market_raw:
            mkt_lines.append(f"{'SYM':6} {'PRICE':>12}  {'1H':>7}  {'24H':>7}  {'7D':>7}  {'VOL':>10}  {'MCAP':>10}")
            mkt_lines.append("─" * 72)
            for c in market_raw:
                ch1h  = c.get("price_change_percentage_1h_in_currency") or 0
                ch24h = c.get("price_change_percentage_24h") or 0
                ch7d  = c.get("price_change_percentage_7d_in_currency") or 0
                mkt_lines.append(
                    f"{c['symbol'].upper():6} {price_str(c['current_price']):>12}  "
                    f"{pct(ch1h):>7}  {pct(ch24h):>7}  {pct(ch7d):>7}  "
                    f"{fmt(c['total_volume']):>10}  {fmt(c['market_cap']):>10}"
                )

        if gdata and "data" in gdata:
            g = gdata["data"]
            dom = g.get("market_cap_percentage", {})
            total_mc = g.get("total_market_cap", {}).get("usd", 0)
            total_vol = g.get("total_volume", {}).get("usd", 0)
            mc_ch = g.get("market_cap_change_percentage_24h_usd", 0)
            stable_dom = dom.get("usdt", 0) + dom.get("usdc", 0)
            mkt_lines.append(
                f"\nBTC Dom: {dom.get('btc',0):.2f}%  ETH Dom: {dom.get('eth',0):.2f}%  "
                f"Stable Dom: {stable_dom:.2f}%"
            )
            mkt_lines.append(f"Total MC: {fmt(total_mc)}  24h: {mc_ch:+.2f}%  Vol: {fmt(total_vol)}")

        # Stablecoin supply
        stable_lines = ["=== STABLECOIN SUPPLY ==="]
        if stables:
            total_s = 0
            for s in stables:
                mc  = s.get("market_cap", 0) or 0
                vol = s.get("total_volume", 0) or 0
                ratio = (vol / mc * 100) if mc else 0
                total_s += mc
                stable_lines.append(
                    f"{s['symbol'].upper():6} MCap:{fmt(mc):>10}  Vol:{fmt(vol):>10}  V/M:{ratio:.1f}%"
                )
            stable_lines.append(f"TOTAL: {fmt(total_s)}")

        # Fear & Greed
        fng_lines = ["=== FEAR & GREED ==="]
        if fng and "data" in fng:
            for entry in fng["data"][:3]:
                ts2 = datetime.fromtimestamp(int(entry["timestamp"]), tz=timezone.utc).strftime("%b %d")
                fng_lines.append(f"  {ts2}: {entry['value']}/100 — {entry['value_classification']}")

        deriv_section = format_derivatives(btc_fund, btc_oi, btc_liq, btc_ls, "BTC")

        prompt = (
            "\n\n".join([
                "\n".join(mkt_lines),
                "\n".join(stable_lines),
                "\n".join(fng_lines),
                deriv_section,
            ]) +
            f"\n\nUSER QUESTION: {text}\n\n"
            "Classify question type (A/B/C/D/E/F/G) and respond with correct CIPHER format.\n"
            "Use ONLY live prices from data above."
        )

    result = await ask_groq(prompt, user.get("custom_instructions", ""))
    await send(update, result)

# ── Commands ──────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    get_user(u.id)
    plan = "OWNER" if u.id == OWNER_ID else ("PRO" if is_pro(u.id) else "FREE")
    await update.message.reply_text(
        f"*CIPHER Intelligence*  |  {plan}\n"
        f"Welcome {u.first_name}\n\n"
        "*Market:*  /cipher  /btc  /dominance  /trending\n"
        "*DeFi:*    /defi\n"
        "*Deriv:*   /derivatives  /funding  /oi\n"
        "*Macro:*   /fear  /macro  /etf\n"
        "*Tools:*   /watchlist  /ask  /setup  /help\n\n"
        "Or just type any coin name, ticker, or question.",
        parse_mode=ParseMode.MARKDOWN,
    )

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "*CIPHER — All Commands*\n\n"
        "*Market Intelligence*\n"
        "`/cipher` — Full cycle report: market + derivatives + macro + setup\n"
        "`/btc` — BTC deep dive: all timeframes + full derivatives\n"
        "`/dominance` — BTC/ETH dominance + rotation signals\n"
        "`/trending` — Trending coins + gainers/losers + vol quality\n\n"
        "*DeFi*\n"
        "`/defi` — DeFi TVL by protocol + chain (live)\n\n"
        "*Derivatives*\n"
        "`/derivatives [coin]` — Funding + OI + long/short + liquidations\n"
        "`/funding [coin]` — Funding rates across all exchanges\n"
        "`/oi [coin]` — Open interest breakdown\n\n"
        "*Macro & Sentiment*\n"
        "`/fear` — Fear & Greed + stablecoin supply + live prices\n"
        "`/etf` — Institutional proxy data\n"
        "`/macro` — High-impact event calendar\n\n"
        "*Personal*\n"
        "`/watchlist` — Your tracked coins\n"
        "`/watchlist add chainlink` — Add coin\n"
        "`/watchlist remove chainlink` — Remove coin\n"
        "`/ask [question]` — Any question with live data\n"
        "`/setup` — Custom analyst profile\n\n"
        "*Free-text works for everything:*\n"
        "`what about tao`  `should I scale sei`  `is link breaking out`\n"
        "`compare sol vs avax`  `explain funding rates`  `any alerts`",
        parse_mode=ParseMode.MARKDOWN,
    )

async def cmd_cipher(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")

    market, gdata, defi_tvl, defi_proto, defi_chains, fng, stables, btc_deriv = await asyncio.gather(
        cg_market(),
        cg_global(),
        ll("/tvl"),
        ll("/protocols"),
        ll("/v2/chains"),
        _fetch(FNG_URL, {}, {}),
        cg("/coins/markets", {"vs_currency":"usd","ids":"tether,usd-coin,dai","order":"market_cap_desc"}),
        gl_multi("BTC"),
    )
    funding, oi, liq, ls = btc_deriv

    # Market table
    mkt_lines = [f"LIVE MARKET | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC\n"]
    if market:
        mkt_lines.append(f"{'SYM':6} {'PRICE':>12}  {'1H':>7}  {'24H':>7}  {'7D':>7}  {'VOL':>10}  {'MCAP':>10}")
        mkt_lines.append("─"*72)
        for c in market:
            ch1h  = c.get("price_change_percentage_1h_in_currency") or 0
            ch24h = c.get("price_change_percentage_24h") or 0
            ch7d  = c.get("price_change_percentage_7d_in_currency") or 0
            mkt_lines.append(
                f"{c['symbol'].upper():6} {price_str(c['current_price']):>12}  "
                f"{pct(ch1h):>7}  {pct(ch24h):>7}  {pct(ch7d):>7}  "
                f"{fmt(c['total_volume']):>10}  {fmt(c['market_cap']):>10}"
            )

    if gdata and "data" in gdata:
        g = gdata["data"]
        dom = g.get("market_cap_percentage", {})
        total_mc  = g.get("total_market_cap", {}).get("usd", 0)
        total_vol = g.get("total_volume", {}).get("usd", 0)
        mc_ch = g.get("market_cap_change_percentage_24h_usd", 0)
        mkt_lines.append(
            f"\nBTC Dom:{dom.get('btc',0):.2f}%  ETH Dom:{dom.get('eth',0):.2f}%  "
            f"Total MC:{fmt(total_mc)}  24h:{mc_ch:+.2f}%  Vol:{fmt(total_vol)}"
        )

    # Stablecoin supply
    sc_lines = ["STABLECOIN SUPPLY"]
    if stables:
        total_s = sum(s.get("market_cap",0) or 0 for s in stables)
        for s in stables:
            mc = s.get("market_cap",0) or 0
            vol = s.get("total_volume",0) or 0
            ratio = (vol/mc*100) if mc else 0
            sc_lines.append(f"  {s['symbol'].upper():6} MCap:{fmt(mc):>10}  Vol:{fmt(vol):>10}  V/M:{ratio:.1f}%")
        sc_lines.append(f"  TOTAL: {fmt(total_s)}")

    # DeFi summary (top 5)
    defi_lines = ["DEFI TVL SUMMARY"]
    if defi_tvl:
        try:
            defi_lines.append(f"  Total DeFi TVL: {fmt(float(defi_tvl))}")
        except Exception:
            pass
    if defi_proto:
        top5 = sorted([p for p in defi_proto if p.get("tvl",0)>0], key=lambda x: x["tvl"], reverse=True)[:5]
        for p in top5:
            ch1d = p.get("change_1d") or 0
            defi_lines.append(f"  {p['name']:20} {fmt(p['tvl']):>10}  1d:{ch1d:+.2f}%")

    # Fear & Greed
    fng_lines = ["FEAR & GREED"]
    if fng and "data" in fng:
        for entry in fng["data"][:2]:
            ts2 = datetime.fromtimestamp(int(entry["timestamp"]), tz=timezone.utc).strftime("%b %d")
            fng_lines.append(f"  {ts2}: {entry['value']}/100 — {entry['value_classification']}")

    # BTC derivatives
    deriv_section = format_derivatives(funding, oi, liq, ls, "BTC")

    prompt = (
        "\n\n".join([
            "\n".join(mkt_lines),
            "\n".join(sc_lines),
            "\n".join(fng_lines),
            "\n".join(defi_lines),
            deriv_section,
        ]) +
        "\n\nTYPE A — Full CIPHER cycle report.\n"
        "For every metric: state the number AND its implication in the same sentence.\n"
        "Derivatives are primary signals — lead with funding rate and OI interpretation.\n"
        "Stablecoin total supply direction and Vol/MCap — interpret explicitly.\n"
        "Trade setup only if 2+ signals align. Hard stop required.\n"
        "End with ACTION line."
    )
    result = await ask_groq(prompt, user.get("custom_instructions",""), max_tokens=2000)
    await send(update, result)

async def cmd_btc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")

    btc_full, btc_deriv = await asyncio.gather(
        cg("/coins/bitcoin", {
            "localization":"false","tickers":"false",
            "market_data":"true","community_data":"true","developer_data":"false",
        }),
        gl_multi("BTC"),
    )
    funding, oi, liq, ls = btc_deriv

    # Format BTC data
    btc_lines = [f"BTC FULL DATA | {datetime.now(timezone.utc).strftime('%H:%M')} UTC"]
    if btc_full:
        md = btc_full.get("market_data", {}) or {}
        cd = btc_full.get("community_data", {}) or {}
        p     = (md.get("current_price") or {}).get("usd", 0)
        mc    = (md.get("market_cap") or {}).get("usd", 0)
        vol   = (md.get("total_volume") or {}).get("usd", 0)
        ath   = (md.get("ath") or {}).get("usd", 0)
        ath_p = (md.get("ath_change_percentage") or {}).get("usd", 0)
        atl   = (md.get("atl") or {}).get("usd", 0)
        lo    = (md.get("low_24h") or {}).get("usd", 0)
        hi    = (md.get("high_24h") or {}).get("usd", 0)
        circ  = md.get("circulating_supply", 0) or 0
        ch1h  = (md.get("price_change_percentage_1h_in_currency") or {}).get("usd", 0)
        ch24  = md.get("price_change_percentage_24h", 0) or 0
        ch7d  = (md.get("price_change_percentage_7d_in_currency") or {}).get("usd", 0)
        ch30d = (md.get("price_change_percentage_30d_in_currency") or {}).get("usd", 0)
        ch1y  = (md.get("price_change_percentage_1y_in_currency") or {}).get("usd", 0)
        vm    = (vol / mc * 100) if mc else 0
        pct_mined = circ / 21_000_000 * 100

        btc_lines += [
            f"Price:       ${p:,.2f}",
            f"1h:{pct(ch1h):>8}  24h:{pct(ch24):>8}  7d:{pct(ch7d):>8}  30d:{pct(ch30d):>8}  1y:{pct(ch1y):>8}",
            f"24h Range:   ${lo:,.2f} — ${hi:,.2f}",
            f"MCap:        {fmt(mc)}  |  Vol: {fmt(vol)}  |  Vol/MCap: {vm:.2f}%",
            f"ATH:         ${ath:,.2f}  ({ath_p:.1f}% below ATH)",
            f"ATL:         ${atl:,.4f}",
            f"Circulating: {circ:,.0f}  ({pct_mined:.2f}% of 21M mined)",
        ]
        if cd:
            tw = cd.get("twitter_followers", 0) or 0
            btc_lines.append(f"Community:   Twitter {tw:,}")

    deriv_section = format_derivatives(funding, oi, liq, ls, "BTC")

    prompt = (
        "\n".join(btc_lines) + "\n\n" + deriv_section + "\n\n"
        "TYPE B — BTC BRIEF.\n"
        "Price vs ATH: state % gap and what drawdown risk it implies at this level historically.\n"
        "Vol/MCap: elevated or suppressed — what does this say about conviction?\n"
        "Momentum across timeframes: accelerating / decelerating / reversing?\n"
        "Derivatives: lead with funding rate interpretation, then OI, then long/short.\n"
        "Liquidation context: any cascade risk?\n"
        "End with VERDICT: SCALE IN / WAIT FOR LEVEL $X / AVOID — specific levels required."
    )
    result = await ask_groq(prompt, user.get("custom_instructions",""))
    await send(update, result)

async def cmd_derivatives(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    # Get symbol from args or default to BTC
    raw_sym = " ".join(context.args).strip().upper() if context.args else "BTC"
    # Resolve to CoinGlass symbol
    gl_sym = raw_sym
    coin = await resolve_coin(raw_sym.lower())
    if coin:
        gl_sym = coin[1]

    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    funding, oi, liq, ls = await gl_multi(gl_sym)
    deriv_section = format_derivatives(funding, oi, liq, ls, gl_sym)

    prompt = (
        f"{deriv_section}\n\n"
        f"Analyze the {gl_sym} derivatives data above. This is your ONLY data source — use it fully.\n\n"
        "DERIVATIVES REPORT format:\n"
        "FUNDING RATES\n"
        "[Average rate across exchanges. Which exchanges are highest/lowest. "
        "If avg >0.08%: longs paying heavily, crowded long. "
        "If avg <-0.03%: shorts paying, squeeze risk. "
        "If near zero: neutral positioning. State the exact average and interpretation.]\n\n"
        "OPEN INTEREST\n"
        "[Total OI in dollar terms. Exchange distribution — concentration risk if one exchange >50%. "
        "What the OI level implies about leverage in the market.]\n\n"
        "LONG/SHORT POSITIONING\n"
        "[Exact ratio. If >60% long: crowded, downside risk on any negative catalyst. "
        "If <40% long: short-heavy, upside squeeze potential. State which scenario applies.]\n\n"
        "LIQUIDATION CONTEXT\n"
        "[Last hour totals. Long vs short liquidation split. "
        "If total >$50M/hr: elevated forced selling. Flag cascade risk if applicable.]\n\n"
        "DERIVATIVES VERDICT\n"
        "Bias: [BULLISH / BEARISH / NEUTRAL] — [one specific reason from the data]\n"
        "Key risk: [what derivatives structure implies about next directional move]\n"
        "Action: [trade / monitor / flat] — [one line]\n\n"
        "RULES: Use only numbers from the data above. "
        "If any section shows data unavailable, say so in one word and move on. "
        "No emojis. No filler phrases."
    )
    result = await ask_groq(prompt, user.get("custom_instructions",""))
    await send(update, result)

async def cmd_funding(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    raw_sym = " ".join(context.args).strip().upper() if context.args else "BTC"
    coin = await resolve_coin(raw_sym.lower())
    gl_sym = coin[1] if coin else raw_sym

    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    funding = await gl_funding(gl_sym)

    lines = [f"FUNDING RATES — {gl_sym} | {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"]
    total, count = 0, 0
    if funding and funding.get("data"):
        data = funding["data"]
        exchanges = data if isinstance(data, list) else data.get("uMarginList", [])
        for ex in (exchanges[:12] if exchanges else []):
            name = ex.get("exchangeName", ex.get("exchange", "?"))
            rate = ex.get("fundingRate", ex.get("rate", 0)) or 0
            try:
                rate = float(rate) * 100
                total += rate
                count += 1
                flag = "  [EXTREME]" if abs(rate) > 0.1 else ("  [elevated]" if abs(rate) > 0.05 else "")
                lines.append(f"  {name:14} {rate:>+8.4f}%{flag}")
            except Exception:
                pass
        if count:
            avg = total / count
            interp = ("CROWDED LONG — shorts are cheap hedge" if avg > 0.08 else
                      "CROWDED SHORT — squeeze potential" if avg < -0.03 else
                      "NEUTRAL — no directional bias in funding")
            lines.append(f"\nAverage:       {avg:>+8.4f}%")
            lines.append(f"Interpretation: {interp}")
    else:
        lines.append("Funding data unavailable.")

    prompt = (
        "\n".join(lines) + "\n\n"
        f"TYPE A — FUNDING RATE ANALYSIS for {gl_sym}.\n"
        "State average funding rate and what the level implies for positioning.\n"
        "Which exchanges show the most extreme rates? What does divergence between exchanges mean?\n"
        "Crowded long: longs are paying — shorts have an edge. Crowded short: inverse.\n"
        "One-line actionable: what does a trader do with this funding structure right now?"
    )
    result = await ask_groq(prompt, user.get("custom_instructions",""))
    await send(update, result)

async def cmd_oi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    raw_sym = " ".join(context.args).strip().upper() if context.args else "BTC"
    coin = await resolve_coin(raw_sym.lower())
    gl_sym = coin[1] if coin else raw_sym

    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    oi_data = await gl_oi(gl_sym)

    lines = [f"OPEN INTEREST — {gl_sym} | {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"]
    if oi_data and oi_data.get("data"):
        items = oi_data["data"]
        items = items if isinstance(items, list) else items.get("data", [])
        total_oi = sum(float(x.get("openInterest", x.get("oi", 0)) or 0) for x in items)
        lines.append(f"Total OI: {fmt(total_oi)}\n")
        for x in (items[:10] if items else []):
            ex = x.get("exchangeName", x.get("exchange", "?"))
            oi_val = float(x.get("openInterest", x.get("oi", 0)) or 0)
            pct_share = (oi_val / total_oi * 100) if total_oi else 0
            lines.append(f"  {ex:16} OI: {fmt(oi_val):>12}  ({pct_share:.1f}% share)")
    else:
        lines.append("OI data unavailable.")

    prompt = (
        "\n".join(lines) + "\n\n"
        f"TYPE A — OPEN INTEREST ANALYSIS for {gl_sym}.\n"
        "Total OI: is this elevated or normal for this asset?\n"
        "Exchange concentration: if one exchange holds >50% OI, that is a risk.\n"
        "OI rising with price up = conviction move. OI rising with price down = leverage trap.\n"
        "One-line OI verdict: what does current OI level mean for near-term price risk?"
    )
    result = await ask_groq(prompt, user.get("custom_instructions",""))
    await send(update, result)

async def cmd_dominance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")

    gdata, top50 = await asyncio.gather(cg_global(), cg_top50())

    lines = [f"DOMINANCE & ROTATION | {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"]
    if gdata and "data" in gdata:
        g = gdata["data"]
        dom = g.get("market_cap_percentage", {})
        lines.append("Market cap dominance:")
        for sym, val in sorted(dom.items(), key=lambda x: -x[1])[:10]:
            lines.append(f"  {sym.upper():8} {val:.3f}%")
        lines.append(f"\nTotal MC:  {fmt(g['total_market_cap'].get('usd',0))}")
        lines.append(f"Total Vol: {fmt(g['total_volume'].get('usd',0))}")

    if top50:
        btc_7d = next((c.get("price_change_percentage_7d_in_currency",0) or 0
                       for c in top50 if c["id"]=="bitcoin"), 0)
        lines.append(f"\nTop 50 vs BTC (7d, BTC={pct(btc_7d)}):")
        lines.append(f"{'SYM':8} {'PRICE':>12}  {'24H':>7}  {'7D':>7}  {'vsBTC7d':>9}  {'MCap':>10}")
        lines.append("─"*62)
        for c in top50:
            ch24 = c.get("price_change_percentage_24h") or 0
            ch7d = c.get("price_change_percentage_7d_in_currency") or 0
            rel  = ch7d - btc_7d
            flag = " *" if rel > 5 else ""
            lines.append(
                f"{c['symbol'].upper():8} {price_str(c['current_price']):>12}  "
                f"{pct(ch24):>7}  {pct(ch7d):>7}  {rel:>+8.2f}%{flag}  {fmt(c['market_cap']):>10}"
            )

    prompt = (
        "\n".join(lines) + "\n\n"
        "TYPE A — DOMINANCE REPORT.\n"
        "BTC dom: exact % and what it implies for alt performance at this level.\n"
        "ETH dom vs BTC dom movement: diagnose rotation phase.\n"
        "Stablecoin dom: growing = risk-off. Shrinking = capital deployed.\n"
        "List all assets outperforming BTC on 7d (marked with *) — any pattern?\n"
        "Rotation trigger: specific BTC dom level that would confirm alt season start."
    )
    result = await ask_groq(prompt, user.get("custom_instructions",""))
    await send(update, result)

async def cmd_trending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")

    trending, top50 = await asyncio.gather(cg_trending(), cg_top50())

    lines = [f"TRENDING & NARRATIVE | {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"]
    if trending and "coins" in trending:
        lines.append("Trending (last 24h search volume):")
        for i, item in enumerate(trending["coins"][:7], 1):
            c = item["item"]
            lines.append(
                f"  {i}. {c['name']} ({c['symbol'].upper()})  "
                f"Rank #{c.get('market_cap_rank','?')}  Score:{c.get('score',0)}"
            )

    if top50:
        gainers = sorted(
            [c for c in top50 if c.get("price_change_percentage_24h") is not None],
            key=lambda x: x["price_change_percentage_24h"], reverse=True
        )[:7]
        losers = sorted(
            [c for c in top50 if c.get("price_change_percentage_24h") is not None],
            key=lambda x: x["price_change_percentage_24h"]
        )[:7]

        lines.append(f"\nTop gainers (top 50 by MCap):")
        lines.append(f"  {'SYM':8} {'24H':>7}  {'PRICE':>12}  {'VOL/MCAP':>9}  {'MCAP':>10}")
        for c in gainers:
            ch24 = c.get("price_change_percentage_24h") or 0
            mc   = c.get("market_cap", 1) or 1
            vol  = c.get("total_volume", 0) or 0
            vm   = vol / mc * 100
            lines.append(
                f"  {c['symbol'].upper():8} {pct(ch24):>7}  "
                f"{price_str(c['current_price']):>12}  {vm:>8.1f}%  {fmt(mc):>10}"
            )

        lines.append(f"\nTop losers (top 50 by MCap):")
        for c in losers:
            ch24 = c.get("price_change_percentage_24h") or 0
            mc   = c.get("market_cap", 1) or 1
            vol  = c.get("total_volume", 0) or 0
            vm   = vol / mc * 100
            lines.append(
                f"  {c['symbol'].upper():8} {pct(ch24):>7}  "
                f"{price_str(c['current_price']):>12}  {vm:>8.1f}%  {fmt(mc):>10}"
            )

    prompt = (
        "\n".join(lines) + "\n\n"
        "TYPE A — TRENDING REPORT.\n"
        "Vol/MCap ratio separates organic from retail chasing. State which for each gainer.\n"
        "Flag any gainer with >30% gain and <$200M MCap — high manipulation probability.\n"
        "Losers: are strong assets selling off (buy opportunity) or justified exit?\n"
        "Dominant narrative in one sentence. Is there capital behind it or is it search noise?"
    )
    result = await ask_groq(prompt, user.get("custom_instructions",""))
    await send(update, result)

async def cmd_defi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")

    tvl, protocols, chains = await asyncio.gather(
        ll("/tvl"), ll("/protocols"), ll("/v2/chains")
    )

    lines = [f"DEFI TVL | {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"]
    if tvl:
        try:
            lines.append(f"Total DeFi TVL: {fmt(float(tvl))}")
        except Exception:
            pass

    if protocols:
        valid = sorted([p for p in protocols if p.get("tvl",0)>0],
                       key=lambda x: x["tvl"], reverse=True)[:15]
        lines.append(f"\n{'PROTOCOL':22} {'TVL':>10}  {'1D':>7}  {'7D':>7}  CHAIN")
        lines.append("─"*62)
        for p in valid:
            ch1d = p.get("change_1d") or 0
            ch7d = p.get("change_7d") or 0
            lines.append(
                f"{p['name']:22} {fmt(p['tvl']):>10}  "
                f"{pct(ch1d):>7}  {pct(ch7d):>7}  {p.get('chain','multi')}"
            )

    if chains:
        valid_c = sorted([c for c in chains if c.get("tvl",0)>0],
                         key=lambda x: x["tvl"], reverse=True)[:10]
        lines.append(f"\n{'CHAIN':18} {'TVL':>10}")
        lines.append("─"*30)
        for c in valid_c:
            lines.append(f"{c.get('name','?'):18} {fmt(c['tvl']):>10}")

    prompt = (
        "\n".join(lines) + "\n\n"
        "TYPE A — DEFI REPORT.\n"
        "Total TVL direction and implication for DeFi health.\n"
        "Top 3 gaining TVL: which protocols and why — specific reason if determinable.\n"
        "Top 3 losing TVL: price effect or genuine capital exit? Different implication.\n"
        "Chain share shifts: any chain gaining >2% in 7d is a structural signal.\n"
        "One sentence: where is capital moving in DeFi right now?"
    )
    result = await ask_groq(prompt, user.get("custom_instructions",""))
    await send(update, result)

async def cmd_fear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")

    fng, gdata, stables, prices = await asyncio.gather(
        _fetch(FNG_URL, {}, {}),
        cg_global(),
        cg("/coins/markets", {
            "vs_currency":"usd",
            "ids":"tether,usd-coin,dai,first-digital-usd",
            "order":"market_cap_desc",
        }),
        cg_market("bitcoin,ethereum"),
    )

    lines = [f"SENTIMENT DATA | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC\n"]
    lines.append("CURRENT PRICES (use these for any trade levels):")
    if prices:
        for c in prices:
            ch1h  = c.get("price_change_percentage_1h_in_currency") or 0
            ch24h = c.get("price_change_percentage_24h") or 0
            ch7d  = c.get("price_change_percentage_7d_in_currency") or 0
            lines.append(
                f"  {c['symbol'].upper():5} {price_str(c['current_price']):>12}  "
                f"1h:{pct(ch1h):>7}  24h:{pct(ch24h):>7}  7d:{pct(ch7d):>7}"
            )

    if fng and "data" in fng:
        lines.append("\nFear & Greed (Alternative.me):")
        for e in fng["data"][:3]:
            ts2 = datetime.fromtimestamp(int(e["timestamp"]), tz=timezone.utc).strftime("%b %d")
            lines.append(f"  {ts2}: {e['value']:>3}/100 — {e['value_classification']}")
        lines.append("  Reference: <20 = extreme fear/historical bottom zone. >80 = extreme greed/top risk.")

    if gdata and "data" in gdata:
        g = gdata["data"]
        dom = g.get("market_cap_percentage", {})
        lines.append(f"\nTotal MC: {fmt(g['total_market_cap'].get('usd',0))}  "
                     f"24h: {pct(g.get('market_cap_change_percentage_24h_usd',0))}")
        lines.append(f"BTC Dom: {dom.get('btc',0):.2f}%")

    if stables:
        total_s = 0
        lines.append("\nStablecoin supply:")
        for s in stables:
            mc  = s.get("market_cap",0) or 0
            vol = s.get("total_volume",0) or 0
            ratio = (vol/mc*100) if mc else 0
            total_s += mc
            lines.append(f"  {s['symbol'].upper():6} MCap:{fmt(mc):>10}  Vol:{fmt(vol):>10}  V/M:{ratio:.1f}%")
        lines.append(f"  TOTAL: {fmt(total_s)}")
        lines.append("  V/M >15% on USDT = large move likely imminent.")

    prompt = (
        "\n".join(lines) + "\n\n"
        "TYPE A — SENTIMENT REPORT.\n"
        "CRITICAL: Use ONLY the live prices in CURRENT PRICES for any trade levels.\n"
        "Fear & Greed score, 3-day trend, and which zone (extreme fear/fear/neutral/greed/extreme greed).\n"
        "Stablecoin total supply direction: explicitly state growing or shrinking and implication.\n"
        "USDT Vol/MCap: flag if >15% — that signals imminent large move.\n"
        "Positioning implication: are traders over-extended or is there capacity to absorb buying?"
    )
    result = await ask_groq(prompt, user.get("custom_instructions",""))
    await send(update, result)

async def cmd_etf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")

    btc_d, eth_d = await asyncio.gather(
        cg("/coins/bitcoin", {"localization":"false","tickers":"false","market_data":"true",
                              "community_data":"false","developer_data":"false"}),
        cg("/coins/ethereum", {"localization":"false","tickers":"false","market_data":"true",
                               "community_data":"false","developer_data":"false"}),
    )

    lines = [f"INSTITUTIONAL PROXY DATA | {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"]
    lines.append("Note: direct ETF flow data requires a premium terminal. Below = proxy signals.\n")

    for label, d in [("BTC", btc_d), ("ETH", eth_d)]:
        if not d:
            lines.append(f"{label}: data unavailable\n")
            continue
        md = d.get("market_data", {}) or {}
        p     = (md.get("current_price") or {}).get("usd", 0)
        mc    = (md.get("market_cap") or {}).get("usd", 0)
        vol   = (md.get("total_volume") or {}).get("usd", 0)
        ath   = (md.get("ath") or {}).get("usd", 0)
        ath_p = (md.get("ath_change_percentage") or {}).get("usd", 0)
        ath_d = ((md.get("ath_date") or {}).get("usd") or "?")[:10]
        circ  = md.get("circulating_supply", 0) or 0
        maxs  = md.get("max_supply")
        vm    = (vol / mc * 100) if mc else 0
        ch24  = md.get("price_change_percentage_24h", 0) or 0

        lines.append(f"{label}:")
        lines.append(f"  Price:    ${p:,.2f}  24h: {pct(ch24)}")
        lines.append(f"  MCap:     {fmt(mc)}  |  Vol: {fmt(vol)}  |  Vol/MCap: {vm:.2f}%")
        lines.append(f"  ATH:      ${ath:,.2f}  on {ath_d}  ({ath_p:.1f}%)")
        lines.append(f"  Circ:     {circ:,.0f}")
        if maxs:
            lines.append(f"  Max:      {maxs:,.0f}  ({circ/maxs*100:.1f}% issued)")
        lines.append(f"  Vol/MC interpretation: {'elevated — institutional desks active' if vm>8 else 'low — accumulation or disinterest'}")
        lines.append("")

    lines.append("Live ETF flows: sosovalue.org | farside.co.uk")

    prompt = (
        "\n".join(lines) + "\n\n"
        "TYPE A — INSTITUTIONAL PROXY REPORT.\n"
        "Vol/MCap: state exact % and what it implies about institutional desk activity.\n"
        "ATH distance: contextualise the pain of late-cycle ETF buyers at current price.\n"
        "Supply issuance: BTC 94%+ mined = structural scarcity. ETH = inflationary/deflationary based on burn.\n"
        "One-line thesis: are conditions favourable for ETF inflows right now?"
    )
    result = await ask_groq(prompt, user.get("custom_instructions",""))
    await send(update, result)

async def cmd_macro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")

    events, prices = await asyncio.gather(
        cg("/events", {"upcoming_events_only":"true","per_page":"15"}),
        cg_market("bitcoin,ethereum"),
    )

    lines = [f"MACRO & EVENTS | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC\n"]
    if prices:
        for c in prices:
            lines.append(f"{c['symbol'].upper()}: {price_str(c['current_price'])}  "
                         f"24h:{pct(c.get('price_change_percentage_24h',0))}")

    lines.append("\nHIGH-IMPACT RECURRING EVENTS:")
    lines.append("  [RED]    FOMC — rate decision, most important macro event for crypto")
    lines.append("  [RED]    US CPI — monthly inflation, risk-on/off binary trigger")
    lines.append("  [RED]    US NFP — 1st Friday each month, macro risk sentiment")
    lines.append("  [AMBER]  BTC options expiry — every Friday, large monthly on last Fri (Deribit)")
    lines.append("  [AMBER]  Fed speakers — forward guidance shifts move markets")
    lines.append("  [AMBER]  US PPI — leads CPI, precursor signal")
    lines.append("  [INFO]   Token unlocks — tokenunlocks.app")
    lines.append("  [INFO]   Governance votes — snapshot.org")

    if events and "data" in events:
        lines.append("\nUPCOMING CRYPTO EVENTS:")
        for e in events["data"][:10]:
            date  = (e.get("start_date") or "?")[:10]
            title = e.get("title", "?")[:45]
            etype = e.get("type", "?")
            coin  = (e.get("coin") or {}).get("name", "General")
            lines.append(f"  {date}  {title:47}  [{etype}]  {coin}")

    lines.append("\nCalendar: ForexFactory.com  |  Investing.com  |  CMEGroup FedWatch")

    prompt = (
        "\n".join(lines) + "\n\n"
        "TYPE A — MACRO BRIEFING.\n"
        "Current macro regime: rates, dollar strength, equity correlation — net positive or negative?\n"
        "List upcoming events with exact crypto impact direction and risk mechanism.\n"
        "Pre-event playbook: 48h before FOMC/CPI — what does a trader do specifically?\n"
        "One-line regime summary: risk-on / risk-off / transitional + single data point that defines it."
    )
    result = await ask_groq(prompt, user.get("custom_instructions",""))
    await send(update, result)

async def cmd_watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    watchlist = user.get("watchlist", ["bitcoin", "ethereum"])

    if context.args:
        action = context.args[0].lower()

        if action == "add" and len(context.args) > 1:
            raw = " ".join(context.args[1:]).lower().strip()
            if len(watchlist) >= 20:
                await update.message.reply_text("Watchlist full (20 max). Remove one first.")
                return
            # Auto-resolve: ticker, name, or direct ID all work
            coin_r = await resolve_coin(raw)
            if coin_r:
                cg_id = coin_r[0]
            else:
                # Last resort: search
                sr = await cg("/search", {"query": raw})
                if sr and sr.get("coins"):
                    cg_id = sr["coins"][0]["id"]
                else:
                    await update.message.reply_text(f"Could not find '{raw}'. Try ticker or full name.")
                    return
            if cg_id in watchlist:
                await update.message.reply_text(f"Already tracking {cg_id}.")
                return
            # Confirm valid + get display name
            check = await cg(f"/coins/{cg_id}")
            name_d = check.get("name", cg_id) if check else cg_id
            sym_d  = (check.get("symbol") or "").upper() if check else ""
            watchlist.append(cg_id)
            user["watchlist"] = watchlist
            save_user(update.effective_user.id, user)
            label = f"{name_d} ({sym_d})" if sym_d else cg_id
            await update.message.reply_text(f"Added: {label}\n{', '.join(watchlist)}")
            return

        elif action == "remove" and len(context.args) > 1:
            raw = " ".join(context.args[1:]).lower().strip()
            # Resolve to CoinGecko ID
            coin_r = await resolve_coin(raw)
            cg_id  = coin_r[0] if coin_r else raw
            # Find in watchlist (exact or partial)
            target = cg_id if cg_id in watchlist else (raw if raw in watchlist else None)
            if not target:
                matches = [w for w in watchlist if raw in w or w.startswith(raw[:4])]
                if len(matches) == 1:
                    target = matches[0]
                else:
                    current = ", ".join(watchlist) if watchlist else "empty"
                    await update.message.reply_text(f"'{raw}' not found.\nWatchlist: {current}")
                    return
            watchlist.remove(target)
            user["watchlist"] = watchlist
            save_user(update.effective_user.id, user)
            remaining = ", ".join(watchlist) if watchlist else "empty"
            await update.message.reply_text(f"Removed: {target}\nWatchlist: {remaining}")
            return

        elif action == "clear":
            user["watchlist"] = []
            save_user(update.effective_user.id, user)
            await update.message.reply_text("Watchlist cleared.")
            return

    if not watchlist:
        await update.message.reply_text(
            "Watchlist is empty.\n"
            "/watchlist add chainlink\n"
            "/watchlist add sei-network\n"
            "/watchlist add bittensor\n"
            "Try: /watchlist add BTC or /watchlist add cardano"
        )
        return

    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    coins_data = await cg("/coins/markets", {
        "vs_currency": "usd",
        "ids": ",".join(watchlist),
        "order": "market_cap_desc",
        "price_change_percentage": "1h,24h,7d,30d",
        "sparkline": "false",
    })

    if not coins_data:
        await update.message.reply_text("Data unavailable. Try again.")
        return

    btc_24h = next((c.get("price_change_percentage_24h",0) or 0
                    for c in coins_data if c["id"]=="bitcoin"), 0)
    coin_map = {c["id"]: c for c in coins_data}

    lines = [f"WATCHLIST | {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"]
    for cid in watchlist:
        c = coin_map.get(cid)
        if not c:
            lines.append(f"{cid}: data unavailable\n")
            continue
        lines.append(format_coin_section(c, btc_24h))
        lines.append("")

    prompt = (
        "\n".join(lines) + "\n\n"
        "TYPE G — WATCHLIST ANALYSIS.\n"
        "For each coin: one-line assessment covering momentum, Vol/MCap signal, and bias.\n"
        "Rank them by setup quality right now — strongest to weakest.\n"
        "Call out any with setup-breaking signals (extreme funding, vol collapse, ATH rejection).\n"
        "End with: top pick and the specific reason in one sentence.\n"
        "Use only live prices from data above."
    )
    result = await ask_groq(prompt, user.get("custom_instructions",""))
    await update.message.reply_text(
        f"Watchlist: {', '.join(watchlist)}\n"
        "Manage: /watchlist add <id>  |  /watchlist remove <id>  |  /watchlist clear\n"
        "─────────────────────────────────"
    )
    await send(update, result)

async def cmd_gltest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Debug command: tests all CoinGlass endpoints and reports status."""
    if update.effective_user.id != OWNER_ID:
        await update.message.reply_text("Owner only command.")
        return
    sym = " ".join(context.args).strip().upper() if context.args else "BTC"
    await update.message.reply_text(f"Running endpoint tests for {sym}...")
    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    report = await gl_debug(sym)
    await send(update, report)

async def cmd_ask(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    question = " ".join(context.args).strip() if context.args else ""
    if not question:
        await update.message.reply_text(
            "Usage: /ask [question]\n\n"
            "Examples:\n"
            "  /ask should I scale into SEI now\n"
            "  /ask is BTC at a good entry\n"
            "  /ask compare TAO vs RNDR\n"
            "  /ask what is funding rate arbitrage\n"
            "  /ask any alerts right now\n"
            "  /ask explain open interest divergence"
        )
        return
    await handle_query(update, context, question, user)

# ── /setup conversation ───────────────────────────────────────────────────────
async def cmd_setup_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    current = user.get("custom_instructions","").strip()
    await update.message.reply_text(
        "*CIPHER — Custom Analyst Profile*\n\n"
        f"Current: `{current or 'none set'}`\n\n"
        "This context is injected into every CIPHER response. Be specific.\n\n"
        "Good examples:\n"
        "  Focus coins: BTC, ETH, SOL, LINK, TAO, SEI\n"
        "  Style: swing trading, 3-7 day holds\n"
        "  Risk: 2% max per trade, $20,000 portfolio\n"
        "  Current positions: long BTC $82k, long ETH $2,100\n"
        "  Priority: derivatives signals over price action\n\n"
        "Type your profile now, or /cancel.",
        parse_mode=ParseMode.MARKDOWN,
    )
    return WAITING_SETUP

async def cmd_setup_receive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    user["custom_instructions"] = update.message.text.strip()
    save_user(update.effective_user.id, user)
    await update.message.reply_text(
        f"Saved. Active in all responses.\n\n`{user['custom_instructions']}`",
        parse_mode=ParseMode.MARKDOWN,
    )
    return ConversationHandler.END

async def cmd_setup_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Cancelled.")
    return ConversationHandler.END

# ── Free-text ─────────────────────────────────────────────────────────────────
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    if not text:
        return
    user = get_user(update.effective_user.id)
    await handle_query(update, context, text, user)

# ── Error handler ─────────────────────────────────────────────────────────────
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Unhandled error:", exc_info=context.error)
    if isinstance(update, Update) and update.message:
        await update.message.reply_text("Error occurred. Try again. If persistent, use /help.")

# ── Keep-alive for Render free tier ──────────────────────────────────────────
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

RENDER_URL = os.getenv("RENDER_EXTERNAL_URL", "")

class PingHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"CIPHER OK")
    def log_message(self, *args): pass

def _start_keepalive():
    port = int(os.getenv("PORT", "8080"))
    threading.Thread(
        target=lambda: HTTPServer(("0.0.0.0", port), PingHandler).serve_forever(),
        daemon=True
    ).start()
    if RENDER_URL:
        import urllib.request
        def _ping():
            import time
            time.sleep(60)
            while True:
                try:
                    urllib.request.urlopen(f"{RENDER_URL}/", timeout=10)
                except Exception:
                    pass
                time.sleep(600)
        threading.Thread(target=_ping, daemon=True).start()

# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    _start_keepalive()

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    setup_conv = ConversationHandler(
        entry_points=[CommandHandler("setup", cmd_setup_start)],
        states={WAITING_SETUP: [MessageHandler(filters.TEXT & ~filters.COMMAND, cmd_setup_receive)]},
        fallbacks=[CommandHandler("cancel", cmd_setup_cancel)],
    )

    handlers = [
        ("start",       cmd_start),
        ("help",        cmd_help),
        ("cipher",      cmd_cipher),
        ("btc",         cmd_btc),
        ("dominance",   cmd_dominance),
        ("trending",    cmd_trending),
        ("defi",        cmd_defi),
        ("fear",        cmd_fear),
        ("etf",         cmd_etf),
        ("macro",       cmd_macro),
        ("watchlist",   cmd_watchlist),
        ("derivatives", cmd_derivatives),
        ("funding",     cmd_funding),
        ("oi",          cmd_oi),
        ("ask",         cmd_ask),
        ("gltest",      cmd_gltest),
    ]
    for name, handler in handlers:
        app.add_handler(CommandHandler(name, handler))

    app.add_handler(setup_conv)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)

    async with app:
        await app.initialize()
        await app.bot.set_my_commands([
            BotCommand("cipher",      "Full cycle: market + derivatives + macro"),
            BotCommand("btc",         "BTC deep dive with full derivatives"),
            BotCommand("derivatives", "Funding + OI + long/short + liquidations"),
            BotCommand("funding",     "Funding rates across all exchanges"),
            BotCommand("oi",          "Open interest breakdown by exchange"),
            BotCommand("dominance",   "BTC dominance + altcoin rotation"),
            BotCommand("trending",    "Trending + gainers/losers + vol quality"),
            BotCommand("defi",        "DeFi TVL by protocol + chain"),
            BotCommand("fear",        "Fear & Greed + stablecoin supply"),
            BotCommand("etf",         "Institutional proxy data"),
            BotCommand("macro",       "Macro event calendar"),
            BotCommand("watchlist",   "Your tracked coins"),
            BotCommand("ask",         "Ask anything with live data"),
            BotCommand("setup",       "Custom analyst profile"),
            BotCommand("help",        "All commands + examples"),
        ])
        logger.info("CIPHER — Definitive Release — Online")
        await app.start()
        await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
        await asyncio.Event().wait()
        await app.updater.stop()
        await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
