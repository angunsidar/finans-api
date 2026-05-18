"""
Kripto para endpoint'leri.
CoinGecko public API üzerinden (API key gerekmez).

Cache mimarisi:
  _coin_cache / _coin_stale → coin_id bazında saklanır (örn. "bitcoin", "ethereum")
  → Hangi kombinasyonda istenirse istensin, warm-up / keep-alive / başka kullanıcı
    tarafından önceden çekilmiş veriler servis edilir.
  → Flutter'ın portfolio'su değişse bile stale verisi mevcut olur.
"""
from __future__ import annotations

import time
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/kripto", tags=["kripto"])

COINGECKO_BASE = "https://api.coingecko.com/api/v3"
_COIN_TTL = 300  # 5 dakika

# Per-coin cache: coin_id → (timestamp, raw_price_dict)
# raw_price_dict = {"try": 3000000, "usd": 90000, "try_24h_change": 1.5, ...}
_coin_cache: dict[str, tuple[float, dict]] = {}
_coin_stale: dict[str, dict] = {}

# Diğer endpoint'ler için genel cache (piyasa listesi, trend, geçmiş)
_gen_cache: dict[str, tuple[float, object]] = {}
_gen_stale: dict[str, object] = {}


# Kısa sembol → CoinGecko id eşleştirmesi
COIN_IDS: dict[str, str] = {
    "BTC":   "bitcoin",
    "ETH":   "ethereum",
    "BNB":   "binancecoin",
    "SOL":   "solana",
    "XRP":   "ripple",
    "ADA":   "cardano",
    "AVAX":  "avalanche-2",
    "DOT":   "polkadot",
    "LINK":  "chainlink",
    "LTC":   "litecoin",
    "DOGE":  "dogecoin",
    "MATIC": "matic-network",
    "UNI":   "uniswap",
    "ATOM":  "cosmos",
    "NEAR":  "near",
    "FTM":   "fantom",
    "TRX":   "tron",
    "SHIB":  "shiba-inu",
    "USDT":  "tether",
    "USDC":  "usd-coin",
}

# Warm-up sırasında ısıtılacak coin listesi (en popüler 10)
WARM_COINS = [
    "bitcoin", "ethereum", "solana", "ripple", "tether",
    "binancecoin", "dogecoin", "cardano", "avalanche-2", "chainlink",
]


def _resolve_id(coin: str) -> str:
    """Sembol (BTC) veya CoinGecko id (bitcoin) çözümle."""
    upper = coin.upper()
    if upper in COIN_IDS:
        return COIN_IDS[upper]
    return coin.lower()


def _fetch_coins_from_api(ids: list[str], vs: str) -> dict[str, dict]:
    """CoinGecko'dan coin listesi çek. Sonuç: {coin_id: raw_dict}"""
    url = f"{COINGECKO_BASE}/simple/price"
    params = {
        "ids": ",".join(ids),
        "vs_currencies": vs,
        "include_24hr_change": "true",
        "include_24hr_vol": "true",
        "include_market_cap": "true",
        "include_last_updated_at": "true",
    }
    with httpx.Client(timeout=15) as client:
        r = client.get(url, params=params)

    if r.status_code == 429:
        raise HTTPException(429, "CoinGecko rate limit aşıldı, lütfen bekleyiniz.")
    if r.status_code == 404:
        raise HTTPException(404, "Kripto para bulunamadı.")
    r.raise_for_status()
    return r.json()  # {coin_id: {"usd": ..., "try": ..., ...}}


def _get_coin_price(coin_id: str, vs: str = "usd,try") -> dict:
    """
    Tek coin fiyatını per-coin cache üzerinden al.
    Hata varsa stale döner; stale da yoksa hata fırlatır.
    """
    now = time.time()
    if coin_id in _coin_cache:
        ts, data = _coin_cache[coin_id]
        if now - ts < _COIN_TTL:
            return data

    # TTL doldu veya hiç çekilmedi — taze veri dene
    try:
        result = _fetch_coins_from_api([coin_id], vs)
        if coin_id in result:
            _coin_cache[coin_id] = (now, result[coin_id])
            _coin_stale[coin_id] = result[coin_id]
            return result[coin_id]
        raise HTTPException(404, f"Kripto para bulunamadı: {coin_id}")
    except HTTPException:
        if coin_id in _coin_stale:
            return _coin_stale[coin_id]
        raise
    except Exception as e:
        if coin_id in _coin_stale:
            return _coin_stale[coin_id]
        raise HTTPException(503, f"CoinGecko erişilemiyor: {e}")


def _get_coins_bulk(coin_ids: list[str], vs: str = "usd,try") -> dict[str, dict]:
    """
    Çoklu coin fiyatlarını per-coin cache üzerinden al.
    - Cache'te taze olanlar direkt döner
    - Cache'i dolmuş veya eksik olanlar tek bir API çağrısıyla çekilir
    - API hatası durumunda stale verisi kullanılır
    Sonuç: {coin_id: raw_dict}  (bulunamayanlar eksik olur)
    """
    now = time.time()
    result: dict[str, dict] = {}
    to_fetch: list[str] = []

    for coin_id in coin_ids:
        if coin_id in _coin_cache:
            ts, data = _coin_cache[coin_id]
            if now - ts < _COIN_TTL:
                result[coin_id] = data
                continue
        to_fetch.append(coin_id)

    if to_fetch:
        try:
            fresh = _fetch_coins_from_api(to_fetch, vs)
            from redis_cache import rset
            for cid, data in fresh.items():
                _coin_cache[cid] = (now, data)
                _coin_stale[cid] = data
                result[cid] = data
                rset(f"finans:kripto:{cid}", data)
            # API'den gelmeyen coin'ler için stale'e bak
            for cid in to_fetch:
                if cid not in result and cid in _coin_stale:
                    result[cid] = _coin_stale[cid]
        except Exception:
            # Tüm fetch başarısız — stale'e düş
            for cid in to_fetch:
                if cid in _coin_stale:
                    result[cid] = _coin_stale[cid]

    return result


def _gen_cached(key: str, ttl: int, fetch_fn):
    """Genel amaçlı (piyasa, trend, geçmiş) TTL + stale cache."""
    now = time.time()
    if key in _gen_cache:
        ts, data = _gen_cache[key]
        if now - ts < ttl:
            return data
    try:
        data = fetch_fn()
        _gen_cache[key] = (now, data)
        _gen_stale[key] = data
        return data
    except Exception:
        if key in _gen_stale:
            return _gen_stale[key]
        raise


def _gen_get(path: str, params: dict = None, ttl: int = 300) -> dict | list:
    """Genel CoinGecko GET — piyasa listesi, trend, geçmiş için."""
    cache_key = path + str(sorted((params or {}).items()))

    def fetch():
        url = f"{COINGECKO_BASE}{path}"
        with httpx.Client(timeout=10) as client:
            r = client.get(url, params=params or {})
        if r.status_code == 429:
            raise HTTPException(429, "CoinGecko rate limit aşıldı.")
        if r.status_code == 404:
            raise HTTPException(404, "Kripto para bulunamadı.")
        r.raise_for_status()
        return r.json()

    try:
        return _gen_cached(cache_key, ttl, fetch)
    except HTTPException:
        raise
    except httpx.TimeoutException:
        raise HTTPException(504, "CoinGecko API zaman aşımına uğradı.")
    except Exception as e:
        raise HTTPException(502, f"CoinGecko API hatası: {str(e)}")


def warm_up(coins: list[str] = None):
    """
    Startup veya manuel çağrı için cache ısıtma.
    coins listesi verilmezse WARM_COINS kullanılır.
    Per-coin stale doldurulur → sonraki hatalarda fallback çalışır.
    """
    target = coins or WARM_COINS
    try:
        data = _fetch_coins_from_api(target, "usd,try")
        now = time.time()
        for cid, raw in data.items():
            _coin_cache[cid] = (now, raw)
            _coin_stale[cid] = raw
        return True, list(data.keys())
    except Exception as e:
        return False, str(e)


# ── Endpoint'ler ──────────────────────────────────────────────────────────────

@router.get("/fiyat/{coin}", summary="Anlık kripto fiyatı")
def kripto_fiyat(
    coin: str,
    para_birimleri: str = Query(
        "usd,try",
        description="Virgülle ayrılmış para birimleri. Örn: usd,try,eur",
    ),
):
    coin_id = _resolve_id(coin)
    raw = _get_coin_price(coin_id, para_birimleri.lower().replace(" ", ""))

    birimleri = para_birimleri.lower().replace(" ", "").split(",")
    fiyatlar = {}
    for b in birimleri:
        if b in raw:
            fiyatlar[b] = {
                "fiyat": raw[b],
                "degisim_24s_yuzde": raw.get(f"{b}_24h_change"),
                "hacim_24s": raw.get(f"{b}_24h_vol"),
                "piyasa_degeri": raw.get(f"{b}_market_cap"),
            }

    return {
        "coin": coin.upper(),
        "coin_id": coin_id,
        "fiyatlar": fiyatlar,
        "son_guncelleme": raw.get("last_updated_at"),
    }


@router.get("/toplu", summary="Çoklu kripto fiyatı")
def kripto_toplu(
    coinler: str = Query(
        ...,
        description="Virgülle ayrılmış semboller veya id'ler. Örn: BTC,ETH,SOL",
    ),
    para_birimleri: str = Query("usd,try", description="Virgülle ayrılmış para birimleri"),
):
    """Birden fazla kripto parayı tek sorguda getir (per-coin cache'li)."""
    liste = [c.strip() for c in coinler.split(",") if c.strip()]
    if not liste:
        raise HTTPException(400, "En az bir coin giriniz.")
    if len(liste) > 30:
        raise HTTPException(400, "En fazla 30 coin sorgulanabilir.")

    vs = para_birimleri.lower().replace(" ", "")
    coin_ids = [_resolve_id(c) for c in liste]

    bulk = _get_coins_bulk(coin_ids, vs)

    sonuclar = []
    for coin, coin_id in zip(liste, coin_ids):
        if coin_id in bulk:
            raw = bulk[coin_id]
            fiyatlar = {}
            for b in vs.split(","):
                if b in raw:
                    fiyatlar[b] = {
                        "fiyat": raw[b],
                        "degisim_24s_yuzde": raw.get(f"{b}_24h_change"),
                    }
            sonuclar.append({
                "coin": coin.upper(),
                "coin_id": coin_id,
                "fiyatlar": fiyatlar,
            })
        else:
            sonuclar.append({"coin": coin.upper(), "hata": "bulunamadı"})

    return {"sayı": len(sonuclar), "veriler": sonuclar}


@router.get("/piyasa", summary="Kripto piyasa listesi")
def kripto_piyasa(
    para_birimi: str = Query("try", description="Ana para birimi: try, usd, eur"),
    limit: int = Query(50, ge=1, le=250, description="Kaç coin getirilsin"),
    sayfa: int = Query(1, ge=1, description="Sayfa numarası"),
    siralama: str = Query(
        "market_cap_desc",
        description="market_cap_desc, volume_desc, id_asc",
        pattern="^(market_cap_desc|market_cap_asc|volume_desc|volume_asc|id_asc|id_desc)$",
    ),
):
    data = _gen_get("/coins/markets", {
        "vs_currency": para_birimi.lower(),
        "order": siralama,
        "per_page": limit,
        "page": sayfa,
        "sparkline": "false",
        "price_change_percentage": "24h,7d",
    }, ttl=300)

    sonuclar = [
        {
            "sira": c["market_cap_rank"],
            "sembol": c["symbol"].upper(),
            "ad": c["name"],
            "fiyat": c["current_price"],
            "piyasa_degeri": c["market_cap"],
            "hacim_24s": c["total_volume"],
            "degisim_24s": c.get("price_change_percentage_24h"),
            "degisim_7g": c.get("price_change_percentage_7d_in_currency"),
            "arz": c.get("circulating_supply"),
            "gorsel": c.get("image"),
        }
        for c in data
    ]

    return {
        "para_birimi": para_birimi.upper(),
        "siralama": siralama,
        "sayfa": sayfa,
        "kayit_sayisi": len(sonuclar),
        "veriler": sonuclar,
    }


@router.get("/gecmis/{coin}", summary="Geçmiş fiyat grafiği")
def kripto_gecmis(
    coin: str,
    gun: int = Query(30, ge=1, le=365, description="Kaç günlük veri (1-365)"),
    para_birimi: str = Query("try", description="try, usd, eur"),
):
    coin_id = _resolve_id(coin)
    data = _gen_get(f"/coins/{coin_id}/market_chart", {
        "vs_currency": para_birimi.lower(),
        "days": gun,
        "interval": "daily" if gun > 1 else "hourly",
    }, ttl=600)

    if "prices" not in data:
        raise HTTPException(404, f"Kripto para bulunamadı: {coin}")

    from datetime import datetime
    prices = [
        {
            "tarih": datetime.utcfromtimestamp(p[0] / 1000).strftime("%Y-%m-%d %H:%M"),
            "fiyat": p[1],
        }
        for p in data["prices"]
    ]

    return {
        "coin": coin.upper(),
        "coin_id": coin_id,
        "para_birimi": para_birimi.upper(),
        "gun": gun,
        "kayit_sayisi": len(prices),
        "veriler": prices,
    }


@router.get("/trend", summary="Trend olan kriptolar")
def kripto_trend():
    data = _gen_get("/search/trending", ttl=600)
    coins = data.get("coins", [])

    return {
        "trend": [
            {
                "sira": i + 1,
                "sembol": c["item"]["symbol"].upper(),
                "ad": c["item"]["name"],
                "piyasa_degeri_sirasi": c["item"].get("market_cap_rank"),
                "fiyat_btc": c["item"].get("price_btc"),
                "gorsel": c["item"].get("small"),
            }
            for i, c in enumerate(coins)
        ]
    }
