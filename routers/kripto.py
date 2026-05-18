"""
Kripto para endpoint'leri.
CoinGecko public API üzerinden (API key gerekmez).
"""
from __future__ import annotations

import time
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

# Basit in-memory TTL cache: key → (timestamp, data)
_cache: dict[str, tuple[float, object]] = {}

# Stale cache: son başarılı veriyi sınırsız sakla (fallback için)
_stale: dict[str, object] = {}

def _cached(key: str, ttl: int, fetch_fn):
    """
    TTL süresi geçmediyse cache'den döndür.
    TTL dolmuşsa taze veri çek; çekemezse eski (stale) veriyi döndür.
    Bu sayede CoinGecko rate-limit'te bile boş dönmez.
    """
    now = time.time()
    if key in _cache:
        ts, data = _cache[key]
        if now - ts < ttl:
            return data
    # TTL doldu — taze veri dene
    try:
        data = fetch_fn()
        _cache[key] = (now, data)
        _stale[key] = data   # başarılı veriyi stale olarak sakla
        return data
    except Exception:
        # HTTPException (429), TimeoutException, her türlü hata — stale varsa döndür
        if key in _stale:
            return _stale[key]
        raise  # stale yoksa (ilk istek) hatayı ilet

router = APIRouter(prefix="/kripto", tags=["kripto"])

COINGECKO_BASE = "https://api.coingecko.com/api/v3"

# Kısa sembol → CoinGecko id eşleştirmesi
COIN_IDS: dict[str, str] = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "BNB": "binancecoin",
    "SOL": "solana",
    "XRP": "ripple",
    "ADA": "cardano",
    "AVAX": "avalanche-2",
    "DOT": "polkadot",
    "LINK": "chainlink",
    "LTC": "litecoin",
    "DOGE": "dogecoin",
    "MATIC": "matic-network",
    "UNI": "uniswap",
    "ATOM": "cosmos",
    "NEAR": "near",
    "FTM": "fantom",
    "TRX": "tron",
    "SHIB": "shiba-inu",
    "USDT": "tether",
    "USDC": "usd-coin",
}


def _resolve_id(coin: str) -> str:
    """Sembol (BTC) veya CoinGecko id (bitcoin) çözümle."""
    upper = coin.upper()
    if upper in COIN_IDS:
        return COIN_IDS[upper]
    return coin.lower()


def _get(path: str, params: dict = None, ttl: int = 300) -> dict | list:  # default 60s → 5 dk
    """CoinGecko'ya istek at. ttl saniye boyunca cache'de tut."""
    cache_key = path + str(sorted((params or {}).items()))

    def fetch():
        url = f"{COINGECKO_BASE}{path}"
        with httpx.Client(timeout=10) as client:
            r = client.get(url, params=params or {})
        if r.status_code == 429:
            raise HTTPException(429, "CoinGecko rate limit aşıldı, lütfen bekleyiniz.")
        if r.status_code == 404:
            raise HTTPException(404, "Kripto para bulunamadı.")
        r.raise_for_status()
        return r.json()
    try:
        return _cached(cache_key, ttl, fetch)
    except HTTPException:
        raise
    except httpx.TimeoutException:
        raise HTTPException(504, "CoinGecko API zaman aşımına uğradı.")
    except Exception as e:
        raise HTTPException(502, f"CoinGecko API hatası: {str(e)}")


@router.get("/fiyat/{coin}", summary="Anlık kripto fiyatı")
def kripto_fiyat(
    coin: str,
    para_birimleri: str = Query(
        "usd,try",
        description="Virgülle ayrılmış para birimleri. Örn: usd,try,eur",
    ),
):
    """
    Tek kripto para fiyatı.

    - `/kripto/fiyat/BTC` → Bitcoin fiyatı
    - `/kripto/fiyat/ETH?para_birimleri=try` → Ethereum TL fiyatı
    - `/kripto/fiyat/bitcoin` → CoinGecko id ile de çalışır
    """
    coin_id = _resolve_id(coin)
    vs = para_birimleri.lower().replace(" ", "")

    data = _get("/simple/price", {
        "ids": coin_id,
        "vs_currencies": vs,
        "include_24hr_change": "true",
        "include_24hr_vol": "true",
        "include_market_cap": "true",
        "include_last_updated_at": "true",
    })

    if coin_id not in data:
        raise HTTPException(404, f"Kripto para bulunamadı: {coin}")

    raw = data[coin_id]
    birimleri = vs.split(",")

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
    """
    Piyasa değerine göre kripto para listesi.

    - `para_birimi=try` → TL cinsinden fiyatlar
    - `limit=10` → İlk 10 coin
    """
    data = _get("/coins/markets", {
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
    """
    Geçmiş fiyat verisi (günlük kapanış).

    - `/kripto/gecmis/BTC?gun=30` → Son 30 gün Bitcoin TL
    - `/kripto/gecmis/ETH?gun=7&para_birimi=usd` → Son 7 gün Ethereum USD
    """
    coin_id = _resolve_id(coin)
    data = _get(f"/coins/{coin_id}/market_chart", {
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
    """CoinGecko'nun trend listesi (son 24 saat en çok aranan 7 coin)."""
    data = _get("/search/trending", ttl=600)
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


@router.get("/toplu", summary="Çoklu kripto fiyatı")
def kripto_toplu(
    coinler: str = Query(
        ...,
        description="Virgülle ayrılmış semboller veya id'ler. Örn: BTC,ETH,SOL",
    ),
    para_birimleri: str = Query("usd,try", description="Virgülle ayrılmış para birimleri"),
):
    """Birden fazla kripto parayı tek sorguda getir."""
    liste = [c.strip() for c in coinler.split(",") if c.strip()]
    if not liste:
        raise HTTPException(400, "En az bir coin giriniz.")
    if len(liste) > 30:
        raise HTTPException(400, "En fazla 30 coin sorgulanabilir.")

    ids = ",".join(_resolve_id(c) for c in liste)
    vs = para_birimleri.lower().replace(" ", "")

    data = _get("/simple/price", {
        "ids": ids,
        "vs_currencies": vs,
        "include_24hr_change": "true",
        "include_24hr_vol": "true",
        "include_market_cap": "true",
        "include_last_updated_at": "true",
    })

    sonuclar = []
    for coin in liste:
        coin_id = _resolve_id(coin)
        if coin_id in data:
            raw = data[coin_id]
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
