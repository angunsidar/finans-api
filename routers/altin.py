"""
Altın fiyatı endpoint'leri.
yfinance üzerinden — futures, ETF ve BIST altın.
Rate-limit koruması: 15 dakika TTL cache + Coinbase fallback.
"""
from __future__ import annotations

import time
import traceback
import requests
import yfinance as yf
from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/altin", tags=["altın"])

KAYNAKLAR = {
    "futures": {"sembol": "GC=F",      "ad": "Altın Futures (COMEX)",  "para": "USD", "birim": "ons"},
    "etf_gld": {"sembol": "GLD",       "ad": "SPDR Gold ETF",           "para": "USD", "birim": "hisse"},
    "etf_iau": {"sembol": "IAU",       "ad": "iShares Gold ETF",        "para": "USD", "birim": "hisse"},
    "bist":    {"sembol": "GLDTR.IS",  "ad": "Altın (BIST)",            "para": "TRY", "birim": "gram"},
}

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; FinansAPI/1.0)",
    "Accept": "application/json",
}

# ── TTL cache (15 dk) + stale fallback ───────────────────────────────────────
_cache: dict[str, tuple[float, dict]] = {}
_stale: dict[str, dict] = {}          # restart sonrası bile fallback için
_CACHE_TTL = 900  # 15 dakika — yfinance rate-limit penceresini aşmak için


def _cache_get(key: str):
    if key in _cache:
        ts, val = _cache[key]
        if time.time() - ts < _CACHE_TTL:
            return val
    return None


def _cache_set(key: str, val: dict):
    _cache[key] = (time.time(), val)
    _stale[key] = val
    from redis_cache import rset
    rset(f"finans:altin:{key}", val)


# ── yfinance ─────────────────────────────────────────────────────────────────
def _fetch(sembol: str) -> dict:
    cached = _cache_get(sembol)
    if cached:
        return cached
    try:
        tick = yf.Ticker(sembol)
        hist = tick.history(period="2d")
        if hist.empty:
            raise ValueError(f"Veri bulunamadı: {sembol}")
        son = hist.iloc[-1]
        onceki = hist.iloc[-2] if len(hist) > 1 else hist.iloc[-1]
        kapanis = round(float(son["Close"]), 4)
        onceki_kapanis = float(onceki["Close"])
        degisim = round(kapanis - onceki_kapanis, 4)
        degisim_yuzde = round((degisim / onceki_kapanis) * 100, 2) if onceki_kapanis else 0
        result = {
            "fiyat": kapanis,
            "acilis": round(float(son["Open"]), 4),
            "yuksek": round(float(son["High"]), 4),
            "dusuk": round(float(son["Low"]), 4),
            "degisim": degisim,
            "degisim_yuzde": degisim_yuzde,
            "tarih": str(hist.index[-1].date()),
        }
        _cache_set(sembol, result)
        return result
    except Exception as e:
        # yfinance hata verdi — stale veri varsa onu döndür
        if sembol in _stale:
            return _stale[sembol]
        raise  # stale da yoksa (ilk istek) hatayı ilet


# ── Coinbase fallback (XAU-USD, XAU-TRY) ─────────────────────────────────────
def _coinbase_xau() -> tuple[dict | None, str]:
    cached = _cache_get("__cb_xau__")
    if cached:
        return cached, ""
    try:
        r_usd = requests.get(
            "https://api.coinbase.com/v2/prices/XAU-USD/spot",
            headers=_HEADERS, timeout=8,
        )
        r_try = requests.get(
            "https://api.coinbase.com/v2/prices/XAU-TRY/spot",
            headers=_HEADERS, timeout=8,
        )
        if r_usd.status_code == 200 and r_try.status_code == 200:
            usd_ons = float(r_usd.json()["data"]["amount"])
            try_ons = float(r_try.json()["data"]["amount"])
            if usd_ons > 0 and try_ons > 0:
                result = {"usd_ons": usd_ons, "try_ons": try_ons}
                _cache_set("__cb_xau__", result)
                return result, ""
        return None, f"Coinbase HTTP {r_usd.status_code}/{r_try.status_code}"
    except Exception as e:
        return None, f"Coinbase: {e}"


# ── Endpoint'ler ──────────────────────────────────────────────────────────────

@router.get("", summary="Tüm altın kaynakları özet")
def altin_ozet():
    """Futures, ETF ve BIST altın fiyatlarını tek sorguda getir."""
    sonuclar = {}
    for key, meta in KAYNAKLAR.items():
        try:
            veri = _fetch(meta["sembol"])
            sonuclar[key] = {**meta, **veri}
        except Exception as e:
            sonuclar[key] = {**meta, "hata": str(e)[:60]}
    return sonuclar


@router.get("/futures", summary="Altın futures (COMEX) — USD/ons")
def altin_futures():
    veri = _fetch("GC=F")
    return {"sembol": "GC=F", "ad": "Altın Futures (COMEX)", "para_birimi": "USD",
            "birim": "ons", **veri}


@router.get("/bist", summary="BIST altın fiyatı — TRY")
def altin_bist():
    veri = _fetch("GLDTR.IS")
    return {"sembol": "GLDTR.IS", "ad": "Altın (BIST)", "para_birimi": "TRY",
            "borsa": "BIST", **veri}


@router.get("/tl", summary="Altın TL karşılığı (hesaplanmış)")
def altin_tl():
    """
    Kaynak zinciri: yfinance GC=F × USDTRY=X → Coinbase XAU-USD/TRY
    15 dakika TTL cache.
    """
    try:
        ons_usd: float | None = None
        ons_tl: float | None = None
        degisim_yuzde: float = 0.0
        tarih: str = ""
        kaynak: str = ""
        hatalar: list[str] = []

        # 1. yfinance GC=F × USDTRY=X
        try:
            gold = _fetch("GC=F")
            usd  = _fetch("USDTRY=X")
            ons_usd = gold["fiyat"]
            ons_tl  = round(ons_usd * usd["fiyat"], 2)
            degisim_yuzde = gold["degisim_yuzde"]
            tarih   = gold["tarih"]
            kaynak  = "yfinance GC=F × USDTRY=X"
        except Exception as e:
            hatalar.append(f"yfinance: {e}")

        # 2. Coinbase XAU-USD + XAU-TRY
        if ons_usd is None:
            cb, cb_err = _coinbase_xau()
            if cb:
                ons_usd = cb["usd_ons"]
                ons_tl  = round(cb["try_ons"], 2)
                kaynak  = "Coinbase XAU-USD/TRY"
            else:
                hatalar.append(cb_err)

        if ons_usd is None or ons_usd == 0:
            raise HTTPException(
                503,
                detail={"hata": "Altın verisi alınamadı — tüm kaynaklar başarısız",
                        "detaylar": hatalar},
            )
        if ons_tl is None or ons_tl == 0:
            raise HTTPException(503, detail={"hata": "USDTRY kuru alınamadı"})

        gram_tl = round(ons_tl / 31.1035, 2)
        usdtry_hesap = round(ons_tl / ons_usd, 4) if ons_usd else 0

        return {
            "altin_usd_ons": round(ons_usd, 4),
            "usd_try": usdtry_hesap,
            "altin_tl_ons": ons_tl,
            "altin_tl_gram": gram_tl,
            "altin_22ayar_gram": round(gram_tl * 0.916, 2),
            "not": f"{kaynak} ile hesaplanmıştır",
            "tarih": tarih,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail={"error": str(e), "trace": traceback.format_exc()[-500:]})


@router.get("/gecmis", summary="Geçmiş altın fiyatları")
def altin_gecmis(
    kaynak: str = Query("futures", description="futures | etf_gld | etf_iau | bist"),
    period: str = Query("1mo", description="1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y",
                        pattern="^(1d|5d|1mo|3mo|6mo|1y|2y|5y)$"),
    aralik: str = Query("1d", description="1d, 1wk, 1mo",
                        pattern="^(1d|1wk|1mo)$"),
):
    """Geçmiş altın fiyat verisi (OHLCV)."""
    if kaynak not in KAYNAKLAR:
        raise HTTPException(400, f"Geçersiz kaynak. Seçenekler: {list(KAYNAKLAR.keys())}")
    meta = KAYNAKLAR[kaynak]
    tick = yf.Ticker(meta["sembol"])
    hist = tick.history(period=period, interval=aralik)
    if hist.empty:
        raise HTTPException(404, "Veri bulunamadı")
    kayitlar = [
        {
            "tarih": str(idx.date()),
            "acilis": round(float(row["Open"]), 4),
            "yuksek": round(float(row["High"]), 4),
            "dusuk": round(float(row["Low"]), 4),
            "kapanis": round(float(row["Close"]), 4),
            "hacim": int(row["Volume"]),
        }
        for idx, row in hist.iterrows()
    ]
    return {
        "kaynak": kaynak,
        "sembol": meta["sembol"],
        "para_birimi": meta["para"],
        "period": period,
        "aralik": aralik,
        "kayit_sayisi": len(kayitlar),
        "veriler": kayitlar,
    }
