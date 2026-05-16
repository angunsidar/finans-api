"""
Gümüş fiyatı endpoint'leri.
yfinance üzerinden — spot (XAG=X) ve SLV ETF.
Rate-limit koruması: 5 dakika TTL cache + CoinGecko fallback.
"""
from __future__ import annotations

import time
import traceback
import requests
import yfinance as yf
from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/gumus", tags=["gümüş"])

KAYNAKLAR = {
    "spot":    {"sembol": "XAG=X", "ad": "Gümüş Spot (XAG/USD)", "para": "USD", "birim": "ons"},
    "etf_slv": {"sembol": "SLV",   "ad": "iShares Silver ETF",    "para": "USD", "birim": "hisse"},
}

# ── Basit TTL cache (5 dk = 300 sn) ──────────────────────────────────────────
_cache: dict[str, tuple[float, dict]] = {}
_CACHE_TTL = 300  # saniye


def _cache_get(key: str):
    if key in _cache:
        ts, val = _cache[key]
        if time.time() - ts < _CACHE_TTL:
            return val
    return None


def _cache_set(key: str, val: dict):
    _cache[key] = (time.time(), val)


# ── yfinance ile veri çek ─────────────────────────────────────────────────────
def _fetch(sembol: str) -> dict:
    cached = _cache_get(sembol)
    if cached:
        return cached

    tick = yf.Ticker(sembol)
    hist = tick.history(period="2d")
    if hist.empty:
        raise HTTPException(404, f"Veri bulunamadı: {sembol}")
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


# ── Fallback 1: metals.live (ücretsiz, auth yok) ─────────────────────────────
def _metalslive_silver_usd() -> float | None:
    """metals.live ücretsiz API — spot silver USD/ons."""
    cached = _cache_get("__ml_silver__")
    if cached:
        return cached.get("usd_ons")
    try:
        resp = requests.get("https://metals.live/api/v1/spot", timeout=8)
        if resp.status_code == 200:
            data = resp.json()
            # [{silver: 32.5, gold: 3200, ...}] formatında gelir
            if isinstance(data, list) and data:
                usd_ons = data[0].get("silver")
            elif isinstance(data, dict):
                usd_ons = data.get("silver")
            else:
                usd_ons = None
            if usd_ons:
                _cache_set("__ml_silver__", {"usd_ons": float(usd_ons)})
                return float(usd_ons)
    except Exception:
        pass
    return None


# ── Fallback 2: CoinGecko kinesis-silver (KXAG token, silver-backed) ─────────
def _coingecko_silver_usd() -> float | None:
    """CoinGecko'dan gümüş TRY/ons — kinesis-silver (KXAG) token ile."""
    cached = _cache_get("__cg_silver__")
    if cached:
        return cached.get("usd_ons")
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": "kinesis-silver", "vs_currencies": "usd"},
            timeout=8,
        )
        if resp.status_code == 200:
            d = resp.json()
            # kinesis-silver fiyatı USD/ons cinsinden gelir
            usd_ons = d.get("kinesis-silver", {}).get("usd")
            if usd_ons:
                _cache_set("__cg_silver__", {"usd_ons": float(usd_ons)})
                return float(usd_ons)
    except Exception:
        pass
    return None


def _usdtry_rate() -> float:
    """USDTRY kurunu döndür — önce cache, sonra yfinance, sonra sabit fallback."""
    try:
        return _fetch("USDTRY=X")["fiyat"]
    except Exception:
        return 0.0


# ── Endpoint'ler ──────────────────────────────────────────────────────────────

@router.get("", summary="Tüm gümüş kaynakları özet")
def gumus_ozet():
    """Spot ve ETF gümüş fiyatlarını tek sorguda getir."""
    sonuclar = {}
    for key, meta in KAYNAKLAR.items():
        try:
            veri = _fetch(meta["sembol"])
            sonuclar[key] = {**meta, **veri}
        except Exception as e:
            sonuclar[key] = {**meta, "hata": str(e)[:80]}
    return sonuclar


@router.get("/tl", summary="Gümüş TL karşılığı (hesaplanmış)")
def gumus_tl():
    """
    Gümüş USD/ons fiyatı × Dolar/TL kuru = TL/ons.
    Gram hesabı için 31.1035'e bölünür.
    Önce yfinance (XAG=X), rate-limit'te CoinGecko'ya düşer.
    """
    try:
        ons_fiyat_usd: float | None = None
        degisim_yuzde: float = 0.0
        tarih: str = ""
        kaynak_notu: str = ""

        # 1. yfinance dene
        try:
            silver = _fetch("XAG=X")
            ons_fiyat_usd = silver["fiyat"]
            degisim_yuzde = silver["degisim_yuzde"]
            tarih = silver["tarih"]
            kaynak_notu = "XAG=X spot × USDTRY=X kuru ile hesaplanmıştır"
        except Exception:
            pass

        # 2. metals.live fallback
        if ons_fiyat_usd is None:
            ons_fiyat_usd = _metalslive_silver_usd()
            if ons_fiyat_usd:
                kaynak_notu = "metals.live spot × USDTRY=X kuru ile hesaplanmıştır (fallback)"
                tarih = ""

        # 3. CoinGecko kinesis-silver fallback
        if ons_fiyat_usd is None:
            ons_fiyat_usd = _coingecko_silver_usd()
            if ons_fiyat_usd:
                kaynak_notu = "CoinGecko kinesis-silver × USDTRY=X kuru ile hesaplanmıştır (fallback)"
                tarih = ""

        if ons_fiyat_usd is None or ons_fiyat_usd == 0:
            raise HTTPException(503, "Gümüş verisi alınamadı — tüm kaynaklar başarısız")

        # 3. USDTRY kuru
        usdtry = _usdtry_rate()
        if usdtry == 0:
            raise HTTPException(503, "USDTRY kuru alınamadı")

        ons_fiyat_tl = round(ons_fiyat_usd * usdtry, 2)
        gram_fiyat_tl = round(ons_fiyat_tl / 31.1035, 2)

        return {
            "gumus_usd_ons": round(ons_fiyat_usd, 4),
            "usd_try": usdtry,
            "gumus_tl_ons": ons_fiyat_tl,
            "gumus_tl_gram": gram_fiyat_tl,
            "degisim_yuzde": degisim_yuzde,
            "not": kaynak_notu,
            "tarih": tarih,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail={"error": str(e), "trace": traceback.format_exc()[-600:]})


@router.get("/gecmis", summary="Geçmiş gümüş fiyatları")
def gumus_gecmis(
    period: str = Query("1mo", description="1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y",
                        pattern="^(1d|5d|1mo|3mo|6mo|1y|2y|5y)$"),
    aralik: str = Query("1d", description="1d, 1wk, 1mo",
                        pattern="^(1d|1wk|1mo)$"),
):
    """Geçmiş gümüş fiyat verisi (OHLCV) — XAG=X."""
    try:
        tick = yf.Ticker("XAG=X")
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
                "hacim": int(row["Volume"]) if row["Volume"] == row["Volume"] else 0,
            }
            for idx, row in hist.iterrows()
        ]
        return {"kaynak": "XAG=X", "period": period, "aralik": aralik, "veri": kayitlar}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail={"error": str(e), "trace": traceback.format_exc()[-600:]})
