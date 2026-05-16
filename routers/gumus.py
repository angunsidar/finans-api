"""
Gümüş fiyatı endpoint'leri.
Kaynak zinciri: yfinance XAG=X → Coinbase XAG-USD → CoinGecko kinesis-silver
5 dakika TTL cache ile rate-limit koruması.
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

# ── 5 dk TTL cache ────────────────────────────────────────────────────────────
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


# ── yfinance ─────────────────────────────────────────────────────────────────
def _fetch_yf(sembol: str) -> dict:
    cached = _cache_get(sembol)
    if cached:
        return cached
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


# ── Coinbase fallback (XAG-USD ve XAG-TRY — auth yok) ────────────────────────
def _coinbase_xag() -> dict | None:
    """Coinbase'den XAG-USD ve XAG-TRY fiyatı çeker."""
    cached = _cache_get("__cb_xag__")
    if cached:
        return cached
    try:
        r_usd = requests.get(
            "https://api.coinbase.com/v2/prices/XAG-USD/spot", timeout=8
        )
        r_try = requests.get(
            "https://api.coinbase.com/v2/prices/XAG-TRY/spot", timeout=8
        )
        if r_usd.status_code == 200 and r_try.status_code == 200:
            usd_ons = float(r_usd.json()["data"]["amount"])
            try_ons = float(r_try.json()["data"]["amount"])
            if usd_ons > 0 and try_ons > 0:
                result = {"usd_ons": usd_ons, "try_ons": try_ons}
                _cache_set("__cb_xag__", result)
                return result
    except Exception:
        pass
    return None


# ── CoinGecko kinesis-silver fallback ────────────────────────────────────────
def _coingecko_silver_usd() -> float | None:
    """CoinGecko kinesis-silver (KXAG) token — USD/ons proxy."""
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
            usd_ons = d.get("kinesis-silver", {}).get("usd")
            if usd_ons:
                _cache_set("__cg_silver__", {"usd_ons": float(usd_ons)})
                return float(usd_ons)
    except Exception:
        pass
    return None


# ── USDTRY yardımcısı ─────────────────────────────────────────────────────────
def _usdtry_rate() -> float:
    try:
        return _fetch_yf("USDTRY=X")["fiyat"]
    except Exception:
        return 0.0


# ── Endpoint'ler ──────────────────────────────────────────────────────────────

@router.get("", summary="Tüm gümüş kaynakları özet")
def gumus_ozet():
    sonuclar = {}
    for key, meta in KAYNAKLAR.items():
        try:
            veri = _fetch_yf(meta["sembol"])
            sonuclar[key] = {**meta, **veri}
        except Exception as e:
            sonuclar[key] = {**meta, "hata": str(e)[:80]}
    return sonuclar


@router.get("/tl", summary="Gümüş TL karşılığı (hesaplanmış)")
def gumus_tl():
    """
    Gümüş USD/ons fiyatı × Dolar/TL kuru = TL/ons.
    Gram hesabı için 31.1035'e bölünür.
    Kaynak zinciri: yfinance → Coinbase → CoinGecko kinesis-silver.
    """
    try:
        ons_usd: float | None = None
        ons_tl: float | None = None
        degisim_yuzde: float = 0.0
        tarih: str = ""
        kaynak: str = ""

        # 1. yfinance XAG=X
        try:
            silver = _fetch_yf("XAG=X")
            usd    = _fetch_yf("USDTRY=X")
            ons_usd = silver["fiyat"]
            ons_tl  = round(ons_usd * usd["fiyat"], 2)
            degisim_yuzde = silver["degisim_yuzde"]
            tarih   = silver["tarih"]
            kaynak  = "yfinance XAG=X × USDTRY=X"
        except Exception:
            pass

        # 2. Coinbase XAG-USD + XAG-TRY (TRY direkt geliyor)
        if ons_usd is None:
            cb = _coinbase_xag()
            if cb:
                ons_usd = cb["usd_ons"]
                ons_tl  = round(cb["try_ons"], 2)
                kaynak  = "Coinbase XAG-USD / XAG-TRY (fallback)"

        # 3. CoinGecko kinesis-silver + yfinance USDTRY
        if ons_usd is None:
            cg_usd = _coingecko_silver_usd()
            if cg_usd:
                ons_usd = cg_usd
                usdtry  = _usdtry_rate()
                if usdtry > 0:
                    ons_tl = round(ons_usd * usdtry, 2)
                kaynak = "CoinGecko kinesis-silver (fallback)"

        if ons_usd is None or ons_usd == 0:
            raise HTTPException(503, "Gümüş verisi alınamadı — tüm kaynaklar başarısız")
        if ons_tl is None or ons_tl == 0:
            raise HTTPException(503, "USDTRY kuru alınamadı")

        gram_tl = round(ons_tl / 31.1035, 2)
        usdtry_hesap = round(ons_tl / ons_usd, 4) if ons_usd else 0

        return {
            "gumus_usd_ons": round(ons_usd, 4),
            "usd_try": usdtry_hesap,
            "gumus_tl_ons": ons_tl,
            "gumus_tl_gram": gram_tl,
            "degisim_yuzde": degisim_yuzde,
            "not": f"{kaynak} ile hesaplanmıştır",
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
