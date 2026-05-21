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

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; FinansAPI/1.0)",
    "Accept": "application/json",
}

# ── 5 dk TTL cache + stale ───────────────────────────────────────────────────
_cache: dict[str, tuple[float, dict]] = {}
_stale: dict[str, dict] = {}
_CACHE_TTL = 300


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
    rset(f"finans:gumus:{key}", val)


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


# ── Coinbase fallback ─────────────────────────────────────────────────────────
def _coinbase_xag() -> tuple[dict | None, str]:
    """Coinbase XAG-USD ve XAG-TRY — (sonuç, hata_mesajı)"""
    cached = _cache_get("__cb_xag__")
    if cached:
        return cached, ""
    try:
        r_usd = requests.get(
            "https://api.coinbase.com/v2/prices/XAG-USD/spot",
            headers=_HEADERS, timeout=8,
        )
        r_try = requests.get(
            "https://api.coinbase.com/v2/prices/XAG-TRY/spot",
            headers=_HEADERS, timeout=8,
        )
        if r_usd.status_code == 200 and r_try.status_code == 200:
            usd_ons = float(r_usd.json()["data"]["amount"])
            try_ons = float(r_try.json()["data"]["amount"])
            if usd_ons > 0 and try_ons > 0:
                result = {"usd_ons": usd_ons, "try_ons": try_ons}
                _cache_set("__cb_xag__", result)
                return result, ""
        return None, f"Coinbase HTTP {r_usd.status_code}/{r_try.status_code}"
    except Exception as e:
        return None, f"Coinbase: {e}"


# ── CoinGecko kinesis-silver fallback ────────────────────────────────────────
def _coingecko_silver_usd() -> tuple[float | None, str]:
    """CoinGecko kinesis-silver — (usd_ons, hata_mesajı)"""
    cached = _cache_get("__cg_silver__")
    if cached:
        return cached.get("usd_ons"), ""
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": "kinesis-silver", "vs_currencies": "usd"},
            headers=_HEADERS,
            timeout=8,
        )
        if resp.status_code == 200:
            d = resp.json()
            usd_ons = d.get("kinesis-silver", {}).get("usd")
            if usd_ons:
                _cache_set("__cg_silver__", {"usd_ons": float(usd_ons)})
                return float(usd_ons), ""
            return None, f"CoinGecko boş yanıt: {d}"
        return None, f"CoinGecko HTTP {resp.status_code}: {resp.text[:100]}"
    except Exception as e:
        return None, f"CoinGecko: {e}"


def _usdtry_rate() -> float:
    """
    USD/TRY kurunu doviz modülünün cache'inden oku — duplicate yfinance çağrısı olmaz.
    Fallback: doviz._cache → Redis → doviz._fetch_kur → yfinance XAG=X yan ürünü
    """
    # 1. doviz memory cache
    try:
        from routers import doviz as _doviz
        cached = _doviz._cache_get("USDTRY=X")
        if cached:
            return cached["kur"]
    except Exception:
        pass

    # 2. Redis
    try:
        from redis_cache import rget
        rv = rget("finans:doviz:USDTRY=X")
        if rv:
            return rv["kur"]
    except Exception:
        pass

    # 3. doviz._fetch_kur (yfinance + doviz cache'e yazar)
    try:
        from routers import doviz as _doviz
        rv = _doviz._fetch_kur("USDTRY=X")
        return rv["kur"]
    except Exception:
        pass

    # 4. Son çare: yerel yfinance
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
def gumus_tl(force: bool = False):
    """
    Kaynak zinciri: yfinance XAG=X → Coinbase XAG-USD/TRY → CoinGecko kinesis-silver
    """
    # Redis'te önceden hesaplanmış sonuç varsa direkt dön (background worker force=True ile yeniler)
    if not force:
        from redis_cache import rget
        cached = rget("finans:gumus:tl")
        if cached:
            _stale["tl"] = cached
            return cached

    try:
        ons_usd: float | None = None
        ons_tl: float | None = None
        degisim_yuzde: float = 0.0
        tarih: str = ""
        kaynak: str = ""
        hatalar: list[str] = []

        # 1. yfinance XAG=X
        try:
            silver = _fetch_yf("XAG=X")
            usd    = _fetch_yf("USDTRY=X")
            ons_usd = silver["fiyat"]
            ons_tl  = round(ons_usd * usd["fiyat"], 2)
            degisim_yuzde = silver["degisim_yuzde"]
            tarih   = silver["tarih"]
            kaynak  = "yfinance XAG=X × USDTRY=X"
        except Exception as e:
            hatalar.append(f"yfinance: {e}")

        # 2. Coinbase XAG-USD + XAG-TRY
        if ons_usd is None:
            cb, cb_err = _coinbase_xag()
            if cb:
                ons_usd = cb["usd_ons"]
                ons_tl  = round(cb["try_ons"], 2)
                kaynak  = "Coinbase XAG-USD/TRY"
            else:
                hatalar.append(cb_err)

        # 3. CoinGecko kinesis-silver
        if ons_usd is None:
            cg_usd, cg_err = _coingecko_silver_usd()
            if cg_usd:
                ons_usd = cg_usd
                usdtry  = _usdtry_rate()
                ons_tl  = round(ons_usd * usdtry, 2) if usdtry > 0 else None
                kaynak  = "CoinGecko kinesis-silver"
            else:
                hatalar.append(cg_err)

        if ons_usd is None or ons_usd == 0:
            if "tl" in _stale:
                return _stale["tl"]
            raise HTTPException(
                503,
                detail={"hata": "Gümüş verisi alınamadı — tüm kaynaklar başarısız",
                        "detaylar": hatalar},
            )
        if ons_tl is None or ons_tl == 0:
            if "tl" in _stale:
                return _stale["tl"]
            raise HTTPException(
                503,
                detail={"hata": "USDTRY kuru alınamadı", "detaylar": hatalar},
            )

        gram_tl = round(ons_tl / 31.1035, 2)
        usdtry_hesap = round(ons_tl / ons_usd, 4) if ons_usd else 0

        result = {
            "gumus_usd_ons": round(ons_usd, 4),
            "usd_try": usdtry_hesap,
            "gumus_tl_ons": ons_tl,
            "gumus_tl_gram": gram_tl,
            "degisim_yuzde": degisim_yuzde,
            "not": f"{kaynak} ile hesaplanmıştır",
            "tarih": tarih,
        }
        _stale["tl"] = result
        from redis_cache import rset
        rset("finans:gumus:tl", result)
        return result

    except HTTPException:
        raise
    except Exception as e:
        if "tl" in _stale:
            return _stale["tl"]
        raise HTTPException(500, detail={"error": str(e), "trace": traceback.format_exc()[-800:]})


@router.get("/debug", summary="Gümüş kaynak debug bilgisi", include_in_schema=False)
def gumus_debug():
    """Her kaynağın durumunu gösterir — production'da kaldırın."""
    sonuc = {}

    # yfinance
    try:
        s = _fetch_yf("XAG=X")
        sonuc["yfinance_xag"] = {"ok": True, "fiyat": s["fiyat"]}
    except Exception as e:
        sonuc["yfinance_xag"] = {"ok": False, "hata": str(e)[:200]}

    # Coinbase
    cb, cb_err = _coinbase_xag()
    sonuc["coinbase"] = {"ok": cb is not None, "veri": cb, "hata": cb_err}

    # CoinGecko
    cg, cg_err = _coingecko_silver_usd()
    sonuc["coingecko"] = {"ok": cg is not None, "fiyat": cg, "hata": cg_err}

    # USDTRY
    try:
        u = _fetch_yf("USDTRY=X")
        sonuc["usdtry"] = {"ok": True, "kur": u["fiyat"]}
    except Exception as e:
        sonuc["usdtry"] = {"ok": False, "hata": str(e)[:200]}

    return sonuc


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
