"""
ABD hisse senedi ve endeks endpoint'leri.
yfinance — NYSE, NASDAQ, AMEX.
5 dakika TTL cache + stale fallback (borsa açılış spike koruması).
"""
from __future__ import annotations

import threading
import time
import yfinance as yf
from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/abd", tags=["abd"])

POPULER_HISSELER: dict[str, str] = {
    "AAPL":  "Apple",
    "MSFT":  "Microsoft",
    "NVDA":  "Nvidia",
    "GOOGL": "Alphabet (Google)",
    "AMZN":  "Amazon",
    "META":  "Meta",
    "TSLA":  "Tesla",
    "BRK-B": "Berkshire Hathaway B",
    "JPM":   "JPMorgan Chase",
    "V":     "Visa",
    "JNJ":   "Johnson & Johnson",
    "WMT":   "Walmart",
    "XOM":   "ExxonMobil",
    "UNH":   "UnitedHealth",
    "LLY":   "Eli Lilly",
    "MA":    "Mastercard",
    "AVGO":  "Broadcom",
    "HD":    "Home Depot",
    "PG":    "Procter & Gamble",
    "COST":  "Costco",
}

POPULER_ETFLER: dict[str, str] = {
    "SPY": "S&P 500 ETF (SPDR)",
    "QQQ": "Nasdaq 100 ETF (Invesco)",
    "IWM": "Russell 2000 ETF",
    "DIA": "Dow Jones ETF",
    "GLD": "Altın ETF (SPDR)",
    "TLT": "20Y Hazine Bonosu ETF",
    "VTI": "Vanguard Total Market ETF",
    "EEM": "Gelişen Piyasalar ETF",
}

ENDEKSLER: dict[str, str] = {
    "^GSPC": "S&P 500",
    "^IXIC": "Nasdaq Composite",
    "^DJI":  "Dow Jones",
    "^RUT":  "Russell 2000",
    "^VIX":  "VIX Korku Endeksi",
}

# ── TTL cache (5 dk) + stale fallback ────────────────────────────────────────
_cache: dict[str, tuple[float, dict]] = {}
_stale: dict[str, dict] = {}
_TTL = 300  # 5 dakika


def _cache_get(key: str) -> dict | None:
    if key in _cache:
        ts, val = _cache[key]
        if time.time() - ts < _TTL:
            return val
    return None


def _cache_set(key: str, val: dict):
    _cache[key] = (time.time(), val)
    _stale[key] = val
    from redis_cache import rset
    rset(f"finans:abd:{key}", val)


_bg_in_progress: set[str] = set()


def _bg_fetch_abd(sembol: str) -> None:
    """Redis miss: arka planda yfinance'den çek, cache'e yaz."""
    if sembol in _bg_in_progress:
        return
    _bg_in_progress.add(sembol)

    def _run():
        try:
            tick = yf.Ticker(sembol)
            hist = tick.history(period="1d", interval="2m")
            hist = hist.dropna(subset=["Close"]) if not hist.empty else hist
            if hist.empty:
                hist = tick.history(period="5d")
                hist = hist.dropna(subset=["Close"])
            if hist.empty:
                return
            son = hist.iloc[-1]
            onceki = hist.iloc[-2] if len(hist) > 1 else hist.iloc[-1]
            kapanis = round(float(son["Close"]), 4)
            onceki_kapanis = float(onceki["Close"])
            degisim = round(kapanis - onceki_kapanis, 4)
            degisim_yuzde = round((degisim / onceki_kapanis) * 100, 2) if onceki_kapanis else 0
            result = {
                "sembol": sembol,
                "fiyat": kapanis,
                "acilis": round(float(son["Open"]), 4),
                "yuksek": round(float(son["High"]), 4),
                "dusuk": round(float(son["Low"]), 4),
                "hacim": int(son["Volume"]),
                "degisim_usd": degisim,
                "degisim_yuzde": degisim_yuzde,
                "para_birimi": "USD",
                "tarih": str(hist.index[-1].date()),
            }
            _cache_set(sembol, result)
        except Exception as e:
            pass
        finally:
            _bg_in_progress.discard(sembol)

    threading.Thread(target=_run, daemon=True).start()


def _fetch(sembol: str, force: bool = False) -> dict | None:
    key = sembol.upper()

    if not force:
        # 1. Memory cache
        cached = _cache_get(key)
        if cached:
            return cached
        # 2. Redis
        from redis_cache import rget
        redis_val = rget(f"finans:abd:{key}")
        if redis_val:
            _cache[key] = (time.time(), redis_val)
            _stale[key] = redis_val
            return redis_val
        # 3. Redis miss → arka planda çek, stale varsa döndür
        _bg_fetch_abd(key)
        return _stale.get(key)

    # force=True → background worker
    try:
        tick = yf.Ticker(sembol.upper())
        hist = tick.history(period="1d", interval="2m")
        hist = hist.dropna(subset=["Close"]) if not hist.empty else hist
        if hist.empty:
            hist = tick.history(period="5d")
            hist = hist.dropna(subset=["Close"])
        if hist.empty:
            raise HTTPException(404, f"Hisse bulunamadı: {sembol}")
        son = hist.iloc[-1]
        onceki = hist.iloc[-2] if len(hist) > 1 else hist.iloc[-1]
        kapanis = round(float(son["Close"]), 4)
        onceki_kapanis = float(onceki["Close"])
        degisim = round(kapanis - onceki_kapanis, 4)
        degisim_yuzde = round((degisim / onceki_kapanis) * 100, 2) if onceki_kapanis else 0
        result = {
            "sembol": sembol.upper(),
            "fiyat": kapanis,
            "acilis": round(float(son["Open"]), 4),
            "yuksek": round(float(son["High"]), 4),
            "dusuk": round(float(son["Low"]), 4),
            "hacim": int(son["Volume"]),
            "degisim_usd": degisim,
            "degisim_yuzde": degisim_yuzde,
            "para_birimi": "USD",
            "tarih": str(hist.index[-1].date()),
        }
        _cache_set(key, result)
        return result
    except HTTPException:
        raise
    except Exception:
        return _stale.get(key)


# Top 6 popüler ABD hissesi — background worker ısıtır (AAPL, MSFT, NVDA, GOOGL, AMZN, META)
WARM_HISSELER = list(POPULER_HISSELER.keys())[:6]


def warm_up() -> list[str]:
    """Background worker: popüler ABD hisseleri için yfinance'den taze veri çek."""
    basarili = []
    for sembol in WARM_HISSELER:
        try:
            _fetch(sembol, force=True)
            basarili.append(sembol)
        except Exception:
            pass
    return basarili


# ── Endpoint'ler ──────────────────────────────────────────────────────────────

@router.get("/liste", summary="Popüler ABD hisseleri ve ETF'ler")
def abd_liste():
    return {
        "hisseler": [{"sembol": s, "ad": a} for s, a in POPULER_HISSELER.items()],
        "etfler":   [{"sembol": s, "ad": a} for s, a in POPULER_ETFLER.items()],
        "endeksler": [{"sembol": s, "ad": a} for s, a in ENDEKSLER.items()],
    }


_BEKLENIYOR = lambda s: {"sembol": s, "fiyat": None, "durum": "bekleniyor", "para_birimi": "USD"}


@router.get("/hisse/{sembol}", summary="ABD hisse fiyatı")
def abd_hisse(sembol: str):
    result = _fetch(sembol.upper())
    return result if result is not None else _BEKLENIYOR(sembol.upper())


@router.get("/hisse/{sembol}/gecmis", summary="ABD hisse geçmiş")
def abd_hisse_gecmis(
    sembol: str,
    period: str = Query("1mo", description="1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y",
                        pattern="^(1d|5d|1mo|3mo|6mo|1y|2y|5y)$"),
    aralik: str = Query("1d", description="1m, 5m, 15m, 1h, 1d, 1wk, 1mo",
                        pattern="^(1m|5m|15m|1h|1d|1wk|1mo)$"),
):
    tick = yf.Ticker(sembol.upper())
    hist = tick.history(period=period, interval=aralik)
    if hist.empty:
        raise HTTPException(404, f"Hisse bulunamadı: {sembol}")
    kayitlar = [
        {
            "tarih": str(idx),
            "acilis": round(float(row["Open"]), 4),
            "yuksek": round(float(row["High"]), 4),
            "dusuk": round(float(row["Low"]), 4),
            "kapanis": round(float(row["Close"]), 4),
            "hacim": int(row["Volume"]),
        }
        for idx, row in hist.iterrows()
    ]
    return {
        "sembol": sembol.upper(),
        "period": period,
        "aralik": aralik,
        "para_birimi": "USD",
        "kayit_sayisi": len(kayitlar),
        "veriler": kayitlar,
    }


@router.get("/endeks/{sembol}", summary="ABD endeks değeri")
def abd_endeks(sembol: str):
    s = sembol.upper().lstrip("^")
    yahoo_sembol = f"^{s}"
    veri = _fetch(yahoo_sembol)
    ad = ENDEKSLER.get(yahoo_sembol, s)
    return {**veri, "sembol": s, "ad": ad}


@router.get("/toplu", summary="Çoklu ABD hissesi")
def abd_toplu(
    semboller: str = Query(..., description="Virgülle ayrılmış semboller. Örn: AAPL,TSLA,NVDA")
):
    """
    Birden fazla ABD hissesini tek sorguda getir.
    Önce memory cache, sonra Redis MGET (tek tur), son olarak yfinance.
    """
    liste = [s.strip().upper() for s in semboller.split(",") if s.strip()]
    if not liste:
        raise HTTPException(400, "En az bir sembol giriniz.")
    if len(liste) > 20:
        raise HTTPException(400, "En fazla 20 sembol sorgulanabilir.")

    bulunanlar: dict[str, dict] = {}
    eksikler: list[str] = []

    # 1. Memory cache
    for s in liste:
        cached = _cache_get(s)
        if cached:
            bulunanlar[s] = cached
        else:
            eksikler.append(s)

    # 2. Redis MGET — tek tur
    if eksikler:
        from redis_cache import rget_many
        redis_vals = rget_many([f"finans:abd:{s}" for s in eksikler])
        hala_eksik: list[str] = []
        now = time.time()
        for s in eksikler:
            v = redis_vals.get(f"finans:abd:{s}")
            if v:
                _cache[s] = (now, v)
                _stale[s] = v
                bulunanlar[s] = v
            else:
                hala_eksik.append(s)

        # 3. Redis'te olmayanlar — bg fetch tetikle
        for s in hala_eksik:
            result = _fetch(s)
            bulunanlar[s] = result if result is not None else _BEKLENIYOR(s)

    return {
        "sayı": len(liste),
        "para_birimi": "USD",
        "veriler": [bulunanlar.get(s, {"sembol": s, "hata": "bulunamadı"}) for s in liste],
    }


@router.get("/etf/{sembol}", summary="ETF fiyatı")
def abd_etf(sembol: str):
    ad = POPULER_ETFLER.get(sembol.upper(), sembol.upper())
    veri = _fetch(sembol.upper())
    return {**veri, "ad": ad, "tur": "ETF"}
