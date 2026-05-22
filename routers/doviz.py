"""
Döviz kuru endpoint'leri.
yfinance — Yahoo Finance currency pairs (=X suffix).
5 dakika TTL cache + stale fallback (yfinance spike koruması).
"""
from __future__ import annotations

import threading
import time
import yfinance as yf
from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/doviz", tags=["döviz"])

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
    rset(f"finans:doviz:{key}", val)


# Desteklenen TL kurları
TL_KURLAR: dict[str, str] = {
    "USD": "Amerikan Doları",
    "EUR": "Euro",
    "GBP": "İngiliz Sterlini",
    "CHF": "İsviçre Frangı",
    "JPY": "Japon Yeni",
    "RUB": "Rus Rublesi",
    "AUD": "Avustralya Doları",
    "CAD": "Kanada Doları",
    "SEK": "İsveç Kronası",
    "NOK": "Norveç Kronası",
    "DKK": "Danimarka Kronası",
    "KWD": "Kuveyt Dinarı",
    "AED": "Birleşik Arap Emirlikleri Dirhemi",
    "QAR": "Katar Riyali",
}

# Popüler çapraz kurlar (TRY dışı)
CAPRAZ_KURLAR: dict[str, str] = {
    "EURUSD": "Euro/Dolar",
    "GBPUSD": "Sterlin/Dolar",
    "USDJPY": "Dolar/Yen",
    "USDCHF": "Dolar/Frank",
    "AUDUSD": "Avustralya Doları/Dolar",
    "USDCAD": "Dolar/Kanada Doları",
    "XAGUSD": "Gümüş/Dolar",
}

# Startup warm-up'ta ısıtılacak dövizler
WARM_DOVIZLER = ["USD", "EUR", "GBP", "CHF", "JPY", "AUD", "CAD"]


_bg_in_progress: set[str] = set()


def _bg_fetch_doviz(sembol: str) -> None:
    """Redis miss: arka planda yfinance'den çek, cache'e yaz."""
    if sembol in _bg_in_progress:
        return
    _bg_in_progress.add(sembol)

    def _run():
        try:
            tick = yf.Ticker(sembol)
            hist = tick.history(period="2d")
            if hist.empty:
                return
            son = hist.iloc[-1]
            onceki = hist.iloc[-2] if len(hist) > 1 else hist.iloc[-1]
            kapanis = round(float(son["Close"]), 6)
            onceki_kapanis = float(onceki["Close"])
            degisim = round(kapanis - onceki_kapanis, 6)
            degisim_yuzde = round((degisim / onceki_kapanis) * 100, 4) if onceki_kapanis else 0
            result = {
                "kur": kapanis,
                "degisim": degisim,
                "degisim_yuzde": degisim_yuzde,
                "acilis": round(float(son["Open"]), 6),
                "yuksek": round(float(son["High"]), 6),
                "dusuk": round(float(son["Low"]), 6),
                "tarih": str(hist.index[-1].date()),
            }
            _cache_set(sembol, result)
        except Exception:
            pass
        finally:
            _bg_in_progress.discard(sembol)

    threading.Thread(target=_run, daemon=True).start()


def _fetch_kur(sembol: str, force: bool = False) -> dict | None:
    """Redis-first döviz çekimi. force=False → asla yfinance bekletmez."""
    if not force:
        # 1. Memory cache
        cached = _cache_get(sembol)
        if cached:
            return cached
        # 2. Redis
        from redis_cache import rget
        redis_val = rget(f"finans:doviz:{sembol}")
        if redis_val:
            _cache[sembol] = (time.time(), redis_val)
            _stale[sembol] = redis_val
            return redis_val
        # 3. Redis miss → arka planda çek, stale veya boş dict döndür
        _bg_fetch_doviz(sembol)
        return _stale.get(sembol) or {"kur": None, "bekleniyor": True}

    # force=True → warm_up / background worker
    try:
        tick = yf.Ticker(sembol)
        hist = tick.history(period="2d")
        if hist.empty:
            raise HTTPException(404, f"Kur verisi bulunamadı: {sembol}")
        son = hist.iloc[-1]
        onceki = hist.iloc[-2] if len(hist) > 1 else hist.iloc[-1]
        kapanis = round(float(son["Close"]), 6)
        onceki_kapanis = float(onceki["Close"])
        degisim = round(kapanis - onceki_kapanis, 6)
        degisim_yuzde = round((degisim / onceki_kapanis) * 100, 4) if onceki_kapanis else 0
        result = {
            "kur": kapanis,
            "degisim": degisim,
            "degisim_yuzde": degisim_yuzde,
            "acilis": round(float(son["Open"]), 6),
            "yuksek": round(float(son["High"]), 6),
            "dusuk": round(float(son["Low"]), 6),
            "tarih": str(hist.index[-1].date()),
        }
        _cache_set(sembol, result)
        return result
    except HTTPException:
        raise
    except Exception as e:
        return _stale.get(sembol)


def warm_up():
    """Background worker: WARM_DOVIZLER için yfinance'den taze veri çek."""
    basarili = []
    for doviz in WARM_DOVIZLER:
        try:
            _fetch_kur(f"{doviz}TRY=X", force=True)
            basarili.append(doviz)
        except Exception:
            pass
    return basarili


@router.get("/liste", summary="Desteklenen para birimleri")
def doviz_liste():
    """TL karşısında takip edilebilen para birimleri."""
    return {
        "tl_karsi": [{"kod": k, "ad": v} for k, v in TL_KURLAR.items()],
        "capraz": [{"kod": k, "ad": v} for k, v in CAPRAZ_KURLAR.items()],
    }


@router.get("/tl/{doviz}", summary="TL kuru")
def doviz_tl(doviz: str):
    """
    Bir dövizin TL karşılığı.

    - `/doviz/tl/USD` → Dolar/TL
    - `/doviz/tl/EUR` → Euro/TL
    - `/doviz/tl/GBP` → Sterlin/TL
    """
    doviz = doviz.upper().strip()
    sembol = f"{doviz}TRY=X"
    ad = TL_KURLAR.get(doviz, f"{doviz}/TL")
    veri = _fetch_kur(sembol)
    return {
        "baz": doviz,
        "karsi": "TRY",
        "sembol": sembol,
        "ad": ad,
        **veri,
    }


@router.get("/tl", summary="Tüm TL kurları")
def doviz_tl_hepsi(
    kodlar: str = Query(
        "USD,EUR,GBP,CHF,JPY",
        description="Virgülle ayrılmış döviz kodları. Örn: USD,EUR,GBP",
    )
):
    """Birden fazla dövizin TL kurunu tek sorguda getir (cache'li)."""
    liste = [k.strip().upper() for k in kodlar.split(",") if k.strip()]
    if not liste:
        raise HTTPException(400, "En az bir döviz kodu giriniz.")
    if len(liste) > 15:
        raise HTTPException(400, "En fazla 15 döviz sorgulanabilir.")

    sonuclar = {}
    for doviz in liste:
        sembol = f"{doviz}TRY=X"
        try:
            veri = _fetch_kur(sembol)
            sonuclar[doviz] = {
                "ad": TL_KURLAR.get(doviz, doviz),
                **veri,
            }
        except HTTPException:
            sonuclar[doviz] = {"hata": "veri bulunamadı"}

    return {"para_birimi": "TRY", "kurlar": sonuclar}


@router.get("/capraz/{cift}", summary="Çapraz kur")
def doviz_capraz(cift: str):
    """
    İki döviz arasındaki kur.

    - `/doviz/capraz/EURUSD` → Euro/Dolar
    - `/doviz/capraz/USDJPY` → Dolar/Yen
    """
    cift = cift.upper().strip().replace("/", "").replace("-", "")
    if len(cift) != 6:
        raise HTTPException(400, "Format: 6 karakter BAZKARSI (Örn: EURUSD)")
    baz = cift[:3]
    karsi = cift[3:]
    sembol = f"{cift}=X"
    ad = CAPRAZ_KURLAR.get(cift, f"{baz}/{karsi}")
    veri = _fetch_kur(sembol)
    return {
        "baz": baz,
        "karsi": karsi,
        "sembol": sembol,
        "ad": ad,
        **veri,
    }


@router.get("/gecmis/{cift}", summary="Geçmiş kur verisi")
def doviz_gecmis(
    cift: str,
    period: str = Query("1mo", description="1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y",
                        pattern="^(1d|5d|1mo|3mo|6mo|1y|2y|5y)$"),
    aralik: str = Query("1d", description="1d, 1wk, 1mo",
                        pattern="^(1d|1wk|1mo)$"),
):
    """
    Geçmiş kur verisi.

    - `/doviz/gecmis/USDTRY?period=1mo` → Son 1 ay USD/TRY
    - `/doviz/gecmis/EURUSD?period=1y&aralik=1wk` → Son 1 yıl haftalık
    """
    cift = cift.upper().strip().replace("/", "").replace("-", "")
    sembol = cift if cift.endswith("=X") else f"{cift}=X"
    tick = yf.Ticker(sembol)
    hist = tick.history(period=period, interval=aralik)
    if hist.empty:
        raise HTTPException(404, f"Kur verisi bulunamadı: {cift}")
    kayitlar = [
        {
            "tarih": str(idx.date()),
            "acilis": round(float(row["Open"]), 6),
            "yuksek": round(float(row["High"]), 6),
            "dusuk": round(float(row["Low"]), 6),
            "kapanis": round(float(row["Close"]), 6),
        }
        for idx, row in hist.iterrows()
    ]
    return {
        "cift": cift,
        "sembol": sembol,
        "period": period,
        "aralik": aralik,
        "kayit_sayisi": len(kayitlar),
        "veriler": kayitlar,
    }
