"""
BIST (Borsa İstanbul) endpoint'leri.
Birincil kaynak: Bigpara yeni REST API (api-bigpara.hurriyet.com.tr)
  - Sembol listesi: bigpara.hurriyet.com.tr/api/v1/hisse/list  (644 hisse)
  - Fiyat (batch): /api/trade/chart/daily/1h/last/1?symbols=... (max 19 sembol)
Fallback: yfinance
Piyasa saatleri: 10:10–18:30 TSİ, Pazartesi–Cuma
Kapalıyken Bigpara'ya istek atılmaz; son bilinen veri Redis/memory'den döner.
"""
from __future__ import annotations

import concurrent.futures as _cf
import logging
import time
import requests
import yfinance as yf
from datetime import date, datetime
from zoneinfo import ZoneInfo
from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/bist", tags=["bist"])

_logger = logging.getLogger("uvicorn.error")
_TZ = ZoneInfo("Europe/Istanbul")

# ── API Base URL'leri ─────────────────────────────────────────────────────────
_API_NEW   = "https://api-bigpara.hurriyet.com.tr/api/trade/chart/daily/1h/last/1"
_API_LIST  = "https://bigpara.hurriyet.com.tr/api/v1/hisse/list"

_HEADERS_NEW = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}
_HEADERS_V1 = {
    **_HEADERS_NEW,
    "Referer": "https://bigpara.hurriyet.com.tr/",
    "X-Requested-With": "XMLHttpRequest",
}

POPULER_HISSELER: dict[str, str] = {
    "THYAO": "Türk Hava Yolları",
    "AKBNK": "Akbank",
    "GARAN": "Garanti BBVA",
    "ISCTR": "İş Bankası C",
    "KCHOL": "Koç Holding",
    "SAHOL": "Sabancı Holding",
    "SISE":  "Şişe Cam",
    "ARCLK": "Arçelik",
    "TOASO": "Tofaş Oto.",
    "FROTO": "Ford Otosan",
    "EKGYO": "Emlak Konut GYO",
    "PETKM": "Petkim",
    "TUPRS": "Tüpraş",
    "EREGL": "Ereğli Demir Çelik",
    "BIMAS": "BİM Mağazaları",
    "MGROS": "Migros",
    "PGSUS": "Pegasus",
    "TAVHL": "TAV Havalimanları",
    "TCELL": "Turkcell",
    "KOZAL": "Koza Altın",
}

ENDEKSLER: dict[str, str] = {
    "XU100": "BIST 100",
    "XU050": "BIST 50",
    "XU030": "BIST 30",
    "XBANK": "BIST Banka",
    "XUSIN": "BIST Sınai",
}

# ── TTL cache (5 dk) + stale fallback ────────────────────────────────────────
_cache: dict[str, tuple[float, dict]] = {}
_stale: dict[str, dict] = {}
_TTL = 300  # 5 dakika

# Bigpara toplu veri cache'i
_bigpara_ts: float = 0.0
_bigpara_data: dict[str, dict] = {}
_BIGPARA_TTL = 270  # 4.5 dakika

# Sembol listesi cache'i (1 saat geçerli)
_sembol_listesi: list[str] = []
_sembol_listesi_ts: float = 0.0
_SEMBOL_TTL = 3600


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
    rset(f"finans:bist:{key}", val)


def _bist_acik() -> bool:
    """Şu an BIST piyasası açık mı? Hafta içi 10:10–18:30 TSİ."""
    now = datetime.now(_TZ)
    if now.weekday() >= 5:          # Cumartesi=5, Pazar=6
        return False
    t = now.hour * 60 + now.minute
    return 610 <= t <= 1110         # 10:10 = 610, 18:30 = 1110


def _get_sembol_listesi() -> list[str]:
    """Bigpara'dan tüm BIST hisse kodlarını çek (1 saatte bir güncellenir)."""
    global _sembol_listesi, _sembol_listesi_ts
    now = time.time()
    if _sembol_listesi and (now - _sembol_listesi_ts) < _SEMBOL_TTL:
        return _sembol_listesi
    r = requests.get(_API_LIST, headers=_HEADERS_V1, timeout=10)
    r.raise_for_status()
    codes = [h["kod"] for h in r.json().get("data", []) if h.get("kod")]
    if codes:
        _sembol_listesi = codes
        _sembol_listesi_ts = now
        _logger.info(f"Bigpara sembol listesi: {len(codes)} hisse")
    return _sembol_listesi


def _parse_current(curr: dict) -> dict:
    """Bigpara current objesini standart veri dict'e çevir."""
    return {
        "sembol": (curr.get("s") or "").strip(),
        "fiyat": round(float(curr.get("c") or 0), 4),
        "acilis": round(float(curr.get("o") or 0), 4),
        "yuksek": round(float(curr.get("h") or 0), 4),
        "dusuk": round(float(curr.get("l") or 0), 4),
        "hacim": int(curr.get("tv") or 0),
        "degisim_tl": round(float(curr.get("ch") or 0), 4),
        "degisim_yuzde": round(float(curr.get("p") or 0), 2),
        "para_birimi": "TRY",
        "tarih": str(date.today()),
        "kaynak": "bigpara",
    }


def _fetch_batch(group: list[str]) -> list[dict]:
    """Bir grup sembolü için Bigpara API'ye istek at (max 19 sembol)."""
    symbols = ",".join(group)
    resp = requests.get(
        f"{_API_NEW}?symbols={symbols}",
        headers=_HEADERS_NEW,
        timeout=15,
    )
    resp.raise_for_status()
    items = resp.json().get("data", [])
    results = []
    for item in items:
        curr = item.get("current", {})
        sembol = (curr.get("s") or "").strip()
        fiyat = float(curr.get("c") or 0)
        if sembol and fiyat > 0:
            results.append(_parse_current(curr))
    return results


def _bigpara_tumu(force: bool = False) -> dict[str, dict]:
    """
    Bigpara'dan tüm BIST hisselerini çek.
    1. Sembol listesini al (1 HTTP isteği, 644 kod)
    2. 19'luk gruplara böl (~34 grup)
    3. 5 thread ile paralel istek at
    Sonuç: {SEMBOL: veri_dict}
    """
    global _bigpara_ts, _bigpara_data
    now = time.time()

    if not force and _bigpara_data and (now - _bigpara_ts) < _BIGPARA_TTL:
        return _bigpara_data

    # Sembol listesi
    codes = _get_sembol_listesi()
    if not codes:
        raise ValueError("Bigpara sembol listesi boş")

    # 19'luk gruplara böl
    groups = [codes[i:i + 19] for i in range(0, len(codes), 19)]
    _logger.info(f"Bigpara: {len(codes)} hisse, {len(groups)} batch → paralel çekiliyor")

    result: dict[str, dict] = {}
    with _cf.ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(_fetch_batch, g): g for g in groups}
        for fut in _cf.as_completed(futures, timeout=30):
            try:
                items = fut.result()
                for veri in items:
                    sembol = veri["sembol"]
                    if sembol:
                        result[sembol] = veri
            except Exception as e:
                _logger.warning(f"Bigpara batch hata: {e}")

    if result:
        _bigpara_ts = now
        _bigpara_data = result
        _logger.info(f"Bigpara tumu ✓ {len(result)} hisse")

    return result


def _fetch_bigpara_single(sembol: str) -> dict:
    """Tek hisse için Bigpara yeni API (piyasa açık, batch cache'de yok)."""
    resp = requests.get(
        f"{_API_NEW}?symbols={sembol}",
        headers=_HEADERS_NEW,
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json().get("data", [])
    if not data:
        raise ValueError(f"Bigpara'da bulunamadı: {sembol}")
    curr = data[0].get("current", {})
    fiyat = float(curr.get("c") or 0)
    if fiyat == 0:
        raise ValueError(f"Bigpara sıfır fiyat: {sembol}")
    return _parse_current(curr)


def _ticker(sembol: str) -> str:
    sembol = sembol.upper().strip()
    if not sembol.endswith(".IS") and not sembol.startswith("^"):
        sembol = sembol + ".IS"
    return sembol


def _fetch_yfinance(sembol: str) -> dict:
    """yfinance fallback — tek hisse için."""
    key = sembol.upper()
    tick = yf.Ticker(_ticker(sembol))

    hist = tick.history(period="1d", interval="2m")
    hist = hist.dropna(subset=["Close"]) if not hist.empty else hist
    if hist.empty:
        hist = tick.history(period="5d")
        hist = hist.dropna(subset=["Close"])
    if hist.empty:
        raise HTTPException(404, f"Hisse bulunamadı veya veri yok: {key}")

    son = hist.iloc[-1]
    onceki = hist.iloc[-2] if len(hist) > 1 else hist.iloc[-1]
    kapanis = float(son["Close"])
    onceki_kapanis = float(onceki["Close"])
    degisim = kapanis - onceki_kapanis
    degisim_yuzde = (degisim / onceki_kapanis * 100) if onceki_kapanis else 0.0

    return {
        "sembol": key,
        "fiyat": round(kapanis, 4),
        "acilis": round(float(son.get("Open", 0)), 4),
        "yuksek": round(float(son.get("High", 0)), 4),
        "dusuk": round(float(son.get("Low", 0)), 4),
        "hacim": int(son.get("Volume", 0)),
        "degisim_tl": round(degisim, 4),
        "degisim_yuzde": round(degisim_yuzde, 2),
        "para_birimi": "TRY",
        "tarih": str(hist.index[-1].date()),
        "kaynak": "yfinance",
    }


def _fetch_info(sembol: str, force: bool = False) -> dict:
    key = sembol.upper()

    if not force:
        # 1. Memory cache
        cached = _cache_get(key)
        if cached:
            return cached

        # 2. Redis — yfinance/Bigpara'ya gitmeden
        from redis_cache import rget
        redis_val = rget(f"finans:bist:{key}")
        if redis_val:
            _cache[key] = (time.time(), redis_val)
            _stale[key] = redis_val
            return redis_val

    # 3. Piyasa açıksa Bigpara batch cache'ini dene
    if _bist_acik():
        # 3a. Toplu cache yeterlince tazeyse oradan al
        if not force and _bigpara_data and (time.time() - _bigpara_ts) < _BIGPARA_TTL:
            if key in _bigpara_data:
                veri = _bigpara_data[key]
                _cache_set(key, veri)
                return veri

        # 3b. Tek sembol Bigpara isteği
        try:
            result = _fetch_bigpara_single(key)
            _cache_set(key, result)
            return result
        except Exception as e:
            _logger.warning(f"Bigpara single hata ({key}): {e}")

    # 4. yfinance fallback
    try:
        result = _fetch_yfinance(key)
        _cache_set(key, result)
        return result
    except HTTPException:
        raise
    except Exception as e:
        if key in _stale:
            return _stale[key]
        raise HTTPException(503, f"Veri alınamadı: {key} — {e}")


def warm_up() -> list[str]:
    """
    Background worker çağrısı.
    Piyasa açıksa Bigpara'dan tüm BIST hisselerini çek,
    bellek + Redis pipeline'a yaz. Piyasa kapalıysa işlem yapmaz.
    """
    if not _bist_acik():
        return []

    try:
        tum = _bigpara_tumu(force=True)
        if not tum:
            return []

        now = time.time()
        redis_data: dict = {}
        for sembol, veri in tum.items():
            _cache[sembol] = (now, veri)
            _stale[sembol] = veri
            redis_data[f"finans:bist:{sembol}"] = veri

        # Tek pipeline: tüm hisseler Redis'e yazılır
        from redis_cache import rset_many
        rset_many(redis_data)

        _logger.info(f"BIST warm_up ✓ {len(tum)} hisse → Redis pipeline")
        return list(tum.keys())

    except Exception as e:
        _logger.warning(f"BIST warm_up ✗ {e}")
        return []


# ── Endpoint'ler ──────────────────────────────────────────────────────────────

@router.get("/liste", summary="Popüler BIST hisseleri listesi")
def liste():
    return {
        "hisseler": [{"sembol": s, "ad": a} for s, a in POPULER_HISSELER.items()],
        "endeksler": [{"sembol": s, "ad": a} for s, a in ENDEKSLER.items()],
    }


@router.get("/hisse/{sembol}", summary="Anlık hisse fiyatı")
def hisse_fiyat(sembol: str):
    return _fetch_info(sembol)


@router.get("/hisse/{sembol}/gecmis", summary="Geçmiş fiyat verisi")
def hisse_gecmis(
    sembol: str,
    period: str = Query("1mo", description="1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y",
                        pattern="^(1d|5d|1mo|3mo|6mo|1y|2y|5y)$"),
    aralik: str = Query("1d", description="Veri aralığı: 1m, 5m, 15m, 1h, 1d, 1wk, 1mo",
                        pattern="^(1m|5m|15m|1h|1d|1wk|1mo)$"),
):
    """Geçmiş BIST hisse verisi (OHLCV) — yfinance."""
    tick = yf.Ticker(_ticker(sembol))
    hist = tick.history(period=period, interval=aralik)
    if hist.empty:
        raise HTTPException(404, f"Hisse bulunamadı: {sembol.upper()}")
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
        "kayit_sayisi": len(kayitlar),
        "para_birimi": "TRY",
        "veriler": kayitlar,
    }


@router.get("/endeks/{sembol}", summary="Endeks değeri")
def endeks_fiyat(sembol: str):
    sembol = sembol.upper()
    yahoo_sembol = sembol + ".IS"
    key = f"ENDEKS_{sembol}"

    cached = _cache_get(key)
    if cached:
        return cached

    from redis_cache import rget
    redis_val = rget(f"finans:bist:{key}")
    if redis_val:
        _cache[key] = (time.time(), redis_val)
        _stale[key] = redis_val
        return redis_val

    try:
        tick = yf.Ticker(yahoo_sembol)
        hist = tick.history(period="2d")
        if hist.empty:
            raise HTTPException(404, f"Endeks bulunamadı: {sembol}")
        son = hist.iloc[-1]
        onceki = hist.iloc[-2] if len(hist) > 1 else hist.iloc[-1]
        kapanis = float(son["Close"])
        onceki_kapanis = float(onceki["Close"])
        degisim = kapanis - onceki_kapanis
        degisim_yuzde = (degisim / onceki_kapanis * 100) if onceki_kapanis else 0.0
        result = {
            "sembol": sembol,
            "ad": ENDEKSLER.get(sembol, sembol),
            "deger": round(kapanis, 2),
            "degisim": round(degisim, 2),
            "degisim_yuzde": round(degisim_yuzde, 2),
            "tarih": str(hist.index[-1].date()),
        }
        _cache_set(key, result)
        return result
    except HTTPException:
        raise
    except Exception as e:
        if key in _stale:
            return _stale[key]
        raise HTTPException(503, f"Endeks verisi alınamadı: {sembol} — {e}")


@router.get("/toplu", summary="Çoklu hisse fiyatı")
def toplu_fiyat(
    semboller: str = Query(..., description="Virgülle ayrılmış semboller. Örn: THYAO,AKBNK,GARAN")
):
    """Birden fazla hisseyi tek sorguda getir. Piyasa açıksa Bigpara batch kullanır."""
    liste = [s.strip().upper() for s in semboller.split(",") if s.strip()]
    if not liste:
        raise HTTPException(400, "En az bir sembol giriniz.")
    if len(liste) > 50:
        raise HTTPException(400, "En fazla 50 sembol sorgulanabilir.")

    sonuclar = []
    for sembol in liste:
        try:
            sonuclar.append(_fetch_info(sembol))
        except HTTPException:
            sonuclar.append({"sembol": sembol, "hata": "veri bulunamadı"})

    return {"sayı": len(sonuclar), "veriler": sonuclar}


@router.get("/piyasa", summary="Piyasa durum bilgisi")
def piyasa_durumu():
    """BIST şu an açık mı? Bigpara cache kaç hisse içeriyor?"""
    return {
        "acik": _bist_acik(),
        "bigpara_cache_adet": len(_bigpara_data),
        "bigpara_cache_yas_sn": round(time.time() - _bigpara_ts, 1) if _bigpara_ts else None,
        "sembol_listesi_adet": len(_sembol_listesi),
    }
