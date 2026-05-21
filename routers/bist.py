"""
BIST (Borsa İstanbul) endpoint'leri.
Birincil kaynak: Bigpara (tek HTTP isteğiyle tüm BIST)
Fallback: yfinance
Piyasa saatleri: 10:10–18:30 TSİ, Pazartesi–Cuma
Kapalıyken Bigpara'ya istek atılmaz; son bilinen veri Redis/memory'den döner.
"""
from __future__ import annotations

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

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; FinansAPI/1.0)",
    "Accept": "application/json",
}

# ── TTL cache (5 dk) + stale fallback ────────────────────────────────────────
_cache: dict[str, tuple[float, dict]] = {}
_stale: dict[str, dict] = {}
_TTL = 300  # 5 dakika

# Bigpara toplu veri cache'i (tüm hisseler birlikte)
_bigpara_ts: float = 0.0
_bigpara_data: dict[str, dict] = {}
_BIGPARA_TTL = 270  # 4.5 dakika — TTL'den biraz daha kısa


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
    return 610 <= t <= 1110         # 10:10 = 610 dk, 18:30 = 1110 dk


def _tr_float(x) -> float:
    """Türkçe sayı formatını float'a çevir.
    "1.234,56" → 1234.56  |  "230,50" → 230.50  |  "230.50" → 230.50
    """
    if x is None:
        return 0.0
    s = str(x).strip()
    if not s or s in ("-", "—"):
        return 0.0
    # Türkçe format: binlik ayraç ".", ondalık ","
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _bigpara_tumu(force: bool = False) -> dict[str, dict]:
    """
    Bigpara'dan tüm BIST hisselerini tek HTTP isteğiyle çek.
    Sonuç: {SEMBOL: veri_dict}
    """
    global _bigpara_ts, _bigpara_data

    now = time.time()
    if not force and _bigpara_data and (now - _bigpara_ts) < _BIGPARA_TTL:
        return _bigpara_data

    url = "https://bigpara.hurriyet.com.tr/api/borsa/hisselisting/data/?ptype=N"
    resp = requests.get(url, headers=_HEADERS, timeout=15)
    resp.raise_for_status()
    payload = resp.json()

    # Yanıt yapısı: {"data": {"LISTE": [...]}} veya {"LISTE": [...]}
    liste = (payload.get("data") or {}).get("LISTE") or payload.get("LISTE") or []
    if not liste:
        raise ValueError(f"Bigpara boş yanıt: {str(payload)[:300]}")

    result: dict[str, dict] = {}
    today = str(date.today())

    for h in liste:
        try:
            sembol = (
                h.get("KAP_SEMBOL") or h.get("SEMBOL") or h.get("kod", "")
            ).strip()
            if not sembol:
                continue

            fiyat = _tr_float(h.get("SONDEGER") or h.get("KAPANIS") or h.get("son"))
            if fiyat == 0:
                continue

            # Hacim: Türkçe format "1.234.567" → int
            hacim_raw = str(h.get("HACIM") or h.get("hacim") or "0")
            hacim_str = hacim_raw.replace(".", "").replace(",", "").strip()
            hacim = int(hacim_str) if hacim_str.lstrip("-").isdigit() else 0

            result[sembol] = {
                "sembol": sembol,
                "fiyat": round(fiyat, 4),
                "acilis": round(_tr_float(h.get("ACILIS") or h.get("acilis")), 4),
                "yuksek": round(_tr_float(h.get("YUKSEK") or h.get("yuksek")), 4),
                "dusuk": round(_tr_float(h.get("DUSUK") or h.get("dusuk")), 4),
                "hacim": hacim,
                "degisim_tl": round(
                    _tr_float(h.get("FARK") or h.get("fark")), 4
                ),
                "degisim_yuzde": round(
                    _tr_float(h.get("YUKDE") or h.get("YUZDE") or h.get("yuzde")), 2
                ),
                "para_birimi": "TRY",
                "tarih": today,
                "kaynak": "bigpara",
            }
        except Exception:
            continue

    if result:
        _bigpara_ts = now
        _bigpara_data = result
        _logger.debug(f"Bigpara: {len(result)} hisse alındı")

    return result


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

    # 3. Piyasa açıksa Bigpara toplu cache'inden doldur
    if _bist_acik():
        try:
            batch = _bigpara_tumu(force=force)
            if key in batch:
                veri = batch[key]
                _cache_set(key, veri)
                return veri
        except Exception as e:
            _logger.warning(f"Bigpara hata ({key}): {e}")

    # 4. yfinance fallback (piyasa kapalıysa veya Bigpara başarısızsa)
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
    Piyasa açıksa Bigpara'dan tüm BIST hisselerini çek, bellek + Redis pipeline'a yaz.
    Piyasa kapalıysa hiçbir şey yapmaz (Bigpara isteği atılmaz).
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
    semboller: str = Query(..., description="Virgülle ayrılmış hisse sembolleri. Örn: THYAO,AKBNK,GARAN")
):
    """Birden fazla hisseyi tek sorguda getir (cache'li, Bigpara batch)."""
    liste = [s.strip() for s in semboller.split(",") if s.strip()]
    if not liste:
        raise HTTPException(400, "En az bir sembol giriniz.")
    if len(liste) > 50:
        raise HTTPException(400, "En fazla 50 sembol sorgulanabilir.")

    sonuclar = []
    for sembol in liste:
        try:
            sonuclar.append(_fetch_info(sembol))
        except HTTPException:
            sonuclar.append({"sembol": sembol.upper(), "hata": "veri bulunamadı"})

    return {"sayı": len(sonuclar), "veriler": sonuclar}


@router.get("/piyasa", summary="Piyasa durum bilgisi")
def piyasa_durumu():
    """BIST şu an açık mı? Bigpara cache kaç hisse içeriyor?"""
    return {
        "acik": _bist_acik(),
        "bigpara_cache_adet": len(_bigpara_data),
        "bigpara_cache_yas_sn": round(time.time() - _bigpara_ts, 1) if _bigpara_ts else None,
    }
