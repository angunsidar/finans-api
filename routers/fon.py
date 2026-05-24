"""
Türk yatırım fonu (TEFAS) endpoint'leri.
Birincil kaynak: TEFAS resmi web servisi (BindHistoryInfo)
  - Birim pay değeri (NAV), günlük getiri, fon adı
  - TEFAS her iş günü ~10:30 İstanbul saatinde önceki günün değerini yayınlar

Çekme zamanı:
  - Saat 10:30 öncesi → TEFAS'a gidilmez, stale/Redis veri döndürülür
  - Saat 10:30 sonrası → bugünün verisi yoksa TEFAS'tan çekilir
  - Bugünün verisi cache'e yazıldıktan sonra ertesi 10:30'a kadar bir daha gidilmez
"""
from __future__ import annotations

import logging
import re
import time
import requests
from datetime import date, datetime
from zoneinfo import ZoneInfo
from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/fon", tags=["fon"])

_logger = logging.getLogger("uvicorn.error")
_TZ = ZoneInfo("Europe/Istanbul")
_TEFAS_SAAT = 630  # 10 * 60 + 30 — TEFAS'ın güncelleme saati

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/javascript, */*",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://www.tefas.gov.tr",
    "Referer": "https://www.tefas.gov.tr/FonAnaliz.aspx",
    "X-Requested-With": "XMLHttpRequest",
}

POPULER_FONLAR: dict[str, str] = {
    "YAS": "Yapı Kredi Portföy Altın Fonu",
    "GAF": "Garanti BBVA Portföy Altın Fonu",
    "TKF": "Türkiye Kurumsal Yönetim End. Fonu",
    "AKF": "Ak Portföy Birinci Fon",
    "MAC": "Marmara Cap. Türkiye Fonu",
    "IPB": "İş Portföy BIST Banka End. Fonu",
    "TTE": "TEB Portföy Tahvil Fonu",
    "AFT": "Ak Portföy Kısa Vad. Tahvil Fonu",
}

# ── Cache + stale fallback ────────────────────────────────────────────────────
# TTL sadece güvenlik ağı — asıl tazelik kontrolü veri tarihi üzerinden yapılır
_cache: dict[str, tuple[float, dict]] = {}
_stale: dict[str, dict] = {}
_TTL = 86400  # 24 saat


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
    rset(f"finans:fon:{key}", val)


# ── Saat + tarih yardımcıları ─────────────────────────────────────────────────

def _tefas_hazir() -> bool:
    """Saat 10:30 İstanbul geçti mi? TEFAS yalnızca o saatten sonra güncel veri yayınlar."""
    now = datetime.now(_TZ)
    return now.hour * 60 + now.minute >= _TEFAS_SAAT


def _bugunun_verisi_var_mi(kod: str) -> bool:
    """
    Cache veya stale'deki verinin tarihi bugüne mi ait?
    TEFAS T+1 yayınlar: Pazartesi kapanış → Salı 10:30'da yayınlanır.
    Bu yüzden hafta içi işlem günlerinde verinin tarihi bir gün öncesi olabilir.
    Önemli olan bugün TEFAS'tan alınmış olması — bunu ts (timestamp) ile izliyoruz.
    """
    if kod not in _cache:
        return False
    ts, _ = _cache[kod]
    # Cache yazılma saati bugün 10:30 sonrasıysa taze kabul et
    yazilma = datetime.fromtimestamp(ts, tz=_TZ)
    bugun = datetime.now(_TZ).date()
    return yazilma.date() == bugun and yazilma.hour * 60 + yazilma.minute >= _TEFAS_SAAT


# ── TEFAS veri çekme ──────────────────────────────────────────────────────────


_TEFAS_FON_URL = "https://www.tefas.gov.tr/FonAnaliz.aspx"


def _tefas_session() -> requests.Session:
    """Basit session — TEFAS sayfası için User-Agent yeterli."""
    s = requests.Session()
    s.headers.update(_HEADERS)
    return s


def _fetch_tefas(kod: str, session: requests.Session | None = None) -> dict:
    """
    TEFAS FonAnaliz sayfasını HTML olarak çek, regex ile fiyat ve getiriyi ayıkla.
    TEFAS birim pay değerleri standart olarak 6 ondalık basamak kullanır (örn: 13,784033).
    Bu pattern sayfada yalnızca fon fiyatına ait olduğundan güvenle seçilebilir.
    """
    s = session or _tefas_session()
    resp = s.get(
        _TEFAS_FON_URL,
        params={"FonKod": kod.upper()},
        timeout=15,
    )
    resp.raise_for_status()
    html = resp.text

    # Birim pay değeri: tam olarak 6 ondalık basamak (örn: 13,784033 veya 0,012345)
    fiyat_match = re.search(r"\b(\d{1,6}[,\.]\d{6})\b", html)
    if not fiyat_match:
        raise ValueError(f"TEFAS fiyat bulunamadı: {kod} (sayfa yüklendi ama değer yok)")

    fiyat = round(float(fiyat_match.group(1).replace(",", ".")), 6)
    if fiyat == 0:
        raise ValueError(f"TEFAS sıfır fiyat: {kod}")

    # Günlük getiri: %XX,XX veya -XX,XX formatı (2-4 ondalık)
    # "Son Fiyat" bloğu yakınındaki ilk getiri değerini al
    gunluk = 0.0
    getiri_match = re.search(
        r"([+-]?\d{1,3}[,\.]\d{2,4})\s*(?:&#37;|%|<span[^>]*>%)",
        html,
    )
    if getiri_match:
        try:
            gunluk = round(float(getiri_match.group(1).replace(",", ".")), 4)
        except Exception:
            gunluk = 0.0

    return {
        "kod": kod.upper(),
        "ad": POPULER_FONLAR.get(kod.upper(), kod.upper()),
        "fiyat": fiyat,
        "degisim_yuzde": gunluk,
        "para_birimi": "TRY",
        "tarih": str(date.today()),
        "kaynak": "tefas",
    }


def _fetch(kod: str) -> dict | None:
    """
    Tek fon için veri döndür.
    - Memory cache tazeyse direkt döner
    - Redis'te varsa yükler; 10:30 öncesiyse veya bugün zaten çekildiyse döner
    - 10:30 sonrası + bugün henüz çekilmemişse TEFAS'a gider
    - Her durumda TEFAS'a ulaşamazsa stale döner
    """
    key = kod.upper()

    # 1. Memory cache — timestamp bazlı tazelik
    if _bugunun_verisi_var_mi(key):
        _, val = _cache[key]
        return val

    # 2. Redis
    from redis_cache import rget
    redis_val = rget(f"finans:fon:{key}")
    if redis_val:
        _cache[key] = (time.time(), redis_val)
        _stale[key] = redis_val
        # 10:30 öncesi veya bugün zaten çekildiyse Redis yeterli
        if not _tefas_hazir():
            return redis_val

    # 3. 10:30 öncesiyse TEFAS'a gitme
    if not _tefas_hazir():
        return _stale.get(key)

    # 4. 10:30 sonrası + cache miss → TEFAS'tan çek
    try:
        result = _fetch_tefas(key)
        _cache_set(key, result)
        return result
    except Exception as e:
        _logger.warning(f"TEFAS hata ({key}): {e}")
        return _stale.get(key)


def warm_up() -> list[str]:
    """
    Startup / background worker çağrısı.

    Saat 10:30 İstanbul öncesi:
      → TEFAS'a gidilmez, stale liste döner (önceki günün verisi yeterli)

    Saat 10:30 sonrası:
      → Bugün henüz çekilmemiş fonlar TEFAS'tan alınır
      → Bugün zaten çekilmişler atlanır (BG worker her 5 dk çağırır, tekrar gitmez)
    """
    if not _tefas_hazir():
        _logger.info("Fon warm_up atlandı — saat 10:30 öncesi")
        return list(_stale.keys())

    # Tek session → bir kez cookie al, tüm fonlara kullan (8 fon = 1+8 istek, 16 değil)
    try:
        session = _tefas_session()
    except Exception as e:
        _logger.warning(f"Fon warm_up: TEFAS session açılamadı: {e}")
        return list(_stale.keys())

    basarili: list[str] = []
    for kod in POPULER_FONLAR:
        if _bugunun_verisi_var_mi(kod):
            basarili.append(kod)
            continue
        try:
            result = _fetch_tefas(kod, session=session)
            _cache_set(kod, result)
            basarili.append(kod)
            _logger.debug(f"Fon warm_up ✓ {kod}")
        except Exception as e:
            _logger.warning(f"Fon warm_up ✗ {kod}: {e}")
    return basarili


# ── Endpoint'ler ──────────────────────────────────────────────────────────────

@router.get("/liste", summary="Popüler Türk yatırım fonları listesi")
def liste():
    return {
        "fonlar": [{"kod": k, "ad": a} for k, a in POPULER_FONLAR.items()],
    }


@router.get("/fon/{kod}", summary="Tek fon birim pay değeri")
def fon_fiyat(kod: str):
    result = _fetch(kod)
    if result is None:
        raise HTTPException(404, f"Fon verisi alınamadı: {kod.upper()}")
    return result


@router.get("/toplu", summary="Çoklu fon birim pay değeri")
def toplu_fiyat(
    fonlar: str = Query(..., description="Virgülle ayrılmış fon kodları. Örn: YAS,GAF,TKF")
):
    """
    Birden fazla fonu tek sorguda getir.
    Önce memory cache, sonra Redis, gerekirse TEFAS (10:30 sonrası).
    """
    liste = [k.strip().upper() for k in fonlar.split(",") if k.strip()]
    if not liste:
        raise HTTPException(400, "En az bir fon kodu giriniz.")
    if len(liste) > 30:
        raise HTTPException(400, "En fazla 30 fon sorgulanabilir.")

    bulunanlar: dict[str, dict] = {}
    eksikler: list[str] = []

    # 1. Memory cache (timestamp bazlı)
    for kod in liste:
        if _bugunun_verisi_var_mi(kod):
            _, val = _cache[kod]
            bulunanlar[kod] = val
        else:
            eksikler.append(kod)

    # 2. Redis MGET
    if eksikler:
        from redis_cache import rget_many
        redis_vals = rget_many([f"finans:fon:{k}" for k in eksikler])
        hala_eksik: list[str] = []
        now = time.time()
        for k in eksikler:
            v = redis_vals.get(f"finans:fon:{k}")
            if v:
                _cache[k] = (now, v)
                _stale[k] = v
                # 10:30 öncesiyse Redis yeterli
                if not _tefas_hazir():
                    bulunanlar[k] = v
                else:
                    hala_eksik.append(k)
            else:
                hala_eksik.append(k)

        # 3. TEFAS (yalnızca 10:30 sonrası ve cache miss)
        for k in hala_eksik:
            result = _fetch(k)
            if result is not None:
                bulunanlar[k] = result

    veriler = []
    for k in liste:
        if k in bulunanlar:
            veriler.append(bulunanlar[k])
        else:
            veriler.append({"kod": k, "fiyat": None, "durum": "bulunamadı"})

    return {"sayı": len(liste), "veriler": veriler}
