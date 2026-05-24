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
import time
import requests
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/fon", tags=["fon"])

_logger = logging.getLogger("uvicorn.error")
_TZ = ZoneInfo("Europe/Istanbul")
_TEFAS_SAAT = 630  # 10 * 60 + 30 — TEFAS'ın güncelleme saati

_TEFAS_URL = "https://www.tefas.gov.tr/api/DB/BindHistoryInfo"
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

def _tarih_aralik() -> tuple[str, str]:
    """TEFAS'ın beklediği DD.MM.YYYY formatında (bugün - 3 gün) aralığı döner."""
    bugun = date.today()
    baslangic = bugun - timedelta(days=3)
    fmt = lambda d: d.strftime("%d.%m.%Y")
    return fmt(baslangic), fmt(bugun)


def _tefas_session() -> requests.Session:
    """
    TEFAS için cookie'li oturum aç.
    Ana sayfa bir kez yüklenir → ASP.NET session cookie'si alınır.
    Bu cookie olmadan BindHistoryInfo boş data döndürür.
    """
    s = requests.Session()
    s.get("https://www.tefas.gov.tr/TarihselVeriler.aspx", headers=_HEADERS, timeout=10)
    return s


def _fetch_tefas(kod: str, session: requests.Session | None = None) -> dict:
    """
    TEFAS'tan tek fon için güncel birim pay değeri çek.
    session parametresi verilirse tekrar cookie alınmaz (toplu çekimde bunu kullan).
    """
    bas, bit = _tarih_aralik()
    payload = {
        "fontip": "YAT",
        "fonkod": kod.upper(),
        "bastarih": bas,
        "bittarih": bit,
    }
    s = session or _tefas_session()
    resp = s.post(_TEFAS_URL, data=payload, headers=_HEADERS, timeout=15)
    resp.raise_for_status()

    rows = resp.json().get("data", [])
    if not rows:
        raise ValueError(f"TEFAS'ta veri yok: {kod}")

    # En güncel satır (son tarih)
    rows.sort(key=lambda r: r.get("TARIH", ""), reverse=True)
    row = rows[0]

    fiyat_str = str(row.get("FIYAT") or "0").replace(",", ".")
    fiyat = round(float(fiyat_str), 6)
    if fiyat == 0:
        raise ValueError(f"TEFAS sıfır fiyat: {kod}")

    gunluk_str = str(row.get("GUNLUKGETIRI") or "0").replace(",", ".")
    gunluk = round(float(gunluk_str), 4)

    # Tarih: "/Date(1716336000000)/" veya "YYYY-MM-DD" formatı
    tarih_raw = str(row.get("TARIH", ""))
    if "/Date(" in tarih_raw:
        try:
            from datetime import timezone
            ms = int(tarih_raw.replace("/Date(", "").replace(")/", "").split("+")[0])
            tarih = date.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()
        except Exception:
            tarih = str(date.today())
    else:
        tarih = tarih_raw[:10] if len(tarih_raw) >= 10 else str(date.today())

    return {
        "kod": kod.upper(),
        "ad": POPULER_FONLAR.get(kod.upper(), str(row.get("FONUNVAN", kod.upper()))),
        "fiyat": fiyat,
        "degisim_yuzde": gunluk,
        "para_birimi": "TRY",
        "tarih": tarih,
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
