"""
Türk yatırım fonu endpoint'leri.
Veri kaynağı: Bigpara fon listesi API (aynı Bigpara BIST için kullandığımız pattern)
  - Birim pay değeri (NAV), günlük getiri, fon adı
  - Bigpara fon verisi her iş günü ~10:30 İstanbul saatinde güncellenir

Çekme zamanı:
  - Saat 10:30 öncesi → Bigpara'ya gidilmez, stale/Redis veri döndürülür
  - Saat 10:30 sonrası → bugünün verisi yoksa Bigpara'dan çekilir
  - Bugünün verisi cache'e yazıldıktan sonra ertesi 10:30'a kadar bir daha gidilmez
"""
from __future__ import annotations

import logging
import time
import requests
from datetime import date, datetime
from zoneinfo import ZoneInfo
from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/fon", tags=["fon"])

_logger = logging.getLogger("uvicorn.error")
_TZ = ZoneInfo("Europe/Istanbul")
_TEFAS_SAAT = 630  # 10 * 60 + 30 — TEFAS'ın güncelleme saati


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


# Bigpara fon API (V1 — hisse listesiyle aynı yetkilendirme paterni)
_BP_FON_LIST  = "https://bigpara.hurriyet.com.tr/api/v1/fon/list"
_BP_FON_DETAY = "https://bigpara.hurriyet.com.tr/api/v1/fon/detay/{kod}"

_BP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Referer": "https://bigpara.hurriyet.com.tr/fonlar/",
    "X-Requested-With": "XMLHttpRequest",
}

# Bigpara toplu fon cache'i (5 dk)
_bp_fon_ts: float = 0.0
_bp_fon_data: dict[str, dict] = {}
_BP_FON_TTL = 300


def _bigpara_fon_tumu(force: bool = False) -> dict[str, dict]:
    """
    Bigpara'dan tüm yatırım fonu listesini çek.
    Döndürülen dict: {FON_KODU: {kod, ad, fiyat, degisim_yuzde, ...}}
    """
    global _bp_fon_ts, _bp_fon_data
    now = time.time()
    if not force and _bp_fon_data and (now - _bp_fon_ts) < _BP_FON_TTL:
        return _bp_fon_data

    resp = requests.get(_BP_FON_LIST, headers=_BP_HEADERS, timeout=15)
    resp.raise_for_status()
    items = resp.json().get("data", [])
    if not items:
        raise ValueError("Bigpara fon listesi boş")

    result: dict[str, dict] = {}
    for item in items:
        kod = str(item.get("kod") or item.get("fonkodu") or "").strip().upper()
        if not kod:
            continue
        fiyat_raw = str(item.get("birimPayDegeri") or item.get("fiyat") or "0").replace(",", ".")
        try:
            fiyat = round(float(fiyat_raw), 6)
        except Exception:
            fiyat = 0.0
        if fiyat == 0:
            continue
        getiri_raw = str(item.get("gunlukGetiri") or item.get("getiri") or "0").replace(",", ".")
        try:
            gunluk = round(float(getiri_raw), 4)
        except Exception:
            gunluk = 0.0

        result[kod] = {
            "kod": kod,
            "ad": POPULER_FONLAR.get(kod, str(item.get("fonUnvani") or item.get("ad") or kod)),
            "fiyat": fiyat,
            "degisim_yuzde": gunluk,
            "para_birimi": "TRY",
            "tarih": str(date.today()),
            "kaynak": "bigpara",
        }

    if result:
        _bp_fon_ts = now
        _bp_fon_data = result
        _logger.info(f"Bigpara fon listesi ✓ {len(result)} fon")
    return result


def _fetch_bigpara(kod: str) -> dict:
    """Bigpara fon listesinden tek fon verisini çek."""
    tum = _bigpara_fon_tumu()
    if kod.upper() in tum:
        return tum[kod.upper()]
    # Listede yoksa detay endpoint'ini dene
    url = _BP_FON_DETAY.format(kod=kod.upper())
    resp = requests.get(url, headers=_BP_HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json().get("data", {})
    if not data:
        raise ValueError(f"Bigpara'da fon bulunamadı: {kod}")
    fiyat_raw = str(data.get("birimPayDegeri") or data.get("fiyat") or "0").replace(",", ".")
    fiyat = round(float(fiyat_raw), 6)
    if fiyat == 0:
        raise ValueError(f"Bigpara sıfır fiyat: {kod}")
    getiri_raw = str(data.get("gunlukGetiri") or "0").replace(",", ".")
    return {
        "kod": kod.upper(),
        "ad": POPULER_FONLAR.get(kod.upper(), str(data.get("fonUnvani") or kod.upper())),
        "fiyat": fiyat,
        "degisim_yuzde": round(float(getiri_raw), 4),
        "para_birimi": "TRY",
        "tarih": str(date.today()),
        "kaynak": "bigpara",
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

    # 4. 10:30 sonrası + cache miss → Bigpara'dan çek
    try:
        result = _fetch_bigpara(key)
        _cache_set(key, result)
        return result
    except Exception as e:
        _logger.warning(f"Bigpara fon hata ({key}): {e}")
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

    # Bigpara'dan tüm fon listesini tek seferde çek, popüler fonları cache'e yaz
    try:
        tum = _bigpara_fon_tumu(force=True)
    except Exception as e:
        _logger.warning(f"Fon warm_up: Bigpara fon listesi alınamadı: {e}")
        return list(_stale.keys())

    basarili: list[str] = []
    for kod in POPULER_FONLAR:
        if _bugunun_verisi_var_mi(kod):
            basarili.append(kod)
            continue
        if kod in tum:
            _cache_set(kod, tum[kod])
            basarili.append(kod)
            _logger.debug(f"Fon warm_up ✓ {kod}")
        else:
            _logger.warning(f"Fon warm_up ✗ {kod}: Bigpara listesinde yok")
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
