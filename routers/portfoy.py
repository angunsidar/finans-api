"""
KAP Fon Portföy Dağılım Raporu endpoint'leri.

Pipeline:
  1. RSC parse → mkkMemberOid   (fon bildirimleri sayfası)
  2. RSC parse → disclosureIndex (bildirim-sorgu-sonuc sayfası)
  3. JSON API  → attachments[0].objId (/tr/api/notification/attachment-detail/{idx})
  4. PDF indir + pdfplumber parse → holdings
  5. Redis cache (TTL 25 gün) — aylık yayın

Endpoint'ler:
  GET /portfoy/{kod}          → tek fon portföyü
  POST /portfoy/seed           → GitHub Actions / harici seed
"""
from __future__ import annotations

import io
import logging
import re
import time

import httpx

try:
    import pdfplumber
    _PDF_OK = True
except ImportError:
    _PDF_OK = False

from fastapi import APIRouter, HTTPException, Request

router = APIRouter(prefix="/portfoy", tags=["portfoy"])
_logger = logging.getLogger("uvicorn.error")

# ── Sabitler ─────────────────────────────────────────────────────────────────
_KAP   = "https://kap.org.tr"
_PORTFOY_BILDIRIM_OID = "8aca490d502e34b801502e380044002b"  # Portföy Dağılım Raporu

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "tr-TR,tr;q=0.9",
    "Referer": _KAP,
}

# ── Fon kodu → KAP slug eşlemesi (popüler fonlar) ──────────────────────────
# Slug = "{kod}-{tefas-unvan-slug}"  (küçük harf, Türkçe karakter → latin)
_SLUG_MAP: dict[str, str] = {
    "TLY": "tly-tera-portfoy-birinci-serbest-fon",
    "YAS": "yas-yapi-kredi-portfoy-altin-fonu",
    "GAF": "gaf-garanti-bbva-portfoy-altin-fonu",
    "MAC": "mac-marmara-cap-turkiye-fonu",
    "IPB": "ipb-is-portfoy-bist-banka-endeksi-fonu",
    "TTE": "tte-teb-portfoy-tahvil-fonu",
    "AFT": "aft-ak-portfoy-kisa-vad-tahvil-fonu",
    "AKF": "akf-ak-portfoy-birinci-fon",
    "TKF": "tkf-turkiye-kurumsal-yonetim-end-fonu",
}

# ── Cache (TTL 25 gün — ay içinde değişmez) ──────────────────────────────────
_TTL = 25 * 86_400
_cache: dict[str, tuple[float, dict]] = {}
_stale: dict[str, dict] = {}


def _cache_get(kod: str) -> dict | None:
    if kod in _cache:
        ts, val = _cache[kod]
        if time.time() - ts < _TTL:
            return val
    return None


def _cache_set(kod: str, val: dict):
    _cache[kod] = (time.time(), val)
    _stale[kod] = val
    try:
        from redis_cache import rset
        rset(f"finans:portfoy:{kod}", val, ex=_TTL)
    except Exception:
        pass


# ── Türkçe → slug yardımcısı ─────────────────────────────────────────────────
_TR_CHARS = str.maketrans("ıİğĞüÜşŞöÖçÇ", "iigguussoocc")

def _to_slug(text: str) -> str:
    text = text.lower().translate(_TR_CHARS)
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text


# ── RSC HTML parser ──────────────────────────────────────────────────────────
def _parse_rsc(html: str) -> str:
    """self.__next_f.push içeriklerini unicode-escape decode ederek birleştirir."""
    matches = re.findall(r'self\.__next_f\.push\(\[1,"((?:[^"\\]|\\.)*)"\]\)', html)
    parts: list[str] = []
    for m in matches:
        try:
            parts.append(m.encode("raw_unicode_escape").decode("unicode_escape"))
        except Exception:
            parts.append(m)
    return "".join(parts)


# ── KAP API çağrıları ────────────────────────────────────────────────────────
def _get_slug(kod: str, unvan: str | None = None) -> str:
    """
    Fon kodu → KAP slug.
    Önce hardcoded tablo, yoksa TEFAS unvanından türet.
    """
    kod = kod.upper()
    if kod in _SLUG_MAP:
        return _SLUG_MAP[kod]
    if unvan:
        return f"{kod.lower()}-{_to_slug(unvan)}"
    raise ValueError(f"KAP slug bilinmiyor: {kod}. SLUG_MAP'e ekleyin.")


def _fetch_mkk_oid(slug: str, client: httpx.Client) -> str:
    """KAP fon bildirimleri RSC'sinden mkkMemberOid çek."""
    r = client.get(f"{_KAP}/tr/fon-bildirimleri/{slug}", timeout=20)
    r.raise_for_status()
    rsc = _parse_rsc(r.text)
    m = re.search(r'"mkkMemberOid"\s*:\s*"([^"]+)"', rsc)
    if not m:
        raise ValueError(f"mkkMemberOid bulunamadı: {slug}")
    return m.group(1)


def _fetch_disclosure_index(mkk_oid: str, client: httpx.Client) -> tuple[int, str]:
    """
    Son Portföy Dağılım Raporu bildirim index'ini ve dönem bilgisini bul.
    Returns: (disclosureIndex, donem_str)  örn. (1601574, "2026-04")
    """
    url = (
        f"{_KAP}/tr/bildirim-sorgu-sonuc"
        f"?srcbar=Y&cmp=N&cat=3&m={mkk_oid}&s={_PORTFOY_BILDIRIM_OID}"
    )
    r = client.get(url, timeout=20)
    r.raise_for_status()
    rsc = _parse_rsc(r.text)

    # disclosureIndex + year + donem
    m_idx = re.search(
        r'"disclosureIndex"\s*:\s*(\d+)'
        r'.*?"year"\s*:\s*(\d+)'
        r'.*?"donem"\s*:\s*(\d+)',
        rsc,
        re.DOTALL,
    )
    if not m_idx:
        # Basit fallback — sadece index
        m_simple = re.search(r'"disclosureIndex"\s*:\s*(\d+)', rsc)
        if not m_simple:
            raise ValueError("disclosureIndex bulunamadı")
        return int(m_simple.group(1)), "bilinmiyor"

    idx  = int(m_idx.group(1))
    year = m_idx.group(2)
    mon  = m_idx.group(3).zfill(2)
    return idx, f"{year}-{mon}"


def _fetch_attachment_oid(disc_idx: int, client: httpx.Client) -> tuple[str, str]:
    """
    attachment-detail API'sinden PDF dosya OID'sini al.
    Returns: (objId, fileName)
    """
    r = client.get(
        f"{_KAP}/tr/api/notification/attachment-detail/{disc_idx}",
        headers={**_HEADERS, "Accept": "application/json"},
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()
    if not data or not isinstance(data, list):
        raise ValueError("attachment-detail boş yanıt")

    attachments = data[0].get("attachments", [])
    pdf_att = next(
        (a for a in attachments if a.get("fileExtension", "").lower() == "pdf"),
        None,
    )
    if not pdf_att:
        raise ValueError("PDF attachment bulunamadı")
    return pdf_att["objId"], pdf_att.get("fileName", "portfoy.pdf")


def _download_pdf(obj_id: str, client: httpx.Client) -> bytes:
    """PDF dosyasını indir."""
    r = client.get(
        f"{_KAP}/tr/api/file/download/{obj_id}",
        timeout=30,
        follow_redirects=True,
    )
    r.raise_for_status()
    if "pdf" not in r.headers.get("content-type", "").lower():
        raise ValueError(f"Beklenen PDF değil: {r.headers.get('content-type')}")
    return r.content


# ── PDF Parsing ───────────────────────────────────────────────────────────────
# ISIN pattern: 2 büyük harf + 10 alfanümerik  (TR..., XS..., US... vb.)
_ISIN_RE   = re.compile(r'\b([A-Z]{2}[A-Z0-9]{10})\b')
_NUMBER_RE = re.compile(r'[\d]{1,3}(?:\.\d{3})*(?:,\d+)?')

# Türkçe ay adı → rakam
_AY = {
    "Ocak": "01", "Şubat": "02", "Mart": "03", "Nisan": "04",
    "Mayıs": "05", "Haziran": "06", "Temmuz": "07", "Ağustos": "08",
    "Eylül": "09", "Ekim": "10", "Kasım": "11", "Aralık": "12",
}


def _tr_float(s: str) -> float:
    """Türkçe sayı (1.234.567,89) → float."""
    return float(s.replace(".", "").replace(",", "."))


def _parse_pdf(pdf_bytes: bytes) -> dict:
    """
    PDF'ten fon bilgilerini ve varlık ağırlıklarını çıkar.
    """
    if not _PDF_OK:
        raise RuntimeError("pdfplumber kurulu değil")

    result: dict = {
        "fon_adi": "",
        "donem": "",
        "nav": None,
        "pay_sayisi": None,
        "ay_sonu_fiyat": None,
        "onceki_ay_fiyat": None,
        "fon_portfoy_degeri": None,
        "fon_toplam_degeri": None,
        "holdings": [],
        "kategori_ozeti": {},
    }

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        # ── Sayfa 1: genel bilgiler ───────────────────────────────────────
        p1 = pdf.pages[0].extract_text() or ""
        _parse_page1(p1, result)

        # ── Sayfa 2+: varlık tablosu ──────────────────────────────────────
        all_text = ""
        for page in pdf.pages[1:]:
            all_text += (page.extract_text() or "") + "\n"

        _parse_holdings(all_text, result)

    return result


def _parse_page1(text: str, result: dict):
    """Sayfa 1'den fon adı, dönem, NAV, fiyat bilgilerini çek."""
    lines = text.split("\n")

    # İlk satır genellikle "KOD-FON ADI" formatında
    for line in lines[:3]:
        if "-" in line and len(line) > 10:
            result["fon_adi"] = line.strip()
            break

    # Dönem — "Nisan-2026" → "2026-04"
    for line in lines:
        for ay, no in _AY.items():
            m = re.search(rf"{ay}[- ](\d{{4}})", line)
            if m:
                result["donem"] = f"{m.group(1)}-{no}"
                break
        if result["donem"]:
            break

    # NAV
    m = re.search(r'D[-—]\s*\)?\s*Toplam[^:]+:\s*([\d.,]+)', text, re.IGNORECASE)
    if m:
        result["nav"] = _tr_float(m.group(1))

    # Pay sayısı
    m = re.search(r'E[-—]\s*\)?\s*Kat[ıi]lma[^:]+:\s*([\d.,]+)', text, re.IGNORECASE)
    if m:
        result["pay_sayisi"] = _tr_float(m.group(1))

    # Ay sonu pay fiyatı
    m = re.search(r'A[-—]\s*\)?\s*Ay Sonu[^:]+:\s*([\d.,]+)', text, re.IGNORECASE)
    if m:
        result["ay_sonu_fiyat"] = _tr_float(m.group(1))

    # Önceki ay pay fiyatı
    m = re.search(r'B[-—]\s*\)?\s*[Öö]nceki[^:]+:\s*([\d.,]+)', text, re.IGNORECASE)
    if m:
        result["onceki_ay_fiyat"] = _tr_float(m.group(1))


def _parse_holdings(text: str, result: dict):
    """
    Varlık tablosundan tek tek pozisyonları ve kategori özetlerini çıkar.

    Strateji:
    - ISIN kodu bulunan satırlar = bireysel pozisyon
    - "GRUP TOPLAMI" satırları = kategori özeti
    - Satır sonu %'leri ayıkla: FPD% | FTD_grup% | FTD%
    """
    holdings: list[dict] = []
    kategori: dict[str, dict] = {}
    current_kat = "DİĞER"

    # Basit kategori tespiti
    _KAT_MAP = {
        "HİSSE": "hisse",
        "DEVLET TAHVİLİ": "devlet_tahvili",
        "ÖZEL SEKTÖR TAHVİLİ": "ozel_tahvil",
        "VARLIK KİRALAMA": "varlik_kiralama",
        "REPO": "repo",
        "VIOP": "turev",
        "DÖVİZ": "doviz",
        "DİĞER": "diger",
        "FONU": "yatirim_fonu",
        "FONA": "yatirim_fonu",
    }

    lines = text.split("\n")
    for line in lines:
        line_up = line.upper()

        # Kategori başlığı tespiti
        for k, v in _KAT_MAP.items():
            if k in line_up and not any(
                skip in line_up
                for skip in ("TOPLAMI", "ISIN", "KODU", "TL ", "USD", "EUR")
            ):
                current_kat = v
                break

        # FON PORTFÖY DEĞERİ
        if "FON PORTFÖY DEĞERİ" in line_up or "FON PORTFOY DEGERI" in line_up:
            nums = _NUMBER_RE.findall(line)
            if nums:
                try:
                    result["fon_portfoy_degeri"] = _tr_float(nums[0])
                except Exception:
                    pass

        # FON TOPLAM DEĞERİ (tablodan)
        if "FON TOPLAM DEĞERİ" in line_up:
            m = re.search(r'A-\)FON PORTFOY DEĞERI\s+([\d.,]+)', line, re.IGNORECASE)
            if not m:
                nums = _NUMBER_RE.findall(line)
                if len(nums) >= 2:
                    try:
                        result["fon_toplam_degeri"] = _tr_float(nums[1])
                    except Exception:
                        pass

        # GRUP TOPLAMI satırı
        if "GRUP TOPLAMI" in line_up:
            nums = _NUMBER_RE.findall(line)
            if len(nums) >= 2:
                try:
                    toplam_deger = _tr_float(nums[0])
                    # Son 2 sayı: FPD% ve FTD%
                    ftd_pct = _tr_float(nums[-1]) if len(nums) >= 3 else None
                    fpd_pct = _tr_float(nums[-3]) if len(nums) >= 3 else None
                    kategori[current_kat] = {
                        "tur": current_kat,
                        "toplam_deger": toplam_deger,
                        "fpd_yuzde": fpd_pct,
                        "ftd_yuzde": ftd_pct,
                    }
                except Exception:
                    pass
            continue

        # ISIN kodu içeren satır → bireysel pozisyon
        isin_m = _ISIN_RE.search(line)
        if not isin_m:
            continue
        isin = isin_m.group(1)

        # Satırdaki tüm sayıları al
        nums = _NUMBER_RE.findall(line)
        if len(nums) < 3:
            continue

        try:
            toplam_deger = _tr_float(nums[-3])
            ftd_grp_pct  = _tr_float(nums[-2])
            ftd_pct      = _tr_float(nums[-1])
        except Exception:
            continue

        # Kod (ISIN öncesindeki ilk kelime)
        before_isin = line[:isin_m.start()].strip()
        tokens = before_isin.split()
        if tokens:
            kod = tokens[0].upper()
            # Kod çok uzunsa (ISIN tekrarı) veya küçükse isin kullan
            if len(kod) > 12 or not kod.isalpha():
                kod = isin
        else:
            kod = isin

        holdings.append({
            "kod": kod,
            "isin": isin,
            "tur": current_kat,
            "toplam_deger": toplam_deger,
            "fpd_yuzde": None,  # satırdan güvenilir çekilemiyor
            "ftd_yuzde": ftd_pct,
        })

    # Holdings'i % büyüklüğüne göre sırala, repo/teminat hariç
    holdings = [h for h in holdings if h.get("ftd_yuzde") is not None]
    holdings.sort(key=lambda h: abs(h["ftd_yuzde"]), reverse=True)

    # Duplikat ISIN'leri temizle (aynı pozisyon farklı satırlarda gelebilir)
    seen: set[str] = set()
    unique: list[dict] = []
    for h in holdings:
        if h["isin"] not in seen:
            seen.add(h["isin"])
            unique.append(h)

    result["holdings"] = unique
    result["kategori_ozeti"] = kategori


# ── Ana fetch fonksiyonu ─────────────────────────────────────────────────────
def _fetch_portfoy(kod: str, unvan: str | None = None) -> dict:
    """
    Tek fon için portföy dağılım verisini çek.
    Cache miss durumunda KAP'tan sırayla çeker.
    """
    kod = kod.upper()

    # Memory cache
    cached = _cache_get(kod)
    if cached:
        return cached

    # Redis
    try:
        from redis_cache import rget
        v = rget(f"finans:portfoy:{kod}")
        if v:
            _cache[kod] = (time.time(), v)
            _stale[kod] = v
            return v
    except Exception:
        pass

    # KAP'tan çek
    slug = _get_slug(kod, unvan)
    try:
        with httpx.Client(
            headers=_HEADERS,
            timeout=30,
            follow_redirects=True,
        ) as client:
            mkk_oid     = _fetch_mkk_oid(slug, client)
            disc_idx, _ = _fetch_disclosure_index(mkk_oid, client)
            obj_id, fname = _fetch_attachment_oid(disc_idx, client)
            pdf_bytes   = _download_pdf(obj_id, client)

        parsed = _parse_pdf(pdf_bytes)
        parsed["kod"] = kod

        # disclosureIndex'ten dönem bilgisi de var
        # parsed["donem"] sayfa 1'den doldurulmuş olmalı

        result = {
            "kod": kod,
            "fon_adi": parsed.get("fon_adi", ""),
            "donem": parsed.get("donem", ""),
            "nav": parsed.get("nav"),
            "pay_sayisi": parsed.get("pay_sayisi"),
            "ay_sonu_fiyat": parsed.get("ay_sonu_fiyat"),
            "onceki_ay_fiyat": parsed.get("onceki_ay_fiyat"),
            "fon_portfoy_degeri": parsed.get("fon_portfoy_degeri"),
            "fon_toplam_degeri": parsed.get("fon_toplam_degeri"),
            "holdings": parsed.get("holdings", []),
            "kategori_ozeti": parsed.get("kategori_ozeti", {}),
            "kaynak": "kap_pdf",
            "guncelleme": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "pdf_dosya": fname,
            "disclosure_index": disc_idx,
        }

        _cache_set(kod, result)
        return result

    except Exception as e:
        _logger.error(f"Portföy çekme hatası ({kod}): {e}")
        # Stale fallback
        if kod in _stale:
            return _stale[kod]
        raise HTTPException(
            503,
            detail=f"Portföy verisi alınamadı ({kod}): {str(e)[:200]}",
        )


# ── Endpoint'ler ─────────────────────────────────────────────────────────────
@router.get("/{kod}", summary="Fon portföy dağılım raporu (KAP)")
def portfoy_getir(kod: str):
    """
    Verilen fon kodunun en son Portföy Dağılım Raporu'nu KAP'tan çeker.

    - **holdings**: Fon içindeki bireysel varlıklar (hisse, tahvil, fon, vb.)
    - **kategori_ozeti**: Varlık sınıfı bazında özet
    - **nav**: Net Varlık Değeri (TL)
    - **ftd_yuzde**: Fon toplam değerine oranı (%)

    Veri aylık yayınlanır; cache süresi 25 gün.
    """
    return _fetch_portfoy(kod.upper())


@router.post("/seed", summary="Dışarıdan portföy verisi yükle")
def seed_portfoy(payload: dict, request: Request):
    """
    GitHub Actions veya harici kaynak tarafından çağrılır.
    `{"kod": "TLY", "donem": "2026-04", "holdings": [...], ...}` formatında.
    X-API-Key gereklidir.
    """
    kod = str(payload.get("kod", "")).strip().upper()
    if not kod:
        raise HTTPException(400, "kod alanı gereklidir")

    _cache_set(kod, payload)
    _logger.info(f"Portföy seed ✓ {kod}")
    return {"basarili": True, "kod": kod}
