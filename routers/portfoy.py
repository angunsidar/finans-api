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
import json
import logging
import os
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
    # Guncel sluglar (TEFAS 2026-05)
    "YKT": "ykt-yapi-kredi-portfoy-altin-fonu",
    "GTA": "gta-garanti-portfoy-altin-fonu",
    "TTA": "tta-is-portfoy-altin-fonu",
    "AFO": "afo-ak-portfoy-altin-fonu",
    "TCA": "tca-ziraat-portfoy-altin-katilim-fonu",
    "HBF": "hbf-hsbc-portfoy-altin-fonu",
    "GOL": "gol-garanti-portfoy-altin-katilim-fonu",
    "DBA": "dba-deniz-portfoy-altin-fonu",
    "TLY": "tly-tera-portfoy-birinci-serbest-fon",
    "MAC": "mac-marmara-capital-portfoy-hisse-senedi-tl-fonu-hisse-senedi-yogun-fon",
    # Manuel çekilen fonlar — watchdog tarafından takip edilir (2026-06)
    "AES": "aes-ak-portfoy-petrol-yabanci-byf-fon-sepeti-fonu",
    "CPU": "cpu-aktif-portfoy-teknoloji-katilim-fonu",
    "KLH": "klh-atlas-portfoy-katilim-hisse-senedi-serbest-fon-hisse-senedi-yogun-fon",
    "DFI": "dfi-atlas-portfoy-serbest-fon",
    "BMU": "bmu-bulls-portfoy-mutlak-getiri-hedefli-hisse-senedi-serbest-fon-hisse-senedi-yogun-fon",
    "BVV": "bvv-bv-portfoy-teknoloji-degisken-fon",
    "SNY": "sny-atlas-portfoy-sanayi-sektoru-hisse-senedi-serbest-fon-hisse-senedi-yogun-fon",
    "BTK": "btk-bv-portfoy-teknoloji-katilim-fonu",
    "SGT": "sgt-garanti-portfoy-siber-guvenlik-teknolojileri-degisken-fon",
    "GUH": "guh-garanti-portfoy-yabanci-teknoloji-hisse-senedi-fonu",
    "YIT": "yit-garanti-portfoy-yari-iletken-teknolojileri-degisken-fon",
    "TTE": "tte-is-portfoy-bist-teknoloji-agirlik-sinirlamali-endeksi-hisse-senedi-tl-fonu-hisse-senedi-yogun-fon",
    "IJC": "ijc-is-portfoy-yari-iletken-teknolojileri-degisken-fon",
    "IJZ": "ijz-is-portfoy-siber-guvenlik-teknolojileri-degisken-fon",
    "NTI": "nti-neo-portfoy-teknoloji-ve-inovasyon-degisken-fon",
    "PHE": "phe-pusula-portfoy-hisse-senedi-fonu-hisse-senedi-yogun-fon",
    "PBR": "pbr-pusula-portfoy-birinci-degisken-fon",
    "DNK": "dnk-tacirler-portfoy-denge-katilim-serbest-fon",
    "CPT": "cpt-rota-portfoy-cip-teknolojileri-degisken-fon",
    "TFF": "tff-teb-portfoy-amerika-teknoloji-yabanci-byf-fon-sepeti-fonu",
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
        rset(f"finans:portfoy:{kod}", val)
    except Exception:
        pass


def _normalize_portfoy(data: dict) -> dict:
    """fetch_portfoy_local.py formatını (isim/oran) standart formata (kod/ftd_yuzde) çevir."""
    holdings = data.get("holdings", [])
    if not holdings:
        return data
    first = holdings[0]
    if "isim" in first and "kod" not in first:
        data["holdings"] = [
            {
                "kod": h.get("isim", ""),
                "ftd_yuzde": h.get("oran"),
                "tur": h.get("tur", "diger"),
                "isin": h.get("isin"),
                "unvan": _bist_name(h.get("isim", "")),
            }
            for h in holdings
        ]
    if "donem" not in data and "tarih" in data:
        data["donem"] = data["tarih"]
    if "kategori_ozeti" not in data:
        data["kategori_ozeti"] = {}
    return data


# Watchdog — her fon için son bilinen disclosureIndex
_stored_indexes: dict[str, int] = {}  # Redis'e de yazar, bu dict sadece memory hız için


def _get_stored_index(kod: str) -> int | None:
    if kod in _stored_indexes:
        return _stored_indexes[kod]
    try:
        from redis_cache import rget
        v = rget(f"finans:portfoy_idx:{kod}")
        if v and isinstance(v, dict):
            idx = v.get("disclosure_index")
            if idx:
                _stored_indexes[kod] = int(idx)
                return _stored_indexes[kod]
    except Exception:
        pass
    return None


def _set_stored_index(kod: str, idx: int):
    _stored_indexes[kod] = idx
    try:
        from redis_cache import rset
        # 30 gün TTL — aylık yayın; 24s varsayılan TTL her sabah idx'i siliyordu
        rset(f"finans:portfoy_idx:{kod}", {"disclosure_index": idx}, ttl=30 * 86_400)
    except Exception:
        pass


def watchdog_check(kod: str) -> bool:
    """
    disclosureIndex değişti mi kontrol et.
    Değiştiyse PDF pipeline'ı çalıştır, cache'i güncelle.
    Returns True = yeni veri çekildi.
    """
    kod = kod.upper()
    slug = _SLUG_MAP.get(kod)
    if not slug:
        return False

    try:
        with httpx.Client(headers=_HEADERS, timeout=20, follow_redirects=True) as client:
            mkk_oid = _fetch_mkk_oid(slug, client)
            disc_idx, _ = _fetch_disclosure_index(mkk_oid, client)

        stored = _get_stored_index(kod)
        if stored is not None and disc_idx == stored:
            return False  # Değişmedi

        # Yeni rapor var — tam pipeline çalıştır
        _logger.info(f"Portföy watchdog: {kod} yeni index {disc_idx} (önceki: {stored})")
        data = _fetch_portfoy(kod)  # PDF indir + parse + cache yaz
        _set_stored_index(kod, disc_idx)
        return True

    except Exception as e:
        _logger.warning(f"Portföy watchdog hata ({kod}): {e}")
        return False


def watchdog_all(max_pdf: int = 2) -> dict[str, bool]:
    """
    Tüm izlenen fonları kontrol et. main.py scheduler tarafından çağrılır.
    max_pdf: tek çalışmada indirilecek maksimum PDF sayısı (OOM koruması).
    """
    results = {}
    pdf_count = 0
    for kod in _SLUG_MAP:
        if pdf_count >= max_pdf:
            results[kod] = False
            continue
        updated = watchdog_check(kod)
        if updated:
            pdf_count += 1
        results[kod] = updated
        time.sleep(2)  # KAP rate-limit + Render CPU nefes
    updated = [k for k, v in results.items() if v]
    skipped = len(_SLUG_MAP) - len([k for k, v in results.items() if v is not None and k in _SLUG_MAP]) + len([k for k in _SLUG_MAP if results.get(k) is False and pdf_count >= max_pdf])
    if updated:
        _logger.info(f"Portföy watchdog: {updated} güncellendi (PDF limit: {pdf_count}/{max_pdf})")
    else:
        _logger.info(f"Portföy watchdog: değişiklik yok")
    return results


# ── Türkçe → slug yardımcısı ─────────────────────────────────────────────────
_TR_CHARS = str.maketrans("ıİğĞüÜşŞöÖçÇ", "iigguussoocc")

def _to_slug(text: str) -> str:
    # translate ONCE lower'dan once: Python'da "I".lower() -> "i̇" (birlesik nokta)
    # bu da regex'e takilip yanlis tire olusturuyor. Once buyuk -> latin, sonra lower.
    text = text.translate(_TR_CHARS).lower()
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text


# ── BIST universe isim lookup ────────────────────────────────────────────────
_BIST_NAMES: dict[str, str] = {}  # sembol → tam_ad (lazy-loaded)

def _bist_name(sembol: str) -> str | None:
    """
    Önce fon_universe.json'dan bak (yatırım fonları dahil),
    bulamazsa bist_universe.json'a bak (BIST hisseleri).
    """
    global _BIST_NAMES
    if not _BIST_NAMES:
        data_dir = os.path.join(os.path.dirname(__file__), "..", "data")
        # fon_universe — kod → unvan
        try:
            with open(os.path.join(data_dir, "fon_universe.json"), encoding="utf-8-sig") as f:
                raw = json.load(f)
            for f_item in raw.get("fonlar", []):
                k = f_item.get("kod", "").upper()
                if k:
                    _BIST_NAMES[k] = f_item.get("unvan", "")
        except Exception as e:
            _logger.warning(f"fon_universe.json okunamadı: {e}")
        # bist_universe — sembol → tam_ad (üstüne yaz — hisse adları daha kısa/doğru)
        try:
            with open(os.path.join(data_dir, "bist_universe.json"), encoding="utf-8-sig") as f:
                raw = json.load(f)
            for h in raw.get("hisseler", []):
                k = h.get("sembol", "").upper()
                if k:
                    _BIST_NAMES[k] = h.get("tam_ad") or h.get("ad", "")
        except Exception as e:
            _logger.warning(f"bist_universe.json okunamadı: {e}")
    return _BIST_NAMES.get(sembol.upper()) or None


# ── ISIN prefix → varlık türü ────────────────────────────────────────────────
# Türkiye ISIN formatı: TR + 10 alfanümerik
# TRE = hisse senedi (Equity), TRB/TR0 = devlet tahvili, TRY = yatırım fonu
# XS = Eurobond, US = ABD menkul kıymeti, vs.
def _isin_tur(isin: str) -> str | None:
    if isin.startswith("TRE"):
        return "hisse"
    if isin.startswith("TRY"):
        return "yatirim_fonu"
    if isin.startswith("TR0") or isin.startswith("TRB"):
        return "devlet_tahvili"
    if isin.startswith("TRC"):
        return "varlik_kiralama"
    if isin.startswith("TRS"):
        return "ozel_tahvil"
    if isin[:2] in ("XS", "US", "DE", "FR", "GB"):
        return "eurobond"
    return None  # Bilinmiyor — mevcut current_kat korunur


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
        if len(nums) < 4:
            continue

        # Sütun düzeni (PDF): ... TOPLAM DEĞER | FPD% | GRUP FTD% | FTD%
        # Tarih (29/04/26) ve ödeme sayısı (80100511) regex'te parçalanır,
        # bu yüzden son 4'e bakıyoruz: toplam_deger, fpd%, grp_ftd%, ftd%
        try:
            toplam_deger = _tr_float(nums[-4])
            fpd_pct      = _tr_float(nums[-3])
            ftd_grp_pct  = _tr_float(nums[-2])
            ftd_pct      = _tr_float(nums[-1])
        except Exception:
            continue

        # ISIN prefix'e göre varlık türünü tespit et (section header'dan daha güvenilir)
        isin_tur = _isin_tur(isin)
        if isin_tur:
            current_kat = isin_tur

        # Kod (ISIN öncesindeki ilk kelime) — hisse kodları KAP PDF'inde ".E" ekiyle gelir: "MAVI.E"
        before_isin = line[:isin_m.start()].strip()
        tokens = before_isin.split()
        if tokens:
            kod = tokens[0].upper().split(".")[0]
            # Kod çok uzunsa (ISIN tekrarı) veya geçersizse isin kullan
            if not kod or len(kod) > 12 or not kod.replace("-", "").isalnum():
                kod = isin
        else:
            kod = isin

        holdings.append({
            "kod": kod,
            "isin": isin,
            "unvan": _bist_name(kod),  # None ise yatırım fonu / bilinmiyor
            "tur": current_kat,
            "toplam_deger": toplam_deger,
            "fpd_yuzde": fpd_pct,
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
    SLUG_MAP'te olmayan fonlar için TEFAS API'den doğru UTF-8 unvan alınır.
    """
    kod = kod.upper()

    # SLUG_MAP'te yoksa unvan bul: 1) fon_universe.json, 2) TEFAS fallback
    if kod not in _SLUG_MAP and not unvan:
        try:
            import json as _j
            from pathlib import Path as _P
            _raw = _j.loads((_P(__file__).parent.parent / "data" / "fon_universe.json").read_text(encoding="utf-8-sig"))
            _match = next((f for f in _raw.get("fonlar", []) if f.get("kod") == kod), None)
            if _match:
                unvan = _match.get("unvan")
        except Exception:
            pass
        # TEFAS fallback — sadece fon_universe'de yoksa
        if not unvan:
            try:
                from routers.fon import _fetch_tefas_api
                tefas = _fetch_tefas_api(kod)
                unvan = tefas.get("ad") or tefas.get("fonUnvan")
            except Exception:
                pass

    # Memory cache
    cached = _cache_get(kod)
    if cached:
        return cached

    # Redis
    try:
        from redis_cache import rget
        v = rget(f"finans:portfoy:{kod}")
        if v:
            v = _normalize_portfoy(v)
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


@router.post("/warm", summary="Tüm SLUG_MAP fonları için portföy verisi çek ve Redis'e yaz")
def portfoy_warm(request: Request):
    """
    Bir kerelik / manuel tetikleme için.
    SLUG_MAP'teki tüm fonların portföy dağılımını KAP'tan çeker, Redis'e yazar.
    X-API-Key gereklidir.
    """
    import time as _time
    sonuclar = {}
    for kod in list(_SLUG_MAP.keys()):
        try:
            _fetch_portfoy(kod)
            sonuclar[kod] = "ok"
        except Exception as e:
            sonuclar[kod] = f"hata: {str(e)[:80]}"
        _time.sleep(0.8)
    ok_count  = sum(1 for v in sonuclar.values() if v == "ok")
    err_count = len(sonuclar) - ok_count
    return {"toplam": len(sonuclar), "ok": ok_count, "hata": err_count, "detay": sonuclar}




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
