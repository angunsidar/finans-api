"""
Evren endpoint'leri — tüm hisse listesi + metadata.
Veriler fetch_universe.py ile önceden çekilip JSON'a kaydedilmiştir.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, BackgroundTasks

router = APIRouter(prefix="/evren", tags=["evren"])

DATA_DIR = Path(__file__).parent.parent / "data"

_cache: dict[str, dict] = {}


def _load(dosya: str) -> dict:
    if dosya not in _cache:
        path = DATA_DIR / dosya
        if not path.exists():
            raise HTTPException(404, f"{dosya} bulunamadı. Önce /evren/guncelle çalıştırın.")
        with open(path, encoding="utf-8") as f:
            _cache[dosya] = json.load(f)
    return _cache[dosya]


def _reload():
    """Cache'i temizle — bir sonraki istekte disk'ten okur."""
    _cache.clear()


def _filtrele(hisseler: list, q: str = "", sektor: str = "",
              limit: int = 100, offset: int = 0) -> list:
    q_low = q.lower()
    sonuc = [
        h for h in hisseler
        if (not q or q_low in h.get("sembol", "").lower()
                  or q_low in h.get("tam_ad", "").lower()
                  or q_low in h.get("ad", "").lower())
        and (not sektor or sektor.lower() in h.get("sektor", "").lower())
    ]
    return sonuc[offset: offset + limit]


# ─── BIST ────────────────────────────────────────────────────────────────────

@router.get("/bist", summary="BIST tüm hisseler")
def evren_bist(
    q: str = Query("", description="Sembol veya isim ile arama"),
    sektor: str = Query("", description="Sektör filtresi (örn: Technology)"),
    limit: int = Query(100, ge=1, le=700),
    offset: int = Query(0, ge=0),
    logo: bool = Query(True, description="Logo URL dahil edilsin mi"),
):
    """
    BIST'teki tüm hisselerin listesi + metadata.
    Sektör, isim, sembol ile filtrelenebilir.
    """
    data = _load("bist_universe.json")
    hisseler = data["hisseler"]
    filtered = _filtrele(hisseler, q, sektor, limit, offset)
    if not logo:
        filtered = [{k: v for k, v in h.items() if k != "logo_url"} for h in filtered]
    return {
        "toplam_evren": data["toplam"],
        "filtreli": len(_filtrele(hisseler, q, sektor, 9999, 0)),
        "offset": offset,
        "limit": limit,
        "guncelleme": data.get("olusturma_tarihi"),
        "hisseler": filtered,
    }


@router.get("/bist/sektorler", summary="BIST sektör listesi")
def bist_sektorler():
    """BIST'teki benzersiz sektörler."""
    data = _load("bist_universe.json")
    sektorler: dict[str, int] = {}
    for h in data["hisseler"]:
        s = h.get("sektor") or "Bilinmiyor"
        sektorler[s] = sektorler.get(s, 0) + 1
    return {
        "toplam_sektor": len(sektorler),
        "sektorler": sorted(
            [{"sektor": k, "hisse_sayisi": v} for k, v in sektorler.items()],
            key=lambda x: -x["hisse_sayisi"]
        ),
    }


@router.get("/bist/{sembol}", summary="BIST hisse metadata")
def bist_meta(sembol: str):
    """Tek BIST hissesinin metadata bilgisi."""
    data = _load("bist_universe.json")
    sembol = sembol.upper()
    hisse = next((h for h in data["hisseler"] if h["sembol"] == sembol), None)
    if not hisse:
        raise HTTPException(404, f"{sembol} bulunamadı")
    return hisse


# ─── S&P 500 ─────────────────────────────────────────────────────────────────

@router.get("/sp500", summary="S&P 500 tüm hisseler")
def evren_sp500(
    q: str = Query("", description="Sembol veya isim ile arama"),
    sektor: str = Query("", description="Sektör filtresi"),
    limit: int = Query(100, ge=1, le=600),
    offset: int = Query(0, ge=0),
    logo: bool = Query(True),
):
    """S&P 500 bileşenleri + metadata."""
    data = _load("sp500_universe.json")
    hisseler = data["hisseler"]
    filtered = _filtrele(hisseler, q, sektor, limit, offset)
    if not logo:
        filtered = [{k: v for k, v in h.items() if k != "logo_url"} for h in filtered]
    return {
        "toplam_evren": data["toplam"],
        "filtreli": len(_filtrele(hisseler, q, sektor, 9999, 0)),
        "offset": offset,
        "limit": limit,
        "guncelleme": data.get("olusturma_tarihi"),
        "hisseler": filtered,
    }


@router.get("/sp500/sektorler", summary="S&P 500 sektör dağılımı")
def sp500_sektorler():
    data = _load("sp500_universe.json")
    sektorler: dict[str, int] = {}
    for h in data["hisseler"]:
        s = h.get("sektor") or "Bilinmiyor"
        sektorler[s] = sektorler.get(s, 0) + 1
    return {
        "toplam_sektor": len(sektorler),
        "sektorler": sorted(
            [{"sektor": k, "hisse_sayisi": v} for k, v in sektorler.items()],
            key=lambda x: -x["hisse_sayisi"]
        ),
    }


@router.get("/sp500/{sembol}", summary="S&P 500 hisse metadata")
def sp500_meta(sembol: str):
    data = _load("sp500_universe.json")
    sembol = sembol.upper()
    hisse = next((h for h in data["hisseler"] if h["sembol"] == sembol), None)
    if not hisse:
        raise HTTPException(404, f"{sembol} bulunamadı")
    return hisse


# ─── Nasdaq ───────────────────────────────────────────────────────────────────

@router.get("/nasdaq", summary="Nasdaq listesi")
def evren_nasdaq(
    q: str = Query(""),
    sektor: str = Query(""),
    limit: int = Query(100, ge=1, le=600),
    offset: int = Query(0, ge=0),
    logo: bool = Query(True),
):
    """Nasdaq bileşenleri + metadata."""
    data = _load("nasdaq100_universe.json")
    hisseler = data["hisseler"]
    filtered = _filtrele(hisseler, q, sektor, limit, offset)
    if not logo:
        filtered = [{k: v for k, v in h.items() if k != "logo_url"} for h in filtered]
    return {
        "toplam_evren": data["toplam"],
        "filtreli": len(_filtrele(hisseler, q, sektor, 9999, 0)),
        "offset": offset,
        "limit": limit,
        "guncelleme": data.get("olusturma_tarihi"),
        "hisseler": filtered,
    }


# ─── Genel arama ─────────────────────────────────────────────────────────────

@router.get("/ara", summary="Tüm evrende arama")
def evren_ara(
    q: str = Query(..., min_length=2, description="Sembol veya şirket adı"),
    limit: int = Query(20, ge=1, le=100),
):
    """BIST + S&P 500 + Nasdaq'da tek sorguda ara."""
    sonuclar = []
    for dosya, kaynak in [
        ("bist_universe.json", "BIST"),
        ("sp500_universe.json", "SP500"),
        ("nasdaq100_universe.json", "NASDAQ"),
    ]:
        try:
            data = _load(dosya)
            hits = _filtrele(data["hisseler"], q=q, limit=limit)
            for h in hits:
                sonuclar.append({**h, "kaynak": kaynak})
        except HTTPException:
            pass
    sonuclar = sonuclar[:limit]
    return {"sorgu": q, "toplam": len(sonuclar), "sonuclar": sonuclar}


# ─── İstatistik ───────────────────────────────────────────────────────────────

@router.get("/istatistik", summary="Evren özet istatistikleri")
def evren_istatistik():
    """Tüm evrenin özet sayıları."""
    ozet = {}
    for dosya, ad in [
        ("bist_universe.json", "bist"),
        ("sp500_universe.json", "sp500"),
        ("nasdaq100_universe.json", "nasdaq"),
    ]:
        try:
            data = _load(dosya)
            logolu = sum(1 for h in data["hisseler"] if h.get("logo_url"))
            ozet[ad] = {
                "toplam": data["toplam"],
                "logolu": logolu,
                "guncelleme": data.get("olusturma_tarihi"),
            }
        except HTTPException:
            ozet[ad] = {"durum": "veri yok"}
    toplam = sum(v.get("toplam", 0) for v in ozet.values())
    return {"toplam_asset": toplam, "kaynaklar": ozet}


# ─── FON EVRENİ ──────────────────────────────────────────────────────────────

@router.get("/fon", summary="Tüm yatırım fonları listesi (2575 fon)")
def evren_fon(
    q: str = Query("", description="Fon kodu veya unvan ile arama"),
    kategori: str = Query("", description="Kategori filtresi (örn: Altın Fonu)"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """TEFAS'taki tüm yatırım fonlarını sayfalı olarak döndürür."""
    data = _load("fon_universe.json")
    fonlar: list = data.get("fonlar", [])

    q_low = q.lower()
    sonuc = [
        f for f in fonlar
        if (not q or q_low in f.get("kod", "").lower()
                  or q_low in f.get("unvan", "").lower())
        and (not kategori or kategori.lower() in f.get("kategori", "").lower())
    ]

    return {
        "toplam": len(sonuc),
        "toplam_evren": data.get("toplam", len(fonlar)),
        "offset": offset,
        "limit": limit,
        "fonlar": sonuc[offset: offset + limit],
    }


@router.get("/fon/kategoriler", summary="Fon kategori listesi")
def fon_kategoriler():
    """Tüm fon kategorilerini döndürür (filtre için)."""
    data = _load("fon_universe.json")
    kategoriler = sorted({f.get("kategori", "") for f in data.get("fonlar", []) if f.get("kategori")})
    return {"kategoriler": kategoriler}


# ─── Güncelle ────────────────────────────────────────────────────────────────

@router.post("/guncelle", summary="Evren verisini yenile (arka planda)")
def evren_guncelle(background_tasks: BackgroundTasks):
    """
    fetch_universe.py'yi arka planda çalıştırır.
    ~2-3 dakika sürer. /evren/istatistik ile ilerlemeyi takip edebilirsiniz.
    """
    def run():
        subprocess.run(
            [sys.executable, str(Path(__file__).parent.parent / "fetch_universe.py")],
            capture_output=True
        )
        _reload()

    background_tasks.add_task(run)
    return {"mesaj": "Evren güncellemesi başlatıldı (~2-3 dk sürer)"}
