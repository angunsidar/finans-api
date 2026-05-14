"""
Altın fiyatı endpoint'leri.
yfinance üzerinden — futures, ETF ve BIST altın.
"""
from __future__ import annotations

import yfinance as yf
from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/altin", tags=["altın"])

# Kaynak tanımları
KAYNAKLAR = {
    "futures": {"sembol": "GC=F",      "ad": "Altın Futures (COMEX)",  "para": "USD", "birim": "ons"},
    "etf_gld": {"sembol": "GLD",       "ad": "SPDR Gold ETF",           "para": "USD", "birim": "hisse"},
    "etf_iau": {"sembol": "IAU",       "ad": "iShares Gold ETF",        "para": "USD", "birim": "hisse"},
    "bist":    {"sembol": "GLDTR.IS",  "ad": "Altın (BIST)",            "para": "TRY", "birim": "gram"},
}


def _fetch(sembol: str) -> dict:
    tick = yf.Ticker(sembol)
    hist = tick.history(period="2d")
    if hist.empty:
        raise HTTPException(404, f"Veri bulunamadı: {sembol}")
    son = hist.iloc[-1]
    onceki = hist.iloc[-2] if len(hist) > 1 else hist.iloc[-1]
    kapanis = round(float(son["Close"]), 4)
    onceki_kapanis = float(onceki["Close"])
    degisim = round(kapanis - onceki_kapanis, 4)
    degisim_yuzde = round((degisim / onceki_kapanis) * 100, 2) if onceki_kapanis else 0
    return {
        "fiyat": kapanis,
        "acilis": round(float(son["Open"]), 4),
        "yuksek": round(float(son["High"]), 4),
        "dusuk": round(float(son["Low"]), 4),
        "degisim": degisim,
        "degisim_yuzde": degisim_yuzde,
        "tarih": str(hist.index[-1].date()),
    }


@router.get("", summary="Tüm altın kaynakları özet")
def altin_ozet():
    """Futures, ETF ve BIST altın fiyatlarını tek sorguda getir."""
    sonuclar = {}
    for key, meta in KAYNAKLAR.items():
        try:
            veri = _fetch(meta["sembol"])
            sonuclar[key] = {**meta, **veri}
        except Exception as e:
            sonuclar[key] = {**meta, "hata": str(e)[:60]}
    return sonuclar


@router.get("/futures", summary="Altın futures (COMEX) — USD/ons")
def altin_futures():
    """COMEX altın futures fiyatı (GC=F). USD/ons cinsinden."""
    veri = _fetch("GC=F")
    return {"sembol": "GC=F", "ad": "Altın Futures (COMEX)", "para_birimi": "USD",
            "birim": "ons", **veri}


@router.get("/bist", summary="BIST altın fiyatı — TRY")
def altin_bist():
    """BIST'te işlem gören altın (GLDTR). TRY cinsinden."""
    veri = _fetch("GLDTR.IS")
    return {"sembol": "GLDTR.IS", "ad": "Altın (BIST)", "para_birimi": "TRY",
            "borsa": "BIST", **veri}


@router.get("/tl", summary="Altın TL karşılığı (hesaplanmış)")
def altin_tl():
    """
    Altın USD/ons fiyatı × Dolar/TL kuru = TL/ons.
    Gram hesabı için 32.1507'ye bölünür.
    """
    try:
        gold = _fetch("GC=F")
        usd = _fetch("USDTRY=X")

        ons_fiyat_usd = gold["fiyat"]
        usdtry = usd["fiyat"]
        ons_fiyat_tl = round(ons_fiyat_usd * usdtry, 2)
        gram_fiyat_tl = round(ons_fiyat_tl / 31.1035, 2)  # 1 troy ons = 31.1035 gram

        return {
            "altin_usd_ons": ons_fiyat_usd,
            "usd_try": usdtry,
            "altin_tl_ons": ons_fiyat_tl,
            "altin_tl_gram": gram_fiyat_tl,
            "altin_22ayar_gram": round(gram_fiyat_tl * 0.916, 2),
            "not": "GC=F futures × USDTRY=X kuru ile hesaplanmıştır",
            "tarih": gold["tarih"],
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@router.get("/gecmis", summary="Geçmiş altın fiyatları")
def altin_gecmis(
    kaynak: str = Query("futures", description="futures | etf_gld | etf_iau | bist"),
    period: str = Query("1mo", description="1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y",
                        pattern="^(1d|5d|1mo|3mo|6mo|1y|2y|5y)$"),
    aralik: str = Query("1d", description="1d, 1wk, 1mo",
                        pattern="^(1d|1wk|1mo)$"),
):
    """Geçmiş altın fiyat verisi (OHLCV)."""
    if kaynak not in KAYNAKLAR:
        raise HTTPException(400, f"Geçersiz kaynak. Seçenekler: {list(KAYNAKLAR.keys())}")
    meta = KAYNAKLAR[kaynak]
    tick = yf.Ticker(meta["sembol"])
    hist = tick.history(period=period, interval=aralik)
    if hist.empty:
        raise HTTPException(404, "Veri bulunamadı")
    kayitlar = [
        {
            "tarih": str(idx.date()),
            "acilis": round(float(row["Open"]), 4),
            "yuksek": round(float(row["High"]), 4),
            "dusuk": round(float(row["Low"]), 4),
            "kapanis": round(float(row["Close"]), 4),
            "hacim": int(row["Volume"]),
        }
        for idx, row in hist.iterrows()
    ]
    return {
        "kaynak": kaynak,
        "sembol": meta["sembol"],
        "para_birimi": meta["para"],
        "period": period,
        "aralik": aralik,
        "kayit_sayisi": len(kayitlar),
        "veriler": kayitlar,
    }
