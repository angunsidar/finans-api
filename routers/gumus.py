"""
Gümüş fiyatı endpoint'leri.
yfinance üzerinden — spot (XAG=X) ve SLV ETF.
"""
from __future__ import annotations

import yfinance as yf
from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/gumus", tags=["gümüş"])

KAYNAKLAR = {
    "spot": {"sembol": "XAG=X", "ad": "Gümüş Spot (XAG/USD)", "para": "USD", "birim": "ons"},
    "etf_slv": {"sembol": "SLV", "ad": "iShares Silver ETF", "para": "USD", "birim": "hisse"},
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


@router.get("", summary="Tüm gümüş kaynakları özet")
def gumus_ozet():
    """Spot ve ETF gümüş fiyatlarını tek sorguda getir."""
    sonuclar = {}
    for key, meta in KAYNAKLAR.items():
        try:
            veri = _fetch(meta["sembol"])
            sonuclar[key] = {**meta, **veri}
        except Exception as e:
            sonuclar[key] = {**meta, "hata": str(e)[:60]}
    return sonuclar


@router.get("/tl", summary="Gümüş TL karşılığı (hesaplanmış)")
def gumus_tl():
    """
    Gümüş USD/ons fiyatı × Dolar/TL kuru = TL/ons.
    Gram hesabı için 31.1035'e bölünür.
    """
    try:
        silver = _fetch("XAG=X")
        usd = _fetch("USDTRY=X")

        ons_fiyat_usd = silver["fiyat"]
        usdtry = usd["fiyat"]
        ons_fiyat_tl = round(ons_fiyat_usd * usdtry, 2)
        gram_fiyat_tl = round(ons_fiyat_tl / 31.1035, 2)

        return {
            "gumus_usd_ons": ons_fiyat_usd,
            "usd_try": usdtry,
            "gumus_tl_ons": ons_fiyat_tl,
            "gumus_tl_gram": gram_fiyat_tl,
            "degisim_yuzde": silver["degisim_yuzde"],
            "not": "XAG=X spot × USDTRY=X kuru ile hesaplanmıştır",
            "tarih": silver["tarih"],
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@router.get("/gecmis", summary="Geçmiş gümüş fiyatları")
def gumus_gecmis(
    period: str = Query("1mo", description="1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y",
                        pattern="^(1d|5d|1mo|3mo|6mo|1y|2y|5y)$"),
    aralik: str = Query("1d", description="1d, 1wk, 1mo",
                        pattern="^(1d|1wk|1mo)$"),
):
    """Geçmiş gümüş fiyat verisi (OHLCV) — XAG=X."""
    tick = yf.Ticker("XAG=X")
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
            "hacim": int(row["Volume"]) if row["Volume"] == row["Volume"] else 0,
        }
        for idx, row in hist.iterrows()
    ]
    return {"kaynak": "XAG=X", "period": period, "aralik": aralik, "veri": kayitlar}
