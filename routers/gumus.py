"""
Gümüş fiyatı endpoint'leri.
yfinance üzerinden — spot ve ETF.
"""
from __future__ import annotations

import yfinance as yf
from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/gumus", tags=["gümüş"])

# Kaynak tanımları
KAYNAKLAR = {
    "spot":    "XAG=X",
    "etf_slv": "SLV",
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
    for key, sembol in KAYNAKLAR.items():
        try:
            veri = _fetch(sembol)
            sonuclar[key] = veri
        except Exception as e:
            sonuclar[key] = {"hata": str(e)[:60]}
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

        xag_usd = silver["fiyat"]
        usdtry = usd["fiyat"]
        gumus_tl_ons = round(xag_usd * usdtry, 2)
        gumus_tl_gram = round((xag_usd * usdtry) / 31.1035, 2)  # 1 troy ons = 31.1035 gram

        return {
            "gumus_usd_ons": xag_usd,
            "usd_try": usdtry,
            "gumus_tl_ons": gumus_tl_ons,
            "gumus_tl_gram": gumus_tl_gram,
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
    aralik: str = Query("1d", description="1d, 1wk, 1mo"),
):
    """Geçmiş gümüş fiyat verisi (OHLCV)."""
    sembol = "XAG=X"
    tick = yf.Ticker(sembol)
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
        "kaynak": sembol,
        "period": period,
        "aralik": aralik,
        "veri": kayitlar,
    }
