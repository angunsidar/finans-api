"""
BIST (Borsa İstanbul) endpoint'leri.
yfinance üzerinden gerçek zamanlı ve geçmiş veriler.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

import yfinance as yf
from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/bist", tags=["bist"])

# Yaygın BIST hisseleri (sembol → tam ad)
POPULER_HISSELER: dict[str, str] = {
    "THYAO": "Türk Hava Yolları",
    "AKBNK": "Akbank",
    "GARAN": "Garanti BBVA",
    "ISCTR": "İş Bankası C",
    "KCHOL": "Koç Holding",
    "SAHOL": "Sabancı Holding",
    "SISE": "Şişe Cam",
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


def _ticker(sembol: str) -> str:
    """Kullanıcı sembolünü Yahoo Finance formatına çevir."""
    sembol = sembol.upper().strip()
    if not sembol.endswith(".IS") and not sembol.startswith("^"):
        sembol = sembol + ".IS"
    return sembol


def _fetch_info(sembol: str) -> dict:
    tick = yf.Ticker(_ticker(sembol))
    info = tick.fast_info
    hist = tick.history(period="2d")
    if hist.empty:
        raise HTTPException(404, f"Hisse bulunamadı veya veri yok: {sembol.upper()}")

    son = hist.iloc[-1]
    onceki = hist.iloc[-2] if len(hist) > 1 else hist.iloc[-1]
    kapanis = float(son["Close"])
    onceki_kapanis = float(onceki["Close"])
    degisim = kapanis - onceki_kapanis
    degisim_yuzde = (degisim / onceki_kapanis * 100) if onceki_kapanis else 0.0

    return {
        "sembol": sembol.upper(),
        "fiyat": round(kapanis, 4),
        "acilis": round(float(son.get("Open", 0)), 4),
        "yuksek": round(float(son.get("High", 0)), 4),
        "dusuk": round(float(son.get("Low", 0)), 4),
        "hacim": int(son.get("Volume", 0)),
        "degisim_tl": round(degisim, 4),
        "degisim_yuzde": round(degisim_yuzde, 2),
        "para_birimi": "TRY",
        "tarih": str(hist.index[-1].date()),
    }


@router.get("/liste", summary="Popüler BIST hisseleri listesi")
def liste():
    """İzlemeye hazır popüler BIST hisseleri ve endeksler."""
    return {
        "hisseler": [
            {"sembol": s, "ad": a} for s, a in POPULER_HISSELER.items()
        ],
        "endeksler": [
            {"sembol": s, "ad": a} for s, a in ENDEKSLER.items()
        ],
    }


@router.get("/hisse/{sembol}", summary="Anlık hisse fiyatı")
def hisse_fiyat(sembol: str):
    """
    Hisse senedi anlık fiyat bilgisi.

    - `/bist/hisse/THYAO` → Türk Hava Yolları
    - `/bist/hisse/AKBNK` → Akbank
    """
    return _fetch_info(sembol)


@router.get("/hisse/{sembol}/gecmis", summary="Geçmiş fiyat verisi")
def hisse_gecmis(
    sembol: str,
    period: str = Query(
        "1mo",
        description="1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y",
        pattern="^(1d|5d|1mo|3mo|6mo|1y|2y|5y)$",
    ),
    aralik: str = Query(
        "1d",
        description="Veri aralığı: 1m, 5m, 15m, 1h, 1d, 1wk, 1mo",
        pattern="^(1m|5m|15m|1h|1d|1wk|1mo)$",
    ),
):
    """
    Geçmiş OHLCV verisi.

    - `period=1mo&aralik=1d` → Son 1 ay, günlük kapanış
    - `period=1d&aralik=5m` → Bugün, 5 dakikalık
    """
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


@router.get("/endeks/{sembol}", summary="Endeks fiyatı")
def endeks_fiyat(sembol: str):
    """
    BIST endeks değeri.

    - `/bist/endeks/XU100` → BIST 100
    - `/bist/endeks/XU030` → BIST 30
    """
    sembol = sembol.upper()
    # BIST endeksleri Yahoo Finance'de XU100.IS formatında
    yahoo_sembol = sembol + ".IS"
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

    return {
        "sembol": sembol,
        "ad": ENDEKSLER.get(sembol, sembol),
        "deger": round(kapanis, 2),
        "degisim": round(degisim, 2),
        "degisim_yuzde": round(degisim_yuzde, 2),
        "tarih": str(hist.index[-1].date()),
    }


@router.get("/toplu", summary="Çoklu hisse fiyatı")
def toplu_fiyat(
    semboller: str = Query(
        ...,
        description="Virgülle ayrılmış hisse sembolleri. Örn: THYAO,AKBNK,GARAN",
    )
):
    """Birden fazla hisseyi tek sorguda getir."""
    liste = [s.strip() for s in semboller.split(",") if s.strip()]
    if not liste:
        raise HTTPException(400, "En az bir sembol giriniz.")
    if len(liste) > 20:
        raise HTTPException(400, "En fazla 20 sembol sorgulanabilir.")

    tickers = [_ticker(s) for s in liste]
    data = yf.download(
        tickers,
        period="2d",
        interval="1d",
        progress=False,
        group_by="ticker",
        auto_adjust=True,
    )

    sonuclar = []
    for sembol in liste:
        try:
            sonuclar.append(_fetch_info(sembol))
        except HTTPException:
            sonuclar.append({"sembol": sembol.upper(), "hata": "veri bulunamadı"})

    return {"sayı": len(sonuclar), "veriler": sonuclar}
