"""
Finans API — BIST · ABD · Altın · Döviz · Kripto
FastAPI tabanlı, API-key korumalı, RapidAPI destekli.

Ortam değişkenleri:
  API_KEYS            → Geçerli key'ler, virgülle ayrılmış. Örn: "key1,key2,key3"
  RAPIDAPI_PROXY_SECRET → RapidAPI dashboard'dan alınan proxy secret (opsiyonel)
  FREE_PATHS          → Key gerektirmeyen path'ler (varsayılan: /health,/docs,/openapi.json,/redoc)
"""
from __future__ import annotations

import os
import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware

from routers import bist, kripto, altin, doviz, abd, evren, gumus

_logger = logging.getLogger("uvicorn.error")


async def _warm_caches():
    """
    API başladığında kritik cache'leri arka planda ısıt.
    Stale dict'ler dolunca restart sonrası ilk istek bile boş dönmez.
    """
    await asyncio.sleep(3)  # Uvicorn tam başlasın

    # Altın (GC=F futures + USD/TRY kuru)
    for sym in ["GC=F", "USDTRY=X"]:
        try:
            altin._fetch(sym)
            _logger.info(f"Warm-up ✓ altin/{sym}")
        except Exception as e:
            _logger.warning(f"Warm-up ✗ altin/{sym}: {e}")
        await asyncio.sleep(1)

    # Kripto (bitcoin, ethereum, solana, ripple)
    try:
        kripto._get("/simple/price", {
            "ids": "bitcoin,ethereum,solana,ripple,tether",
            "vs_currencies": "usd,try",
            "include_24hr_change": "true",
            "include_24hr_vol": "true",
            "include_market_cap": "true",
            "include_last_updated_at": "true",
        })
        _logger.info("Warm-up ✓ kripto")
    except Exception as e:
        _logger.warning(f"Warm-up ✗ kripto: {e}")

    await asyncio.sleep(1)

    # Gümüş
    try:
        gumus.gumus_tl()
        _logger.info("Warm-up ✓ gumus")
    except Exception as e:
        _logger.warning(f"Warm-up ✗ gumus: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(_warm_caches())
    yield

# ─── Ortam değişkenleri ────────────────────────────────────────────────────────
_raw_keys = os.getenv("API_KEYS", "")
VALID_KEYS: set[str] = {k.strip() for k in _raw_keys.split(",") if k.strip()}

RAPIDAPI_SECRET = os.getenv("RAPIDAPI_PROXY_SECRET", "")

# Bu path'ler key kontrolünden muaf
FREE_PATHS = {
    "/",
    "/health",
    "/docs",
    "/redoc",
    "/openapi.json",
}


# ─── API Key Middleware ────────────────────────────────────────────────────────
class ApiKeyMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Muaf path'ler
        if request.url.path in FREE_PATHS:
            return await call_next(request)

        # Eğer hiç key tanımlanmamışsa (geliştirme modu) geçir
        if not VALID_KEYS and not RAPIDAPI_SECRET:
            return await call_next(request)

        # RapidAPI üzerinden gelen istekler
        if RAPIDAPI_SECRET:
            proxy_secret = request.headers.get("X-RapidAPI-Proxy-Secret", "")
            if proxy_secret == RAPIDAPI_SECRET:
                return await call_next(request)

        # Direkt erişim: X-API-Key veya ?api_key=... ile
        key = (
            request.headers.get("X-API-Key")
            or request.headers.get("Authorization", "").removeprefix("Bearer ")
            or request.query_params.get("api_key", "")
        )
        if key and key in VALID_KEYS:
            return await call_next(request)

        raise HTTPException(
            status_code=401,
            detail={
                "hata": "Geçersiz veya eksik API key.",
                "kullanim": "X-API-Key header'ı veya ?api_key=<key> ile gönderin.",
                "satin_al": "https://rapidapi.com/angunsidar/api/finans-api",
            },
        )


# ─── FastAPI App ───────────────────────────────────────────────────────────────
app = FastAPI(
    lifespan=lifespan,
    title="Finans API",
    description=(
        "## Türkiye & Dünya Finans Verileri\n\n"
        "Gerçek zamanlı (Yahoo Finance 15dk gecikme) ve anlık (CoinGecko) veriler.\n\n"
        "### Kapsam\n"
        "- **BIST** — 600+ hisse, 5 endeks, geçmiş veri\n"
        "- **ABD** — S&P 500, Nasdaq, NYSE, ETF'ler\n"
        "- **Altın** — Gram · Çeyrek · Yarım · Tam · Reşat · Ata (TL)\n"
        "- **Döviz** — USD · EUR · GBP · CHF · JPY + 10 döviz/TL, 7 çapraz kur\n"
        "- **Kripto** — 5000+ coin (CoinGecko), piyasa listesi, trend\n"
        "- **Evren** — 1600+ hisse metadata + logo URL\n\n"
        "### Kimlik Doğrulama\n"
        "Her istekte `X-API-Key` header'ı gereklidir.\n"
        "RapidAPI üzerinden abone olanlarda otomatik eklenir."
    ),
    version="1.0.0",
    contact={"name": "Sidar", "email": "angunsidar@gmail.com"},
    license_info={"name": "Commercial"},
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(ApiKeyMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(bist.router)
app.include_router(kripto.router)
app.include_router(altin.router)
app.include_router(doviz.router)
app.include_router(abd.router)
app.include_router(evren.router)
app.include_router(gumus.router)


# ─── Genel Endpoints ──────────────────────────────────────────────────────────
@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/docs")


@app.get("/health", tags=["meta"], summary="Servis durumu")
def health():
    """API key gerektirmez. Servis ayakta mı kontrol et."""
    return {"status": "ok", "version": "1.0.0"}
