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
import concurrent.futures
import logging
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware

from routers import bist, kripto, altin, doviz, abd, evren, gumus

_logger = logging.getLogger("uvicorn.error")

# Background worker için ayrı thread pool — FastAPI'nin pool'unu işgal etmez
_bg_executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=3, thread_name_prefix="bg_worker"
)


async def _warm_caches():
    """
    API başladığında kritik cache'leri arka planda ısıt.
    1. Önce Redis'ten oku → stale dict'leri anında doldur (milisaniye)
    2. Sonra yfinance/CoinGecko'dan taze veri çek → stale + Redis güncelle
    """
    await asyncio.sleep(1)  # Uvicorn tam başlasın

    # ── Adım 1: Redis'ten anında yükle ──────────────────────────────────────
    from redis_cache import rget_many

    altin_keys   = ["GC=F", "__cb_xau__"]
    doviz_keys   = ["USDTRY=X", "EURTRY=X", "GBPTRY=X", "CHFTRY=X",
                    "JPYTRY=X", "AUDTRY=X", "CADTRY=X"]
    kripto_coins = kripto.WARM_COINS

    # BIST ve ABD: kullanıcının portföyündeki semboller dinamik olduğundan
    # Redis'te hangi key'lerin olduğunu önceden bilemeyiz — scan ile alıyoruz
    redis_altin  = rget_many([f"finans:altin:{k}"  for k in altin_keys])
    redis_doviz  = rget_many([f"finans:doviz:{k}"  for k in doviz_keys])
    redis_kripto = rget_many([f"finans:kripto:{c}" for c in kripto_coins])
    redis_gumus  = rget_many(["finans:gumus:tl"])

    from redis_cache import rget_prefix
    redis_bist = rget_prefix("finans:bist:")
    redis_abd  = rget_prefix("finans:abd:")

    # _stale VE _cache ikisini birden doldur.
    # _stale: hata durumunda fallback
    # _cache: ilk kullanıcı isteği direkt cache'ten döner, yfinance/CoinGecko beklemez
    now = time.time()
    loaded = 0

    for k, rk in zip(altin_keys, [f"finans:altin:{k}" for k in altin_keys]):
        v = redis_altin.get(rk)
        if v:
            altin._stale[k] = v
            altin._cache[k] = (now, v)
            loaded += 1
    for k, rk in zip(doviz_keys, [f"finans:doviz:{k}" for k in doviz_keys]):
        v = redis_doviz.get(rk)
        if v:
            doviz._stale[k] = v
            doviz._cache[k] = (now, v)
            loaded += 1
    for c, rk in zip(kripto_coins, [f"finans:kripto:{c}" for c in kripto_coins]):
        v = redis_kripto.get(rk)
        if v:
            kripto._coin_stale[c] = v
            kripto._coin_cache[c] = (now, v)
            loaded += 1
    v = redis_gumus.get("finans:gumus:tl")
    if v:
        gumus._stale["tl"] = v
        gumus._cache["tl"] = (now, v)
        loaded += 1
    for rk, val in redis_bist.items():
        if val:
            sembol = rk.replace("finans:bist:", "")
            bist._stale[sembol] = val
            bist._cache[sembol] = (now, val)
            loaded += 1
    for rk, val in redis_abd.items():
        if val:
            sembol = rk.replace("finans:abd:", "")
            abd._stale[sembol] = val
            abd._cache[sembol] = (now, val)
            loaded += 1

    _logger.info(f"Redis pre-load: {loaded} key yüklendi → _cache + _stale dolu, ilk istek <10ms")

    await asyncio.sleep(2)  # Kısa bekleme sonrası taze veri çek

    # ── Önce döviz — USDTRY dahil; altin + gumus bunu okur ───────────────────
    warmed = doviz.warm_up()
    if warmed:
        _logger.info(f"Warm-up ✓ doviz: {warmed}")
    else:
        _logger.warning("Warm-up ✗ doviz: hiçbiri alınamadı")

    await asyncio.sleep(1)

    # Altın futures (GC=F) — USDTRY artık doviz cache'inden okunur
    try:
        altin._fetch("GC=F", force=True)
        _logger.info("Warm-up ✓ altin/GC=F")
    except Exception as e:
        _logger.warning(f"Warm-up ✗ altin/GC=F: {e}")

    await asyncio.sleep(1)

    # Kripto — top 10 coin per-coin cache'e alınır
    ok, info = kripto.warm_up()
    if ok:
        _logger.info(f"Warm-up ✓ kripto: {info}")
    else:
        _logger.warning(f"Warm-up ✗ kripto: {info}")

    await asyncio.sleep(1)

    # Gümüş — USDTRY artık doviz cache'inden okunur
    try:
        gumus.gumus_tl(force=True)
        _logger.info("Warm-up ✓ gumus")
    except Exception as e:
        _logger.warning(f"Warm-up ✗ gumus: {e}")


async def _fetch_all():
    """
    Tüm kritik veriyi paralel olarak dedicated thread pool'da çeker.
    FastAPI'nin thread pool'unu işgal etmez — kullanıcı istekleri etkilenmez.
    force=True → yfinance/Bigpara/CoinGecko'ya gider (Redis fallback'i atlar).

    USDTRY: artık sadece doviz.warm_up içinde çekiliyor.
    altin ve gumus, doviz modülünün cache'inden okur (duplicate yfinance yok).
    BIST: bist.warm_up() Bigpara'ya istek atar; borsa kapalıysa kendi kendine döner.
    """
    loop = asyncio.get_event_loop()
    results = await asyncio.gather(
        loop.run_in_executor(_bg_executor, lambda: altin._fetch("GC=F", force=True)),
        loop.run_in_executor(_bg_executor, kripto.warm_up),
        loop.run_in_executor(_bg_executor, doviz.warm_up),       # USDTRY dahil
        loop.run_in_executor(_bg_executor, lambda: gumus.gumus_tl(force=True)),
        loop.run_in_executor(_bg_executor, bist.warm_up),        # Bigpara, sadece borsa açıkken
        loop.run_in_executor(_bg_executor, abd.warm_up),         # Top 6 ABD hissesi
        return_exceptions=True,
    )
    for name, r in zip(
        ["altin/GC=F", "kripto", "doviz", "gumus", "bist", "abd"], results
    ):
        if isinstance(r, Exception):
            _logger.warning(f"BG fetch ✗ {name}: {r}")
        else:
            _logger.debug(f"BG fetch ✓ {name}")


async def _background_worker():
    """
    Arka plan döngüsü — her 5 dakikada bir _fetch_all() çağırır.
    Kullanıcı isteği beklemeden cache her zaman sıcak kalır.
    """
    await asyncio.sleep(35)  # İlk warm-up bitsin, sonra döngüye gir
    while True:
        try:
            await _fetch_all()
            _logger.info("BG worker: tüm veri güncellendi")
        except Exception as e:
            _logger.warning(f"BG worker genel hata: {e}")
        await asyncio.sleep(300)  # 5 dakika bekle, tekrar çek


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(_warm_caches())
    asyncio.create_task(_background_worker())
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


@app.api_route("/health", methods=["GET", "HEAD"], tags=["meta"], summary="Servis durumu")
def health():
    """API key gerektirmez. Servis ayakta mı kontrol et."""
    return {"status": "ok", "version": "1.0.0"}
