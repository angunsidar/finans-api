# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Deployment

- **Platform:** Render.com free tier — `https://finans-api-ztnv.onrender.com`
- **Keep-alive:** `github.com/angunsidar/api-keepalive` (public repo, GitHub Actions, 5 dk ping)
- **Deploy:** `git push origin main` → Render otomatik deploy eder (~3 dk)
- **Local dev:** `uvicorn main:app --reload`

## Auth

`API_KEYS` env var (Render dashboard) — virgülle ayrılmış geçerli keyler.  
Kendi Flutter uygulaması için: `myapp-8b91ec2472bf052240d3968eac17dc2d`  
RapidAPI satışı: `RAPIDAPI_PROXY_SECRET` env var ile proxy secret.  
`/health`, `/docs`, `/openapi.json` key gerektirmez.

## Router Mimarisi

Her router `routers/` altında bağımsız bir dosya. `main.py` sadece middleware + router include yapar.

| Router | Veri kaynağı | Cache | Fallback |
|---|---|---|---|
| `altin.py` | yfinance GC=F × USDTRY=X | 15 dk | Coinbase XAU-USD/TRY |
| `gumus.py` | yfinance XAG=X × USDTRY=X | 5 dk | Coinbase XAG-USD/TRY → CoinGecko kinesis-silver |
| `doviz.py` | yfinance {KOD}TRY=X | yok | yok |
| `bist.py` | yfinance {SEMBOL}.IS | 5 dk | stale (son başarılı veri) |
| `abd.py` | yfinance direkt sembol | 5 dk | stale (son başarılı veri) |
| `kripto.py` | CoinGecko /simple/price | 5 dk | stale (son başarılı veri) |
| `evren.py` | `data/*.json` (disk) | bellek | — |

## Cache + Stale Fallback Standart Paterni

Yeni router eklerken bu paterni kullan (bist.py veya abd.py'yi referans al):

```python
_cache: dict[str, tuple[float, dict]] = {}
_stale: dict[str, dict] = {}
_TTL = 300  # saniye

def _cache_get(key): ...   # TTL kontrolü
def _cache_set(key, val):  # hem _cache hem _stale güncelle

def _fetch(sembol):
    cached = _cache_get(sembol)
    if cached: return cached
    try:
        # ... yfinance çağrısı ...
        _cache_set(key, result)
        return result
    except Exception as e:
        if key in _stale: return _stale[key]   # stale fallback
        raise HTTPException(503, ...)
```

## Evren Endpoint'leri (Özel Durum)

`/evren/*` — gerçek zamanlı değil, `data/` klasöründeki JSON dosyalarından okur:
- `data/bist_universe.json` — ~644 BIST hissesi + metadata + logo URL
- `data/sp500_universe.json` — S&P 500 bileşenleri
- `data/nasdaq100_universe.json` — Nasdaq bileşenleri

Bu dosyalar `fetch_universe.py` ile güncellenir veya API üzerinden `POST /evren/guncelle` çağrılabilir.

## Bilinen Sorunlar

- **yfinance rate-limit:** Yahoo Finance özellikle sabah 10'da (BIST açılışı) rate-limit uygular. Cache + stale fallback ile çözüldü.
- **metals.live:** JSON döndürmüyor, HTML redirect — kullanma.
- **CoinGecko ücretsiz limit:** ~30 istek/dk. 5 dk cache ile aşılıyor.
- **`doviz.py`'de cache yok:** Döviz kurları az değiştiği için sorun yaşanmadı; yoğun kullanımda eklenebilir.
