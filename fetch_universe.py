"""
Evren Çekici — tek seferlik çalıştır.

Çeker:
  - BIST: BigPara'dan 600+ hisse listesi
  - S&P 500: GitHub/datahub CSV
  - Nasdaq 100: iShares ETF holdings JSON
  - Her hisse için yfinance metadata (sektör, sektör, website, piyasa değeri...)
  - Logo URL: yfinance logo_url → Clearbit fallback

Kaydeder:
  - api/data/bist_universe.json
  - api/data/sp500_universe.json
  - api/data/nasdaq100_universe.json
"""

import json
import re
import time
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime

import httpx
import yfinance as yf

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

H = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
    )
}

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# KAYNAK 1: BIST listesi — BigPara
# ─────────────────────────────────────────────────────────────────────────────
def fetch_bist_list() -> list[dict]:
    log("BIST listesi çekiliyor (BigPara)...")
    r = httpx.get(
        "https://bigpara.hurriyet.com.tr/api/v1/hisse/list/",
        headers=H, timeout=15
    )
    data = r.json()["data"]
    hisseler = [
        {"sembol": x["kod"], "ad": x["ad"].title(), "borsa": "BIST"}
        for x in data if x.get("tip") == "Hisse"
    ]
    log(f"  BIST: {len(hisseler)} hisse bulundu")
    return hisseler


# ─────────────────────────────────────────────────────────────────────────────
# KAYNAK 2: S&P 500 — GitHub datahub CSV
# ─────────────────────────────────────────────────────────────────────────────
def fetch_sp500_list() -> list[dict]:
    log("S&P 500 listesi çekiliyor...")
    sources = [
        "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv",
        "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv",
    ]
    for url in sources:
        try:
            r = httpx.get(url, headers=H, timeout=15, follow_redirects=True)
            if r.status_code == 200 and "Symbol" in r.text[:200]:
                lines = r.text.strip().split("\n")
                hisseler = []
                for line in lines[1:]:
                    parts = line.split(",")
                    if len(parts) >= 3:
                        sembol = parts[0].strip().strip('"')
                        ad = parts[1].strip().strip('"')
                        sektor = parts[2].strip().strip('"') if len(parts) > 2 else ""
                        hisseler.append({
                            "sembol": sembol,
                            "ad": ad,
                            "sektor": sektor,
                            "borsa": "NYSE/NASDAQ",
                            "endeks": "SP500"
                        })
                log(f"  S&P 500: {len(hisseler)} şirket ({url[:50]})")
                return hisseler
        except Exception as e:
            log(f"  {url[:50]}: {e}")
    log("  S&P 500 listesi alınamadı")
    return []


# ─────────────────────────────────────────────────────────────────────────────
# KAYNAK 3: Nasdaq 100 — iShares ETF holdings
# ─────────────────────────────────────────────────────────────────────────────
def fetch_nasdaq100_list() -> list[dict]:
    log("Nasdaq 100 listesi çekiliyor...")
    sources = [
        # iShares QQQ ETF holdings CSV
        "https://www.ishares.com/us/products/239726/NASDAQ_100_ETF/1467271812596.ajax?fileType=csv&fileName=QQQ_holdings&dataType=fund",
        # Wikipedia ile dene (farklı UA)
        "https://en.wikipedia.org/wiki/Nasdaq-100",
        # Statik GitHub liste
        "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nasdaq/nasdaq_full_tickers.json",
    ]

    # iShares CSV dene
    try:
        r = httpx.get(sources[0], headers={**H, "Referer": "https://www.ishares.com/"}, timeout=20, follow_redirects=True)
        if r.status_code == 200 and len(r.text) > 1000:
            lines = r.text.strip().split("\n")
            hisseler = []
            header_found = False
            for line in lines:
                if "Ticker" in line and "Name" in line:
                    header_found = True
                    continue
                if header_found and line.strip() and not line.startswith(","):
                    parts = line.split(",")
                    if len(parts) >= 2:
                        sembol = parts[0].strip().strip('"')
                        ad = parts[1].strip().strip('"')
                        if sembol and sembol.isalpha() and len(sembol) <= 5:
                            hisseler.append({
                                "sembol": sembol,
                                "ad": ad,
                                "borsa": "NASDAQ",
                                "endeks": "NDX100"
                            })
            if hisseler:
                log(f"  Nasdaq 100: {len(hisseler)} şirket (iShares)")
                return hisseler
    except Exception as e:
        log(f"  iShares HATA: {e}")

    # Wikipedia ile dene
    try:
        r = httpx.get(sources[1], headers={
            **H,
            "Accept": "text/html,application/xhtml+xml",
        }, timeout=15, follow_redirects=True)
        if r.status_code == 200:
            import re
            # Tablo satırlarını bul
            rows = re.findall(r'<tr[^>]*>.*?</tr>', r.text, re.DOTALL)
            hisseler = []
            for row in rows:
                cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
                if len(cells) >= 2:
                    # HTML tag'leri temizle
                    def clean(s):
                        return re.sub(r'<[^>]+>', '', s).strip()
                    sembol = clean(cells[0])
                    ad = clean(cells[1]) if len(cells) > 1 else ""
                    if sembol and len(sembol) <= 5 and sembol.replace("-","").isalpha():
                        hisseler.append({
                            "sembol": sembol,
                            "ad": ad,
                            "borsa": "NASDAQ",
                            "endeks": "NDX100"
                        })
            if len(hisseler) >= 90:
                log(f"  Nasdaq 100: {len(hisseler)} şirket (Wikipedia)")
                return hisseler
    except Exception as e:
        log(f"  Wikipedia HATA: {e}")

    log("  Nasdaq 100 listesi alınamadı, sabit liste kullanılıyor")
    # Fallback — sabit Nasdaq 100 listesi
    return [
        {"sembol": s, "ad": "", "borsa": "NASDAQ", "endeks": "NDX100"}
        for s in [
            "AAPL","MSFT","NVDA","AMZN","META","GOOGL","GOOG","TSLA","AVGO","COST",
            "NFLX","TMUS","AMD","PEP","ADBE","CSCO","QCOM","TXN","INTU","AMGN",
            "HON","BKNG","ISRG","SBUX","VRTX","MDLZ","REGN","LRCX","KLAC","PANW",
            "ASML","ABNB","SNPS","CDNS","MRVL","FTNT","ORLY","NXPI","PYPL","MELI",
            "ADP","KDP","CRWD","MNST","CTAS","PAYX","DXCM","IDXX","WDAY","ROST",
            "PCAR","SGEN","ILMN","ZS","ODFL","FANG","CEG","ON","CPRT","EXC",
            "XEL","FAST","VRSK","TEAM","MTCH","ENPH","DDOG","LCID","SIRI","RIVN",
            "WBD","DLTR","BIIB","MRNA","ZM","DOCU","OKTA","SPLK","COUP","RGEN",
            "SMCI","ARM","MSTR","TTD","HOOD","COIN","RBLX","SNAP","PINS","LYFT",
            "UBER","ABNB","DASH","AFRM","OPEN","ACMR","AMAT","LAM","KLAC","AEHR",
        ]
    ]


# ─────────────────────────────────────────────────────────────────────────────
# yfinance metadata çekici
# ─────────────────────────────────────────────────────────────────────────────
def logo_url(info: dict) -> str | None:
    """Logo URL'sini bul: yfinance logo_url → Google Favicon fallback."""
    # 1. yfinance direkt
    lu = info.get("logo_url")
    if lu:
        return lu
    # 2. Website'den Google Favicon URL oluştur
    website = info.get("website", "")
    if website:
        domain = re.sub(r"^https?://(www\.)?", "", website).rstrip("/").split("/")[0]
        if domain:
            return (
                f"https://t3.gstatic.com/faviconV2"
                f"?client=SOCIAL&type=FAVICON&fallback_opts=TYPE,SIZE,URL"
                f"&url=http://{domain}&size=128"
            )
    return None


def fetch_meta(sembol_yahoo: str, extra: dict) -> dict:
    """Tek hisse için yfinance metadata çek."""
    try:
        info = yf.Ticker(sembol_yahoo).info
        if not info or info.get("trailingPegRatio") == None and info.get("symbol") is None:
            return {**extra, "sembol_yahoo": sembol_yahoo, "meta_hata": "bilgi yok"}
        return {
            **extra,
            "sembol_yahoo": sembol_yahoo,
            "tam_ad": info.get("longName") or info.get("shortName") or extra.get("ad", ""),
            "sektor": info.get("sector", ""),
            "sektör_detay": info.get("industry", ""),
            "ulke": info.get("country", ""),
            "para_birimi": info.get("currency", ""),
            "borsa_kodu": info.get("exchange", ""),
            "piyasa_degeri": info.get("marketCap"),
            "website": info.get("website", ""),
            "logo_url": logo_url(info),
            "aciklama": (info.get("longBusinessSummary") or "")[:300],
        }
    except Exception as e:
        return {**extra, "sembol_yahoo": sembol_yahoo, "meta_hata": str(e)[:80]}


def enrich_batch(hisseler: list[dict], suffix: str = "", workers: int = 8,
                  delay: float = 0.3, limit: int | None = None) -> list[dict]:
    """Paralel yfinance info çekimi."""
    if limit:
        hisseler = hisseler[:limit]

    toplam = len(hisseler)
    sonuclar = []
    tamam = 0

    log(f"  {toplam} hisse için metadata çekiliyor ({workers} paralel iş)...")

    def job(item):
        sembol = item["sembol"] + suffix
        return fetch_meta(sembol, item)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(job, h): h for h in hisseler}
        for future in as_completed(futures):
            try:
                result = future.result()
                sonuclar.append(result)
            except Exception as e:
                sonuclar.append({**futures[future], "meta_hata": str(e)[:80]})
            tamam += 1
            if tamam % 50 == 0:
                log(f"    {tamam}/{toplam} tamamlandı...")
            time.sleep(delay / workers)  # hafif throttle

    return sonuclar


# ─────────────────────────────────────────────────────────────────────────────
# ANA AKIŞ
# ─────────────────────────────────────────────────────────────────────────────
def main():
    baslangic = time.time()
    log("=" * 60)
    log("EVREN ÇEKİCİ BAŞLADI")
    log("=" * 60)

    # ── BIST ────────────────────────────────────────────────────────────────
    log("\n[1/3] BIST")
    bist_liste = fetch_bist_list()

    log(f"  yfinance metadata çekiliyor ({len(bist_liste)} hisse)...")
    bist_zengin = enrich_batch(bist_liste, suffix=".IS", workers=10, delay=0.5)

    basarili = sum(1 for x in bist_zengin if "meta_hata" not in x)
    logolu = sum(1 for x in bist_zengin if x.get("logo_url"))
    log(f"  Metadata: {basarili}/{len(bist_zengin)} başarılı | Logo URL: {logolu}")

    bist_out = {
        "olusturma_tarihi": datetime.now().isoformat(),
        "toplam": len(bist_zengin),
        "hisseler": sorted(bist_zengin, key=lambda x: x["sembol"]),
    }
    (DATA_DIR / "bist_universe.json").write_text(
        json.dumps(bist_out, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log(f"  ✓ bist_universe.json kaydedildi ({len(bist_zengin)} hisse)")

    # ── S&P 500 ──────────────────────────────────────────────────────────────
    log("\n[2/3] S&P 500")
    sp500_liste = fetch_sp500_list()

    if sp500_liste:
        log(f"  yfinance metadata çekiliyor ({len(sp500_liste)} şirket)...")
        sp500_zengin = enrich_batch(sp500_liste, suffix="", workers=10, delay=0.5)

        basarili = sum(1 for x in sp500_zengin if "meta_hata" not in x)
        logolu = sum(1 for x in sp500_zengin if x.get("logo_url"))
        log(f"  Metadata: {basarili}/{len(sp500_zengin)} başarılı | Logo URL: {logolu}")

        sp500_out = {
            "olusturma_tarihi": datetime.now().isoformat(),
            "toplam": len(sp500_zengin),
            "hisseler": sorted(sp500_zengin, key=lambda x: x["sembol"]),
        }
        (DATA_DIR / "sp500_universe.json").write_text(
            json.dumps(sp500_out, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        log(f"  ✓ sp500_universe.json kaydedildi")

    # ── Nasdaq 100 ────────────────────────────────────────────────────────────
    log("\n[3/3] Nasdaq 100")
    ndx_liste = fetch_nasdaq100_list()

    log(f"  yfinance metadata çekiliyor ({len(ndx_liste)} şirket)...")
    ndx_zengin = enrich_batch(ndx_liste, suffix="", workers=10, delay=0.5)

    basarili = sum(1 for x in ndx_zengin if "meta_hata" not in x)
    logolu = sum(1 for x in ndx_zengin if x.get("logo_url"))
    log(f"  Metadata: {basarili}/{len(ndx_zengin)} başarılı | Logo URL: {logolu}")

    ndx_out = {
        "olusturma_tarihi": datetime.now().isoformat(),
        "toplam": len(ndx_zengin),
        "hisseler": sorted(ndx_zengin, key=lambda x: x["sembol"]),
    }
    (DATA_DIR / "nasdaq100_universe.json").write_text(
        json.dumps(ndx_out, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log(f"  ✓ nasdaq100_universe.json kaydedildi")

    # ── ÖZET ─────────────────────────────────────────────────────────────────
    sure = round(time.time() - baslangic, 1)
    log(f"\n{'='*60}")
    log(f"TAMAMLANDI — {sure} saniye")
    log(f"  BIST     : {len(bist_zengin):>4} hisse  → bist_universe.json")
    log(f"  S&P 500  : {len(sp500_zengin) if sp500_liste else 0:>4} şirket → sp500_universe.json")
    log(f"  Nasdaq100: {len(ndx_zengin):>4} şirket → nasdaq100_universe.json")
    log(f"  TOPLAM   : {len(bist_zengin) + (len(sp500_zengin) if sp500_liste else 0) + len(ndx_zengin):>4} asset")
    log(f"{'='*60}")


if __name__ == "__main__":
    main()
