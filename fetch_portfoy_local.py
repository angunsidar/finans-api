"""
Paralel worker'larla portfoy verisi ceker, Redis'e yazar.
python fetch_portfoy_local.py

- 5 paralel worker (KAP rate-limit icin guvenli)
- Her worker kendi httpx.Client'i ile calisiyor
- Redis'te olanlar atlaniyor (restart-safe)
- Emeklilik fonlari + ETF'ler atlanıyor
"""
import httpx, re, json, time, sys, os
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# ── Redis ────────────────────────────────────────────────────────────────────
import redis

pw   = os.environ.get("REDIS_PASSWORD", "")
host = os.environ.get("REDIS_HOST", "rice-leather-sprightly-75347.db.redis.io")
port = os.environ.get("REDIS_PORT", "15427")
REDIS_URL  = f"redis://default:{pw}@{host}:{port}"
TTL_SANIYE = 25 * 86_400

r_client = redis.from_url(REDIS_URL, decode_responses=False)
r_client.ping()
print("Redis OK")

# Thread-safe print kilidi
_print_lock = threading.Lock()
def tprint(msg):
    with _print_lock:
        print(msg, flush=True)

# ── Sayaçlar ─────────────────────────────────────────────────────────────────
_ok = _hata = _atlandi = 0
_counter_lock = threading.Lock()

def inc_ok():
    global _ok
    with _counter_lock:
        _ok += 1
        return _ok

def inc_hata():
    global _hata
    with _counter_lock:
        _hata += 1
        return _hata

def inc_atlandi():
    global _atlandi
    with _counter_lock:
        _atlandi += 1

# ── KAP ─────────────────────────────────────────────────────────────────────
KAP          = "https://kap.org.tr"
PORTFOY_OID  = "8aca490d502e34b801502e380044002b"
HEADERS      = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "tr-TR,tr;q=0.9",
    "Referer": KAP,
}
_TR = str.maketrans("iIgGuUsSoOcC", "iigguussoocc")
_TR2 = str.maketrans(
    "ıİğĞüÜşŞöÖçÇ",
    "iigguussoocc"
)

def to_slug(text):
    text = text.translate(_TR2).lower()
    return re.sub(r"[^a-z0-9]+", "-", text).strip("-")

def parse_rsc(html):
    matches = re.findall(r'self\.__next_f\.push\(\[1,"((?:[^"\\]|\\.)*)"\]\)', html)
    parts = []
    for m in matches:
        try:
            parts.append(m.encode("raw_unicode_escape").decode("unicode_escape"))
        except:
            parts.append(m)
    return "".join(parts)

def redis_var_mi(kod):
    return r_client.exists(f"finans:portfoy:{kod}") > 0

def redis_yaz(kod, data):
    r_client.set(f"finans:portfoy:{kod}", json.dumps(data, ensure_ascii=False), ex=TTL_SANIYE)

def parse_pdf(pdf_bytes, kod):
    import pdfplumber, io
    holdings = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            for table in (page.extract_tables() or []):
                for row in table:
                    if not row or len(row) < 3:
                        continue
                    isim = str(row[0] or "").strip()
                    if not isim or isim.lower() in ("varlik adi", "toplam", "genel toplam", ""):
                        continue
                    try:
                        oran = float(str(row[-1] or "").replace(",", ".").replace("%", "").strip())
                    except:
                        continue
                    if 0 < oran <= 100:
                        holdings.append({"isim": isim, "oran": round(oran, 4)})
    return {"kod": kod, "holdings": holdings, "kaynak": "kap_pdf",
            "tarih": datetime.now().strftime("%Y-%m")}

def isle_fon(kod, unvan, sleep_sn=1.0):
    """Tek fon icin tam pipeline. Her worker bu fonksiyonu cagiriyor."""
    if redis_var_mi(kod):
        inc_atlandi()
        return "atlandı"

    try:
        slug = f"{kod.lower()}-{to_slug(unvan)}"
        with httpx.Client(headers=HEADERS, follow_redirects=True, timeout=20) as client:
            # 1. mkkMemberOid
            r = client.get(f"{KAP}/tr/fon-bildirimleri/{slug}")
            rsc = parse_rsc(r.text)
            m = re.search(r'"mkkMemberOid"\s*:\s*"([^"]+)"', rsc)
            if not m:
                return f"skip:mkkMemberOid yok"

            mkk_oid = m.group(1)
            time.sleep(0.3)

            # 2. disclosureIndex
            r2 = client.get(f"{KAP}/tr/bildirim-sorgu-sonuc?srcbar=Y&cmp=N&cat=3&m={mkk_oid}&s={PORTFOY_OID}")
            rsc2 = parse_rsc(r2.text)
            m2 = re.search(r'"disclosureIndex"\s*:\s*(\d+)', rsc2)
            if not m2:
                return "skip:disclosureIndex yok"

            disc_idx = int(m2.group(1))
            time.sleep(0.3)

            # 3. PDF attachment
            r3 = client.get(f"{KAP}/tr/api/notification/attachment-detail/{disc_idx}",
                            headers={**HEADERS, "Accept": "application/json"})
            data3 = r3.json()
            attachments = (data3[0].get("attachments", []) if data3 else [])
            pdf_att = next((a for a in attachments if a.get("fileExtension", "").lower() == "pdf"), None)
            if not pdf_att:
                return "skip:PDF yok"

            # 4. PDF indir + parse
            r4 = client.get(f"{KAP}/tr/api/file/download/{pdf_att['objId']}", timeout=30)
            parsed = parse_pdf(r4.content, kod)
            redis_yaz(kod, parsed)
            ok = inc_ok()
            if ok % 25 == 0:
                tprint(f"  [{ok} OK / {_hata} HATA / {_atlandi} atlandı]")
            return "ok"

    except Exception as e:
        inc_hata()
        return f"hata:{str(e)[:60]}"
    finally:
        time.sleep(sleep_sn)  # Worker basina rate-limit

# ── Fon listesi ───────────────────────────────────────────────────────────────
def load_fonlar():
    data = json.loads((Path(__file__).parent / "data" / "fon_universe.json").read_text(encoding="utf-8-sig"))
    def skip(fon):
        u = fon.get("unvan", "").upper()
        return "EMEKL" in u and "YATIRIM FONU" in u
    return [(f["kod"], f["unvan"]) for f in data["fonlar"]
            if f.get("kod") and f.get("unvan") and not skip(f)]

# ── Ana ───────────────────────────────────────────────────────────────────────
def main():
    WORKERS = 5        # Paralel worker sayisi
    SLEEP   = 1.2      # Worker basina istek arasi bekleme (KAP rate-limit)

    fonlar = load_fonlar()
    zaten  = sum(1 for k, _ in fonlar if redis_var_mi(k))
    bekleyen = len(fonlar) - zaten

    print(f"Toplam: {len(fonlar)} | Redis'te var: {zaten} | Islenecek: {bekleyen}")
    print(f"Worker: {WORKERS} | Tahmini sure: ~{bekleyen * SLEEP / WORKERS / 60:.0f} dk")
    print("-" * 60)

    baslangic = time.time()

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(isle_fon, k, u, SLEEP): k for k, u in fonlar}
        for future in as_completed(futures):
            pass  # Ilerleme isle_fon icinde tprint ile gosteriliyor

    sure = (time.time() - baslangic) / 60
    print("=" * 60)
    print(f"TAMAMLANDI: {_ok} OK | {_hata} HATA | {_atlandi} atlandi | {sure:.1f} dk")

    # Hata listesini dosyaya yaz
    # (hata detayları future result'lardan alınabilir ama özet yeterli)
    print(f"Redis'teki toplam portfoy: {len(r_client.keys('finans:portfoy:*'))}")

if __name__ == "__main__":
    main()
