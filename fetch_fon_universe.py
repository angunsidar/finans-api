# -*- coding: utf-8 -*-
"""
TEFAS Fon Universe Çekici — tek seferlik veya aylık çalıştır.

fonUnvanAra (boş arama) → 2500+ fon kodu + unvan
fonBilgiGetir             → kategori, portföy büyüklüğü, yatırımcı sayısı (seçili fonlar)

Kaydeder: data/fon_universe.json
"""
from __future__ import annotations

import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import httpx

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
OUT_FILE = DATA_DIR / "fon_universe.json"

_TEFAS_BEARER = "ST-tefaswebwse3irfmSBj4iRAzGPbAlS94Se"
_HEADERS = {
    "Authorization": f"Bearer {_TEFAS_BEARER}",
    "Content-Type": "application/json",
    "Accept": "*/*",
    "Accept-Language": "tr-TR,tr;q=0.9",
    "Origin": "https://www.tefas.gov.tr",
    "Referer": "https://www.tefas.gov.tr/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
}


def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def fetch_all_fund_codes() -> list[dict]:
    """fonUnvanAra boş aramayla tüm fon listesini çek."""
    log("TEFAS fonUnvanAra çekiliyor...")
    with httpx.Client(timeout=20, follow_redirects=True) as c:
        r = c.post(
            "https://www.tefas.gov.tr/api/funds/fonUnvanAra",
            json={"aramaMetni": "", "dil": "TR"},
            headers=_HEADERS,
        )
        r.raise_for_status()
        data = r.json()
        funds = data.get("resultList", data if isinstance(data, list) else [])
        log(f"  {len(funds)} fon bulundu")
        return funds


def fetch_fund_detail(kod: str, client: httpx.Client) -> dict | None:
    """Tek fon için kategori ve ek metadata çek."""
    try:
        r = client.post(
            "https://www.tefas.gov.tr/api/funds/fonBilgiGetir",
            json={"fonKodu": kod, "dil": "TR"},
            headers=_HEADERS,
        )
        if r.status_code != 200:
            return None
        d = r.json()
        rl = d.get("resultList", [d] if isinstance(d, dict) and d.get("fonKodu") else [])
        if not rl:
            return None
        item = rl[0] if isinstance(rl, list) else rl
        return {
            "kategori":        item.get("fonKategori", ""),
            "port_buyukluk":   item.get("portBuyukluk"),
            "yatirimci_sayi":  item.get("yatirimciSayi"),
            "pazar_payi":      item.get("pazarPayi"),
            "gunluk_getiri":   item.get("gunlukGetiri"),
        }
    except Exception:
        return None


def build_universe(enrich: bool = True, max_workers: int = 8) -> list[dict]:
    """
    Tüm fonları çek, isteğe bağlı olarak kategori bilgisiyle zenginleştir.

    enrich=True → her fon için fonBilgiGetir çağrılır (~3-5 dk, 2500+ istek)
    enrich=False → sadece kod + unvan (saniyeler içinde)
    """
    funds = fetch_all_fund_codes()

    universe = []
    for f in funds:
        universe.append({
            "kod":   f.get("fonKodu", "").upper(),
            "unvan": f.get("fonUnvan", ""),
            "kategori":       "",
            "port_buyukluk":  None,
            "yatirimci_sayi": None,
        })

    if not enrich:
        log("Zenginleştirme atlandı (enrich=False)")
        return universe

    # Paralel kategori çekme
    log(f"Kategori zenginleştirme başlıyor ({len(universe)} fon, {max_workers} worker)...")
    kod_map = {f["kod"]: f for f in universe}
    basarili = 0
    hata = 0

    with httpx.Client(timeout=10, follow_redirects=True) as client:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(fetch_fund_detail, f["kod"], client): f["kod"]
                for f in universe
            }
            for i, future in enumerate(as_completed(futures)):
                kod = futures[future]
                try:
                    detail = future.result()
                    if detail:
                        kod_map[kod].update(detail)
                        basarili += 1
                    else:
                        hata += 1
                except Exception:
                    hata += 1

                if (i + 1) % 200 == 0:
                    log(f"  {i+1}/{len(universe)} tamamlandı ({basarili} başarılı, {hata} hata)")
                    time.sleep(0.5)  # Rate-limit koruması

    log(f"Tamamlandı: {basarili} başarılı, {hata} hata")
    return list(kod_map.values())


def save(universe: list[dict]):
    out = {
        "olusturma_tarihi": datetime.now().isoformat(),
        "toplam": len(universe),
        "fonlar": universe,
    }
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    log(f"Kaydedildi: {OUT_FILE} ({len(universe)} fon)")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-enrich", action="store_true",
                        help="Kategori zenginleştirmeyi atla (hızlı, sadece kod+unvan)")
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    universe = build_universe(enrich=not args.no_enrich, max_workers=args.workers)
    save(universe)
