"""
Fiyat alarmı endpoint'leri.
Kullanıcılar döviz/altın/gümüş için hedef fiyat alarmı kurar, backend periyodik
olarak (/alarmlar/kontrol-et) güncel fiyatlarla karşılaştırıp FCM push gönderir.

Kalıcı depolama Firestore'dadır (Render free tier disk ephemeral — JSON dosyası
kalıcı veri için kullanılamaz). Firestore erişimi firebase_client.py üzerinden.
"""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/alarmlar", tags=["alarmlar"])

# Çeyrek altın için gram altın TL fiyatı üzerinden yaklaşık katsayı.
# Gerçek çeyrek altın piyasa fiyatı bankadan bankaya değişir, bu yaklaşık bir
# katsayıdır, gerekirse Sidar ayarlayabilir.
ÇEYREK_ALTIN_GRAM_KATSAYISI = 1.65

GEÇERLİ_ASSET_TYPES = {"usd", "eur", "gbp", "ons", "gram_altin", "ceyrek_altin", "gumus"}
GEÇERLİ_DIRECTIONS = {"above", "below"}

FIRESTORE_KOLEKSIYON = "alarmlar"


class AlarmOlusturRequest(BaseModel):
    deviceToken: str
    assetType: str  # usd, eur, gbp, ons, gram_altin, ceyrek_altin, gumus
    targetPrice: float
    direction: str  # above | below


def _guncel_fiyat_cek(asset_type: str) -> float | None:
    """
    Bir asset type için güncel fiyatı ilgili router'ın hesaplama fonksiyonundan çeker.
    Import döngüsünü önlemek için fonksiyon içinde lazy import (repo'daki diğer
    router'ların birbirini içe aktarma stiliyle aynı — bkz. altin.py _get_usdtry()).
    """
    from routers import doviz, altin, gumus

    try:
        if asset_type in ("usd", "eur", "gbp"):
            veri = doviz._fetch_kur(f"{asset_type.upper()}TRY=X")
            return veri["kur"] if veri else None
        if asset_type == "ons":
            return altin.altin_tl_hesapla()["altin_tl_ons"]
        if asset_type == "gram_altin":
            return altin.altin_tl_hesapla()["altin_tl_gram"]
        if asset_type == "ceyrek_altin":
            gram_tl = altin.altin_tl_hesapla()["altin_tl_gram"]
            return gram_tl * ÇEYREK_ALTIN_GRAM_KATSAYISI
        if asset_type == "gumus":
            return gumus.gumus_tl()["gumus_tl_gram"]
    except Exception:
        return None
    return None


@router.post("", summary="Yeni fiyat alarmı oluştur")
def alarm_olustur(body: AlarmOlusturRequest):
    """Firestore'a yeni bir alarm dokümanı ekler ve id'siyle birlikte döner."""
    asset_type = body.assetType.strip().lower()
    direction = body.direction.strip().lower()

    if asset_type not in GEÇERLİ_ASSET_TYPES:
        raise HTTPException(400, f"Geçersiz assetType. Seçenekler: {sorted(GEÇERLİ_ASSET_TYPES)}")
    if direction not in GEÇERLİ_DIRECTIONS:
        raise HTTPException(400, f"Geçersiz direction. Seçenekler: {sorted(GEÇERLİ_DIRECTIONS)}")
    if body.targetPrice <= 0:
        raise HTTPException(400, "targetPrice pozitif bir sayı olmalı.")

    from firebase_client import get_firestore_client

    db = get_firestore_client()
    if db is None:
        raise HTTPException(503, "Firestore bağlantısı kurulamadı (FIREBASE_SERVICE_ACCOUNT_JSON eksik olabilir).")

    alarm_verisi = {
        "deviceToken": body.deviceToken,
        "assetType": asset_type,
        "targetPrice": body.targetPrice,
        "direction": direction,
        "isTriggered": False,
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }

    _, doc_ref = db.collection(FIRESTORE_KOLEKSIYON).add(alarm_verisi)
    return {"id": doc_ref.id, **alarm_verisi}


@router.get("/{deviceToken}", summary="Cihaza ait tüm alarmlar")
def alarmlari_listele(deviceToken: str):
    """O device'a ait tüm alarmları Firestore'dan sorgular, liste döner."""
    from firebase_client import get_firestore_client

    db = get_firestore_client()
    if db is None:
        raise HTTPException(503, "Firestore bağlantısı kurulamadı (FIREBASE_SERVICE_ACCOUNT_JSON eksik olabilir).")

    sonuc = []
    query = db.collection(FIRESTORE_KOLEKSIYON).where("deviceToken", "==", deviceToken)
    for doc in query.stream():
        veri = doc.to_dict()
        veri["id"] = doc.id
        sonuc.append(veri)
    return sonuc


@router.delete("/{alarmId}", summary="Alarmı sil")
def alarm_sil(alarmId: str):
    """Firestore'dan alarmı siler, yoksa 404 döner."""
    from firebase_client import get_firestore_client

    db = get_firestore_client()
    if db is None:
        raise HTTPException(503, "Firestore bağlantısı kurulamadı (FIREBASE_SERVICE_ACCOUNT_JSON eksik olabilir).")

    doc_ref = db.collection(FIRESTORE_KOLEKSIYON).document(alarmId)
    if not doc_ref.get().exists:
        raise HTTPException(404, "Alarm bulunamadı.")

    doc_ref.delete()
    return {"silindi": True, "id": alarmId}


@router.post("/kontrol-et", summary="Tüm aktif alarmları kontrol et ve tetiklenenlere push gönder")
def alarmlari_kontrol_et():
    """
    Bu endpoint API_KEYS middleware'i tarafından zaten korunuyor
    (main.py — ApiKeyMiddleware tüm path'leri kapsar, FREE_PATHS listesinde değil).
    Render cron/keepalive job'ı bu endpoint'i periyodik çağırmalı.

    Adımlar:
      1. isTriggered == False olan tüm alarmları Firestore'dan çek.
      2. assetType'a göre grupla, her tip için fiyatı BİR KEZ çek (cache'le).
      3. direction'a göre eşleşme kontrolü yap.
      4. Eşleşenleri Firestore'da isTriggered = True yap.
      5. Eşleşenler için FCM push gönder (best-effort, hata response'u bozmaz).
    """
    from firebase_client import get_firestore_client, send_push_notification

    db = get_firestore_client()
    if db is None:
        raise HTTPException(503, "Firestore bağlantısı kurulamadı (FIREBASE_SERVICE_ACCOUNT_JSON eksik olabilir).")

    aktif_alarmlar = []
    query = db.collection(FIRESTORE_KOLEKSIYON).where("isTriggered", "==", False)
    for doc in query.stream():
        veri = doc.to_dict()
        veri["id"] = doc.id
        aktif_alarmlar.append(veri)

    # Asset type başına fiyatı bir kez çek (cache)
    fiyat_cache: dict[str, float | None] = {}
    for alarm in aktif_alarmlar:
        asset_type = alarm.get("assetType", "")
        if asset_type not in fiyat_cache:
            fiyat_cache[asset_type] = _guncel_fiyat_cek(asset_type)

    tetiklenen_idler: list[str] = []

    for alarm in aktif_alarmlar:
        asset_type = alarm.get("assetType", "")
        guncel_fiyat = fiyat_cache.get(asset_type)
        if guncel_fiyat is None:
            continue

        target_price = alarm.get("targetPrice")
        direction = alarm.get("direction")

        eslesti = (
            (direction == "above" and guncel_fiyat >= target_price)
            or (direction == "below" and guncel_fiyat <= target_price)
        )
        if not eslesti:
            continue

        alarm_id = alarm["id"]
        db.collection(FIRESTORE_KOLEKSIYON).document(alarm_id).update({"isTriggered": True})
        tetiklenen_idler.append(alarm_id)

        try:
            device_token = alarm.get("deviceToken", "")
            yon_metni = "üstüne çıktı" if direction == "above" else "altına düştü"
            send_push_notification(
                device_token=device_token,
                title="Fiyat Alarmı Tetiklendi",
                body=f"{asset_type} hedeflediğiniz {target_price} değerinin {yon_metni} (güncel: {guncel_fiyat}).",
            )
        except Exception:
            # Push gönderimi asla response'u bozmasın — FCM credential henüz kurulu olmayabilir.
            pass

    return {"kontrol_edilen": len(aktif_alarmlar), "tetiklenen": tetiklenen_idler}
