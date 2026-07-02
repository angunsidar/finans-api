"""
Firebase Admin SDK — Firestore + FCM push bildirim lazy-init client.

FIREBASE_SERVICE_ACCOUNT_JSON env var'ı Render dashboard'unda ayarlanmalı
(Firebase Console > Project Settings > Service Accounts > Generate new private key
ile indirilen JSON'un TÜM içeriği tek satır/escape edilmiş string olarak).
Bu proje içinde gerçek bir credential dosyası YOKTUR ve olmamalıdır.

Env var yoksa/boşsa: modülün fonksiyonları sessizce None/False döner ve uyarı loglar.
Import zamanında ASLA crash olmaz — Sidar henüz kurmadıysa API'nin geri kalanı
çalışmaya devam eder (redis_cache.py'deki lazy-init pattern'i ile aynı yaklaşım).
"""
from __future__ import annotations

import json
import logging
import os

_logger = logging.getLogger("uvicorn.error")

_SERVICE_ACCOUNT_JSON = os.getenv("FIREBASE_SERVICE_ACCOUNT_JSON", "")

_initialized = False
_init_ok = False
_firestore_client = None


def _init_firebase() -> bool:
    """
    Bir kez çalışır (global flag ile). Env var yoksa veya hatalıysa False döner,
    exception fırlatmaz — çağıran taraf her zaman bool alır.
    """
    global _initialized, _init_ok, _firestore_client

    if _initialized:
        return _init_ok
    _initialized = True

    if not _SERVICE_ACCOUNT_JSON:
        _logger.warning(
            "Firebase devre dışı: FIREBASE_SERVICE_ACCOUNT_JSON env var tanımlı değil."
        )
        return False

    try:
        import firebase_admin
        from firebase_admin import credentials, firestore

        cred_dict = json.loads(_SERVICE_ACCOUNT_JSON)
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred)
        _firestore_client = firestore.client()
        _init_ok = True
        _logger.info("Firebase Admin SDK başlatıldı (Firestore + FCM hazır).")
        return True
    except Exception as e:
        _logger.warning(f"Firebase başlatma hatası: {e}")
        _init_ok = False
        return False


def get_firestore_client():
    """Lazy-init tetikler. Firestore client döner ya da None."""
    if not _init_firebase():
        return None
    return _firestore_client


def send_push_notification(device_token: str, title: str, body: str) -> bool:
    """
    FCM push bildirimi gönderir. Credential yoksa veya hata olursa False döner,
    exception fırlatmaz — caller'ı (alarmlar.py) bozmaz.
    """
    if not _init_firebase():
        return False
    try:
        from firebase_admin import messaging

        message = messaging.Message(
            notification=messaging.Notification(title=title, body=body),
            token=device_token,
        )
        messaging.send(message)
        return True
    except Exception as e:
        _logger.warning(f"FCM push gönderim hatası (device_token={device_token[:12]}...): {e}")
        return False
