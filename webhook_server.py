"""Facebook Messenger webhook server for capturing recipient PSID."""

from __future__ import annotations

import hashlib
import hmac
import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from config import get_settings
from database import Database

LOGGER = logging.getLogger(__name__)

settings = get_settings()
db: Database | None = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    """Initialize and close resources for webhook server lifecycle."""

    global db
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    db = Database(settings.database_path)
    db.init_schema()
    LOGGER.info("Webhook server started.")
    try:
        yield
    finally:
        if db is not None:
            db.close()
            db = None
        LOGGER.info("Webhook server stopped.")


app = FastAPI(title="Facebook Webhook", lifespan=lifespan)


@app.get("/health")
def health() -> dict[str, str]:
    """Simple health check."""

    return {"status": "ok"}


@app.get("/webhook")
def verify_webhook(request: Request):
    """Facebook webhook verification endpoint."""

    mode = request.query_params.get("hub.mode")
    verify_token = request.query_params.get("hub.verify_token")
    challenge = request.query_params.get("hub.challenge", "")

    if mode != "subscribe":
        raise HTTPException(status_code=400, detail="Invalid hub.mode")

    if not settings.facebook_webhook_verify_token:
        raise HTTPException(status_code=500, detail="FACEBOOK_WEBHOOK_VERIFY_TOKEN is not configured")

    if verify_token != settings.facebook_webhook_verify_token:
        raise HTTPException(status_code=403, detail="Verify token mismatch")

    LOGGER.info("Webhook verification completed successfully.")
    return PlainTextResponse(content=challenge, status_code=200)


@app.post("/webhook")
async def receive_webhook(request: Request):
    """Receive Messenger webhook events and capture sender PSID."""

    raw_body = await request.body()
    _validate_signature(request.headers.get("X-Hub-Signature-256"), raw_body)

    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid JSON payload: {exc}") from exc

    captured_psids: list[str] = []
    for event in _iter_messaging_events(payload):
        sender_id = str(event.get("sender", {}).get("id", "")).strip()
        if not sender_id:
            continue
        if db is None:
            raise HTTPException(status_code=500, detail="Database not initialized")
        db.upsert_facebook_psid(sender_id)
        captured_psids.append(sender_id)

    if captured_psids:
        unique_psids = sorted(set(captured_psids))
        LOGGER.info("Captured PSID(s): %s", ", ".join(unique_psids))
        return JSONResponse({"ok": True, "captured_psids": unique_psids})

    return JSONResponse({"ok": True, "captured_psids": []})


def _validate_signature(signature_header: str | None, raw_body: bytes) -> None:
    """Validate Facebook signature when FACEBOOK_APP_SECRET is configured."""

    if not settings.facebook_app_secret:
        return

    if not signature_header or not signature_header.startswith("sha256="):
        raise HTTPException(status_code=403, detail="Missing or invalid X-Hub-Signature-256 header")

    received_signature = signature_header.removeprefix("sha256=")
    computed_signature = hmac.new(
        settings.facebook_app_secret.encode("utf-8"),
        raw_body,
        hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(received_signature, computed_signature):
        raise HTTPException(status_code=403, detail="Signature mismatch")


def _iter_messaging_events(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract messaging-like events from webhook payload."""

    events: list[dict[str, Any]] = []
    for entry in payload.get("entry", []):
        for key in ("messaging", "standby"):
            for event in entry.get(key, []):
                if isinstance(event, dict):
                    events.append(event)
    return events
