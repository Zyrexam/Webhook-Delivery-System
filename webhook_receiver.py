import uvicorn
import logging
import hmac
import os
import hashlib
import json
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()
app = FastAPI()


WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")

def verify_signature(secret: str, payload: dict, signature: str) -> bool:
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    expected = "sha256=" + hmac.new(
        secret.encode(),
        body.encode(),
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)

@app.post("/webhook")
@app.post("/webhook/receive")
async def receive_webhook(request: Request):
    body = await request.json()
    headers = dict(request.headers)
    signature = headers.get("x-webhook-signature", "")
    
    # Verify signature
    if not verify_signature(WEBHOOK_SECRET, body, signature):
        logger.warning(f"Invalid signature! Rejecting webhook")
        raise HTTPException(status_code=401, detail="Invalid signature")
    
    # If signature is valid, process the webhook
    logger.info("="*50)
    logger.info(f"Signature verified!")
    logger.info(f"Received webhook!")
    logger.info(f"Event Type: {body.get('event_type')}")
    logger.info(f"Event ID: {body.get('event_id')}")
    logger.info(f"Payload: {body.get('payload')}")
    logger.info(f"Delivery ID: {headers.get('x-webhook-delivery')}")
    logger.info(f"Attempt: {headers.get('x-webhook-attempt')}")
    logger.info("="*50)
    
    return {"status": "success", "message": "Webhook received"}

@app.get("/health")
async def health():
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9000)