from fastapi import FastAPI, Request
import uvicorn
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

@app.post("/webhook")
@app.post("/webhook/receive")
async def receive_webhook(request: Request):
    body = await request.json()
    headers = dict(request.headers)
    
    logger.info(f"📨 Received webhook!")
    logger.info(f"Event Type: {body.get('event_type')}")
    logger.info(f"Event ID: {body.get('event_id')}")
    logger.info(f"Payload: {body.get('payload')}")
    logger.info(f"Delivery ID: {headers.get('x-webhook-delivery')}")
    logger.info(f"Attempt: {headers.get('x-webhook-attempt')}")
    
    logger.info("\n\n\n")
    
    return {"status": "success", "message": "Webhook received"}

@app.get("/health")
async def health():
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9000)