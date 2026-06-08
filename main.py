from fastapi import FastAPI
from routers.events import router as events_router
from routers.subscriptions import router as subscriptions_router
from routers.deliveries import router as deliveries_router

app = FastAPI(
    title="Webhook Delivery System",
    version="1.0.0"
)

app.include_router(events_router)
app.include_router(subscriptions_router)
app.include_router(deliveries_router)

@app.get("/")
async def root():
    return {
        "message": "Webhook Delivery System running",
        "routes": {
            "POST /events": "emit an event",
            "POST /subscriptions": "register endpoint",
            "GET  /subscriptions": "list subscriptions",
            "GET  /deliveries": "delivery history",
            "GET  /deliveries/stats": "success rates"
        }
    }