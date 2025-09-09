import json
import os
import sys
import threading
from datetime import datetime, timedelta
from typing import Dict, Any
from datetime import timezone
import redis
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.base import JobLookupError
from fastapi import FastAPI, HTTPException, Depends, Header
from pydantic import BaseModel
import uvicorn
from dotenv import load_dotenv

load_dotenv()

# -------------------
# Configuration
# -------------------
API_TOKEN = os.getenv("API_TOKEN")
if not API_TOKEN:
    print("API_TOKEN is required")
    sys.exit(1)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
MAX_RETRIES = 3

# -------------------
# Redis setup
# -------------------
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    decode_responses=True,
    socket_connect_timeout=5,
    socket_timeout=5,
)

try:
    redis_client.ping()
    print(f"[{datetime.now().isoformat()}] Redis connected")
except redis.ConnectionError as e:
    print(f"[{datetime.now().isoformat()}] Redis connection failed: {e}")
    sys.exit(1)

# -------------------
# Scheduler setup
# -------------------
scheduler = BackgroundScheduler(
    executors={"default": ThreadPoolExecutor(20)},
    timezone="UTC",
)
scheduler.start()
print(f"[{datetime.now().isoformat()}] Scheduler started")

# -------------------
# FastAPI app
# -------------------
app = FastAPI(title="Scheduler API", version="1.0.0")

# -------------------
# Models & Auth
# -------------------
class ScheduleMessage(BaseModel):
    id: str
    scheduleTo: str
    payload: Dict[str, Any]
    webhookUrl: str

def verify_token(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid token format")
    token = authorization.replace("Bearer ", "")
    if token != API_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid token")
    return token

# -------------------
# Webhook firing logic
# -------------------
def fire_webhook(message_id: str, retry_count: int = 0):
    redis_key = f"message:{message_id}"
    message_json = redis_client.get(redis_key)
    if not message_json:
        print(f"[{datetime.now().isoformat()}] Message {message_id} not found in Redis. Skipping.")
        try:
            scheduler.remove_job(message_id)
        except JobLookupError:
            pass
        return

    message_data = json.loads(message_json)
    webhook_url = message_data["webhookUrl"]
    payload = message_data["payload"]
    log_prefix = f"[{datetime.now().isoformat()}] [ID: {message_id}] [Try: {retry_count + 1}/{MAX_RETRIES}]"

    try:
        print(f"{log_prefix} Firing webhook to {webhook_url}")
        response = requests.post(webhook_url, json=payload, timeout=30)
        response.raise_for_status()
        print(f"{log_prefix} Webhook fired successfully ({response.status_code})")
        # Remove from Redis after success
        redis_client.delete(redis_key)
        try:
            scheduler.remove_job(message_id)
        except JobLookupError:
            pass

    except requests.RequestException as e:
        print(f"{log_prefix} Failed to fire webhook: {e}")
        if retry_count + 1 < MAX_RETRIES:
            retry_time = datetime.utcnow() + timedelta(minutes=5)
            scheduler.add_job(
                fire_webhook,
                "date",
                run_date=retry_time,
                args=[message_id, retry_count + 1],
                id=message_id,
                replace_existing=True
            )
            print(f"{log_prefix} Rescheduled retry at {retry_time.isoformat()}")
        else:
            print(f"{log_prefix} Max retries reached. Message remains in Redis for manual handling.")

# -------------------
# Helper to schedule a message
# -------------------
def schedule_message(message_data: Dict[str, Any]):
    message_id = message_data["id"]
    schedule_time = datetime.fromisoformat(message_data["scheduleTo"].replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    if schedule_time <= now:
        # Fire immediately in a separate thread
        threading.Thread(target=fire_webhook, args=(message_id, 0), daemon=True).start()
        return

    scheduler.add_job(
        fire_webhook,
        "date",
        run_date=schedule_time,
        args=[message_id],
        id=message_id,
        replace_existing=True,
    )
    print(f"[{datetime.now().isoformat()}] Scheduled message {message_id} for {schedule_time.isoformat()}")

# -------------------
# Restore messages from Redis on startup
# -------------------
def restore_scheduled_messages():
    keys = redis_client.keys("message:*")
    for key in keys:
        try:
            message_data = json.loads(redis_client.get(key))
            schedule_message(message_data)
        except Exception as e:
            print(f"Failed to restore message {key}: {e}")

restore_scheduled_messages()

# -------------------
# API Endpoints
# -------------------
@app.post("/messages")
async def create_scheduled_message(message: ScheduleMessage, token: str = Depends(verify_token)):
    redis_key = f"message:{message.id}"
    if redis_client.exists(redis_key):
        raise HTTPException(status_code=409, detail="Message already exists")

    message_data = message.model_dump()  # Pydantic V2
    redis_client.set(redis_key, json.dumps(message_data))
    schedule_message(message_data)
    return {"status": "scheduled", "messageId": message.id}

@app.delete("/messages/{message_id}")
async def delete_scheduled_message(message_id: str, token: str = Depends(verify_token)):
    redis_key = f"message:{message_id}"
    if not redis_client.exists(redis_key):
        raise HTTPException(status_code=404, detail="Message not found")

    redis_client.delete(redis_key)
    scheduler.remove_job(message_id)
    return {"status": "deleted", "messageId": message_id}

@app.get("/messages")
async def list_scheduled_messages(token: str = Depends(verify_token)):
    jobs = []
    for job in scheduler.get_jobs():
        jobs.append({
            "messageId": job.id,
            "nextRun": job.next_run_time.isoformat() if job.next_run_time else None
        })
    return {"scheduledJobs": jobs, "count": len(jobs)}

@app.get("/health")
async def health_check():
    try:
        redis_client.ping()
        return {"status": "healthy", "redis": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "redis": "disconnected", "error": str(e)}

# -------------------
# Run server
# -------------------
if __name__ == "__main__":
    print(f"[{datetime.now().isoformat()}] Starting Scheduler API server")
    uvicorn.run(app, host="0.0.0.0", port=8000)
