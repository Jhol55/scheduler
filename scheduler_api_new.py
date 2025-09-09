import json
import os
import sys
import traceback
from datetime import datetime, timedelta
from typing import Dict, Any
import threading
import signal

import redis
import requests
from apscheduler.jobstores.redis import RedisJobStore
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
try:
    API_TOKEN = os.getenv('API_TOKEN')
    if not API_TOKEN:
        raise ValueError("API_TOKEN environment variable is required")

    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
    REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
    MAX_RETRIES = 3

    print(f"[{datetime.now().isoformat()}] Configuration loaded:")
    print(f"  - Redis Host: {REDIS_HOST}")
    print(f"  - Redis Port: {REDIS_PORT}")
    print(f"  - Redis Password: {'***' if REDIS_PASSWORD else 'None'}")
    print(f"  - API Token: {'***' if API_TOKEN else 'None'}")

except Exception as e:
    print(f"[{datetime.now().isoformat()}] Configuration error: {e}")
    sys.exit(1)

# -------------------
# Redis setup
# -------------------
try:
    print(f"[{datetime.now().isoformat()}] Testing Redis connection...")
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5
    )
    redis_client.ping()
    print(f"[{datetime.now().isoformat()}] Redis connection successful")
except redis.ConnectionError as e:
    print(f"[{datetime.now().isoformat()}] Redis connection failed: {e}")
    sys.exit(1)
except Exception as e:
    print(f"[{datetime.now().isoformat()}] Redis setup error: {e}")
    sys.exit(1)

# -------------------
# Scheduler setup
# -------------------
try:
    jobstores = {
        'default': RedisJobStore(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            db=1
        )
    }
    executors = {
        'default': ThreadPoolExecutor(20)
    }
    scheduler = BackgroundScheduler(
        jobstores=jobstores,
        executors=executors,
        timezone="UTC"
    )
    scheduler.start()
    print(f"[{datetime.now().isoformat()}] Scheduler started successfully")
except Exception as e:
    print(f"[{datetime.now().isoformat()}] Scheduler setup error: {e}")
    sys.exit(1)

# -------------------
# FastAPI app
# -------------------
app = FastAPI(
    title="Scheduler API",
    version="2.0.1",
    description="A FastAPI application for scheduling webhook messages"
)

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
        raise HTTPException(
            status_code=401,
            detail="Authorization header in 'Bearer <token>' format required"
        )

    token = authorization.replace("Bearer ", "")
    if token != API_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid token")
    return token

# -------------------
# Webhook firing logic
# -------------------
def fire_webhook(message_data: Dict[str, Any], retry_count: int = 0):
    message_id = message_data.get("id")
    webhook_url = message_data.get("webhookUrl")
    payload = message_data.get("payload")

    log_prefix = f"[{datetime.now().isoformat()}] [ID: {message_id}] [Try: {retry_count + 1}/{MAX_RETRIES}]"

    try:
        print(f"{log_prefix} Firing webhook to {webhook_url}")
        response = requests.post(webhook_url, json=payload, timeout=30)
        response.raise_for_status()
        print(f"{log_prefix} Webhook fired successfully. Response: {response.status_code}")

    except requests.RequestException as e:
        print(f"{log_prefix} Failed to fire webhook: {e}")
        if retry_count + 1 < MAX_RETRIES:
            retry_time = datetime.now() + timedelta(minutes=5)
            retry_job_id = f"{message_id}_retry_{retry_count + 1}"
            try:
                scheduler.add_job(
                    fire_webhook,
                    'date',
                    run_date=retry_time,
                    args=[message_data, retry_count + 1],
                    id=retry_job_id
                )
                print(f"{log_prefix} Rescheduled for retry at {retry_time.isoformat()}")
            except Exception as retry_error:
                print(f"{log_prefix} Failed to reschedule retry: {retry_error}")
        else:
            print(f"{log_prefix} Max retries reached. Moving to dead-letter queue.")
            try:
                redis_client.lpush("dead_letter_queue", json.dumps(message_data))
            except Exception as dlq_error:
                print(f"{log_prefix} Failed to add to dead-letter queue: {dlq_error}")

# -------------------
# API Endpoints
# -------------------
@app.post("/messages")
async def create_scheduled_message(message: ScheduleMessage, token: str = Depends(verify_token)):
    try:
        if scheduler.get_job(message.id):
            raise HTTPException(status_code=409, detail=f"Message with ID '{message.id}' is already scheduled.")

        schedule_time = datetime.fromisoformat(message.scheduleTo.replace('Z', '+00:00'))

        if schedule_time <= datetime.now(schedule_time.tzinfo):
            print(f"[{datetime.now().isoformat()}] [ID: {message.id}] Scheduled time is in the past. Firing immediately.")
            threading.Thread(target=fire_webhook, args=(message.dict(),)).start()
            return {"status": "fired_immediately", "messageId": message.id}

        scheduler.add_job(
            fire_webhook,
            'date',
            run_date=schedule_time,
            args=[message.dict()],
            id=message.id
        )
        print(f"[{datetime.now().isoformat()}] Message scheduled - ID: {message.id} for {schedule_time.isoformat()}")
        return {"status": "scheduled", "messageId": message.id, "scheduledTo": schedule_time.isoformat()}

    except HTTPException:
        raise
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] Failed to schedule message {message.id}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to schedule message: {str(e)}")

@app.delete("/messages/{message_id}")
async def delete_scheduled_message(message_id: str, token: str = Depends(verify_token)):
    try:
        scheduler.remove_job(message_id)
        print(f"[{datetime.now().isoformat()}] Message deleted - ID: {message_id}")
        return {"status": "deleted", "messageId": message_id}
    except JobLookupError:
        raise HTTPException(status_code=404, detail=f"Message with ID '{message_id}' not found.")
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] Failed to delete message {message_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to delete message: {str(e)}")

@app.get("/messages")
async def list_scheduled_messages(token: str = Depends(verify_token)):
    try:
        jobs = []
        for job in scheduler.get_jobs():
            jobs.append({
                "messageId": job.id,
                "nextRun": job.next_run_time.isoformat() if job.next_run_time else None,
                "jobFunction": job.func.__name__
            })
        return {"scheduledJobs": jobs, "count": len(jobs)}
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] Failed to list messages: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list messages: {str(e)}")

@app.get("/health")
async def health_check():
    try:
        redis_ok = redis_client.ping()
        scheduler_running = scheduler.running
        job_count = len(scheduler.get_jobs()) if scheduler_running else 0
        status = "healthy" if redis_ok and scheduler_running else "unhealthy"
        return {
            "status": status,
            "redis": "connected" if redis_ok else "disconnected",
            "scheduler": "running" if scheduler_running else "stopped",
            "jobCount": job_count,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get("/")
async def root():
    return {
        "message": "Scheduler API v2.0.1",
        "status": "running",
        "timestamp": datetime.now().isoformat()
    }

# -------------------
# Graceful shutdown
# -------------------
def signal_handler(signum, frame):
    print(f"\n[{datetime.now().isoformat()}] Received signal {signum}. Shutting down gracefully...")
    scheduler.shutdown(wait=True)
    sys.exit(0)

# signal.signal(signal.SIGINT, signal_handler)
# signal.signal(signal.SIGTERM, signal_handler)

# -------------------
# Run server
# -------------------
if __name__ == "__main__":
    print(f"[{datetime.now().isoformat()}] Starting Scheduler API server v2.0.1")
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
        access_log=True
    )
