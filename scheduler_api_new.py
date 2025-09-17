import json
import os
import sys
import threading
from datetime import datetime, timedelta, timezone
from typing import Dict, Any

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
app = FastAPI(title="Scheduler API", version="2.0.0")

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

        # Remove from Redis and scheduler after success
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

    # Check if job already exists
    try:
        existing_job = scheduler.get_job(message_id)
        if existing_job:
            print(f"[{datetime.now().isoformat()}] Job {message_id} already exists, skipping")
            return
    except JobLookupError:
        pass  # Job doesn't exist, continue

    if schedule_time <= now:
        print(f"[{datetime.now().isoformat()}] Message {message_id} is due now, executing immediately")
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
# Instance management for EasyPanel deployments
# -------------------
INSTANCE_ID = f"scheduler-{os.getpid()}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
INSTANCE_LOCK_KEY = "scheduler:instance:lock"
INSTANCE_LOCK_TTL = 30  # seconds

def acquire_instance_lock():
    """Acquire instance lock to prevent multiple instances from running simultaneously"""
    try:
        # Try to set lock with TTL
        result = redis_client.set(INSTANCE_LOCK_KEY, INSTANCE_ID, nx=True, ex=INSTANCE_LOCK_TTL)
        if result:
            print(f"[{datetime.now().isoformat()}] Instance lock acquired: {INSTANCE_ID}")
            return True
        else:
            print(f"[{datetime.now().isoformat()}] Another instance is already running, skipping restoration")
            return False
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] Failed to acquire instance lock: {e}")
        return False

def refresh_instance_lock():
    """Refresh instance lock to keep it alive"""
    try:
        redis_client.set(INSTANCE_LOCK_KEY, INSTANCE_ID, ex=INSTANCE_LOCK_TTL)
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] Failed to refresh instance lock: {e}")

# -------------------
# Restore messages from Redis on startup
# -------------------
def restore_scheduled_messages(force_restore=False):
    """
    Restore scheduled messages from Redis.
    If force_restore=True, bypasses the instance lock system.
    """
    if not force_restore:
        # Try to acquire instance lock, but don't fail if we can't
        if not acquire_instance_lock():
            print(f"[{datetime.now().isoformat()}] Instance lock not acquired, but continuing with restoration anyway")
    else:
        print(f"[{datetime.now().isoformat()}] Force restoration mode - bypassing instance lock")
    
    keys = redis_client.keys("message:*")
    restored_count = 0
    skipped_count = 0
    error_count = 0
    
    print(f"[{datetime.now().isoformat()}] Starting restoration of {len(keys)} messages from Redis")
    
    for key in keys:
        try:
            message_data = json.loads(redis_client.get(key))
            message_id = message_data['id']
            schedule_time_str = message_data['scheduleTo']
            
            print(f"[{datetime.now().isoformat()}] Processing message {message_id} with schedule {schedule_time_str}")
            
            # Check if job already exists in scheduler
            try:
                existing_job = scheduler.get_job(message_id)
                if existing_job:
                    print(f"[{datetime.now().isoformat()}] Job {message_id} already exists in scheduler, skipping")
                    skipped_count += 1
                    continue
            except JobLookupError:
                pass  # Job doesn't exist, continue
            
            # Parse schedule time for logging
            schedule_time = datetime.fromisoformat(schedule_time_str.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            
            print(f"[{datetime.now().isoformat()}] Message {message_id}: schedule_time={schedule_time.isoformat()}, now={now.isoformat()}")
            
            schedule_message(message_data)
            restored_count += 1
            print(f"[{datetime.now().isoformat()}] Restored message {message_id}")
            
        except Exception as e:
            print(f"[{datetime.now().isoformat()}] Failed to restore message {key}: {e}")
            error_count += 1
    
    print(f"[{datetime.now().isoformat()}] Restoration complete: {restored_count} restored, {skipped_count} skipped, {error_count} errors")

# Start restoration - always restore on startup
restore_scheduled_messages(force_restore=True)

# Start background task to refresh instance lock
def refresh_lock_task():
    while True:
        try:
            refresh_instance_lock()
            threading.Event().wait(10)  # Refresh every 10 seconds
        except Exception as e:
            print(f"[{datetime.now().isoformat()}] Error in lock refresh task: {e}")
            break

lock_refresh_thread = threading.Thread(target=refresh_lock_task, daemon=True)
lock_refresh_thread.start()

# -------------------
# API Endpoints
# -------------------
@app.post("/messages")
async def create_scheduled_message(message: ScheduleMessage, token: str = Depends(verify_token)):
    redis_key = f"message:{message.id}"
    
    # Check if message exists in Redis
    if redis_client.exists(redis_key):
        raise HTTPException(status_code=409, detail="Message already exists")
    
    # Check if job exists in scheduler
    try:
        existing_job = scheduler.get_job(message.id)
        if existing_job:
            raise HTTPException(status_code=409, detail="Message job already exists in scheduler")
    except JobLookupError:
        pass  # Job doesn't exist, continue

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
    try:
        scheduler.remove_job(message_id)
    except JobLookupError:
        pass

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
        
        # Check if this instance has the lock
        current_lock = redis_client.get(INSTANCE_LOCK_KEY)
        is_active_instance = current_lock == INSTANCE_ID
        
        return {
            "status": "healthy", 
            "redis": "connected",
            "instanceId": INSTANCE_ID,
            "isActiveInstance": is_active_instance,
            "scheduledJobs": len(scheduler.get_jobs())
        }
    except Exception as e:
        return {"status": "unhealthy", "redis": "disconnected", "error": str(e)}

@app.get("/instance/status")
async def instance_status(token: str = Depends(verify_token)):
    """Get detailed instance status for debugging"""
    try:
        current_lock = redis_client.get(INSTANCE_LOCK_KEY)
        is_active_instance = current_lock == INSTANCE_ID
        
        jobs = []
        for job in scheduler.get_jobs():
            jobs.append({
                "id": job.id,
                "nextRun": job.next_run_time.isoformat() if job.next_run_time else None,
                "func": job.func.__name__ if job.func else None
            })
        
        return {
            "instanceId": INSTANCE_ID,
            "isActiveInstance": is_active_instance,
            "currentLock": current_lock,
            "scheduledJobs": jobs,
            "jobCount": len(jobs)
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/messages/{message_id}/status")
async def get_message_status(message_id: str, token: str = Depends(verify_token)):
    """Get detailed status of a specific message"""
    try:
        redis_key = f"message:{message_id}"
        
        # Check Redis
        message_json = redis_client.get(redis_key)
        if not message_json:
            return {"status": "not_found", "location": "redis", "message": "Message not found in Redis"}
        
        message_data = json.loads(message_json)
        
        # Check scheduler
        try:
            job = scheduler.get_job(message_id)
            if job:
                return {
                    "status": "scheduled",
                    "location": "both",
                    "redis_data": message_data,
                    "scheduler_info": {
                        "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                        "func": job.func.__name__ if job.func else None
                    }
                }
        except JobLookupError:
            pass
        
        return {
            "status": "redis_only",
            "location": "redis",
            "redis_data": message_data,
            "message": "Message exists in Redis but not in scheduler"
        }
        
    except Exception as e:
        return {"error": str(e)}

@app.post("/restore-messages")
async def force_restore_messages(token: str = Depends(verify_token)):
    """Force restoration of messages from Redis (for debugging)"""
    try:
        print(f"[{datetime.now().isoformat()}] Manual restoration triggered by API call")
        
        # Use the restore function with force_restore=True
        restore_scheduled_messages(force_restore=True)
        
        # Get final counts
        keys = redis_client.keys("message:*")
        jobs = scheduler.get_jobs()
        
        return {
            "status": "completed",
            "totalMessages": len(keys),
            "scheduledJobs": len(jobs),
            "message": "Restoration completed successfully"
        }
        
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] Error in force restore: {e}")
        return {"error": str(e)}

@app.post("/clear-instance-lock")
async def clear_instance_lock(token: str = Depends(verify_token)):
    """Clear the instance lock (for debugging)"""
    try:
        redis_client.delete(INSTANCE_LOCK_KEY)
        print(f"[{datetime.now().isoformat()}] Instance lock cleared")
        return {"status": "cleared", "message": "Instance lock has been cleared"}
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] Error clearing instance lock: {e}")
        return {"error": str(e)}

# -------------------
# Run server
# -------------------
if __name__ == "__main__":
    print(f"[{datetime.now().isoformat()}] Starting Scheduler API server")
    uvicorn.run("scheduler_api_new:app", host="0.0.0.0", port=8000)
