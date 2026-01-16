"""
Webhook System - Event-driven webhook notifications
Provides webhook registration, delivery, and management
"""
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from pydantic import BaseModel, HttpUrl
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum
import httpx
import hmac
import hashlib
import json
import secrets
import logging

from ..middleware.auth import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


class WebhookEvent(str, Enum):
    """Available webhook events"""
    CAMPAIGN_CREATED = "campaign.created"
    CAMPAIGN_UPDATED = "campaign.updated"
    CAMPAIGN_DELETED = "campaign.deleted"
    ANALYSIS_COMPLETED = "analysis.completed"
    REPORT_GENERATED = "report.generated"
    UPLOAD_COMPLETED = "upload.completed"


class WebhookCreate(BaseModel):
    """Webhook creation request"""
    url: HttpUrl
    events: List[WebhookEvent]
    description: Optional[str] = None


class WebhookResponse(BaseModel):
    """Webhook response"""
    id: str
    url: str
    events: List[str]
    description: Optional[str]
    is_active: bool
    created_at: datetime
    last_triggered_at: Optional[datetime]
    success_count: int
    failure_count: int


class WebhookDelivery(BaseModel):
    """Webhook delivery attempt"""
    id: str
    webhook_id: str
    event: str
    payload: Dict[str, Any]
    status_code: Optional[int]
    success: bool
    error: Optional[str]
    timestamp: datetime


# In-memory storage (replace with database in production)
_webhooks_store: Dict[str, Dict] = {}
_deliveries_store: List[Dict] = []


@router.post("", response_model=WebhookResponse)
async def register_webhook(
    request: WebhookCreate,
    current_user: dict = Depends(get_current_user)
):
    """
    Register a new webhook
    
    Webhook will receive POST requests with event data when triggered.
    """
    webhook_id = secrets.token_urlsafe(8)
    webhook_secret = secrets.token_urlsafe(32)
    
    webhook = {
        "id": webhook_id,
        "user_id": current_user.get("id", "unknown"),
        "url": str(request.url),
        "events": [e.value for e in request.events],
        "description": request.description,
        "secret": webhook_secret,
        "is_active": True,
        "created_at": datetime.utcnow(),
        "last_triggered_at": None,
        "success_count": 0,
        "failure_count": 0
    }
    
    _webhooks_store[webhook_id] = webhook
    
    logger.info(f"Webhook registered: {webhook_id} -> {request.url}")
    
    return WebhookResponse(
        id=webhook_id,
        url=str(request.url),
        events=[e.value for e in request.events],
        description=request.description,
        is_active=True,
        created_at=webhook["created_at"],
        last_triggered_at=None,
        success_count=0,
        failure_count=0
    )


@router.get("", response_model=List[WebhookResponse])
async def list_webhooks(
    current_user: dict = Depends(get_current_user)
):
    """List user's registered webhooks"""
    user_id = current_user.get("id", "unknown")
    
    return [
        WebhookResponse(
            id=w["id"],
            url=w["url"],
            events=w["events"],
            description=w["description"],
            is_active=w["is_active"],
            created_at=w["created_at"],
            last_triggered_at=w["last_triggered_at"],
            success_count=w["success_count"],
            failure_count=w["failure_count"]
        )
        for w in _webhooks_store.values()
        if w["user_id"] == user_id
    ]


@router.get("/{webhook_id}", response_model=WebhookResponse)
async def get_webhook(
    webhook_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Get webhook details"""
    if webhook_id not in _webhooks_store:
        raise HTTPException(status_code=404, detail="Webhook not found")
    
    webhook = _webhooks_store[webhook_id]
    if webhook["user_id"] != current_user.get("id", "unknown"):
        raise HTTPException(status_code=403, detail="Not authorized")
    
    return WebhookResponse(**webhook)


@router.delete("/{webhook_id}")
async def delete_webhook(
    webhook_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Delete a webhook"""
    if webhook_id not in _webhooks_store:
        raise HTTPException(status_code=404, detail="Webhook not found")
    
    webhook = _webhooks_store[webhook_id]
    if webhook["user_id"] != current_user.get("id", "unknown"):
        raise HTTPException(status_code=403, detail="Not authorized")
    
    del _webhooks_store[webhook_id]
    
    logger.info(f"Webhook deleted: {webhook_id}")
    
    return {"message": "Webhook deleted successfully"}


@router.post("/{webhook_id}/test")
async def test_webhook(
    webhook_id: str,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user)
):
    """Send a test event to webhook"""
    if webhook_id not in _webhooks_store:
        raise HTTPException(status_code=404, detail="Webhook not found")
    
    webhook = _webhooks_store[webhook_id]
    if webhook["user_id"] != current_user.get("id", "unknown"):
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Send test event
    test_payload = {
        "event": "test",
        "data": {
            "message": "This is a test webhook delivery",
            "webhook_id": webhook_id,
            "timestamp": datetime.utcnow().isoformat()
        }
    }
    
    background_tasks.add_task(
        deliver_webhook,
        webhook,
        "test",
        test_payload["data"]
    )
    
    return {"message": "Test webhook queued for delivery"}


async def deliver_webhook(
    webhook: Dict,
    event: str,
    data: Dict[str, Any],
    retries: int = 3
):
    """
    Deliver webhook with retry logic
    
    Args:
        webhook: Webhook configuration
        event: Event type
        data: Event data
        retries: Number of retry attempts
    """
    payload = {
        "event": event,
        "data": data,
        "timestamp": datetime.utcnow().isoformat(),
        "webhook_id": webhook["id"]
    }
    
    payload_json = json.dumps(payload)
    
    # Generate signature
    signature = hmac.new(
        webhook["secret"].encode(),
        payload_json.encode(),
        hashlib.sha256
    ).hexdigest()
    
    headers = {
        "Content-Type": "application/json",
        "X-Webhook-Signature": f"sha256={signature}",
        "X-Webhook-Event": event,
        "X-Webhook-ID": webhook["id"]
    }
    
    delivery_id = secrets.token_urlsafe(8)
    
    for attempt in range(retries):
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    webhook["url"],
                    content=payload_json,
                    headers=headers,
                    timeout=10.0
                )
                response.raise_for_status()
                
                # Success
                _webhooks_store[webhook["id"]]["last_triggered_at"] = datetime.utcnow()
                _webhooks_store[webhook["id"]]["success_count"] += 1
                
                _deliveries_store.append({
                    "id": delivery_id,
                    "webhook_id": webhook["id"],
                    "event": event,
                    "payload": payload,
                    "status_code": response.status_code,
                    "success": True,
                    "error": None,
                    "timestamp": datetime.utcnow()
                })
                
                logger.info(f"Webhook delivered: {webhook['id']} -> {event}")
                return
                
        except Exception as e:
            logger.warning(f"Webhook delivery attempt {attempt + 1} failed: {e}")
            
            if attempt == retries - 1:
                # Final failure
                _webhooks_store[webhook["id"]]["failure_count"] += 1
                
                _deliveries_store.append({
                    "id": delivery_id,
                    "webhook_id": webhook["id"],
                    "event": event,
                    "payload": payload,
                    "status_code": None,
                    "success": False,
                    "error": str(e),
                    "timestamp": datetime.utcnow()
                })
                
                logger.error(f"Webhook delivery failed: {webhook['id']} -> {event}: {e}")


async def trigger_webhook_event(
    user_id: str,
    event: WebhookEvent,
    data: Dict[str, Any],
    background_tasks: BackgroundTasks
):
    """
    Trigger webhook event for all matching webhooks
    
    Args:
        user_id: User ID
        event: Event type
        data: Event data
        background_tasks: FastAPI background tasks
    """
    for webhook in _webhooks_store.values():
        if webhook["user_id"] == user_id and event.value in webhook["events"]:
            if webhook["is_active"]:
                background_tasks.add_task(
                    deliver_webhook,
                    webhook,
                    event.value,
                    data
                )


# Helper function to integrate with existing endpoints
def webhook_trigger(event: WebhookEvent):
    """
    Decorator to trigger webhook after endpoint execution
    
    Usage:
        @router.post("/campaigns")
        @webhook_trigger(WebhookEvent.CAMPAIGN_CREATED)
        async def create_campaign(...):
            pass
    """
    def decorator(func):
        async def wrapper(*args, **kwargs):
            result = await func(*args, **kwargs)
            
            # Get background_tasks and current_user from kwargs
            background_tasks = kwargs.get("background_tasks")
            current_user = kwargs.get("current_user")
            
            if background_tasks and current_user:
                await trigger_webhook_event(
                    user_id=current_user.get("id", "unknown"),
                    event=event,
                    data=result if isinstance(result, dict) else {"result": str(result)},
                    background_tasks=background_tasks
                )
            
            return result
        return wrapper
    return decorator
