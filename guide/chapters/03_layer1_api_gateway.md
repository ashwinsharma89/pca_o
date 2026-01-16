**CHAPTER 3**

**LAYER 1: API GATEWAY & REQUEST ROUTING**

---

**Navigation:** [← Chapter 2](file:///Users/ashwin/Desktop/pca_agent%20copy/guide/chapters/02_architecture_overview.md) | [Index](file:///Users/ashwin/Desktop/pca_agent%20copy/guide/INDEX.md) | [Next: Chapter 4 →](file:///Users/ashwin/Desktop/pca_agent%20copy/guide/chapters/04_layer2_query_understanding.md)

---

## 3.1 Overview

This is the **entry point** for all user interactions with the PCA Agent.

> **Office Analogy:** Layer 1 is like the security desk and reception area of a corporate office building. Before you can meet with anyone or access any resources, you must:
> 1. Show your employee badge (authentication)
> 2. Sign in (not too many times in one day - rate limiting)
> 3. Prove you're not an imposter (CSRF protection)
> 4. Get directed to the right floor/department (routing)
>
> Every single person entering the building goes through this process. No exceptions!

---

## 📥 INPUT: What Comes Into This Layer

```json
{
  "type": "HTTP Request",
  "method": "POST",
  "endpoint": "/api/v1/campaigns/chat",
  "headers": {
    "Authorization": "Bearer <JWT_TOKEN>",
    "Content-Type": "application/json",
    "X-CSRF-Token": "<CSRF_TOKEN>"
  },
  "body": {
    "question": "What is my CPA for last week?",
    "context": {
      "user_id": "user_123",
      "session_id": "sess_456"
    }
  }
}
```

**Key Components of Input:**
- **Endpoint**: The URL path that determines which "department" handles the request
- **Headers**: Security credentials and metadata
- **Body**: The actual question or command from the user

---

## ⚙️ WHAT HAPPENS INSIDE: The Processing Pipeline

```mermaid
graph TD
    A[HTTP Request Arrives] --> B{Security Middleware}
    B -->|Valid Token| C[Rate Limiter]
    B -->|Invalid| Z1[Return 401 Unauthorized]
    
    C -->|Under Limit| D[CSRF Validator]
    C -->|Over Limit| Z2[Return 429 Too Many Requests]
    
    D -->|Valid| E[Router: Match Endpoint]
    D -->|Invalid| Z3[Return 403 Forbidden]
    
    E --> F{Which Router?}
    F -->|/chat| G[Chat Router]
    F -->|/analytics| H[Analytics Router]
    F -->|/reports| I[Reports Router]
    F -->|/upload| J[Ingestion Router]
    
    G --> K[Extract Question]
    K --> L[Pass to Layer 2]
```

### Step-by-Step Breakdown

#### Step 1: Security Middleware (Authentication)
**File**: `src/interface/api/middleware/auth.py`

> **Office Analogy - The Badge Scanner:**
>
> When you arrive at the office, you scan your employee badge at the turnstile:
> 1. **Badge scan** (extract JWT token from header)
> 2. **System checks** if the badge is real (validate signature)
> 3. **System checks** if the badge has expired (check expiration date)
> 4. **System reads** your employee ID from the badge (extract user_id)
>
> **If valid:** Turnstile opens, you enter
> **If invalid:** Red light, access denied (HTTP 401 Unauthorized)

**What it does**:
1. Extracts the JWT token from the `Authorization` header
2. Decodes and validates the token signature
3. Checks if the token has expired
4. Extracts the `user_id` from the token payload

> **Technical Term - JWT (JSON Web Token):**
>
> **What it is:** A digital ID card that proves who you are
>
> **Office Analogy:** Your employee badge contains:
> - Your name and employee ID (user_id)
> - When it was issued (issued_at)
> - When it expires (expiration_date)
> - A hologram/security feature that can't be forged (cryptographic signature)
>
> Every time you make a request, you show this badge instead of typing your password. The system can verify it's real by checking the signature!

**Code Example**:
```python
async def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        user_id: str = payload.get("sub")
        if user_id is None:
            raise HTTPException(status_code=401)
        return user_id
    except JWTError:
        raise HTTPException(status_code=401)
```

**Output**: `user_id` (string) or HTTP 401 error

---

#### Step 2: Rate Limiter

![Authentication Flow](../diagrams/authentication_flow.png)

**File**: `src/interface/api/middleware/rate_limit.py`

> **Office Analogy - The Sign-In Sheet:**
>
> Imagine the security desk has a sign-in sheet. They notice you've entered and exited the building 100 times in the last hour. That's suspicious!
>
> **Normal behavior:** 5-10 entries per day
> **Suspicious behavior:** 100 entries per hour (maybe you're trying to overwhelm the system?)
>
> The security guard says: "Sorry, you've exceeded the maximum entries per hour. Please wait before trying again." (HTTP 429: Too Many Requests)
>
> **Why this matters:** Prevents someone from overwhelming the system with thousands of requests per second, which could slow it down for everyone else!

**What it does**:
1. Checks how many requests this user has made in the last minute
2. If under the limit (e.g., 60 requests/minute), allows the request
3. If over the limit, blocks the request

**Why this exists**: Prevents abuse and ensures fair resource allocation

> **Real-World Example:**
> - **Normal user:** Asks 5-10 questions per minute (totally fine)
> - **Malicious bot:** Tries to ask 1,000 questions per second (blocked!)
> - **Accidental loop:** Your code has a bug and keeps asking the same question repeatedly (blocked to protect the system)

**Code Example**:
```python
@limiter.limit("60/minute")
async def chat_endpoint(request: Request):
    # Your logic here
```

**Output**: Either continues to next step OR returns HTTP 429 error

![Rate Limiting](../diagrams/rate_limiting.png)

---

#### Step 3: CSRF Validator
**File**: `src/interface/api/middleware/csrf.py`

**What it does**:
1. Compares the `X-CSRF-Token` header with the expected value
2. Ensures the request came from your legitimate frontend, not a malicious site

**Output**: Either continues OR returns HTTP 403 error

---

#### Step 4: Router Matching
**File**: `src/interface/api/v1/__init__.py`

**What it does**:
The FastAPI framework looks at the URL path and matches it to the correct "router" (think of routers as departments):

| Endpoint Pattern | Router File | Purpose |
|:---|:---|:---|
| `/api/v1/campaigns/chat` | `routers/chat.py` | Natural language queries |
| `/api/v1/campaigns/analytics` | `routers/analytics.py` | Pre-built dashboards |
| `/api/v1/campaigns/upload` | `routers/ingestion.py` | File uploads |
| `/api/v1/campaigns/pacing-reports` | `routers/pacing.py` | Excel report generation |

**Code Example**:
```python
# In src/interface/api/v1/__init__.py
from .routers import chat, analytics, ingestion, pacing

router_v1 = APIRouter(prefix="/api/v1")
router_v1.include_router(chat.router)
router_v1.include_router(analytics.router)
router_v1.include_router(ingestion.router)
router_v1.include_router(pacing.router)
```

---

## 🏢 COMPONENTS IN THIS LAYER

### 1. **Main Application** (`src/interface/api/main_v3.py`)
- **Role**: The "building" itself - starts the server
- **Key Responsibility**: Registers all middleware and routers

### 2. **Middleware Stack**
- **Authentication** (`middleware/auth.py`): Bouncer at the door
- **Rate Limiter** (`middleware/rate_limit.py`): Traffic controller
- **CSRF Protection** (`middleware/csrf.py`): Security guard
- **CORS Handler** (`main_v3.py`): Allows frontend to talk to backend

### 3. **Router Registry** (`v1/__init__.py`)
- **Role**: Directory that maps URLs to handlers
- **Routers**:
  - `chat.router` - Handles `/chat` endpoint
  - `analytics.router` - Handles `/analytics` endpoint
  - `ingestion.router` - Handles `/upload` endpoint
  - `pacing.router` - Handles `/pacing-reports` endpoint

---

## 📤 OUTPUT: What Leaves This Layer

```python
{
  "validated_request": {
    "user_id": "user_123",
    "question": "What is my CPA for last week?",
    "endpoint_type": "chat",
    "session_context": {...}
  },
  "destination": "Layer 2 (Query Understanding)"
}
```

**Key Transformations**:
- ✅ Security validated
- ✅ Rate limit checked
- ✅ User identified
- ✅ Correct handler selected
- ➡️ Ready to be processed by business logic

---

## 🔄 Complete Flow Diagram

```mermaid
sequenceDiagram
    participant User as User Browser
    participant Gateway as Layer 1: API Gateway
    participant Auth as Auth Middleware
    participant Rate as Rate Limiter
    participant Router as Router
    participant Layer2 as Layer 2: Query Understanding
    
    User->>Gateway: POST /api/v1/campaigns/chat
    Gateway->>Auth: Validate JWT Token
    Auth-->>Gateway: ✓ user_id: "user_123"
    
    Gateway->>Rate: Check request count
    Rate-->>Gateway: ✓ 45/60 requests used
    
    Gateway->>Router: Match endpoint pattern
    Router-->>Gateway: ✓ Route to chat.py
    
    Gateway->>Layer2: Forward validated request
    Note over Layer2: Processing begins...
```

---

## 🎓 Key Concepts for Beginners

### What is Middleware?
Middleware is code that runs **before** your main business logic. It's like a series of checkpoints:
- Checkpoint 1: Are you who you say you are? (Auth)
- Checkpoint 2: Are you making too many requests? (Rate Limit)
- Checkpoint 3: Is this request safe? (CSRF)

### What is a Router?
A router is a mapping between a URL pattern and a Python function. When a request comes in for `/chat`, the router says "I know who handles that!" and calls the right function.

### What is JWT?
JWT (JSON Web Token) is like a digital ID card. It contains:
- Who you are (`user_id`)
- When it was issued
- When it expires
- A signature to prove it's legitimate

---

## 📊 Performance Metrics

| Metric | Target | Actual |
|:---|:---|:---|
| Authentication Time | < 5ms | ~3ms |
| Rate Limit Check | < 2ms | ~1ms |
| Router Matching | < 1ms | ~0.5ms |
| **Total Layer 1 Overhead** | **< 10ms** | **~5ms** |

> [!NOTE]
> Layer 1 is designed to be extremely fast because it runs on **every single request**. The entire security and routing process takes less than 10 milliseconds.
