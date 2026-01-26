# Environment Configuration Summary

## Current Setup

### Backend (.env)
**Location:** `/Users/ashwin/Desktop/pca_agent copy/.env`

Key configuration:
```bash
API_HOST=0.0.0.0
API_PORT=8001
```

The backend server runs on **port 8001** by default.

---

### Frontend (.env.local)
**Location:** `/Users/ashwin/Desktop/pca_agent copy/frontend/.env.local`

Key configuration:
```bash
NEXT_PUBLIC_API_URL=http://localhost:8001/api/v1
NEXT_PUBLIC_ENABLE_ANALYTICS=true
```

The frontend is configured to connect to the backend at **http://localhost:8001/api/v1**.

---

## Code References

All frontend code properly uses the environment variable with appropriate fallbacks:

1. **upload-utils.ts** (line 76, 224):
   ```typescript
   apiUrl: string = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8001/api/v1'
   ```

2. **ad-explorer/page.tsx** (line 111):
   ```typescript
   const apiUrl = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8001/api/v1";
   ```

---

## How to Change Ports

### To change the backend port:
1. Edit `.env` in the project root
2. Update `API_PORT=8001` to your desired port
3. Restart the backend server

### To change the frontend API URL:
1. Edit `frontend/.env.local`
2. Update `NEXT_PUBLIC_API_URL=http://localhost:8001/api/v1` to match your backend
3. Restart the Next.js dev server (`npm run dev`)

---

## Status: ✅ Complete

Both frontend and backend are properly configured to read from their respective `.env` files. No hardcoded URLs exist in the codebase - all references use environment variables with sensible fallbacks.
