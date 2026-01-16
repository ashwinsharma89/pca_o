# Visualization-2 Debugging Guide

## Current Status
- ✅ Backend server running on port 8000
- ✅ Frontend server running on port 3000  
- ✅ Page loads successfully (no build errors)
- ❌ No data displaying on visualizations-2 page
- ❌ "Campaign not found" error reported

## What to Check in Browser Console

### Step 1: Open Browser Console
1. Navigate to http://localhost:3000/visualizations-2
2. Press F12 (or Cmd+Option+I on Mac)
3. Click on the "Console" tab

### Step 2: Look for These Logs
You should see these console messages in order:

```
AdsOverviewContent v2.1 (Route Fix Applied)
fetchData called with filters: {...}
Sending filterParams to API: {...}
Calling getGlobalVisualizations...
getGlobalVisualizations success: true
Calling dashboard-stats...
dashboard-stats success: true
Calling fetchFilterOptions (/campaigns/filters)...
Filter options from API success: true
```

### Step 3: Identify the Failing Call
If you see an error, note which call failed:
- ❌ `getGlobalVisualizations failed:` → Issue with `/campaigns/visualizations` endpoint
- ❌ `dashboard-stats failed:` → Issue with `/campaigns/dashboard-stats` endpoint  
- ❌ `Failed to fetch filter options:` → Issue with `/campaigns/filters` endpoint

### Step 4: Check Network Tab
1. Click on the "Network" tab in DevTools
2. Filter by "Fetch/XHR"
3. Look for requests to:
   - `/api/v1/campaigns/visualizations`
   - `/api/v1/campaigns/dashboard-stats`
   - `/api/v1/campaigns/filters`
4. Click on each request and check:
   - Status code (should be 200)
   - Response body (should contain data, not error)

## Common Issues and Solutions

### Issue 1: "Campaign not found" Error
**Cause**: A greedy route `/{campaign_id}` is catching requests meant for static routes.
**Solution**: Already attempted - moved greedy routes to bottom of campaigns.py (lines 2010-2076)
**Verify**: Check that `/campaigns/visualizations` returns 200, not 404

### Issue 2: "Not authenticated" Error  
**Cause**: Frontend not sending auth token or token expired
**Solution**: 
1. Check localStorage for 'token' key
2. Try logging out and back in
3. Check that Authorization header is being sent

### Issue 3: Empty Data Response
**Cause**: Database has no data or query filters are too restrictive
**Solution**:
1. Run: `python scripts/test_dashboard_data.py` to verify data exists
2. Check date range filters aren't excluding all data
3. Try removing all filters and refresh

### Issue 4: CORS Error
**Cause**: Backend not allowing frontend origin
**Solution**: Check backend CORS settings in `src/api/main.py`

## Quick Tests

### Test 1: Check if data exists in database
```bash
cd "/Users/ashwin/Desktop/pca_agent copy"
python scripts/test_dashboard_data.py
```
Expected: Should show "5634 rows" or similar

### Test 2: Check backend is responding
```bash
curl http://localhost:8000/health
```
Expected: `{"status":"healthy",...}`

### Test 3: Test with authentication
1. Open browser console on visualizations-2 page
2. Run: `localStorage.getItem('token')`
3. Should return a long JWT token string (not null)

## Next Steps Based on Console Output

**If you see "Calling getGlobalVisualizations..." but then an error:**
→ The issue is with the `/campaigns/visualizations` endpoint
→ Check if it's returning 404 (route shadowing) or 500 (server error)

**If you don't see any "Calling..." logs:**
→ The fetchData function isn't running
→ Check for JavaScript errors earlier in the console

**If all calls succeed but no data shows:**
→ The API is returning empty arrays
→ Check database has data and filters aren't too restrictive

## Report Back
Please share:
1. Screenshot of browser console
2. Screenshot of Network tab showing the failed request
3. The exact error message you see

This will help me provide the exact fix needed.
