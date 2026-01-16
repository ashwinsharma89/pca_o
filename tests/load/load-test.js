/**
 * k6 Load Testing Script for PCA Agent API
 * 
 * Run: k6 run load-test.js
 * With options: k6 run --vus 50 --duration 5m load-test.js
 */

import http from 'k6/http';
import { check, sleep, group } from 'k6';
import { Rate, Trend } from 'k6/metrics';

// ============================================================================
// Configuration
// ============================================================================

const BASE_URL = __ENV.API_URL || 'http://localhost:8000';

// Custom metrics
const errorRate = new Rate('errors');
const authLatency = new Trend('auth_latency');
const apiLatency = new Trend('api_latency');

// Test scenarios
export const options = {
    scenarios: {
        // Smoke test - verify system works
        smoke: {
            executor: 'constant-vus',
            vus: 1,
            duration: '30s',
            startTime: '0s',
            tags: { test_type: 'smoke' },
        },
        // Load test - normal load
        load: {
            executor: 'ramping-vus',
            startVUs: 0,
            stages: [
                { duration: '2m', target: 50 },   // Ramp up
                { duration: '5m', target: 50 },   // Stay at 50
                { duration: '2m', target: 100 },  // Ramp to 100
                { duration: '5m', target: 100 },  // Stay at 100
                { duration: '2m', target: 0 },    // Ramp down
            ],
            startTime: '30s',
            tags: { test_type: 'load' },
        },
        // Stress test - breaking point
        stress: {
            executor: 'ramping-vus',
            startVUs: 0,
            stages: [
                { duration: '2m', target: 100 },
                { duration: '5m', target: 200 },
                { duration: '2m', target: 300 },
                { duration: '5m', target: 300 },
                { duration: '2m', target: 0 },
            ],
            startTime: '17m',
            tags: { test_type: 'stress' },
        },
        // Spike test - sudden traffic
        spike: {
            executor: 'ramping-vus',
            startVUs: 0,
            stages: [
                { duration: '10s', target: 500 },  // Spike
                { duration: '1m', target: 500 },
                { duration: '10s', target: 0 },    // Down
            ],
            startTime: '35m',
            tags: { test_type: 'spike' },
        },
    },
    thresholds: {
        http_req_duration: ['p(95)<2000'], // 95% of requests under 2s
        http_req_failed: ['rate<0.05'],     // Error rate under 5%
        errors: ['rate<0.1'],               // Custom error rate under 10%
    },
};

// ============================================================================
// Setup - Get Auth Token
// ============================================================================

let authToken = null;

export function setup() {
    // Login to get token
    const loginRes = http.post(`${BASE_URL}/api/v1/auth/login`, JSON.stringify({
        email: 'loadtest@example.com',
        password: 'loadtest123'
    }), {
        headers: { 'Content-Type': 'application/json' }
    });

    if (loginRes.status === 200) {
        const body = JSON.parse(loginRes.body);
        return { token: body.access_token };
    }

    console.log('Login failed, using unauthenticated tests');
    return { token: null };
}

// ============================================================================
// Main Test Script
// ============================================================================

export default function (data) {
    const headers = {
        'Content-Type': 'application/json',
    };

    if (data.token) {
        headers['Authorization'] = `Bearer ${data.token}`;
    }

    // -------------------------------------------------------------------------
    // Health Check
    // -------------------------------------------------------------------------
    group('Health Check', () => {
        const res = http.get(`${BASE_URL}/health`);

        check(res, {
            'health status is 200': (r) => r.status === 200,
            'health response time < 500ms': (r) => r.timings.duration < 500,
        });

        errorRate.add(res.status !== 200);
    });

    sleep(1);

    // -------------------------------------------------------------------------
    // Campaigns List
    // -------------------------------------------------------------------------
    group('Campaigns API', () => {
        const res = http.get(`${BASE_URL}/api/v1/campaigns`, { headers });

        const success = check(res, {
            'campaigns status is 200': (r) => r.status === 200,
            'campaigns response time < 1000ms': (r) => r.timings.duration < 1000,
            'campaigns has data': (r) => {
                try {
                    const body = JSON.parse(r.body);
                    return Array.isArray(body) || body.data !== undefined;
                } catch {
                    return false;
                }
            },
        });

        errorRate.add(!success);
        apiLatency.add(res.timings.duration);
    });

    sleep(1);

    // -------------------------------------------------------------------------
    // Analytics Endpoint
    // -------------------------------------------------------------------------
    group('Analytics API', () => {
        const res = http.get(`${BASE_URL}/api/v1/analytics/overview`, { headers });

        const success = check(res, {
            'analytics status is 200': (r) => r.status === 200,
            'analytics response time < 2000ms': (r) => r.timings.duration < 2000,
        });

        errorRate.add(!success);
        apiLatency.add(res.timings.duration);
    });

    sleep(1);

    // -------------------------------------------------------------------------
    // Chat / Q&A Endpoint (heavier load)
    // -------------------------------------------------------------------------
    group('Chat API', () => {
        const res = http.post(`${BASE_URL}/api/v1/chat`, JSON.stringify({
            question: 'What is my total spend this month?'
        }), { headers, timeout: '30s' });

        const success = check(res, {
            'chat status is 200': (r) => r.status === 200,
            'chat response time < 10000ms': (r) => r.timings.duration < 10000,
        });

        errorRate.add(!success);
        apiLatency.add(res.timings.duration);
    });

    sleep(2);

    // -------------------------------------------------------------------------
    // Reports List
    // -------------------------------------------------------------------------
    group('Reports API', () => {
        const res = http.get(`${BASE_URL}/api/v1/reports`, { headers });

        const success = check(res, {
            'reports status is 200': (r) => r.status === 200,
            'reports response time < 1000ms': (r) => r.timings.duration < 1000,
        });

        errorRate.add(!success);
        apiLatency.add(res.timings.duration);
    });

    sleep(1);
}

// ============================================================================
// Teardown
// ============================================================================

export function teardown(data) {
    console.log('Load test completed');
}
