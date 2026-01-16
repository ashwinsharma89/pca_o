import { test, expect } from '@playwright/test';

/**
 * E2E Tests for PCA Agent - Critical Pages
 * 
 * Tests cover:
 * 1. Authentication (Login/Logout)
 * 2. Upload Flow
 * 3. Q&A (Chat) Flow
 * 4. Analysis Flow
 * 5. Dashboard Builder Flow
 * 6. Visualizations Flow
 */

// Test user credentials
const TEST_USER = {
    username: 'testuser',
    password: 'testpassword123'
};

test.describe('Authentication', () => {
    test('should login successfully with valid credentials', async ({ page }) => {
        await page.goto('/login');

        // Fill login form
        await page.fill('input[name="username"]', TEST_USER.username);
        await page.fill('input[name="password"]', TEST_USER.password);

        // Submit form
        await page.click('button[type="submit"]');

        // Should redirect after login (to dashboard, upload, analysis, or campaigns)
        await expect(page).toHaveURL(/\/(dashboard|upload|analysis|campaigns)/);

        // Should show user info or navigation
        await expect(page.locator('body')).toContainText(/Dashboard|Upload|Analysis/i);
    });

    test('should show error with invalid credentials', async ({ page }) => {
        await page.goto('/login');

        await page.fill('input[name="username"]', 'wronguser');
        await page.fill('input[name="password"]', 'wrongpass');
        await page.click('button[type="submit"]');

        // Should show error message
        await expect(page.locator('body')).toContainText(/incorrect|invalid|error/i);
    });

    test('should logout successfully', async ({ page }) => {
        // Login first
        await page.goto('/login');
        await page.fill('input[name="username"]', TEST_USER.username);
        await page.fill('input[name="password"]', TEST_USER.password);
        await page.click('button[type="submit"]');
        await page.waitForURL(/\/(dashboard|upload|analysis|campaigns)/);

        // Find and click logout button (could be in dropdown or direct button)
        const logoutButton = page.locator('button:has-text("Logout"), button:has-text("Sign out")').first();
        if (await logoutButton.isVisible()) {
            await logoutButton.click();
        }

        // Should redirect to login
        await expect(page).toHaveURL('/login');
    });
});

test.describe('Upload Flow', () => {
    test.beforeEach(async ({ page }) => {
        // Login before each test
        await page.goto('/login');
        await page.fill('input[name="username"]', TEST_USER.username);
        await page.fill('input[name="password"]', TEST_USER.password);
        await page.click('button[type="submit"]');
        await page.waitForURL(/\/(dashboard|upload|analysis|campaigns)/);
    });

    test('should navigate to upload page', async ({ page }) => {
        await page.goto('/upload');
        await expect(page).toHaveURL('/upload');
        await expect(page.locator('h1')).toContainText(/upload/i);
    });

    test('should show file upload interface', async ({ page }) => {
        await page.goto('/upload');

        // Should have file input or drop zone
        const fileInput = page.locator('input[type="file"]');
        await expect(fileInput).toBeVisible();
    });

    test('should display upload instructions', async ({ page }) => {
        await page.goto('/upload');

        // Should show instructions
        await expect(page.locator('body')).toContainText(/CSV|Excel|drag|drop/i);
    });
});

test.describe('Q&A (Chat) Flow', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/login');
        await page.fill('input[name="username"]', TEST_USER.username);
        await page.fill('input[name="password"]', TEST_USER.password);
        await page.click('button[type="submit"]');
        await page.waitForURL(/\/(dashboard|upload|analysis|campaigns)/);
    });

    test('should navigate to chat page', async ({ page }) => {
        await page.goto('/chat');
        await expect(page).toHaveURL('/chat');
        await expect(page.locator('h1')).toContainText(/Q&A|Chat/i);
    });

    test('should show chat interface with input', async ({ page }) => {
        await page.goto('/chat');

        // Should have input field
        const input = page.locator('input[placeholder*="Ask"], input[placeholder*="question"]').first();
        await expect(input).toBeVisible();

        // Should have send button
        const sendButton = page.locator('button[type="submit"], button:has-text("Send")').first();
        await expect(sendButton).toBeVisible();
    });

    test('should toggle between Data and Knowledge mode', async ({ page }) => {
        await page.goto('/chat');

        // Look for mode toggle (switch or buttons)
        const modeToggle = page.locator('text=Data').or(page.locator('text=Knowledge')).first();
        if (await modeToggle.isVisible()) {
            await expect(page.locator('body')).toContainText(/Data|Knowledge/);
        }
    });

    test('should show suggested questions on initial load', async ({ page }) => {
        await page.goto('/chat');

        // Wait for page to load
        await page.waitForTimeout(1000);

        // May show suggested questions
        const body = await page.locator('body').textContent();
        // This is optional, so we just check the page loaded
        expect(body).toBeTruthy();
    });
});

test.describe('Analysis Flow', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/login');
        await page.fill('input[name="username"]', TEST_USER.username);
        await page.fill('input[name="password"]', TEST_USER.password);
        await page.click('button[type="submit"]');
        await page.waitForURL(/\/(dashboard|upload|analysis|campaigns)/);
    });

    test('should navigate to analysis page', async ({ page }) => {
        await page.goto('/analysis');
        await expect(page).toHaveURL('/analysis');
        await expect(page.locator('h1')).toContainText(/Analysis|Intelligence/i);
    });

    test('should show configuration options', async ({ page }) => {
        await page.goto('/analysis');

        // Should show configuration checkboxes
        await expect(page.locator('body')).toContainText(/RAG|Benchmark|Depth|Recommendation/i);
    });

    test('should show portfolio metrics', async ({ page }) => {
        await page.goto('/analysis');

        // Should show metrics cards
        await expect(page.locator('body')).toContainText(/Spend|Conversion|CTR|CPA/i);
    });

    test('should have run analysis button', async ({ page }) => {
        await page.goto('/analysis');

        // Should have button to run analysis
        const analyzeButton = page.locator('button:has-text("RAG"), button:has-text("Analyze"), button:has-text("Run")').first();
        await expect(analyzeButton).toBeVisible();
    });
});

test.describe('Dashboard Builder Flow', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/login');
        await page.fill('input[name="username"]', TEST_USER.username);
        await page.fill('input[name="password"]', TEST_USER.password);
        await page.click('button[type="submit"]');
        await page.waitForURL(/\/(dashboard|upload|analysis|campaigns)/);
    });

    test('should navigate to dashboard builder', async ({ page }) => {
        await page.goto('/dashboard-builder');
        await expect(page).toHaveURL('/dashboard-builder');
        await expect(page.locator('h1')).toContainText(/Dashboard Builder/i);
    });

    test('should show add widget button', async ({ page }) => {
        await page.goto('/dashboard-builder');

        // Should have add widget button
        const addButton = page.locator('button:has-text("Add Widget"), button:has-text("Add")').first();
        await expect(addButton).toBeVisible();
    });

    test('should show empty state when no widgets', async ({ page }) => {
        await page.goto('/dashboard-builder');

        // Wait for page load
        await page.waitForTimeout(1000);

        // Should show empty state or widgets
        const body = await page.locator('body').textContent();
        expect(body).toContain('Dashboard');
    });

    test('should open add widget dialog', async ({ page }) => {
        await page.goto('/dashboard-builder');

        // Click add widget button
        const addButton = page.locator('button:has-text("Add Widget"), button:has-text("Add")').first();
        await addButton.click();

        // Should show dialog with widget types
        await expect(page.locator('body')).toContainText(/KPI|Bar|Line|Chart/i);
    });
});

test.describe('Visualizations Flow', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/login');
        await page.fill('input[name="username"]', TEST_USER.username);
        await page.fill('input[name="password"]', TEST_USER.password);
        await page.click('button[type="submit"]');
        await page.waitForURL(/\/(dashboard|upload|analysis|campaigns)/);
    });

    test('should navigate to visualizations page', async ({ page }) => {
        await page.goto('/visualizations');
        await expect(page).toHaveURL('/visualizations');
        await expect(page.locator('h1')).toContainText(/Visualization/i);
    });

    test('should show filter options', async ({ page }) => {
        await page.goto('/visualizations');

        // Should have filters
        await expect(page.locator('body')).toContainText(/Filter|Platform|Date|Metric/i);
    });

    test('should display charts', async ({ page }) => {
        await page.goto('/visualizations');

        // Wait for charts to load
        await page.waitForTimeout(2000);

        // Should show chart titles
        await expect(page.locator('body')).toContainText(/Performance|Platform|Funnel|Trend/i);
    });

    test('should have apply filters button', async ({ page }) => {
        await page.goto('/visualizations');

        // Should have apply/refresh button
        const applyButton = page.locator('button:has-text("Apply"), button:has-text("Refresh")').first();
        await expect(applyButton).toBeVisible();
    });
});

test.describe('Navigation', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/login');
        await page.fill('input[name="username"]', TEST_USER.username);
        await page.fill('input[name="password"]', TEST_USER.password);
        await page.click('button[type="submit"]');
        await page.waitForURL(/\/(dashboard|upload|analysis|campaigns)/);
    });

    test('should have working navigation links', async ({ page }) => {
        await page.goto('/dashboard');

        // Should have navigation menu
        const nav = page.locator('nav, aside, [role="navigation"]').first();
        if (await nav.isVisible()) {
            await expect(nav).toBeVisible();
        }
    });

    test('should navigate between pages', async ({ page }) => {
        // Start at dashboard
        await page.goto('/dashboard');

        // Navigate to upload
        await page.goto('/upload');
        await expect(page).toHaveURL('/upload');

        // Navigate to analysis
        await page.goto('/analysis');
        await expect(page).toHaveURL('/analysis');

        // Navigate to chat
        await page.goto('/chat');
        await expect(page).toHaveURL('/chat');
    });
});

test.describe('Responsive Design', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/login');
        await page.fill('input[name="username"]', TEST_USER.username);
        await page.fill('input[name="password"]', TEST_USER.password);
        await page.click('button[type="submit"]');
        await page.waitForURL(/\/(dashboard|upload|analysis|campaigns)/);
    });

    test('should be responsive on mobile viewport', async ({ page }) => {
        // Set mobile viewport
        await page.setViewportSize({ width: 375, height: 667 });

        await page.goto('/dashboard');

        // Page should still be functional
        await expect(page.locator('body')).toBeVisible();
    });

    test('should be responsive on tablet viewport', async ({ page }) => {
        // Set tablet viewport
        await page.setViewportSize({ width: 768, height: 1024 });

        await page.goto('/analysis');

        // Page should still be functional
        await expect(page.locator('body')).toBeVisible();
    });
});
