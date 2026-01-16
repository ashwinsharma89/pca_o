import { test, expect } from '@playwright/test';

/**
 * E2E Tests for PCA Agent - Core Flows
 */

const TEST_USER = {
    username: 'ashwin',
    password: 'Pca12345!'
};

test.describe('Authentication & Navigation', () => {
    test('should login and navigate to all major pages', async ({ page }) => {
        // 1. Visit Login Page
        await page.goto('/login');
        await expect(page).toHaveTitle(/PCA Agent/i);

        // 2. Perform Login
        await page.fill('input[id="username"]', TEST_USER.username);
        await page.fill('input[id="password"]', TEST_USER.password);
        await page.click('button[type="submit"]');

        // 3. Verify Landing on Upload (default redirect)
        await expect(page).toHaveURL(/\/upload/);
        await expect(page.locator('h1')).toContainText(/Upload/i);

        // 4. Navigate to Dashboard
        await page.goto('/dashboard');
        await expect(page).toHaveURL(/\/dashboard/);
        await expect(page.locator('h1')).toContainText(/Dashboard/i);

        // 5. Navigate to Intelligence Studio
        await page.goto('/intelligence-studio');
        await expect(page).toHaveURL(/\/intelligence-studio/);
        await expect(page.locator('h1')).toContainText(/Intelligence Studio/i);

        // 6. Navigate to Anomaly Detective
        await page.goto('/anomaly-detective');
        await expect(page).toHaveURL(/\/anomaly-detective/);
        await expect(page.locator('h1')).toContainText(/Anomaly Detective/i);

        // 7. Navigate to Real-Time Command
        await page.goto('/real-time-command');
        await expect(page).toHaveURL(/\/real-time-command/);
        await expect(page.locator('h1')).toContainText(/Real-Time Command/i);
    });

    test('should show error with invalid credentials', async ({ page }) => {
        await page.goto('/login');

        await page.fill('input[id="username"]', 'wronguser');
        await page.fill('input[id="password"]', 'wrongpass');
        await page.click('button[type="submit"]');

        // Should show error message
        await expect(page.locator('body')).toContainText(/Invalid|failed|Incorrect|error/i);
    });
});

test.describe('Intelligence Studio Interaction', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/login');
        await page.fill('input[id="username"]', TEST_USER.username);
        await page.fill('input[id="password"]', TEST_USER.password);
        await page.click('button[type="submit"]');
        await page.waitForURL(/\/upload/);
    });

    test('should allow asking a question in Intelligence Studio', async ({ page }) => {
        await page.goto('/intelligence-studio');

        const input = page.locator('input[placeholder*="Ask"]');
        await expect(input).toBeVisible();

        await input.fill('What is the total spend?');
        await page.keyboard.press('Enter');

        // Verify loading state appears
        await expect(page.locator('text=Analyzing')).toBeVisible();
    });
});
