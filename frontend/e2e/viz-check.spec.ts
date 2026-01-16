import { test, expect } from '@playwright/test';

const TEST_USER = {
    username: 'ashwin',
    password: 'Pca12345!'
};

test.describe('Visualizations-2 Data Verification', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/login');
        await page.fill('input[id="username"]', TEST_USER.username);
        await page.fill('input[id="password"]', TEST_USER.password);
        await page.click('button[type="submit"]');
        await page.waitForURL(/\/upload/);
    });

    test('should verify data is populating in visualizations-2 tab', async ({ page }) => {
        // Navigate to Visualizations-2
        await page.goto('/visualizations-2');
        await expect(page).toHaveURL(/\/visualizations-2/);

        // Wait for page load
        await expect(page.locator('h1')).toContainText(/Executive Overview/i);

        // Check for loading state to finish (if any)
        await page.waitForSelector('.loader', { state: 'detached', timeout: 30000 }).catch(() => { });

        // Take a screenshot for visual debugging
        await page.screenshot({ path: 'visualizations-2-debug.png', fullPage: true });

        // Check for specific chart elements or data indicators
        const kpiCards = page.locator('.text-xl.font-bold');
        await kpiCards.first().waitFor({ state: 'visible', timeout: 10000 }).catch(() => { });
        const cardCount = await kpiCards.count();
        console.log(`Found ${cardCount} KPI cards`);

        for (let i = 0; i < cardCount; i++) {
            const text = await kpiCards.nth(i).innerText();
            console.log(`KPI Card ${i}: ${text}`);
        }

        // Check for Recharts elements
        const charts = page.locator('.recharts-responsive-container');
        await charts.first().waitFor({ state: 'visible', timeout: 10000 }).catch(() => { });
        const chartCount = await charts.count();
        console.log(`Found ${chartCount} charts`);
        expect(chartCount).toBeGreaterThan(0);
    });
});
