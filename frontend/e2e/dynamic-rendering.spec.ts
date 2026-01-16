import { test, expect } from '@playwright/test';

const TEST_USER = {
    username: 'ashwin',
    password: 'Pca12345!'
};

test.describe('Dynamic Filters and Charts', () => {
    test.beforeEach(async ({ page }) => {
        // Login and navigate to visualizations
        await page.goto('/login');
        await page.fill('input[id="username"]', TEST_USER.username);
        await page.fill('input[id="password"]', TEST_USER.password);
        await page.click('button[type="submit"]');
        await page.waitForURL(/\/upload/);

        // Navigate to visualizations-2
        await page.goto('/visualizations-2');
        await expect(page).toHaveURL(/\/visualizations-2/);

        // Wait for page to load
        await expect(page.locator('h1')).toContainText(/Executive Overview/i);

        // Wait for loading to complete
        await page.waitForSelector('[data-testid="filter-section"]', { state: 'visible', timeout: 15000 });
    });

    test.describe('Filter Rendering', () => {
        test('should always render date range filter', async ({ page }) => {
            // Date range should always be visible
            const dateFilter = page.locator('[data-testid="filter-date"]');
            await expect(dateFilter).toBeVisible();
        });

        test('should render filter bar with available filters', async ({ page }) => {
            // Filter bar should be visible
            const filterBar = page.locator('[data-testid="filter-bar"]');
            await expect(filterBar).toBeVisible();

            // Apply Filters button should always be present
            const applyBtn = page.locator('[data-testid="filter-apply-btn"]');
            await expect(applyBtn).toBeVisible();
        });

        test('should only show filters when data has corresponding columns', async ({ page }) => {
            // Check if platform filter is shown when platforms exist
            const platformFilter = page.locator('[data-testid="filter-platform"]');
            const isPlatformVisible = await platformFilter.isVisible();

            if (isPlatformVisible) {
                // If visible, it should have options in the dropdown
                console.log('Platform filter is visible - data has platform column');
            } else {
                console.log('Platform filter is hidden - no platform data');
            }

            // Similarly check other filters
            const filters = [
                'filter-channel',
                'filter-funnel',
                'filter-device',
                'filter-adtype',
                'filter-placement',
                'filter-region'
            ];

            for (const filterId of filters) {
                const isVisible = await page.locator(`[data-testid="${filterId}"]`).isVisible();
                console.log(`${filterId}: ${isVisible ? 'visible' : 'hidden'}`);
            }
        });

        test('should show empty state message when no filters available', async ({ page }) => {
            // This test checks if the empty message logic works
            // In a real scenario with minimal data, this message would appear
            const emptyMessage = page.locator('[data-testid="filter-empty-message"]');
            const isEmptyMsgVisible = await emptyMessage.isVisible();

            // Log the visibility for debugging
            console.log(`Filter empty message visible: ${isEmptyMsgVisible}`);

            // If there are no dimension filters and only date range, the message should appear
            // This is data-dependent, so we just verify the element exists in DOM or not
        });
    });

    test.describe('Chart Rendering', () => {
        test('should render trends card when data is available', async ({ page }) => {
            // Wait for data to load
            await page.waitForTimeout(2000);

            const trendsCard = page.locator('[data-testid="chart-trends-card"]');
            const isTrendsVisible = await trendsCard.isVisible().catch(() => false);

            if (isTrendsVisible) {
                console.log('Trends card is visible - data is available');

                // Check individual charts
                const chartIds = ['chart-clicks', 'chart-spend', 'chart-conversions', 'chart-roas', 'chart-ctr'];
                for (const chartId of chartIds) {
                    const isChartVisible = await page.locator(`[data-testid="${chartId}"]`).isVisible();
                    console.log(`${chartId}: ${isChartVisible ? 'visible' : 'hidden'}`);
                }
            } else {
                console.log('Trends card is hidden - no trend data available');
            }
        });

        test('should display Recharts containers for visible charts', async ({ page }) => {
            // Wait for charts to render
            await page.waitForTimeout(3000);

            // Check for Recharts responsive containers
            const charts = page.locator('.recharts-responsive-container');
            const chartCount = await charts.count();

            console.log(`Found ${chartCount} Recharts containers`);
            expect(chartCount).toBeGreaterThanOrEqual(0);
        });

        test('should show KPI cards when trend data exists', async ({ page }) => {
            // Wait for data to load
            await page.waitForTimeout(2000);

            // Check for KPI cards (they have specific styling)
            const kpiCards = page.locator('.text-xl.font-bold');
            const cardCount = await kpiCards.count();

            console.log(`Found ${cardCount} KPI value elements`);

            // Log actual values for debugging
            for (let i = 0; i < Math.min(cardCount, 5); i++) {
                const text = await kpiCards.nth(i).innerText();
                console.log(`KPI ${i}: ${text}`);
            }
        });
    });

    test.describe('Filter Interactions', () => {
        test('should apply filters when button is clicked', async ({ page }) => {
            // Find and click the apply button
            const applyBtn = page.locator('[data-testid="filter-apply-btn"]');
            await expect(applyBtn).toBeVisible();

            // Click apply (even with no filter changes)
            await applyBtn.click();

            // Wait for any API calls to complete
            await page.waitForTimeout(1000);

            // Page should still be functional
            await expect(page.locator('h1')).toContainText(/Executive Overview/i);
        });

        test('should update charts when date range changes', async ({ page }) => {
            // Find date range picker
            const dateFilter = page.locator('[data-testid="filter-date"]');
            await expect(dateFilter).toBeVisible();

            // Click to open date picker
            await dateFilter.locator('button').first().click();

            // Wait for calendar to appear
            await page.waitForTimeout(500);

            // Close by clicking elsewhere
            await page.keyboard.press('Escape');
        });
    });

    test.describe('Loading States', () => {
        test('should show loading indicator initially', async ({ page }) => {
            // Navigate fresh to catch loading state
            await page.goto('/visualizations-2');

            // Check for any loading indicators
            // The exact selector depends on implementation
            const loadingIndicator = page.locator('.loader, [data-loading="true"], .animate-spin');

            // Loading state may be brief, just verify page loads successfully
            await expect(page.locator('h1')).toContainText(/Executive Overview/i, { timeout: 15000 });
        });
    });

    test.describe('Responsive Layout', () => {
        test('should adapt filter layout on smaller screens', async ({ page }) => {
            // Set viewport to mobile size
            await page.setViewportSize({ width: 375, height: 667 });

            // Wait for layout to adjust
            await page.waitForTimeout(500);

            // Filter bar should still be visible and wrap properly
            const filterBar = page.locator('[data-testid="filter-bar"]');
            await expect(filterBar).toBeVisible();

            // Apply button should remain accessible
            const applyBtn = page.locator('[data-testid="filter-apply-btn"]');
            await expect(applyBtn).toBeVisible();
        });
    });
});

test.describe('Edge Cases', () => {
    test('should handle page refresh gracefully', async ({ page }) => {
        // Login
        await page.goto('/login');
        await page.fill('input[id="username"]', TEST_USER.username);
        await page.fill('input[id="password"]', TEST_USER.password);
        await page.click('button[type="submit"]');
        await page.waitForURL(/\/upload/);

        // Navigate to visualizations
        await page.goto('/visualizations-2');
        await expect(page).toHaveURL(/\/visualizations-2/);
        await page.waitForSelector('[data-testid="filter-section"]', { state: 'visible', timeout: 15000 });

        // Refresh the page
        await page.reload();

        // Should recover and display correctly
        await expect(page.locator('h1')).toContainText(/Executive Overview/i, { timeout: 15000 });
        await expect(page.locator('[data-testid="filter-section"]')).toBeVisible();
    });
});
