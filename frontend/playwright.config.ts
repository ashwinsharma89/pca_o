import { defineConfig, devices } from '@playwright/test';

/**
 * Playwright Configuration for PCA Agent E2E Tests
 */
export default defineConfig({
    testDir: './e2e',
    timeout: 60000,
    expect: {
        timeout: 10000,
    },
    fullyParallel: false, // Run file by file to avoid overloading the dev server
    forbidOnly: !!process.env.CI,
    retries: process.env.CI ? 2 : 1, // Add a retry locally for flakiness
    workers: process.env.CI ? 1 : 1, // Use single worker locally to avoid overloading dev server
    reporter: [
        ['html', { outputFolder: 'playwright-report' }],
        ['json', { outputFile: 'test-results.json' }],
        ['junit', { outputFile: 'junit.xml' }]
    ],
    use: {
        baseURL: process.env.BASE_URL || 'http://localhost:3000',
        trace: 'on-first-retry',
        screenshot: 'only-on-failure',
        video: 'retain-on-failure',
        navigationTimeout: 30000,
        actionTimeout: 15000,
    },

    projects: [
        {
            name: 'chromium',
            use: { ...devices['Desktop Chrome'] },
        },
        {
            name: 'firefox',
            use: { ...devices['Desktop Firefox'] },
        },
        {
            name: 'webkit',
            use: { ...devices['Desktop Safari'] },
        },
        {
            name: 'Mobile Chrome',
            use: { ...devices['Pixel 5'] },
        },
        {
            name: 'Mobile Safari',
            use: { ...devices['iPhone 12'] },
        },
    ],

    webServer: {
        command: 'npm run dev',
        url: 'http://localhost:3000',
        reuseExistingServer: !process.env.CI,
        timeout: 120 * 1000,
    },
});
