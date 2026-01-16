# Testing & Quality Assurance Setup

## Prerequisites

```bash
# Install Node.js 18+ and npm
# Install k6 for load testing: https://k6.io/docs/getting-started/installation/
```

## E2E Testing with Playwright

### Installation

```bash
cd frontend

# Install dependencies
npm install

# Install Playwright browsers
npx playwright install
```

### Running Tests

```bash
# Run all E2E tests
npm test

# Run with UI mode (interactive)
npm run test:ui

# Run with browser visible
npm run test:headed

# Debug mode
npm run test:debug

# View test report
npm run test:report
```

### Test Files

- `frontend/e2e/pca-agent.spec.ts` - Main test suite
- `frontend/playwright.config.ts` - Configuration

### Test Coverage

| Category | Tests |
|----------|-------|
| Authentication | Login, logout, error handling |
| Dashboard | Metrics display, campaign list |
| Data Upload | File upload, validation |
| Analytics | Charts, filtering |
| Chat/Q&A | Question submission, responses |
| Reports | Report creation, listing |
| Responsive | Mobile, tablet viewports |
| Performance | Page load times |
| API Health | Health checks, version |

---

## Load Testing with k6

### Installation

```bash
# macOS
brew install k6

# Windows
choco install k6

# Linux
sudo apt-get install k6
# or
sudo snap install k6
```

### Running Load Tests

```bash
# Run full load test suite
k6 run tests/load/load-test.js

# Quick smoke test
k6 run --vus 1 --duration 30s tests/load/load-test.js

# Custom load test
k6 run --vus 50 --duration 5m tests/load/load-test.js

# With environment variables
k6 run -e API_URL=https://api.pca-agent.com tests/load/load-test.js
```

### Test Scenarios

| Scenario | VUs | Duration | Purpose |
|----------|-----|----------|---------|
| Smoke | 1 | 30s | Verify system works |
| Load | 50-100 | 16min | Normal load |
| Stress | 100-300 | 16min | Find breaking point |
| Spike | 500 | 1min | Sudden traffic |

### Thresholds

- 95th percentile response time < 2s
- Error rate < 5%
- Custom error rate < 10%

---

## Component Documentation with Storybook

### Running Storybook

```bash
cd frontend

# Development mode
npm run storybook

# Build static site
npm run build-storybook
```

### Accessing Storybook

Open [http://localhost:6006](http://localhost:6006) in your browser.

### Adding Stories

Create story files alongside components:

```tsx
// src/components/Button/Button.stories.tsx
import type { Meta, StoryObj } from '@storybook/react';
import { Button } from './Button';

const meta: Meta<typeof Button> = {
  title: 'Components/Button',
  component: Button,
  tags: ['autodocs'],
};

export default meta;
type Story = StoryObj<typeof Button>;

export const Primary: Story = {
  args: {
    variant: 'primary',
    children: 'Click me',
  },
};

export const Secondary: Story = {
  args: {
    variant: 'secondary',
    children: 'Click me',
  },
};
```

---

## CI/CD Integration

### GitHub Actions

The CI pipeline automatically runs:

1. **Unit Tests**: `pytest tests/unit/`
2. **Integration Tests**: `pytest tests/integration/`
3. **E2E Tests**: `npx playwright test`
4. **Load Tests** (optional): `k6 run tests/load/load-test.js`

### Local Pre-commit

```bash
# Install pre-commit hooks
cd /Users/ashwin/Desktop/pca_agent\ copy
pip install pre-commit
pre-commit install

# Run manually
pre-commit run --all-files
```

---

## Quick Reference

| Command | Description |
|---------|-------------|
| `cd frontend && npm test` | Run E2E tests |
| `cd frontend && npm run storybook` | Start Storybook |
| `k6 run tests/load/load-test.js` | Run load tests |
| `pre-commit run --all-files` | Run code quality checks |
