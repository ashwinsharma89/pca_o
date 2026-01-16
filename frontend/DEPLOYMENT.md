# Frontend Deployment Guide

## Quick Start

### Local Development
```bash
# Install dependencies
npm install

# Copy environment template
cp env.template .env.local

# Start development server
npm run dev
```

Access at: http://localhost:3000

---

## Deployment Options

### Option 1: Docker (Recommended for Production)

#### Build and Run
```bash
# Build Docker image
docker build -t pca-frontend .

# Run container
docker run -p 3000:3000 \
  -e NEXT_PUBLIC_API_URL=http://localhost:8000/api/v1 \
  pca-frontend
```

#### Using Docker Compose
```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f frontend

# Stop services
docker-compose down
```

---

### Option 2: Vercel (Easiest)

#### Deploy
```bash
# Install Vercel CLI
npm i -g vercel

# Deploy
vercel

# Set environment variables in Vercel dashboard
# NEXT_PUBLIC_API_URL=https://your-api.com/api/v1
```

---

### Option 3: Self-Hosted

#### Build
```bash
npm run build
npm start
```

#### With PM2
```bash
# Install PM2
npm install -g pm2

# Start
pm2 start npm --name "pca-frontend" -- start

# Save process list
pm2 save

# Setup startup script
pm2 startup
```

---

## Environment Variables

Required variables (set in `.env.local` or deployment platform):

```env
NEXT_PUBLIC_API_URL=http://localhost:8000/api/v1
NEXT_PUBLIC_APP_NAME=PCA Agent
```

---

## Health Check

Frontend health: http://localhost:3000/api/health

---

## Troubleshooting

### Build fails
```bash
# Clear cache
rm -rf .next node_modules
npm install
npm run build
```

### Docker build fails
```bash
# Check Docker is running
docker ps

# Rebuild without cache
docker build --no-cache -t pca-frontend .
```

### API connection fails
- Check `NEXT_PUBLIC_API_URL` is correct
- Verify backend is running
- Check CORS settings on backend
