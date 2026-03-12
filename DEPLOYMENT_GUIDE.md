# PCA Agent - Production Deployment Guide

## 🎯 Quick Start

### Option 1: Automated CI/CD (Recommended)
1. Push code to GitHub
2. Pipeline automatically builds and deploys
3. Done! ✅

### Option 2: Manual Deployment
```bash
# Fix requirements first
chmod +x fix-requirements.sh
./fix-requirements.sh

# Build and deploy
chmod +x deploy-scripts/*.sh
./deploy-scripts/quick-deploy.sh
```

## 📋 Best Practices Implemented

### ✅ Docker Best Practices
1. **Multi-stage builds** - Separate build and runtime stages
2. **Small base image** - Using python:3.11-slim (not full OS)
3. **Layer caching** - Requirements copied before code
4. **Minimal dependencies** - Only runtime libs in final image
5. **Cache cleanup** - All apt caches removed
6. **Non-root user** - Runs as appuser (UID 1000)
7. **.dockerignore** - Excludes unnecessary files
8. **Health checks** - Built into Dockerfile
9. **Version pinning** - All dependencies pinned
10. **Security** - No secrets baked into image

### ✅ CI/CD Best Practices
1. **Automated builds** - On every push to main
2. **Image registry** - Docker Hub with caching
3. **Automated deployment** - Direct to EC2
4. **Health checks** - Verifies deployment success
5. **Rollback capability** - Tagged images for rollback

## 🏗️ Architecture

```
┌─────────────┐
│   GitHub    │
│  Repository │
└──────┬──────┘
       │ push
       ▼
┌─────────────┐
│   GitHub    │
│   Actions   │
└──────┬──────┘
       │ build
       ▼
┌─────────────┐
│  Docker Hub │
│   Registry  │
└──────┬──────┘
       │ pull
       ▼
┌─────────────┐
│  EC2 Server │
│ 13.200.71.62│
└─────────────┘
```

## 📁 Files Created

```
.
├── .dockerignore                    # Exclude files from Docker build
├── .github/
│   └── workflows/
│       └── deploy.yml              # CI/CD pipeline
├── Dockerfile.production           # Optimized production Dockerfile
├── deploy-scripts/
│   ├── build-local.sh             # Build image locally
│   ├── deploy-to-ec2.sh           # Deploy to EC2
│   └── quick-deploy.sh            # Build + Deploy in one command
├── fix-requirements.sh            # Fix missing dependencies
├── ci-cd-setup.md                 # CI/CD overview
└── DEPLOYMENT_GUIDE.md            # This file
```

## 🔧 Setup Instructions

### 1. Fix Requirements
```bash
chmod +x fix-requirements.sh
./fix-requirements.sh
```

This adds missing dependencies:
- `loguru>=0.7.0` (for logging)
- Removes duplicate `gunicorn` entries

### 2. Test Build Locally
```bash
chmod +x deploy-scripts/build-local.sh
./deploy-scripts/build-local.sh
```

### 3. Deploy to EC2
```bash
chmod +x deploy-scripts/deploy-to-ec2.sh
./deploy-scripts/deploy-to-ec2.sh
```

### 4. Or Do Both at Once
```bash
chmod +x deploy-scripts/quick-deploy.sh
./deploy-scripts/quick-deploy.sh
```

## 🚀 GitHub Actions Setup (Optional but Recommended)

### Step 1: Create GitHub Repository
```bash
cd ~/Desktop/pca_agent_copy
git init
git add .
git commit -m "Initial commit with CI/CD"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/pca-agent.git
git push -u origin main
```

### Step 2: Add GitHub Secrets
Go to: Repository → Settings → Secrets and variables → Actions

Add these secrets:
- `DOCKER_USERNAME` - Your Docker Hub username
- `DOCKER_PASSWORD` - Docker Hub access token
- `EC2_HOST` - `13.200.71.62`
- `EC2_USERNAME` - `ubuntu`
- `EC2_SSH_KEY` - Your private SSH key (entire content)
- `ENCRYPTION_KEY` - Your app encryption key

### Step 3: Push and Watch
```bash
git push origin main
```

Go to Actions tab to watch the build and deployment!

## 🔍 Verification

### Check Container Status
```bash
ssh ubuntu@13.200.71.62 'docker ps'
```

### View Logs
```bash
ssh ubuntu@13.200.71.62 'docker logs pca-api --tail 50'
```

### Health Check
```bash
curl http://13.200.71.62:8000/health
```

## 🐛 Troubleshooting

### Build Fails
```bash
# Check Docker is running
docker ps

# Check requirements.txt
cat ~/Desktop/pca_agent_copy/requirements.txt | grep -E "loguru|gunicorn|uvicorn"
```

### Deployment Fails
```bash
# Check EC2 connectivity
ssh ubuntu@13.200.71.62 'docker ps'

# Check container logs
ssh ubuntu@13.200.71.62 'docker logs pca-api'

# Check if ports are available
ssh ubuntu@13.200.71.62 'netstat -tlnp | grep 8000'
```

### Container Crashes
```bash
# View full logs
ssh ubuntu@13.200.71.62 'docker logs pca-api --tail 100'

# Check container resources
ssh ubuntu@13.200.71.62 'docker stats pca-api --no-stream'

# Restart container
ssh ubuntu@13.200.71.62 'docker restart pca-api'
```

## 📊 Current Status

**Running Services on EC2:**
- ✅ PostgreSQL (pca-postgres)
- ✅ Redis (pca-redis)
- ⏳ API (pca-api) - Will be deployed after running scripts

**Not Running:**
- ❌ Frontend (no image available)
- ❌ Celery workers (can be added later)
- ❌ Neo4j (removed per your request)

## 🎯 Next Steps

1. Run `./fix-requirements.sh` to add missing dependencies
2. Run `./deploy-scripts/quick-deploy.sh` to build and deploy
3. Verify deployment with health check
4. (Optional) Set up GitHub Actions for automated deployments

## 📝 Notes

- The production Dockerfile runs as non-root user (appuser)
- All sensitive data is passed via environment variables
- Images are tagged with timestamps for easy rollback
- Health checks ensure the API is responding before marking deployment as successful
