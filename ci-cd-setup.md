# CI/CD Pipeline Setup for PCA Agent

## Overview
This document outlines the CI/CD pipeline setup using GitHub Actions to automatically build, test, and deploy your application to EC2.

## Architecture
```
GitHub Push → GitHub Actions → Build Docker Image → Push to Registry → Deploy to EC2
```

## Files Created
1. `.github/workflows/deploy.yml` - Main CI/CD pipeline
2. `Dockerfile.optimized` - Production-ready Dockerfile
3. `.dockerignore` - Exclude unnecessary files
4. `deploy-scripts/deploy.sh` - Deployment script for EC2
5. `requirements.production.txt` - Production dependencies only

## Prerequisites
1. GitHub repository for your code
2. Docker Hub account (or AWS ECR)
3. EC2 instance with Docker installed
4. SSH access to EC2

## GitHub Secrets Required
Add these in GitHub Settings → Secrets and variables → Actions:
- `DOCKER_USERNAME` - Docker Hub username
- `DOCKER_PASSWORD` - Docker Hub password/token
- `EC2_HOST` - EC2 IP (13.200.71.62)
- `EC2_USERNAME` - EC2 user (ubuntu)
- `EC2_SSH_KEY` - Private SSH key for EC2 access
- `ENCRYPTION_KEY` - Your app encryption key

## Setup Steps
1. Copy all files to your repository
2. Add GitHub secrets
3. Push to main branch
4. Pipeline will automatically build and deploy

## Manual Deployment
If you need to deploy manually:
```bash
./deploy-scripts/build-and-push.sh
./deploy-scripts/deploy-to-ec2.sh
```
