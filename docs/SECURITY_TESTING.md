# Security Testing Guide

This guide covers security features implemented in PCA Agent and how to test them during an audit.

## Security Features Overview

### 1. Authentication & Authorization
- **JWT-based authentication** with secure token generation
- **Password hashing** using bcrypt (cost factor: 12)
- **Token expiration** (default: 24 hours)
- **Role-based access control** (RBAC)
- **Session management** with refresh tokens

### 2. Input Validation & Sanitization
- **SQL injection prevention** via parameterized queries
- **XSS protection** with input sanitization
- **File upload validation** (type, size, content)
- **Request validation** using Pydantic models

### 3. API Security
- **CORS configuration** with whitelist
- **Rate limiting** on sensitive endpoints
- **HTTPS enforcement** in production
- **Security headers** (CSP, X-Frame-Options, etc.)

### 4. Data Protection
- **Sensitive data anonymization** before LLM API calls
- **Database encryption** at rest (PostgreSQL)
- **Secure credential storage** (environment variables)
- **Audit logging** for sensitive operations

---

## Testing Procedures

### 1. Authentication Testing

#### Test JWT Token Generation
```bash
# Login and get token
curl -X POST http://localhost:8000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"auditor","password":"audit123"}'

# Expected: 200 OK with access_token
```

#### Test Invalid Credentials
```bash
curl -X POST http://localhost:8000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"auditor","password":"wrongpassword"}'

# Expected: 401 Unauthorized
```

#### Test Token Expiration
```bash
# Use an expired token
curl -X GET http://localhost:8000/api/v1/campaigns \
  -H "Authorization: Bearer expired_token_here"

# Expected: 401 Unauthorized with "Token expired" message
```

#### Test Missing Token
```bash
curl -X GET http://localhost:8000/api/v1/campaigns

# Expected: 403 Forbidden or 401 Unauthorized
```

---

### 2. SQL Injection Testing

#### Test Parameterized Queries
```python
# Run SQL injection tests
pytest tests/security/test_sql_injection.py -v
```

#### Manual SQL Injection Attempts
```bash
# Try SQL injection in campaign filter
curl -X GET "http://localhost:8000/api/v1/campaigns?platform=Google%20Ads';%20DROP%20TABLE%20campaigns;--" \
  -H "Authorization: Bearer $TOKEN"

# Expected: Query should be safely parameterized, no SQL execution
```

---

### 3. XSS Protection Testing

#### Test Input Sanitization
```bash
# Try XSS in campaign name
curl -X POST http://localhost:8000/api/v1/campaigns/upload \
  -H "Authorization: Bearer $TOKEN" \
  -F "file=@malicious.csv"

# malicious.csv contains: <script>alert('XSS')</script>
# Expected: Script tags should be sanitized or escaped
```

#### Run XSS Tests
```python
pytest tests/security/test_xss_protection.py -v
```

---

### 4. CORS Testing

#### Test Allowed Origins
```bash
# Request from allowed origin
curl -X GET http://localhost:8000/api/v1/campaigns \
  -H "Origin: http://localhost:3000" \
  -H "Authorization: Bearer $TOKEN"

# Expected: 200 OK with Access-Control-Allow-Origin header
```

#### Test Blocked Origins
```bash
# Request from disallowed origin
curl -X GET http://localhost:8000/api/v1/campaigns \
  -H "Origin: http://malicious-site.com" \
  -H "Authorization: Bearer $TOKEN"

# Expected: CORS error or missing Access-Control-Allow-Origin header
```

---

### 5. File Upload Security

#### Test File Type Validation
```bash
# Try uploading executable file
curl -X POST http://localhost:8000/api/v1/campaigns/upload \
  -H "Authorization: Bearer $TOKEN" \
  -F "file=@malicious.exe"

# Expected: 400 Bad Request with "Invalid file type" message
```

#### Test File Size Limits
```bash
# Try uploading large file (> MAX_UPLOAD_SIZE_MB)
curl -X POST http://localhost:8000/api/v1/campaigns/upload \
  -H "Authorization: Bearer $TOKEN" \
  -F "file=@large_file.csv"

# Expected: 413 Payload Too Large
```

#### Run File Upload Tests
```python
pytest tests/security/test_file_upload.py -v
```

---

### 6. Rate Limiting Testing

#### Test Rate Limits
```bash
# Send multiple requests rapidly
for i in {1..100}; do
  curl -X POST http://localhost:8000/api/v1/auth/login \
    -H "Content-Type: application/json" \
    -d '{"username":"auditor","password":"wrongpassword"}'
done

# Expected: After N requests, should receive 429 Too Many Requests
```

---

### 7. Authorization Testing

#### Test Access Control
```bash
# Try accessing another user's data
curl -X GET http://localhost:8000/api/v1/campaigns/999 \
  -H "Authorization: Bearer $TOKEN"

# Expected: 403 Forbidden if campaign belongs to different user
```

#### Run Authorization Tests
```python
pytest tests/security/test_authorization.py -v
```

---

## Automated Security Testing

### Run All Security Tests
```bash
# Run complete security test suite
pytest tests/security/ -v --cov=src

# Generate coverage report
pytest tests/security/ --cov=src --cov-report=html
open htmlcov/index.html
```

### Security Scanning with Bandit
```bash
# Scan for common security issues
bandit -r src/ -f json -o bandit_report.json

# View high-severity issues
bandit -r src/ -ll

# Scan specific modules
bandit -r src/api/ src/database/ -f screen
```

### Dependency Vulnerability Scanning
```bash
# Check for known vulnerabilities in dependencies
pip install safety
safety check --json

# Or use pip-audit
pip install pip-audit
pip-audit
```

---

## Security Configuration Checklist

### Production Deployment

- [ ] `JWT_SECRET_KEY` is a strong random value (32+ characters)
- [ ] `PRODUCTION_MODE=true` in `.env`
- [ ] `DEBUG=false` in production
- [ ] `CORS_ALLOWED_ORIGINS` does not include `*`
- [ ] HTTPS is enforced (no HTTP)
- [ ] Database credentials are stored securely
- [ ] API keys are in environment variables, not code
- [ ] Rate limiting is enabled
- [ ] File upload size limits are configured
- [ ] Security headers are enabled
- [ ] Audit logging is enabled

### Environment Variables to Review

```bash
# Critical security settings
JWT_SECRET_KEY=<strong-random-secret>
PRODUCTION_MODE=true
DEBUG=false
CORS_ALLOWED_ORIGINS=https://yourdomain.com
API_HOST=0.0.0.0  # Only in production with firewall
```

---

## Common Security Issues & Fixes

### Issue: Weak JWT Secret
**Problem**: Default or weak `JWT_SECRET_KEY`
**Fix**: Generate strong secret
```bash
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

### Issue: CORS Misconfiguration
**Problem**: `CORS_ALLOWED_ORIGINS=*` in production
**Fix**: Whitelist specific origins
```bash
CORS_ALLOWED_ORIGINS=https://app.yourdomain.com,https://www.yourdomain.com
```

### Issue: SQL Injection Vulnerability
**Problem**: String concatenation in SQL queries
**Fix**: Use parameterized queries
```python
# Bad
query = f"SELECT * FROM campaigns WHERE platform = '{platform}'"

# Good
query = "SELECT * FROM campaigns WHERE platform = ?"
conn.execute(query, [platform])
```

### Issue: XSS Vulnerability
**Problem**: Unescaped user input in responses
**Fix**: Sanitize input and escape output
```python
from html import escape
safe_input = escape(user_input)
```

---

## Penetration Testing

### OWASP Top 10 Coverage

1. **Broken Access Control** ✅ - RBAC implemented
2. **Cryptographic Failures** ✅ - bcrypt password hashing
3. **Injection** ✅ - Parameterized queries
4. **Insecure Design** ✅ - Security by design
5. **Security Misconfiguration** ✅ - Secure defaults
6. **Vulnerable Components** ✅ - Dependency scanning
7. **Authentication Failures** ✅ - JWT with expiration
8. **Data Integrity Failures** ✅ - Input validation
9. **Logging Failures** ✅ - Audit logging
10. **SSRF** ✅ - URL validation

### Tools for Penetration Testing

```bash
# OWASP ZAP
docker run -t owasp/zap2docker-stable zap-baseline.py \
  -t http://localhost:8000

# SQLMap (SQL injection testing)
sqlmap -u "http://localhost:8000/api/v1/campaigns?platform=test" \
  --cookie="token=$TOKEN"

# Nikto (web server scanner)
nikto -h http://localhost:8000
```

---

## Security Audit Checklist

### Code Review
- [ ] No hardcoded credentials
- [ ] No sensitive data in logs
- [ ] Proper error handling (no stack traces to users)
- [ ] Input validation on all endpoints
- [ ] Output encoding for user-generated content

### Configuration Review
- [ ] Environment variables properly set
- [ ] Database access restricted
- [ ] API keys rotated regularly
- [ ] HTTPS enforced
- [ ] Security headers configured

### Testing Review
- [ ] All security tests passing
- [ ] No high-severity Bandit findings
- [ ] No known vulnerable dependencies
- [ ] CORS properly configured
- [ ] Rate limiting functional

---

## Reporting Security Issues

If you discover a security vulnerability during your audit:

1. **Do not** create a public GitHub issue
2. Email security concerns to: security@example.com
3. Include:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Suggested fix (if available)

---

## References

- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- [JWT Best Practices](https://tools.ietf.org/html/rfc8725)
- [FastAPI Security](https://fastapi.tiangolo.com/tutorial/security/)
- [Python Security Best Practices](https://python.readthedocs.io/en/stable/library/security_warnings.html)
