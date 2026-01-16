#!/usr/bin/env python3
"""
Security validation and environment setup script.
Run this before deploying to production to ensure secure configuration.
"""
import os
import sys
import secrets
from pathlib import Path

def generate_jwt_secret():
    """Generate a secure JWT secret."""
    return secrets.token_urlsafe(32)

def check_env_file():
    """Check if .env file exists and has required variables."""
    env_path = Path(".env")
    
    if not env_path.exists():
        print("❌ .env file not found!")
        print("Creating .env from .env.example...")
        
        example_path = Path(".env.example")
        if example_path.exists():
            # Copy .env.example to .env
            with open(example_path, 'r') as f:
                content = f.read()
            
            # Generate secure JWT secret
            jwt_secret = generate_jwt_secret()
            content = content.replace(
                "JWT_SECRET_KEY=your-secure-random-secret-here-change-this",
                f"JWT_SECRET_KEY={jwt_secret}"
            )
            
            with open(env_path, 'w') as f:
                f.write(content)
            
            print(f"✅ Created .env with secure JWT secret")
            print(f"🔑 JWT_SECRET_KEY={jwt_secret}")
        else:
            print("❌ .env.example not found!")
            return False
    
    return True

def validate_env_security():
    """Validate environment variables for security issues."""
    issues = []
    warnings = []
    
    # Load .env
    from dotenv import load_dotenv
    load_dotenv()
    
    # Check JWT secret
    jwt_secret = os.getenv("JWT_SECRET_KEY")
    if not jwt_secret:
        issues.append("JWT_SECRET_KEY is not set")
    elif jwt_secret in ["change-this-secret-key", "your-secure-random-secret-here-change-this"]:
        issues.append("JWT_SECRET_KEY is using default/insecure value")
    elif len(jwt_secret) < 32:
        warnings.append("JWT_SECRET_KEY is shorter than recommended (32+ characters)")
    
    # Check API host
    api_host = os.getenv("API_HOST", "127.0.0.1")
    production_mode = os.getenv("PRODUCTION_MODE", "false").lower() == "true"
    
    if api_host == "0.0.0.0":
        if production_mode:
            warnings.append("API_HOST is 0.0.0.0 in production mode - ensure firewall is configured")
        else:
            warnings.append("API_HOST is 0.0.0.0 - consider using 127.0.0.1 for development")
    
    # Check CORS
    cors_origins = os.getenv("CORS_ALLOWED_ORIGINS", "")
    if "*" in cors_origins and production_mode:
        issues.append("CORS_ALLOWED_ORIGINS contains wildcard (*) in production mode")
    
    # Check OpenAI API key
    if not os.getenv("OPENAI_API_KEY"):
        warnings.append("OPENAI_API_KEY is not set - AI features will be disabled")
    
    return issues, warnings

def main():
    """Main validation function."""
    print("=" * 60)
    print("PCA Agent - Security Validation")
    print("=" * 60)
    print()
    
    # Check and create .env if needed
    if not check_env_file():
        sys.exit(1)
    
    # Validate security
    issues, warnings = validate_env_security()
    
    # Report issues
    if issues:
        print("\n🔴 CRITICAL SECURITY ISSUES:")
        for issue in issues:
            print(f"  - {issue}")
        print("\nFix these issues before deploying to production!")
        sys.exit(1)
    
    if warnings:
        print("\n⚠️  WARNINGS:")
        for warning in warnings:
            print(f"  - {warning}")
        print()
    
    if not issues and not warnings:
        print("✅ All security checks passed!")
    elif not issues:
        print("✅ No critical issues found (warnings can be reviewed)")
    
    print("\n" + "=" * 60)
    print("Security validation complete")
    print("=" * 60)
    
    return 0 if not issues else 1

if __name__ == "__main__":
    sys.exit(main())
