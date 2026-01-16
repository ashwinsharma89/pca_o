import pytest
from sqlalchemy.orm import Session
from src.core.database.connection import get_db_manager
from src.engine.services.user_service import UserService
from src.core.database.user_models import User

class TestUserService:
    @pytest.fixture
    def db_session(self):
        from sqlalchemy import create_engine
        from src.core.database.models import Base
        from src.core.database.user_models import User # Ensure User is registered
        
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(bind=engine)
        
        from sqlalchemy.orm import sessionmaker
        SessionLocal = sessionmaker(bind=engine)
        session = SessionLocal()
        
        yield session
        session.close()

    @pytest.fixture
    def user_service(self, db_session):
        return UserService(db_session)

    def test_mfa_enable_disable(self, user_service, db_session):
        # Create a test user
        username = "test_mfa_user"
        email = "mfa@example.com"
        password = "Password123!"
        
        user = user_service.create_user(username, email, password)
        assert user.mfa_enabled == False
        
        # Enable MFA
        secret = "JBSWY3DPEHPK3PXP"
        user_service.enable_mfa(user.id, secret)
        
        db_session.refresh(user)
        assert user.mfa_enabled == True
        assert user.mfa_secret != secret
        assert user_service.encrypter.decrypt(user.mfa_secret) == secret
        
        # Disable MFA
        user_service.disable_mfa(user.id)
        db_session.refresh(user)
        assert user.mfa_enabled == False
        assert user.mfa_secret is None
        
        # Cleanup
        db_session.delete(user)
        db_session.commit()
