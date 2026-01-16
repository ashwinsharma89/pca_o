export DATABASE_URL=sqlite:///data/pca_agent.db
export JWT_SECRET_KEY=test-secret-key-must-be-long-enough-32-chars
export PYTHONPATH=$PYTHONPATH:.
.venv312/bin/uvicorn src.interface.api.main_v3:app --reload --host 0.0.0.0 --port 8000
