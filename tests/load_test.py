"""
Locust Load Test for Pacing Reports API.

Run with:
    locust -f tests/load_test.py --host=http://localhost:8000

Or headless:
    locust -f tests/load_test.py --host=http://localhost:8000 --headless -u 10 -r 2 -t 60s
"""
from locust import HttpUser, task, between
import json


class PacingReportsUser(HttpUser):
    """Simulates a user interacting with Pacing Reports API."""
    
    wait_time = between(1, 3)  # Wait 1-3 seconds between tasks
    
    def on_start(self):
        """Called when user starts - get list of templates."""
        response = self.client.get("/api/v1/pacing-reports/templates")
        if response.status_code == 200:
            data = response.json()
            self.templates = [t['filename'] for t in data.get('data', {}).get('templates', [])]
        else:
            self.templates = []
    
    @task(3)
    def list_templates(self):
        """List available templates - most common operation."""
        self.client.get("/api/v1/pacing-reports/templates")
    
    @task(2)
    def list_reports(self):
        """List generated reports."""
        self.client.get("/api/v1/pacing-reports/reports")
    
    @task(2)
    def list_jobs(self):
        """Check job status - frequent for progress monitoring."""
        self.client.get("/api/v1/pacing-reports/jobs")
    
    @task(1)
    def validate_template(self):
        """Validate a template - uses caching."""
        if self.templates:
            template = self.templates[0]
            self.client.get(f"/api/v1/pacing-reports/templates/{template}/validate")
    
    @task(1)
    def generate_report(self):
        """Generate a report - most expensive operation."""
        if self.templates:
            self.client.post(
                "/api/v1/pacing-reports/generate",
                json={
                    "template_filename": self.templates[0],
                    "aggregation": "daily"
                }
            )
    
    @task(1)
    def health_check(self):
        """Health check endpoint."""
        self.client.get("/api/v1/pacing-reports/health")


class BatchOperationsUser(HttpUser):
    """Tests batch operations specifically."""
    
    wait_time = between(5, 10)  # Batch operations are less frequent
    
    def on_start(self):
        """Get templates for batch testing."""
        response = self.client.get("/api/v1/pacing-reports/templates")
        if response.status_code == 200:
            data = response.json()
            self.templates = [t['filename'] for t in data.get('data', {}).get('templates', [])]
        else:
            self.templates = []
    
    @task(1)
    def batch_generate(self):
        """Test batch generation with multiple templates."""
        if len(self.templates) >= 2:
            self.client.post(
                "/api/v1/pacing-reports/batch-generate",
                json={
                    "requests": [
                        {"template_filename": self.templates[0]},
                        {"template_filename": self.templates[1]}
                    ]
                }
            )
    
    @task(2)
    def monitor_jobs(self):
        """Monitor batch job progress."""
        self.client.get("/api/v1/pacing-reports/jobs?limit=50")
