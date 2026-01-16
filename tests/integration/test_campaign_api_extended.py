import pytest
import uuid
from datetime import date, timedelta
from tests.routes import APIV1

class TestCampaignApiExtended:
    @pytest.mark.parametrize("campaign_data", [
        {"campaign_name": f"Camp {i}", "objective": "Awareness", "start_date": "2023-01-01", "end_date": "2023-01-31"}
        for i in range(10) # 10 basic success cases
    ])
    def test_create_campaign_success_batch(self, client, auth_headers, campaign_data):
        response = client.post(
            APIV1.CAMPAIGNS,
            json=campaign_data,
            headers=auth_headers
        )
        assert response.status_code == 201
        assert response.json()["campaign_name"] == campaign_data["campaign_name"]

    @pytest.mark.parametrize("invalid_data", [
        {"campaign_name": "", "objective": "Awareness", "start_date": "2023-01-01", "end_date": "2023-01-31"}, # Empty name
        {"campaign_name": "Valid", "objective": "", "start_date": "2023-01-01", "end_date": "2023-01-31"}, # Empty objective
        {"campaign_name": "Valid", "objective": "Awareness", "start_date": "2023-05-01", "end_date": "2023-01-31"}, # End before start
        {"campaign_name": "Valid", "objective": "Awareness", "start_date": "invalid", "end_date": "2023-01-31"}, # Invalid date
    ])
    def test_create_campaign_validation_failures(self, client, auth_headers, invalid_data):
        response = client.post(
            APIV1.CAMPAIGNS,
            json=invalid_data,
            headers=auth_headers
        )
        # Should be 400 or 422
        assert response.status_code in [400, 422]

    def test_get_campaigns_unauthorized(self, client):
        response = client.get(APIV1.CAMPAIGNS)
        assert response.status_code == 401

    def test_get_campaign_metrics_flow(self, client, auth_headers):
        # Create a campaign first (actually upload data is better for metrics)
        response = client.get(APIV1.METRICS, headers=auth_headers)
        assert response.status_code == 200
        # Even with no data, it should return a structured response (likely empty or defaults)
        assert "summary" in response.json() or "total_spend" in response.json()

    @pytest.mark.parametrize("endpoint", [
        APIV1.VISUALIZATIONS,
        APIV1.SUGGESTED_QUESTIONS,
        APIV1.METRICS,
        f"{APIV1.CHART_DATA}?x_axis=platform&y_axis=spend"
    ])
    def test_campaign_endpoints_auth_check(self, client, endpoint):
        response = client.get(endpoint)
        assert response.status_code == 401

    def test_upload_csv_flow(self, client, auth_headers):
        # Mock CSV data
        csv_content = "campaign_id,campaign_name,platform,spend,impressions,clicks,conversions,date\n" \
                      "C1,Test Campaign,Google,100,1000,50,5,2023-01-01"
        
        files = {'file': ('test.csv', csv_content, 'text/csv')}
        response = client.post(
            APIV1.UPLOAD,
            files=files,
            headers=auth_headers
        )
        assert response.status_code == 201
        assert response.json()["imported_count"] > 0

    @pytest.mark.parametrize("field_to_group", ["platform", "channel", "objective", "funnel_stage"])
    def test_chart_data_groupings(self, client, auth_headers, field_to_group):
        response = client.get(
            f"{APIV1.CHART_DATA}?x_axis={field_to_group}&y_axis=spend",
            headers=auth_headers
        )
        assert response.status_code == 200
        assert "data" in response.json()
