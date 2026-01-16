# PCA Agent - Quick Start Guide

Get started with Post Campaign Analysis Agent in 5 minutes!

## Prerequisites

- Python 3.11 or higher
- OpenAI API key (for GPT-4V)
- Anthropic API key (optional, for Claude)

## Installation

### 1. Clone or Download

```bash
cd PCA_Agent
```

### 2. Create Virtual Environment

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux/Mac
python -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment

```bash
# Copy example environment file
cp .env.example .env

# Edit .env and add your API keys
# Required:
OPENAI_API_KEY=your_openai_api_key_here

# Optional:
ANTHROPIC_API_KEY=your_anthropic_api_key_here
```

## Running the Application

### Option 1: Streamlit Dashboard (Recommended for Testing)

```bash
streamlit run streamlit_app.py
```

Access at: http://localhost:8501

### Option 2: API Server

```bash
# Start API server
python -m uvicorn src.api.main:app --reload --port 8000

# Access API docs
# Open browser: http://localhost:8000/docs
```

## Using the Application

### Via Streamlit Dashboard

1. **Open Dashboard**: Navigate to http://localhost:8501
2. **Create Campaign**:
   - Enter campaign name (e.g., "Q4 2024 Holiday Campaign")
   - Select objectives (awareness, conversion, etc.)
   - Set date range
3. **Upload Snapshots**:
   - Drag and drop dashboard screenshots
   - Supported: PNG, JPG, PDF
   - Upload from multiple platforms
4. **Analyze**:
   - Click "Analyze Campaign"
   - Wait for processing (2-5 minutes)
5. **Download Report**:
   - View results in "Campaigns" tab
   - Download PowerPoint report

### Via API

```python
import requests

# 1. Create campaign
response = requests.post("http://localhost:8000/api/campaigns", params={
    "campaign_name": "Q4 2024 Holiday Campaign",
    "objectives": ["awareness", "conversion"],
    "start_date": "2024-10-01",
    "end_date": "2024-12-31"
})
campaign_id = response.json()["campaign_id"]

# 2. Upload snapshots
files = [
    ("files", open("google_ads.png", "rb")),
    ("files", open("meta_ads.png", "rb"))
]
requests.post(f"http://localhost:8000/api/campaigns/{campaign_id}/snapshots", files=files)

# 3. Start analysis
requests.post(f"http://localhost:8000/api/campaigns/{campaign_id}/analyze")

# 4. Check status
status = requests.get(f"http://localhost:8000/api/campaigns/{campaign_id}/status")
print(status.json())

# 5. Download report
report = requests.get(f"http://localhost:8000/api/campaigns/{campaign_id}/report")
with open("report.pptx", "wb") as f:
    f.write(report.content)
```

## Sample Dashboard Screenshots

For testing, you can use screenshots from:

1. **Google Ads**: Campaign overview page
2. **Meta Ads Manager**: Campaign dashboard
3. **LinkedIn Campaign Manager**: Performance tab
4. **DV360**: Campaign summary
5. **CM360**: Campaign report
6. **Snapchat Ads Manager**: Campaign overview

### Tips for Best Screenshots

- Use full-screen captures
- Ensure all metrics are visible
- Include date range in the screenshot
- Use consistent date ranges across platforms
- Capture high-resolution images

## What Gets Generated

The system will:

1. **Extract Data**: Read metrics from your screenshots
   - Impressions, Clicks, CTR
   - Conversions, CPA, ROAS
   - Spend, CPM, CPC
   - Engagement metrics

2. **Analyze Performance**: Generate insights
   - Channel-by-channel analysis
   - Cross-channel synergies
   - Performance rankings
   - Efficiency metrics

3. **Detect Achievements**: Highlight wins
   - Top performing channels
   - Goal attainment
   - Exceptional metrics
   - Efficiency wins

4. **Provide Recommendations**: Actionable insights
   - Budget allocation
   - Creative optimization
   - Audience targeting
   - Channel strategy

5. **Generate Report**: PowerPoint presentation
   - Executive summary
   - Channel performance slides
   - Cross-channel insights
   - Key achievements
   - Strategic recommendations
   - Visualizations and charts

## Troubleshooting

### API Key Errors

```
Error: OpenAI API key not found
```

**Solution**: Add your API key to `.env` file:
```
OPENAI_API_KEY=sk-...
```

### Import Errors

```
ModuleNotFoundError: No module named 'openai'
```

**Solution**: Install dependencies:
```bash
pip install -r requirements.txt
```

### Kaleido Error (Chart Generation)

```
ValueError: The kaleido package is required
```

**Solution**: Install kaleido:
```bash
pip install kaleido
```

### Port Already in Use

```
ERROR: [Errno 48] Address already in use
```

**Solution**: Use a different port:
```bash
uvicorn src.api.main:app --reload --port 8001
```

## Testing Without API Keys

To test the system structure without API keys:

```bash
python test_workflow.py
```

This will:
- Initialize all agents
- Show workflow structure
- Display sample API usage
- Print setup instructions

## Next Steps

1. **Explore API Documentation**: http://localhost:8000/docs
2. **Read Architecture**: See `ARCHITECTURE.md`
3. **Customize Templates**: Modify report templates
4. **Add More Platforms**: Extend platform support
5. **Deploy to Production**: See deployment guide

## Support

For issues or questions:
- Check `README.md` for detailed documentation
- Review `ARCHITECTURE.md` for system design
- See API docs at `/docs` endpoint

## Example Output

After analysis, you'll receive a PowerPoint report with:

- **Slide 1**: Title slide with campaign name
- **Slide 2**: Executive summary with key metrics
- **Slide 3**: Channel performance overview table
- **Slides 4-9**: Individual channel deep-dives
- **Slide 10**: Cross-channel insights
- **Slide 11**: Key achievements (with icons)
- **Slide 12**: Strategic recommendations
- **Slides 13+**: Visualizations (charts and graphs)

Total processing time: 2-5 minutes depending on number of snapshots.

---

**Ready to analyze your campaigns? Start the Streamlit dashboard and upload your first screenshots!**

```bash
streamlit run streamlit_app.py
```
