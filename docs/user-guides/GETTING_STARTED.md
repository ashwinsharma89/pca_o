# 🚀 Getting Started with PCA Agent

Welcome! This guide will help you get PCA Agent up and running in minutes.

## 📋 What You'll Need

1. **Python 3.11+** - [Download here](https://www.python.org/downloads/)
2. **OpenAI API Key** - [Get one here](https://platform.openai.com/api-keys)
3. **Dashboard Screenshots** - From Google Ads, Meta Ads, LinkedIn, etc.

## ⚡ Quick Start (5 Minutes)

### Step 1: Install Dependencies

Open terminal in the `PCA_Agent` directory and run:

```bash
pip install -r requirements.txt
```

This installs all required packages (~50 dependencies).

### Step 2: Configure API Key

```bash
# Copy the example environment file
cp .env.example .env
```

Open `.env` file and add your OpenAI API key:

```
OPENAI_API_KEY=sk-your-actual-key-here
```

### Step 3: Start the Application

**Option A: Streamlit Dashboard (Recommended)**
```bash
streamlit run streamlit_app.py
```

**Option B: API Server**
```bash
uvicorn src.api.main:app --reload
```

### Step 4: Open in Browser

- **Streamlit**: http://localhost:8501
- **API Docs**: http://localhost:8000/docs

## 🎯 Your First Campaign Analysis

### Using Streamlit Dashboard

1. **Open Dashboard**: Navigate to http://localhost:8501

2. **Create Campaign**:
   - Campaign Name: "Q4 2024 Holiday Campaign"
   - Objectives: Select "awareness" and "conversion"
   - Date Range: Set your campaign dates

3. **Upload Screenshots**:
   - Drag and drop dashboard screenshots
   - Supported: PNG, JPG, PDF
   - Upload from multiple platforms (Google Ads, Meta, LinkedIn, etc.)

4. **Analyze**:
   - Click "🚀 Analyze Campaign"
   - Wait 2-5 minutes for processing

5. **Download Report**:
   - Go to "Campaigns" tab
   - Click "Download Report"
   - Open the PowerPoint file

### Using API

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
    ("files", open("google_ads_dashboard.png", "rb")),
    ("files", open("meta_ads_dashboard.png", "rb"))
]
requests.post(f"http://localhost:8000/api/campaigns/{campaign_id}/snapshots", files=files)

# 3. Start analysis
requests.post(f"http://localhost:8000/api/campaigns/{campaign_id}/analyze")

# 4. Check status
status = requests.get(f"http://localhost:8000/api/campaigns/{campaign_id}/status")
print(status.json())

# 5. Download report (when completed)
report = requests.get(f"http://localhost:8000/api/campaigns/{campaign_id}/report")
with open("campaign_report.pptx", "wb") as f:
    f.write(report.content)
```

## 📸 Preparing Dashboard Screenshots

### Best Practices

1. **Use Full Screen**: Capture entire dashboard
2. **High Resolution**: At least 1920x1080
3. **Clear Text**: Ensure metrics are readable
4. **Include Date Range**: Show campaign dates
5. **Consistent Dates**: Use same date range across platforms

### Recommended Screenshots

**Google Ads**:
- Campaign overview page
- Performance metrics visible
- Include: Impressions, Clicks, CTR, Conversions, Spend

**Meta Ads Manager**:
- Campaign dashboard
- Include: Reach, Impressions, Clicks, ROAS, Engagement

**LinkedIn Campaign Manager**:
- Performance tab
- Include: Impressions, Clicks, Conversions, Spend, CPC

**DV360**:
- Campaign summary
- Include: Impressions, Clicks, CPM, Viewability

**CM360**:
- Campaign report
- Include: Impressions, Clicks, Reach, Frequency

**Snapchat Ads Manager**:
- Campaign overview
- Include: Impressions, Swipe-ups, Video views

### Sample Screenshots

You can test with sample screenshots from:
- Your own campaigns
- Demo accounts
- Public case studies

## 🎨 What You'll Get

### Generated Report Includes:

1. **Title Slide**: Campaign name and date range
2. **Executive Summary**: Key metrics and overview
3. **Channel Overview**: Performance comparison table
4. **Channel Deep-Dives**: 1 slide per platform with:
   - Performance score
   - Key metrics
   - Strengths
   - Opportunities
5. **Cross-Channel Insights**: Synergies and patterns
6. **Key Achievements**: Top 5 wins highlighted
7. **Recommendations**: Strategic next steps
8. **Visualizations**: Charts and graphs

### Sample Insights Generated:

- "Meta Ads achieved 4.2x ROAS, exceeding the 3.5x target by 20%"
- "LinkedIn generated highest quality leads with $45 CPA vs $78 average"
- "Instagram Stories drove 40% of Google Search brand queries"
- "Recommend reallocating 15% budget from Snapchat to Meta for better ROAS"

## 🔧 Troubleshooting

### Issue: "OpenAI API key not found"

**Solution**: Make sure you've:
1. Created `.env` file (copy from `.env.example`)
2. Added your API key: `OPENAI_API_KEY=sk-...`
3. Restarted the application

### Issue: "Module not found"

**Solution**: Install dependencies:
```bash
pip install -r requirements.txt
```

### Issue: "Kaleido error" (chart generation)

**Solution**: Install kaleido:
```bash
pip install kaleido
```

### Issue: Port already in use

**Solution**: Use different port:
```bash
# For Streamlit
streamlit run streamlit_app.py --server.port 8502

# For API
uvicorn src.api.main:app --reload --port 8001
```

### Issue: Low extraction accuracy

**Solutions**:
- Use higher resolution screenshots
- Ensure text is clear and readable
- Avoid cropped or partial screenshots
- Use consistent date ranges

## 📚 Next Steps

### Learn More
- **Architecture**: Read `ARCHITECTURE.md` for system design
- **API Reference**: Visit http://localhost:8000/docs
- **Deployment**: See `DEPLOYMENT.md` for production setup

### Customize
- **Add Platforms**: Extend `src/models/platform.py`
- **Custom Templates**: Modify `src/agents/report_agent.py`
- **New Visualizations**: Add to `src/agents/visualization_agent.py`

### Test
```bash
# Run test workflow
python test_workflow.py
```

## 💡 Tips for Best Results

1. **Multiple Platforms**: Upload at least 2 platforms for cross-channel analysis
2. **Clear Objectives**: Select accurate campaign objectives
3. **Complete Data**: Include all relevant metrics in screenshots
4. **Consistent Dates**: Ensure all platforms show same date range
5. **High Quality**: Use clear, high-resolution screenshots

## 🎯 Common Use Cases

### Use Case 1: Monthly Performance Review
- Upload screenshots from all active platforms
- Generate comprehensive report
- Share with stakeholders

### Use Case 2: Campaign Optimization
- Analyze mid-campaign performance
- Get AI-powered recommendations
- Adjust budget allocation

### Use Case 3: Client Reporting
- Automated report generation
- Professional PowerPoint output
- Consistent formatting

### Use Case 4: Competitive Analysis
- Compare multiple campaigns
- Identify best practices
- Benchmark performance

## 🚀 Advanced Features

### API Integration

Integrate PCA Agent into your workflow:

```python
# Automated weekly reports
import schedule

def weekly_report():
    # Upload latest screenshots
    # Generate report
    # Email to stakeholders
    pass

schedule.every().monday.at("09:00").do(weekly_report)
```

### Custom Templates

Modify report templates in `src/agents/report_agent.py`:
- Change colors
- Add company logo
- Customize slide layouts
- Add custom sections

### Batch Processing

Process multiple campaigns:

```python
campaigns = [
    {"name": "Campaign A", "files": [...]},
    {"name": "Campaign B", "files": [...]},
]

for campaign in campaigns:
    # Create and analyze
    pass
```

## 📞 Getting Help

### Resources
- **Documentation**: See `README.md`
- **API Docs**: http://localhost:8000/docs
- **Architecture**: See `ARCHITECTURE.md`
- **Examples**: See `test_workflow.py`

### Common Questions

**Q: How long does analysis take?**  
A: 2-5 minutes for 6 snapshots

**Q: What platforms are supported?**  
A: Google Ads, CM360, DV360, Meta Ads, Snapchat Ads, LinkedIn Ads

**Q: Can I customize the report?**  
A: Yes, modify templates in `src/agents/report_agent.py`

**Q: Is my data secure?**  
A: Data is processed locally. Only images sent to OpenAI API.

**Q: Can I use Claude instead of GPT-4?**  
A: Yes, add `ANTHROPIC_API_KEY` to `.env`

## ✅ Success Checklist

Before your first analysis:
- [ ] Python 3.11+ installed
- [ ] Dependencies installed (`pip install -r requirements.txt`)
- [ ] `.env` file created with API key
- [ ] Application started (Streamlit or API)
- [ ] Dashboard screenshots prepared
- [ ] Browser opened to http://localhost:8501

## 🎉 You're Ready!

Start analyzing your campaigns with AI-powered insights!

```bash
streamlit run streamlit_app.py
```

Open http://localhost:8501 and upload your first campaign screenshots.

---

**Need help?** Check the troubleshooting section or review the documentation files.

**Ready for production?** See `DEPLOYMENT.md` for cloud deployment guide.
