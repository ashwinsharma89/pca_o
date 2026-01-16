# 🚀 Predictive Analytics - Quick Start Guide

## **Get Started in 15 Minutes!**

---

## 📦 **Step 1: Install Dependencies** (2 minutes)

```bash
cd c:\Users\asharm08\OneDrive - dentsu\Desktop\windsurf\PCA_Agent

# Install new ML dependencies
pip install scikit-learn==1.3.2 scipy==1.11.4 xgboost==2.0.3 prophet==1.1.5 joblib==1.3.2

# Or install all requirements
pip install -r requirements.txt
```

---

## 📊 **Step 2: Generate Sample Data** (1 minute)

```bash
# Generate 100 sample historical campaigns
python generate_sample_historical_data.py
```

This creates `data/historical_campaigns_sample.csv` with:
- 100 campaigns
- Budget range: $50k - $500k
- ROAS range: 1.5 - 8.0
- Multiple channels, creative types, objectives

---

## 🚀 **Step 3: Launch Dashboard** (1 minute)

```bash
# Launch the predictive analytics dashboard
streamlit run streamlit_predictive.py
```

The dashboard will open at: `http://localhost:8501`

---

## 🎯 **Step 4: Train Your First Model** (5 minutes)

### **In the Dashboard:**

1. **Go to "Model Training" tab**

2. **Upload Historical Data**:
   - Click "Browse files" in sidebar
   - Upload `data/historical_campaigns_sample.csv`
   - You'll see "✅ Loaded 100 campaigns"

3. **Set Success Criteria**:
   - Target ROAS: 3.0
   - Target CPA: $75

4. **Train Model**:
   - Click "🚀 Train Model"
   - Wait 30-60 seconds
   - See accuracy metrics (should be >80%)

5. **Save Model**:
   - Click "💾 Save Model"
   - Model saved to `models/campaign_success_predictor.pkl`

---

## 🔮 **Step 5: Make Your First Prediction** (3 minutes)

### **In the Dashboard:**

1. **Go to "Campaign Success Predictor" tab**

2. **Load Model**:
   - Click "🔄 Load Prediction Model"
   - Wait for "✅ Model loaded successfully!"

3. **Enter Campaign Details**:
   - Campaign Name: "Q4_Holiday_Campaign"
   - Budget: $250,000
   - Duration: 30 days
   - Audience Size: 500,000
   - Channels: Meta, Google
   - Creative Type: video
   - Objective: conversion

4. **Get Prediction**:
   - Click "🎯 Predict Success"
   - See results:
     - Success Probability: XX%
     - Confidence Level: HIGH/MEDIUM/LOW
     - Risk Level: LOW/MEDIUM/HIGH
     - Key Drivers
     - Insights
     - Recommendations

---

## 📈 **Step 6: Test Early Performance Monitor** (3 minutes)

### **Create Sample Early Data:**

```python
# Run this in Python or Jupyter
import pandas as pd
import numpy as np

# Generate 24 hours of sample data
early_data = pd.DataFrame({
    'hours_since_start': range(24),
    'impressions': np.random.randint(10000, 50000, 24),
    'clicks': np.random.randint(100, 500, 24),
    'conversions': np.random.randint(5, 30, 24),
    'spend': np.random.uniform(500, 2000, 24),
    'revenue': np.random.uniform(2000, 8000, 24)
})

early_data.to_csv('data/early_performance_sample.csv', index=False)
print("✅ Sample early performance data created!")
```

### **In the Dashboard:**

1. **Go to "Early Performance Monitor" tab**

2. **Upload Early Data**:
   - Upload `data/early_performance_sample.csv`

3. **Analyze**:
   - Campaign ID: CAMP_001
   - Hours Elapsed: 24
   - Click "⚡ Analyze Performance"

4. **Review Results**:
   - Success probability
   - Warnings (if any)
   - Immediate action recommendations

---

## 💰 **Step 7: Test Budget Optimizer** (Optional)

### **In the Dashboard:**

1. **Go to "Budget Optimizer" tab**

2. **Set Parameters**:
   - Total Budget: $1,000,000
   - Campaign Goal: roas
   - Min Spend per Channel: $50,000

3. **Optimize**:
   - Click "💰 Optimize Allocation"
   - See recommended channel mix
   - Expected ROAS and revenue

---

## ✅ **You're Done!**

You now have a fully functional predictive analytics system!

---

## 🎯 **What You Can Do Now**

### **Pre-Campaign Planning**:
- Predict success probability before launch
- Get risk assessment
- Receive optimization recommendations
- Avoid launching doomed campaigns

### **In-Campaign Monitoring**:
- Monitor first 24-48 hours
- Get early warning alerts
- Optimize mid-campaign
- Prevent wasted spend

### **Budget Optimization**:
- Find optimal channel mix
- Maximize ROAS
- Identify saturation points
- Plan quarterly budgets

---

## 📊 **Next Steps**

### **Use Your Own Data**:

1. **Prepare Your Historical Campaigns**:
   ```csv
   campaign_id,budget,duration,audience_size,channels,creative_type,objective,start_date,roas,cpa,conversions
   CAMP_001,250000,30,500000,"Meta,Google",video,conversion,2024-01-15,4.5,65,3846
   CAMP_002,150000,21,300000,LinkedIn,image,lead_generation,2024-02-01,3.2,85,1765
   ...
   ```

2. **Upload to Dashboard**:
   - Go to sidebar
   - Upload your CSV
   - Train model with your data

3. **Make Real Predictions**:
   - Use for upcoming campaigns
   - Track prediction accuracy
   - Measure ROI

### **Integrate with Existing Workflow**:

1. **Pre-Campaign Review**:
   - Run prediction for every new campaign
   - Present to stakeholders
   - Adjust based on recommendations

2. **Launch Monitoring**:
   - Export hourly data after 24h
   - Upload to Early Performance Monitor
   - Act on warnings immediately

3. **Quarterly Planning**:
   - Use Budget Optimizer for allocation
   - Compare scenarios
   - Present to finance team

---

## 🎓 **Training Your Team**

### **Week 1: Introduction**
- Show dashboard capabilities
- Explain prediction logic
- Demo with sample data

### **Week 2: Pilot**
- Test on 5-10 upcoming campaigns
- Compare predictions vs actuals
- Gather feedback

### **Week 3: Rollout**
- Make predictions mandatory
- Set up automated alerts
- Track success metrics

---

## 📈 **Measuring Success**

Track these KPIs:

| Metric | Target | How to Measure |
|--------|--------|----------------|
| **Prediction Accuracy** | >80% | Compare predicted vs actual ROAS |
| **Early Detection Rate** | >70% | % of issues caught in first 24h |
| **Cost Savings** | Track ₹ | Campaigns paused/optimized early |
| **Revenue Increase** | Track ₹ | From budget optimization |
| **Campaign Success Rate** | +20% | Before vs after using predictions |

---

## 🆘 **Troubleshooting**

### **Model Not Loading**:
- Check if `models/campaign_success_predictor.pkl` exists
- Retrain model if file is missing
- Ensure you clicked "Save Model" after training

### **Low Accuracy (<70%)**:
- Need more historical data (50-100 campaigns minimum)
- Check data quality (no missing values)
- Ensure success criteria matches your goals

### **Predictions Seem Off**:
- Verify input data format matches training data
- Check if channels/creative types are in training set
- Retrain with more recent campaigns

---

## 💡 **Pro Tips**

1. **Retrain Monthly**: Add new campaigns and retrain for better accuracy

2. **Track Everything**: Log all predictions and actual results

3. **A/B Test**: Compare model recommendations vs manual decisions

4. **Start Small**: Test on 5-10 campaigns before full rollout

5. **Communicate Value**: Share success stories with stakeholders

---

## 📚 **Additional Resources**

- **Complete Architecture**: `PREDICTIVE_ANALYTICS_ARCHITECTURE.md`
- **Implementation Guide**: `PREDICTIVE_IMPLEMENTATION_GUIDE.md`
- **Analysis Framework**: `ANALYSIS_FRAMEWORK.md`
- **Code Documentation**: `src/predictive/` directory

---

## 🎉 **Congratulations!**

You've successfully set up predictive analytics for your PCA Agent!

**You can now:**
- ✅ Predict campaign success before launch
- ✅ Monitor early performance in real-time
- ✅ Optimize budget allocation
- ✅ Make data-driven decisions
- ✅ Save costs and increase revenue

**Start predicting and optimizing today!** 🚀

---

## 🆘 **Need Help?**

- Check documentation in `docs/` folder
- Review code examples in `src/predictive/`
- Test with sample data first
- Start with Campaign Success Predictor (easiest)

**Happy Predicting!** 🔮
