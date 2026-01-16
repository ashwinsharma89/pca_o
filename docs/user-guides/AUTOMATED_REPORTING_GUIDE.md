# Automated Daily/Weekly Reporting System

## Overview

Fully automated reporting system that:
- ✅ **Fetches campaign data** automatically
- ✅ **Calculates all KPIs** (CTR, ROAS, CPC, CPM, CPA, Conversion Rate)
- ✅ **Tracks budget utilization** (spent %, remaining %, days left)
- ✅ **Generates alerts** for budget and performance issues
- ✅ **Creates reports** in JSON, CSV, and Excel formats
- ✅ **Schedules automatically** (daily at 9 AM, weekly on Mondays)

---

## Quick Start

### 1. Setup Configuration

**Campaign Budgets** (`config/campaign_budgets.csv`):
```csv
campaign_id,campaign_name,total_budget,start_date,end_date,daily_budget,alert_threshold
CAMP001,Q4 Brand Campaign,50000,2024-10-01,2024-12-31,543.48,0.8
CAMP002,Holiday Sales,75000,2024-11-15,2024-12-25,1875.00,0.85
```

**Reporting Config** (`config/reporting_config.json`):
```json
{
  "data_source": "csv",
  "schedule": {
    "daily_report_time": "09:00",
    "weekly_report_day": "Monday"
  },
  "alerts": {
    "budget_threshold": 0.8,
    "performance_threshold": {
      "min_roas": 2.0,
      "max_cpa": 50.0,
      "min_ctr": 1.0
    }
  }
}
```

### 2. Prepare Campaign Data

**Format** (`data/campaign_data.csv`):
```csv
Date,Campaign_ID,Campaign_Name,Spend,Impressions,Clicks,Conversions,Revenue
2024-12-04,CAMP001,Q4 Brand Campaign,1500.50,45000,450,25,7500.00
2024-12-04,CAMP002,Holiday Sales,2800.75,68000,820,45,14000.00
```

### 3. Run Reporter

```python
from src.reporting.automated_reporter import AutomatedReporter

# Initialize
reporter = AutomatedReporter()

# Load budgets
reporter.load_budgets('config/campaign_budgets.csv')

# Generate daily report
daily_report = reporter.generate_daily_report()

# Generate weekly report
weekly_report = reporter.generate_weekly_report()

# Export to Excel
reporter.export_to_excel(daily_report, 'reports/daily_report.xlsx')
```

---

## Automated KPI Calculations

### 1. Click-Through Rate (CTR)
```
Formula: (Clicks / Impressions) × 100
Example: (450 / 45,000) × 100 = 1.00%
```

### 2. Cost Per Click (CPC)
```
Formula: Spend / Clicks
Example: $1,500 / 450 = $3.33
```

### 3. Cost Per Mille (CPM)
```
Formula: (Spend / Impressions) × 1,000
Example: ($1,500 / 45,000) × 1,000 = $33.33
```

### 4. Cost Per Acquisition (CPA)
```
Formula: Spend / Conversions
Example: $1,500 / 25 = $60.00
```

### 5. Return on Ad Spend (ROAS)
```
Formula: Revenue / Spend
Example: $7,500 / $1,500 = 5.0x
```

### 6. Conversion Rate
```
Formula: (Conversions / Clicks) × 100
Example: (25 / 450) × 100 = 5.56%
```

---

## Budget Tracking

### Automatic Calculations

```python
# Budget Configuration
total_budget = $50,000
start_date = 2024-10-01
end_date = 2024-12-31
days_total = 92 days

# Auto-calculated
daily_budget = $50,000 / 92 = $543.48/day

# Current Status (as of 2024-12-04)
days_elapsed = 65 days
days_remaining = 27 days
budget_spent = $35,426.20
budget_remaining = $14,573.80

# Percentages
budget_spent_pct = 70.85%
budget_remaining_pct = 29.15%

# Pacing
expected_spend = 65 × $543.48 = $35,326.20
actual_spend = $35,426.20
pacing = +0.28% (slightly over)
```

### Budget Alerts

Automatic alerts when:
- ✅ **80% budget spent** (default threshold)
- ✅ **90% budget spent** (high severity)
- ✅ **100% budget reached** (critical)
- ✅ **Overspending** detected
- ✅ **Underspending** detected (< 50% with < 25% time left)

---

## Report Formats

### 1. Daily Report Structure

```json
{
  "report_type": "daily",
  "date": "2024-12-04",
  "generated_at": "2024-12-04T09:00:00",
  "campaigns": [
    {
      "campaign_id": "CAMP001",
      "campaign_name": "Q4 Brand Campaign",
      "spend": 1500.50,
      "impressions": 45000,
      "clicks": 450,
      "conversions": 25,
      "revenue": 7500.00,
      "ctr": 1.00,
      "cpc": 3.33,
      "cpm": 33.33,
      "cpa": 60.00,
      "roas": 5.00,
      "conversion_rate": 5.56,
      "budget_total": 50000.00,
      "budget_spent": 35426.20,
      "budget_remaining": 14573.80,
      "budget_spent_pct": 70.85,
      "budget_remaining_pct": 29.15,
      "daily_budget": 543.48,
      "days_remaining": 27
    }
  ],
  "summary": {
    "total_campaigns": 5,
    "total_spend": 8250.75,
    "total_revenue": 41250.00,
    "total_impressions": 245000,
    "total_clicks": 2450,
    "total_conversions": 135,
    "avg_ctr": 1.00,
    "avg_cpc": 3.37,
    "avg_cpm": 33.68,
    "avg_cpa": 61.12,
    "overall_roas": 5.00,
    "conversion_rate": 5.51
  },
  "alerts": [
    {
      "type": "budget",
      "severity": "medium",
      "campaign_id": "CAMP002",
      "campaign_name": "Holiday Sales",
      "message": "Budget 85.5% spent ($64,125 of $75,000)",
      "value": 85.5
    }
  ]
}
```

### 2. Weekly Report Structure

Same as daily, plus:
```json
{
  "trends": {
    "spend_change_pct": +12.5,
    "conversions_change_pct": +8.3,
    "roas_change_pct": -2.1
  }
}
```

### 3. Excel Report

**Sheet 1: Summary**
- Report metadata
- Overall KPIs
- Total spend, revenue, ROAS
- Campaign count

**Sheet 2: Campaigns**
- All campaign metrics
- Formatted table
- Color-coded performance

**Sheet 3: Alerts**
- Budget alerts (red)
- Performance alerts (yellow)
- Action items

---

## Scheduling

### Setup Automated Schedule

```python
reporter = AutomatedReporter()
reporter.load_budgets('config/campaign_budgets.csv')

# Schedule reports
reporter.schedule_reports()

# Run scheduler (keeps running)
reporter.run_scheduler()
```

### Default Schedule

- **Daily Report**: Every day at 9:00 AM
- **Weekly Report**: Every Monday at 10:00 AM

### Custom Schedule

Edit `config/reporting_config.json`:
```json
{
  "schedule": {
    "daily_report_time": "08:30",
    "weekly_report_day": "Friday",
    "weekly_report_time": "17:00"
  }
}
```

---

## Alert System

### Budget Alerts

| Threshold | Severity | Action |
|-----------|----------|--------|
| 80% spent | Medium | Monitor closely |
| 90% spent | High | Review pacing |
| 95% spent | Critical | Pause or adjust |
| 100% spent | Critical | Campaign paused |

### Performance Alerts

| Metric | Threshold | Alert |
|--------|-----------|-------|
| ROAS | < 2.0x | Low return |
| CPA | > $50 | High cost |
| CTR | < 1.0% | Low engagement |
| Conv Rate | < 2.0% | Poor conversion |

### Alert Example

```json
{
  "type": "budget",
  "severity": "high",
  "campaign_id": "CAMP002",
  "campaign_name": "Holiday Sales",
  "message": "Budget 92.3% spent ($69,225 of $75,000)",
  "value": 92.3,
  "recommended_action": "Review pacing and consider budget adjustment"
}
```

---

## Integration Examples

### 1. Run as Cron Job (Linux/Mac)

```bash
# Edit crontab
crontab -e

# Add daily report at 9 AM
0 9 * * * cd /path/to/PCA_Agent && python -m src.reporting.automated_reporter

# Add weekly report on Monday at 10 AM
0 10 * * 1 cd /path/to/PCA_Agent && python -m src.reporting.automated_reporter weekly
```

### 2. Run as Windows Task

```powershell
# Create scheduled task
$action = New-ScheduledTaskAction -Execute "python" -Argument "src/reporting/automated_reporter.py"
$trigger = New-ScheduledTaskTrigger -Daily -At 9am
Register-ScheduledTask -Action $action -Trigger $trigger -TaskName "PCA Daily Report"
```

### 3. Docker Container

```dockerfile
FROM python:3.9
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "-m", "src.reporting.automated_reporter"]
```

### 4. Cloud Function (AWS Lambda)

```python
def lambda_handler(event, context):
    reporter = AutomatedReporter()
    reporter.load_budgets('config/campaign_budgets.csv')
    
    if event.get('report_type') == 'weekly':
        report = reporter.generate_weekly_report()
    else:
        report = reporter.generate_daily_report()
    
    return {
        'statusCode': 200,
        'body': json.dumps(report)
    }
```

---

## Advanced Features

### 1. Custom Data Sources

**API Integration:**
```python
class AutomatedReporter:
    def fetch_campaign_data(self, date):
        if self.config['data_source'] == 'api':
            response = requests.get(
                f"{API_URL}/campaigns",
                params={'date': date.strftime('%Y-%m-%d')},
                headers={'Authorization': f'Bearer {API_KEY}'}
            )
            return pd.DataFrame(response.json())
```

**Database Integration:**
```python
def fetch_campaign_data(self, date):
    if self.config['data_source'] == 'database':
        query = """
            SELECT * FROM campaigns
            WHERE date = %s
        """
        return pd.read_sql(query, conn, params=[date])
```

### 2. Email Notifications

```python
def send_email_report(self, report):
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    
    msg = MIMEMultipart()
    msg['Subject'] = f"Daily Campaign Report - {report['date']}"
    msg['From'] = 'reports@company.com'
    msg['To'] = ', '.join(self.config['notifications']['email_recipients'])
    
    # Create HTML email
    html = self._generate_email_html(report)
    msg.attach(MIMEText(html, 'html'))
    
    # Send
    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.send_message(msg)
```

### 3. Slack Notifications

```python
def send_slack_alert(self, alert):
    webhook_url = self.config['notifications']['slack_webhook']
    
    message = {
        "text": f"🚨 {alert['severity'].upper()} Alert",
        "attachments": [{
            "color": "danger" if alert['severity'] == 'high' else "warning",
            "fields": [
                {"title": "Campaign", "value": alert['campaign_name'], "short": True},
                {"title": "Type", "value": alert['type'], "short": True},
                {"title": "Message", "value": alert['message'], "short": False}
            ]
        }]
    }
    
    requests.post(webhook_url, json=message)
```

### 4. Historical Tracking

```python
def track_historical_performance(self):
    """Track performance over time for trend analysis."""
    df = pd.DataFrame([m.to_dict() for m in self.historical_data])
    
    # Calculate moving averages
    df['spend_7d_avg'] = df.groupby('campaign_id')['spend'].transform(
        lambda x: x.rolling(7, min_periods=1).mean()
    )
    
    # Detect anomalies
    df['spend_anomaly'] = (
        df['spend'] > df['spend_7d_avg'] * 1.5
    ) | (
        df['spend'] < df['spend_7d_avg'] * 0.5
    )
    
    return df
```

---

## Troubleshooting

### Issue: No data in reports

**Solution:**
1. Check data file exists: `data/campaign_data.csv`
2. Verify date format: `YYYY-MM-DD`
3. Check column names match expected format
4. Run with debug logging: `logger.setLevel('DEBUG')`

### Issue: Budget calculations incorrect

**Solution:**
1. Verify budget file format
2. Check date ranges are valid
3. Ensure spend data is cumulative or daily
4. Review budget configuration

### Issue: Scheduler not running

**Solution:**
1. Check schedule configuration
2. Verify time format (24-hour)
3. Ensure script has permissions
4. Check system timezone

---

## Best Practices

1. **Data Quality**
   - Validate data before processing
   - Handle missing values
   - Check for duplicates

2. **Budget Management**
   - Update budgets regularly
   - Set appropriate alert thresholds
   - Review pacing weekly

3. **Performance Monitoring**
   - Track trends over time
   - Compare week-over-week
   - Identify seasonal patterns

4. **Alert Management**
   - Don't ignore alerts
   - Adjust thresholds as needed
   - Document actions taken

5. **Report Distribution**
   - Share with stakeholders
   - Archive historical reports
   - Review insights regularly

---

## Next Steps

1. ✅ Set up configuration files
2. ✅ Prepare campaign data
3. ✅ Run first report manually
4. ✅ Review output and alerts
5. ✅ Schedule automated runs
6. ✅ Configure notifications
7. ✅ Monitor and optimize

---

**Questions?** Check the code documentation or create an issue on GitHub.

**Last Updated:** December 2024
