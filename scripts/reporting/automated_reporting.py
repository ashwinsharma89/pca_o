"""
Automated Reporting and Alert System
Scheduled analysis, email reports, and performance alerts
"""
import schedule
import time
import pandas as pd
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from loguru import logger
import sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

# Load environment
load_dotenv()

# Configure logger
logger.remove()
logger.add(sys.stdout, format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>")
logger.add("logs/automated_reports_{time:YYYY-MM-DD}.log", rotation="1 day", retention="30 days")


class AutomatedReportingSystem:
    """Automated reporting and alert system."""
    
    def __init__(self):
        """Initialize reporting system."""
        self.data_source = os.getenv('DATA_SOURCE_PATH', 'data/campaign_data.csv')
        self.email_recipients = os.getenv('REPORT_RECIPIENTS', '').split(',')
        self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
        self.smtp_user = os.getenv('SMTP_USER', '')
        self.smtp_password = os.getenv('SMTP_PASSWORD', '')
        
        logger.info("Initialized AutomatedReportingSystem")
    
    def load_latest_data(self) -> pd.DataFrame:
        """Load latest campaign data."""
        logger.info(f"Loading data from: {self.data_source}")
        
        try:
            df = pd.read_csv(self.data_source)
            logger.success(f"Loaded {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return pd.DataFrame()
    
    def generate_weekly_insights(self, df: pd.DataFrame) -> dict:
        """Generate weekly insights."""
        logger.info("Generating weekly insights...")
        
        from src.query_engine import NaturalLanguageQueryEngine
        
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            logger.error("OpenAI API key not found")
            return {}
        
        engine = NaturalLanguageQueryEngine(api_key)
        engine.load_data(df)
        
        # Key questions for weekly report
        questions = [
            "Identify performance anomalies in the last week",
            "Which campaigns show declining trends over the last 2 weeks?",
            "What are the top 3 optimization opportunities based on last week's data?",
            "Compare last week's performance vs the previous week",
            "Identify top 20% of campaigns driving 80% of results"
        ]
        
        insights = {}
        for q in questions:
            logger.info(f"Asking: {q}")
            result = engine.ask(q)
            
            if result['success']:
                insights[q] = {
                    'answer': result['answer'],
                    'data': result['results'].to_dict('records') if len(result['results']) < 20 else []
                }
                logger.success(f"✓ Got answer")
            else:
                logger.warning(f"✗ Failed: {result['error']}")
                insights[q] = {'answer': f"Error: {result['error']}", 'data': []}
        
        return insights
    
    def check_for_alerts(self, df: pd.DataFrame) -> list:
        """Check for performance alerts."""
        logger.info("Checking for performance alerts...")
        
        alerts = []
        
        # Ensure Date column
        df = df.copy()
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            
            # Get last 7 days and previous 7 days
            last_7_days = df[df['Date'] >= (datetime.now() - timedelta(days=7))]
            prev_7_days = df[(df['Date'] >= (datetime.now() - timedelta(days=14))) & 
                            (df['Date'] < (datetime.now() - timedelta(days=7)))]
            
            if len(last_7_days) > 0 and len(prev_7_days) > 0:
                # Calculate metrics
                last_cpa = (last_7_days['Spend'].sum() / last_7_days['Conversions'].sum()) if 'Conversions' in last_7_days.columns else 0
                prev_cpa = (prev_7_days['Spend'].sum() / prev_7_days['Conversions'].sum()) if 'Conversions' in prev_7_days.columns else 0
                
                # CPA Alert (>20% increase)
                if last_cpa > prev_cpa * 1.2:
                    alerts.append({
                        'type': 'CPA Spike',
                        'severity': 'High',
                        'message': f"CPA increased {((last_cpa - prev_cpa) / prev_cpa * 100):.1f}% in last 7 days (${prev_cpa:.2f} → ${last_cpa:.2f})",
                        'action': 'Review targeting, bids, and landing pages immediately'
                    })
                
                # Conversions Alert (>15% decline)
                last_conv = last_7_days['Conversions'].sum() if 'Conversions' in last_7_days.columns else 0
                prev_conv = prev_7_days['Conversions'].sum() if 'Conversions' in prev_7_days.columns else 0
                
                if last_conv < prev_conv * 0.85:
                    alerts.append({
                        'type': 'Conversion Decline',
                        'severity': 'High',
                        'message': f"Conversions dropped {((prev_conv - last_conv) / prev_conv * 100):.1f}% in last 7 days ({prev_conv:.0f} → {last_conv:.0f})",
                        'action': 'Investigate traffic quality, creative fatigue, or technical issues'
                    })
        
        logger.info(f"Found {len(alerts)} alerts")
        return alerts
    
    def format_email_report(self, insights: dict, alerts: list) -> str:
        """Format email report HTML."""
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; text-align: center; }}
                .section {{ margin: 20px 0; padding: 15px; border-left: 4px solid #667eea; background: #f8f9fa; }}
                .alert {{ background: #fff3cd; border-left: 4px solid #ffc107; padding: 15px; margin: 10px 0; }}
                .alert-high {{ background: #f8d7da; border-left: 4px solid #dc3545; }}
                .insight {{ margin: 15px 0; }}
                .footer {{ text-align: center; color: #666; margin-top: 30px; padding: 20px; border-top: 1px solid #ddd; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>📊 PCA Agent - Weekly Performance Report</h1>
                <p>Generated on {datetime.now().strftime('%B %d, %Y at %I:%M %p')}</p>
            </div>
        """
        
        # Alerts section
        if alerts:
            html += """
            <div class="section">
                <h2>⚠️ Performance Alerts</h2>
            """
            for alert in alerts:
                severity_class = 'alert-high' if alert['severity'] == 'High' else 'alert'
                html += f"""
                <div class="alert {severity_class}">
                    <strong>[{alert['severity']}] {alert['type']}</strong><br>
                    {alert['message']}<br>
                    <em>→ {alert['action']}</em>
                </div>
                """
            html += "</div>"
        
        # Insights section
        html += """
        <div class="section">
            <h2>💡 Weekly Insights</h2>
        """
        
        for question, data in insights.items():
            html += f"""
            <div class="insight">
                <h3>{question}</h3>
                <p>{data['answer']}</p>
            </div>
            """
        
        html += "</div>"
        
        # Footer
        html += f"""
            <div class="footer">
                <p>PCA Agent - Automated Campaign Analytics</p>
                <p>This is an automated report. For questions, contact your analytics team.</p>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def send_email_report(self, subject: str, html_content: str):
        """Send email report."""
        logger.info(f"Sending email report to {len(self.email_recipients)} recipients...")
        
        if not self.smtp_user or not self.smtp_password:
            logger.warning("SMTP credentials not configured. Skipping email send.")
            logger.info("To enable email reports, set SMTP_USER and SMTP_PASSWORD in .env")
            return
        
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.smtp_user
            msg['To'] = ', '.join(self.email_recipients)
            
            # Attach HTML content
            html_part = MIMEText(html_content, 'html')
            msg.attach(html_part)
            
            # Send email
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.smtp_user, self.smtp_password)
            server.send_message(msg)
            server.quit()
            
            logger.success("✓ Email sent successfully")
        
        except Exception as e:
            logger.error(f"Error sending email: {e}")
    
    def run_weekly_report(self):
        """Run weekly report generation and distribution."""
        logger.info("="*80)
        logger.info("RUNNING WEEKLY REPORT")
        logger.info("="*80)
        
        # Load data
        df = self.load_latest_data()
        if df.empty:
            logger.error("No data available for report")
            return
        
        # Generate insights
        insights = self.generate_weekly_insights(df)
        
        # Check for alerts
        alerts = self.check_for_alerts(df)
        
        # Format email
        subject = f"PCA Agent Weekly Report - {datetime.now().strftime('%B %d, %Y')}"
        html_content = self.format_email_report(insights, alerts)
        
        # Send email
        self.send_email_report(subject, html_content)
        
        # Save report locally
        report_filename = f"reports/weekly_report_{datetime.now().strftime('%Y%m%d')}.html"
        os.makedirs('reports', exist_ok=True)
        with open(report_filename, 'w') as f:
            f.write(html_content)
        logger.info(f"Report saved to: {report_filename}")
        
        logger.success("Weekly report complete!")
    
    def run_daily_alerts(self):
        """Run daily performance alerts check."""
        logger.info("Running daily alerts check...")
        
        df = self.load_latest_data()
        if df.empty:
            return
        
        alerts = self.check_for_alerts(df)
        
        if alerts:
            logger.warning(f"Found {len(alerts)} alerts!")
            
            # Send alert email
            subject = f"⚠️ PCA Agent Performance Alert - {datetime.now().strftime('%B %d, %Y')}"
            
            html = f"""
            <html>
            <body style="font-family: Arial, sans-serif;">
                <h2 style="color: #dc3545;">⚠️ Performance Alerts Detected</h2>
                <p>The following performance issues were detected:</p>
            """
            
            for alert in alerts:
                html += f"""
                <div style="background: #f8d7da; border-left: 4px solid #dc3545; padding: 15px; margin: 10px 0;">
                    <strong>[{alert['severity']}] {alert['type']}</strong><br>
                    {alert['message']}<br>
                    <em>→ {alert['action']}</em>
                </div>
                """
            
            html += """
                <p>Please review and take action as needed.</p>
            </body>
            </html>
            """
            
            self.send_email_report(subject, html)
        else:
            logger.success("No alerts detected - all systems normal")


def setup_schedule():
    """Setup scheduled tasks."""
    system = AutomatedReportingSystem()
    
    # Weekly report every Monday at 9 AM
    schedule.every().monday.at("09:00").do(system.run_weekly_report)
    
    # Daily alerts check every day at 8 AM
    schedule.every().day.at("08:00").do(system.run_daily_alerts)
    
    logger.info("Scheduled tasks configured:")
    logger.info("  - Weekly report: Every Monday at 9:00 AM")
    logger.info("  - Daily alerts: Every day at 8:00 AM")
    
    return system


def run_scheduler():
    """Run the scheduler loop."""
    logger.info("Starting automated reporting scheduler...")
    
    system = setup_schedule()
    
    logger.info("Scheduler running. Press Ctrl+C to stop.")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='PCA Agent Automated Reporting')
    parser.add_argument('--mode', choices=['schedule', 'weekly', 'daily'], default='schedule',
                       help='Run mode: schedule (continuous), weekly (one-time), daily (one-time)')
    
    args = parser.parse_args()
    
    system = AutomatedReportingSystem()
    
    if args.mode == 'weekly':
        logger.info("Running one-time weekly report...")
        system.run_weekly_report()
    elif args.mode == 'daily':
        logger.info("Running one-time daily alerts check...")
        system.run_daily_alerts()
    else:
        run_scheduler()
