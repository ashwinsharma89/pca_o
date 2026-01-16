"""
Integrated PCA Agent Workflow
Combines screenshot analysis, CSV analysis, Q&A, and predictive analytics
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment
load_dotenv()

# Page config
st.set_page_config(
    page_title="PCA Agent - Integrated Analytics Platform",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.5rem;
    }
    .integration-badge {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 20px;
        font-size: 0.9rem;
        display: inline-block;
        margin: 0.5rem;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'workflow_stage' not in st.session_state:
    st.session_state.workflow_stage = 'input'
if 'analysis_data' not in st.session_state:
    st.session_state.analysis_data = None
if 'df' not in st.session_state:
    st.session_state.df = None
if 'qa_engine' not in st.session_state:
    st.session_state.qa_engine = None

# Sidebar
with st.sidebar:
    st.image("https://via.placeholder.com/200x80/667eea/ffffff?text=PCA+Agent", use_container_width=True)
    st.markdown("---")
    
    st.markdown("### 🚀 Integrated Workflow")
    
    # Workflow stages
    stages = {
        'input': '📤 Data Input',
        'analysis': '🔍 Analysis',
        'qa': '💬 Q&A',
        'predictive': '🔮 Predictive',
        'report': '📊 Report'
    }
    
    current_stage = st.session_state.workflow_stage
    for stage_key, stage_name in stages.items():
        if stage_key == current_stage:
            st.markdown(f"**→ {stage_name}** ✓")
        else:
            st.markdown(f"   {stage_name}")
    
    st.markdown("---")
    
    st.markdown("### 🎯 Capabilities")
    st.markdown("""
    <div class="integration-badge">Screenshot Analysis</div>
    <div class="integration-badge">CSV Analysis</div>
    <div class="integration-badge">Natural Language Q&A</div>
    <div class="integration-badge">Predictive Analytics</div>
    <div class="integration-badge">Auto Reports</div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    if st.button("🔄 Reset Workflow", use_container_width=True):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()

# Main header
st.markdown('<h1 class="main-header">🚀 PCA Agent - Integrated Analytics Platform</h1>', unsafe_allow_html=True)
st.markdown("**Complete Campaign Analysis Workflow: Data → Insights → Predictions → Actions**")
st.markdown("---")

# Main workflow tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📤 1. Data Input",
    "🔍 2. Analysis",
    "💬 3. Q&A & Insights",
    "🔮 4. Predictive Analytics",
    "📊 5. Reports & Actions"
])

with tab1:
    st.markdown("## 📤 Step 1: Data Input")
    st.markdown("Choose your data source to begin the analysis workflow")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Option A: Upload CSV Data")
        st.info("💡 **Best for:** Quick analysis, historical data, multi-campaign comparison")
        
        uploaded_file = st.file_uploader(
            "Upload campaign CSV",
            type=["csv"],
            help="CSV with campaign metrics (Campaign_Name, Platform, Date, Spend, Conversions, etc.)"
        )
        
        if uploaded_file:
            df = pd.read_csv(uploaded_file)
            st.session_state.df = df
            st.session_state.workflow_stage = 'analysis'
            
            st.success(f"✅ Loaded {len(df)} rows with {len(df.columns)} columns")
            
            with st.expander("📋 Data Preview"):
                st.dataframe(df.head(10), use_container_width=True)
                
                col_a, col_b, col_c = st.columns(3)
                with col_a:
                    st.metric("Rows", len(df))
                with col_b:
                    campaigns = df['Campaign_Name'].nunique() if 'Campaign_Name' in df.columns else 0
                    st.metric("Campaigns", campaigns)
                with col_c:
                    spend = df['Spend'].sum() if 'Spend' in df.columns else 0
                    st.metric("Total Spend", f"${spend:,.0f}")
    
    with col2:
        st.markdown("### Option B: Screenshot Analysis")
        st.info("💡 **Best for:** Dashboard exports, visual data, platform-specific reports")
        
        uploaded_screenshots = st.file_uploader(
            "Upload dashboard screenshots",
            type=["png", "jpg", "jpeg", "pdf"],
            accept_multiple_files=True,
            help="Screenshots from Google Ads, Meta, LinkedIn, etc."
        )
        
        if uploaded_screenshots:
            st.success(f"✅ {len(uploaded_screenshots)} screenshots uploaded")
            
            with st.expander("Preview Screenshots"):
                cols = st.columns(3)
                for i, file in enumerate(uploaded_screenshots):
                    with cols[i % 3]:
                        st.image(file, caption=file.name, use_container_width=True)
            
            st.warning("⚠️ Screenshot analysis requires Vision Agent. Use CSV for instant analysis.")

with tab2:
    st.markdown("## 🔍 Step 2: Automated Analysis")
    
    if st.session_state.df is not None:
        df = st.session_state.df
        
        if st.button("🚀 Run Comprehensive Analysis", type="primary", use_container_width=True):
            with st.spinner("🤖 AI analyzing your data... This may take 30-60 seconds..."):
                try:
                    from src.analytics import MediaAnalyticsExpert
                    
                    api_key = os.getenv('OPENAI_API_KEY') or os.getenv('ANTHROPIC_API_KEY')
                    if not api_key:
                        st.error("❌ API key not found. Set OPENAI_API_KEY or ANTHROPIC_API_KEY in .env")
                    else:
                        expert = MediaAnalyticsExpert()
                        analysis = expert.analyze_all(df)
                        
                        st.session_state.analysis_data = analysis
                        st.session_state.workflow_stage = 'qa'
                        st.success("✅ Analysis complete!")
                        st.rerun()
                
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")
                    import traceback
                    with st.expander("Error Details"):
                        st.code(traceback.format_exc())
                    
                    # Provide simple analysis as fallback
                    st.warning("⚠️ Full analysis failed. Generating basic metrics...")
                    
                    basic_analysis = {
                        'executive_summary': 'Basic analysis completed. Use Q&A tab for detailed insights.',
                        'metrics': {
                            'overview': {
                                'total_spend': df['Total_Spent'].sum() if 'Total_Spent' in df.columns else 0,
                                'total_conversions': df['Site_Visit'].sum() if 'Site_Visit' in df.columns else 0,
                                'avg_roas': 0,
                                'avg_cpa': 0
                            }
                        },
                        'recommendations': []
                    }
                    
                    st.session_state.analysis_data = basic_analysis
                    st.session_state.workflow_stage = 'qa'
                    st.info("✅ Basic analysis complete. Use Q&A tab for detailed insights.")
                    st.rerun()
        
        # Show analysis results if available
        if st.session_state.analysis_data:
            analysis = st.session_state.analysis_data
            
            st.markdown("### 📊 Analysis Results")
            
            # Executive Summary
            st.info(analysis.get('executive_summary', 'Analysis complete'))
            
            # Key Metrics
            col1, col2, col3, col4 = st.columns(4)
            metrics = analysis.get('metrics', {}).get('overview', {})
            
            with col1:
                st.metric("Total Spend", f"${metrics.get('total_spend', 0):,.0f}")
            with col2:
                st.metric("Conversions", f"{metrics.get('total_conversions', 0):,.0f}")
            with col3:
                st.metric("Avg ROAS", f"{metrics.get('avg_roas', 0):.2f}x")
            with col4:
                st.metric("Avg CPA", f"${metrics.get('avg_cpa', 0):.2f}")
    
    else:
        st.info("👆 Upload data in Step 1 first")

with tab3:
    st.markdown("## 💬 Step 3: Q&A & Deep Insights")
    
    if st.session_state.df is not None:
        df = st.session_state.df
        
        # Initialize Q&A engine
        if st.session_state.qa_engine is None:
            api_key = os.getenv('OPENAI_API_KEY')
            if api_key:
                from src.query_engine import NaturalLanguageQueryEngine
                engine = NaturalLanguageQueryEngine(api_key)
                engine.load_data(df)
                st.session_state.qa_engine = engine
                st.success("✅ Q&A Engine ready!")
        
        engine = st.session_state.qa_engine
        
        if engine:
            # Quick Insights (Auto-generated)
            with st.expander("⚡ Quick Insights (Auto-Generated)", expanded=True):
                st.markdown("**Automatically generated insights from your data:**")
                
                quick_questions = [
                    "Identify top 20% of campaigns driving 80% of results",
                    "Which channels have the best ROAS?",
                    "Calculate performance volatility for each campaign",
                ]
                
                for q in quick_questions:
                    with st.spinner(f"Analyzing: {q}..."):
                        result = engine.ask(q)
                        if result['success']:
                            st.markdown(f"**Q:** {q}")
                            st.write(result['answer'])
                            with st.expander("📊 Data"):
                                st.dataframe(result['results'], use_container_width=True)
                            st.markdown("---")
            
            # Custom Q&A
            st.markdown("### 🎯 Ask Your Own Questions")
            
            # Categorized suggestions
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**📈 Strategic Insights:**")
                insight_questions = [
                    "What's the underlying story behind our performance in the last 2 months?",
                    "Why did our CPA change? Conduct a root cause analysis.",
                    "What hidden patterns exist in our top-performing campaigns?",
                ]
                for q in insight_questions:
                    if st.button(q, key=f"insight_{hash(q)}", use_container_width=True):
                        st.session_state.current_question = q
            
            with col2:
                st.markdown("**💡 Actionable Recommendations:**")
                rec_questions = [
                    "How should we reallocate budget to maximize conversions?",
                    "Create a 30-day optimization roadmap",
                    "If we increased budget by 25%, which channels should receive it?",
                ]
                for q in rec_questions:
                    if st.button(q, key=f"rec_{hash(q)}", use_container_width=True):
                        st.session_state.current_question = q
            
            # Question input
            question = st.text_area(
                "Your question:",
                value=st.session_state.get('current_question', ''),
                placeholder="e.g., What are the top 3 optimization opportunities?",
                height=100
            )
            
            if st.button("🔍 Get Answer", type="primary"):
                if question:
                    with st.spinner("🤔 Analyzing..."):
                        result = engine.ask(question)
                        
                        if result['success']:
                            st.success("✅ Analysis Complete")
                            
                            # Answer
                            st.markdown("### 📝 Answer")
                            st.markdown(result['answer'])
                            
                            # SQL Query
                            with st.expander("🔧 Generated SQL Query"):
                                st.code(result['sql_query'], language="sql")
                            
                            # Results
                            with st.expander("📊 Detailed Results"):
                                st.dataframe(result['results'], use_container_width=True)
                                
                                # Auto-generate chart if applicable
                                if len(result['results']) > 0 and len(result['results']) < 50:
                                    try:
                                        if 'Date' in result['results'].columns:
                                            fig = px.line(result['results'], x='Date', y=result['results'].columns[1])
                                            st.plotly_chart(fig, use_container_width=True)
                                        elif len(result['results'].columns) >= 2:
                                            fig = px.bar(result['results'], x=result['results'].columns[0], y=result['results'].columns[1])
                                            st.plotly_chart(fig, use_container_width=True)
                                    except:
                                        pass
                        else:
                            st.error(f"❌ Error: {result['error']}")
    else:
        st.info("👆 Upload data in Step 1 first")

with tab4:
    st.markdown("## 🔮 Step 4: Predictive Analytics")
    
    if st.session_state.df is not None:
        df = st.session_state.df
        
        st.markdown("### 🎯 Predictive Capabilities")
        
        pred_col1, pred_col2 = st.columns(2)
        
        with pred_col1:
            st.markdown("#### 📈 Performance Forecasting")
            st.info("Predict next month's performance based on historical trends")
            
            if st.button("🔮 Generate Forecast", use_container_width=True):
                st.info("Predictive analytics integration coming soon...")
                st.markdown("""
                **Will include:**
                - Next month CPA/ROAS forecast
                - Confidence intervals
                - Trend analysis
                - Early warning indicators
                """)
        
        with pred_col2:
            st.markdown("#### 💰 Budget Optimization")
            st.info("Optimal budget allocation across channels")
            
            if st.button("⚡ Optimize Budget", use_container_width=True):
                st.info("Budget optimizer integration coming soon...")
                st.markdown("""
                **Will include:**
                - Optimal channel mix
                - Expected ROAS by allocation
                - Saturation curve analysis
                - Diminishing returns detection
                """)
    else:
        st.info("👆 Upload data in Step 1 first")

with tab5:
    st.markdown("## 📊 Step 5: Reports & Actions")
    
    if st.session_state.analysis_data:
        st.markdown("### 📥 Export Options")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("📄 Export to PowerPoint", use_container_width=True):
                st.info("PowerPoint export integration coming soon...")
        
        with col2:
            if st.button("📧 Email Report", use_container_width=True):
                st.info("Email integration coming soon...")
        
        with col3:
            if st.button("📅 Schedule Weekly Report", use_container_width=True):
                st.info("Scheduling integration coming soon...")
        
        st.markdown("---")
        
        st.markdown("### 🎯 Recommended Actions")
        
        if st.session_state.analysis_data:
            recommendations = st.session_state.analysis_data.get('recommendations', [])
            
            for i, rec in enumerate(recommendations[:5], 1):
                with st.expander(f"#{i}: {rec.get('recommendation', 'Recommendation')[:60]}...", expanded=i<=2):
                    st.markdown(f"**{rec.get('recommendation', '')}**")
                    st.markdown(f"**Expected Impact:** {rec.get('expected_impact', '')}")
                    st.markdown(f"**Implementation:** {rec.get('implementation', '')}")
                    
                    col_a, col_b, col_c = st.columns(3)
                    with col_a:
                        st.metric("Priority", rec.get('priority', 'Medium'))
                    with col_b:
                        st.metric("Timeline", rec.get('timeline', 'TBD'))
                    with col_c:
                        st.metric("Est. ROI", rec.get('estimated_roi', 'TBD'))
    else:
        st.info("👆 Complete analysis in Steps 1-2 first")

# Footer
st.markdown("---")
st.markdown(
    '<div style="text-align: center; color: #666;">PCA Agent - Integrated Analytics Platform | Built with Streamlit, OpenAI & Anthropic</div>',
    unsafe_allow_html=True
)
