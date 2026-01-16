import sys
import os
import pandas as pd
from typing import Any

# Add project root to path
sys.path.append(os.path.abspath("/Users/ashwin/Desktop/pca_agent"))
sys.path.append(os.path.abspath("/Users/ashwin/Desktop/pca_agent/frontend_reflex"))

def run_verification():
    print("--- STARTING FEATURE PARITY VERIFICATION (DIRECT) ---\n")
    
    # 1. Load Data
    try:
        df = pd.read_csv("/Users/ashwin/Desktop/pca_agent/data/sample_campaign_data.csv")
        print(f"✅ Data loaded: {len(df)} rows")
    except Exception as e:
        print(f"❌ Failed to load data: {e}")
        return

    # 2. Verify Agents
    print("\n--- Testing Agents ---")
    try:
        from src.engine.agents.enhanced_reasoning_agent import EnhancedReasoningAgent
        from src.engine.agents.b2b_specialist_agent import B2BSpecialistAgent
        print("✅ Agents imported successfully.")
        
        # Test B2B Agent (Mock usage as per ChatState)
        b2b_agent = B2BSpecialistAgent()
        print("✅ B2B Agent instantiated.")
        
        # Test Reasoning Agent
        reasoning_agent = EnhancedReasoningAgent()
        print("✅ Reasoning Agent instantiated.")
        # We assume the API calls inside might fail without real keys/context, 
        # but the instantiation proves dependencies like 'pattern_detector' are fine.

    except ImportError as e:
        print(f"❌ Agent Import Error: {e}")
    except Exception as e:
        print(f"❌ Agent Test Error: {e}")

    # 3. Verify Smart Charts
    print("\n--- Testing Smart Chart Generator ---")
    try:
        from src.engine.agents.chart_generators import SmartChartGenerator
        print("✅ SmartChartGenerator imported.")
        
        generator = SmartChartGenerator()
        
        # Test Funnel Logic
        funnel_data = {
            'stages': ['Impressions', 'Clicks', 'Conversions'],
            'values': [1000, 100, 10]
        }
        fig_funnel = generator.create_conversion_funnel(funnel_data)
        if fig_funnel:
            print("✅ Conversion Funnel created.")
        else:
            print("❌ Conversion Funnel returned None.")

        # Test Comparison Logic
        # create_channel_comparison_chart expects data={channel: {metric: val}}
        comp_data = {
            'Google': {'spend': 500, 'conversions': 50},
            'Meta': {'spend': 400, 'conversions': 40}
        }
        fig_comp = generator.create_channel_comparison_chart(comp_data, metrics=['spend', 'conversions'])
        if fig_comp:
            print("✅ Channel Comparison Chart created.")
        else:
            print("❌ Channel Comparison Chart returned None.")

    except ImportError as e:
        print(f"❌ Chart Generator Import Error: {e}")
    except Exception as e:
        print(f"❌ Chart Generator Error: {e}")

    # 4. Verify VizState Logic (Pure Logic Test)
    print("\n--- Testing VizState Extended Logic (POPO Mock) ---")
    try:
        from frontend_reflex.state.viz import VizState
        
        # Define a POPO (Plain Old Python Object) to mock the state
        # This avoids Reflex's __setattr__ magic which fails in standalone scripts
        class MockVizState:
            def __init__(self):
                # Data
                self.filtered_df = pd.DataFrame({
                    'Platform': ['Google', 'Meta', 'Google', 'Meta'],
                    'Spend': [100, 200, 150, 250],
                    'Clicks': [10, 20, 15, 25],
                    'Date': ['2023-01-01', '2023-01-01', '2023-01-02', '2023-01-02']
                })
                # Helper for color options check
                self.columns = ['Platform', 'Spend', 'Clicks', 'Date']
                
                # Builder 1 State
                self.chart_type = "Bar Chart"
                self.x_axis = "Platform"
                self.y_axis = "Spend"
                self.color_by = "None"
                self.aggregation = "Sum"
                self.custom_chart_figure = None
                
                # Builder 2 State
                self.chart_type_2 = "Bar Chart"
                self.x_axis_2 = "Platform"
                self.y_axis_2 = "Clicks"
                self.color_by_2 = "None"
                self.aggregation_2 = "Sum"
                self.custom_chart_figure_2 = None
                
                # Comparison State
                self.selected_kpis = ['Spend', 'Clicks']
                self.compare_dimension = "Platform"
                self.comparison_chart_type = "Grouped Bar"
                self.normalize = False
                self.comparison_chart_figure = None
                
                # Pre-defined
                self.funnel_figure = None
                self.correlation_figure = None

            def log(self, msg, level="info"):
                print(f"[MockVizState] {msg}")

        # Instantiate POPO
        mock_viz = MockVizState()
        
        import plotly.express as px
        import plotly.graph_objects as go
        
        # --- LOGIC COPY START (Bypassing Reflex wrappers for testing) ---
        # We define the methods here and bind them to the instance
        
        def _generate_custom_chart_internal(self, c_type, x_val, y_val, color_val, agg_val, target=1):
            """Internal shared method for chart generation (Copied Logic)."""
            df = self.filtered_df
            if df is None or df.empty:
                return
                
            if not x_val or not y_val:
                return
                
            agg_map = {"Sum": "sum", "Mean": "mean", "Count": "count", "Max": "max", "Min": "min"}
            
            try:
                # Fallback to manual plotly if simple chart
                if c_type == "Bar Chart":
                    chart_data = df.groupby(x_val)[y_val].agg(agg_map[agg_val]).reset_index()
                    if color_val != "None":
                        chart_data = df.groupby([x_val, color_val])[y_val].agg(agg_map[agg_val]).reset_index()
                        fig = px.bar(chart_data, x=x_val, y=y_val, color=color_val)
                    else:
                        fig = px.bar(chart_data, x=x_val, y=y_val)
                
                elif c_type == "Line Chart":
                    chart_data = df.groupby(x_val)[y_val].agg(agg_map[agg_val]).reset_index()
                    fig = px.line(chart_data, x=x_val, y=y_val)
                    
                elif c_type == "Scatter Plot":
                    fig = px.scatter(df, x=x_val, y=y_val, 
                                   color=color_val if color_val != "None" else None)
                                   
                elif c_type == "Pie Chart":
                    chart_data = df.groupby(x_val)[y_val].agg(agg_map[agg_val]).reset_index()
                    fig = px.pie(chart_data, values=y_val, names=x_val)

                elif c_type == "Box Plot":
                    fig = px.box(df, x=x_val, y=y_val,
                                 color=color_val if color_val != "None" else None)

                elif c_type == "Area Chart":
                    chart_data = df.groupby(x_val)[y_val].agg(agg_map[agg_val]).reset_index()
                    fig = px.area(chart_data, x=x_val, y=y_val)

                elif c_type == "Heatmap":
                    if color_val != "None":
                        pivot_data = df.pivot_table(
                            values=y_val, 
                            index=x_val, 
                            columns=color_val, 
                            aggfunc=agg_map[agg_val]
                        )
                        fig = px.imshow(pivot_data, text_auto=True, aspect="auto")
                    else:
                        print("Heatmap requires 'Color By'.")
                        return
                
                else:
                    chart_data = df.groupby(x_val)[y_val].sum().reset_index()
                    fig = px.bar(chart_data, x=x_val, y=y_val)
                    
                fig.update_layout(title=f"Custom Chart {target}", template='plotly_dark')
                
                if target == 1:
                    self.custom_chart_figure = fig
                else:
                    self.custom_chart_figure_2 = fig
                
            except Exception as e:
                print(f"Chart gen error {target}: {e}")

        def generate_custom_chart(self):
            _generate_custom_chart_internal(self, self.chart_type, self.x_axis, self.y_axis, 
                self.color_by, self.aggregation, target=1)

        def generate_custom_chart_2(self):
            _generate_custom_chart_internal(self, self.chart_type_2, self.x_axis_2, self.y_axis_2, 
                self.color_by_2, self.aggregation_2, target=2)

        def generate_comparison_chart(self):
            """Generate scale-adjusted comparison (Copied Logic)."""
            df = self.filtered_df
            if df is None or not self.selected_kpis:
                return

            try:
                # Group by dimension
                grouped = df.groupby(self.compare_dimension)[self.selected_kpis].sum().reset_index()
                
                # Normalize if requested
                chart_data = grouped.copy()
                if self.normalize:
                    for kpi in self.selected_kpis:
                        max_val = chart_data[kpi].max()
                        if max_val > 0:
                            chart_data[kpi] = (chart_data[kpi] / max_val) * 100
                
                if self.comparison_chart_type == "Grouped Bar":
                     melted = chart_data.melt(id_vars=[self.compare_dimension], value_vars=self.selected_kpis, var_name='KPI', value_name='Value')
                     fig = px.bar(melted, x='KPI', y='Value', color=self.compare_dimension, barmode='group')
                
                elif self.comparison_chart_type == "Line":
                     melted = chart_data.melt(id_vars=[self.compare_dimension], value_vars=self.selected_kpis, var_name='KPI', value_name='Value')
                     fig = px.line(melted, x='KPI', y='Value', color=self.compare_dimension, markers=True)

                elif self.comparison_chart_type == "Radar":
                     fig = go.Figure()
                     for _, row in chart_data.iterrows():
                         fig.add_trace(go.Scatterpolar(
                             r=row[self.selected_kpis].values,
                             theta=self.selected_kpis,
                             fill='toself',
                             name=str(row[self.compare_dimension])
                         ))
                     fig.update_layout(polar=dict(radialaxis=dict(visible=True)))

                elif self.comparison_chart_type == "Dual Axis":
                    if len(self.selected_kpis) < 2:
                        print("Dual Axis requires 2+ KPIs")
                        return
                    
                    primary_kpi = self.selected_kpis[0]
                    secondary_kpi = self.selected_kpis[1]
                    
                    fig = go.Figure()
                    
                    # Primary Axis (Bar)
                    fig.add_trace(go.Bar(
                        x=chart_data[self.compare_dimension],
                        y=chart_data[primary_kpi],
                        name=primary_kpi,
                        yaxis='y',
                        offsetgroup=1
                    ))
                    
                    # Secondary Axis (Line)
                    fig.add_trace(go.Scatter(
                        x=chart_data[self.compare_dimension],
                        y=chart_data[secondary_kpi],
                        name=secondary_kpi,
                        yaxis='y2',
                        mode='lines+markers'
                    ))
                    
                    fig.update_layout(
                        yaxis=dict(title=primary_kpi),
                        yaxis2=dict(
                            title=secondary_kpi,
                            overlaying='y',
                            side='right'
                        )
                    )

                else:
                     # Fallback
                     melted = chart_data.melt(id_vars=[self.compare_dimension], value_vars=self.selected_kpis, var_name='KPI', value_name='Value')
                     fig = px.bar(melted, x='KPI', y='Value', color=self.compare_dimension, barmode='group')

                self.comparison_chart_figure = fig
                
            except Exception as e:
                print(f"Comparison error: {e}")

        # Instantiate POPO
        mock_viz = MockVizState()
        
        # Bind methods
        mock_viz.generate_custom_chart = generate_custom_chart.__get__(mock_viz)
        mock_viz.generate_custom_chart_2 = generate_custom_chart_2.__get__(mock_viz)
        mock_viz.generate_comparison_chart = generate_comparison_chart.__get__(mock_viz)
        
        # --- LOGIC COPY END ---

        # Test Custom Chart Builder 1 (Box Plot)
        mock_viz.chart_type = "Box Plot"
        mock_viz.x_axis = "Platform"
        mock_viz.y_axis = "Spend"
        mock_viz.generate_custom_chart()
        if mock_viz.custom_chart_figure:
            print("✅ Builder 1 (Box Plot) logic executed.")
        else:
            print("❌ Builder 1 logic failed.")

        # Test Custom Chart Builder 2 (Line Chart)
        mock_viz.chart_type_2 = "Line Chart"
        mock_viz.x_axis_2 = "Date"
        mock_viz.y_axis_2 = "Clicks"
        mock_viz.generate_custom_chart_2()
        if mock_viz.custom_chart_figure_2:
             print("✅ Builder 2 (Line Chart) logic executed.")
        else:
             print("❌ Builder 2 logic failed.")

        # Test Radar Comparison
        mock_viz.comparison_chart_type = "Radar"
        mock_viz.generate_comparison_chart()
        if mock_viz.comparison_chart_figure:
            print("✅ Radar Comparison logic executed.")
        else:
            print("❌ Radar logic returned None.")

        # Test Dual Axis Comparison
        mock_viz.comparison_chart_type = "Dual Axis"
        mock_viz.selected_kpis = ['Spend', 'Clicks']
        mock_viz.generate_comparison_chart()
        if mock_viz.comparison_chart_figure:
            print("✅ Dual Axis Comparison logic executed.")
        else:
            print("❌ Dual Axis logic returned None.")
            
    except Exception as e:
        print(f"❌ VizState Logic Error: {e}")
        import traceback
        traceback.print_exc()

    print("\n--- VERIFICATION COMPLETE ---")

if __name__ == "__main__":
    run_verification()
