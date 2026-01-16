#!/usr/bin/env python3
"""
Create comprehensive system architecture diagrams
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, Rectangle
from pathlib import Path

OUTPUT_DIR = Path("guide/diagrams")
OUTPUT_DIR.mkdir(exist_ok=True)

PRIMARY_COLOR = '#2563eb'
SUCCESS_COLOR = '#10b981'
WARNING_COLOR = '#f59e0b'
ERROR_COLOR = '#ef4444'

def create_overall_system_architecture():
    """Create comprehensive system architecture diagram"""
    fig, ax = plt.subplots(figsize=(16, 12))
    ax.set_xlim(0, 16)
    ax.set_ylim(0, 14)
    ax.axis('off')
    
    # Title
    ax.text(8, 13.5, 'PCA Agent: Complete System Architecture', 
            ha='center', fontsize=22, fontweight='bold', color='#111827')
    
    # User/Frontend Layer
    frontend_box = FancyBboxPatch((1, 11.5), 14, 1.5, boxstyle="round,pad=0.1",
                                 edgecolor='#6366f1', facecolor='#e0e7ff', linewidth=3)
    ax.add_patch(frontend_box)
    ax.text(8, 12.5, 'FRONTEND LAYER', ha='center', fontsize=14, fontweight='bold', color='#4338ca')
    ax.text(3, 12.1, '🌐 React UI', ha='center', fontsize=10)
    ax.text(6, 12.1, '📊 Dashboard', ha='center', fontsize=10)
    ax.text(9, 12.1, '💬 Chat Interface', ha='center', fontsize=10)
    ax.text(12, 12.1, '📈 Analytics', ha='center', fontsize=10)
    
    # API Gateway (Layer 1)
    l1_box = FancyBboxPatch((1, 10), 14, 1, boxstyle="round,pad=0.1",
                           edgecolor='#ff9999', facecolor='#ffe5e5', linewidth=2)
    ax.add_patch(l1_box)
    ax.text(8, 10.5, 'LAYER 1: API Gateway & Security', ha='center', fontsize=12, fontweight='bold')
    ax.text(4, 10.2, '🔐 Auth', ha='center', fontsize=9)
    ax.text(6.5, 10.2, '⏱️ Rate Limit', ha='center', fontsize=9)
    ax.text(9, 10.2, '🛡️ CSRF', ha='center', fontsize=9)
    ax.text(11.5, 10.2, '🔀 Router', ha='center', fontsize=9)
    
    # Query Understanding (Layer 2)
    l2_box = FancyBboxPatch((1, 8.5), 14, 1, boxstyle="round,pad=0.1",
                           edgecolor='#ffcc99', facecolor='#fff4e6', linewidth=2)
    ax.add_patch(l2_box)
    ax.text(8, 9, 'LAYER 2: Query Understanding', ha='center', fontsize=12, fontweight='bold')
    ax.text(5, 8.7, '⚡ Bulletproof (80%)', ha='center', fontsize=9)
    ax.text(11, 8.7, '🤖 LLM Path (20%)', ha='center', fontsize=9)
    
    # Data Retrieval (Layer 3)
    l3_box = FancyBboxPatch((1, 7), 14, 1, boxstyle="round,pad=0.1",
                           edgecolor='#ffff99', facecolor='#fffbeb', linewidth=2)
    ax.add_patch(l3_box)
    ax.text(8, 7.5, 'LAYER 3: Data Retrieval & SQL', ha='center', fontsize=12, fontweight='bold')
    ax.text(5, 7.2, '🗄️ DuckDB', ha='center', fontsize=9)
    ax.text(8, 7.2, '📊 Parquet Files', ha='center', fontsize=9)
    ax.text(11, 7.2, '⚡ Query Cache', ha='center', fontsize=9)
    
    # Analysis (Layer 4)
    l4_box = FancyBboxPatch((1, 5.5), 14, 1, boxstyle="round,pad=0.1",
                           edgecolor='#99ff99', facecolor='#ecfdf5', linewidth=2)
    ax.add_patch(l4_box)
    ax.text(8, 6, 'LAYER 4: AI Analysis & Intelligence', ha='center', fontsize=12, fontweight='bold')
    ax.text(4, 5.7, '🧠 Reasoning', ha='center', fontsize=9)
    ax.text(7, 5.7, '💼 B2B Expert', ha='center', fontsize=9)
    ax.text(10, 5.7, '📈 Trend', ha='center', fontsize=9)
    ax.text(12.5, 5.7, '🔍 Anomaly', ha='center', fontsize=9)
    
    # Visualization (Layer 5)
    l5_box = FancyBboxPatch((1, 4), 14, 1, boxstyle="round,pad=0.1",
                           edgecolor='#99ccff', facecolor='#eff6ff', linewidth=2)
    ax.add_patch(l5_box)
    ax.text(8, 4.5, 'LAYER 5: Visualization & Charts', ha='center', fontsize=12, fontweight='bold')
    ax.text(5, 4.2, '📊 Plotly', ha='center', fontsize=9)
    ax.text(8, 4.2, '📈 Chart Selection', ha='center', fontsize=9)
    ax.text(11, 4.2, '🎨 Styling', ha='center', fontsize=9)
    
    # Response (Layer 6)
    l6_box = FancyBboxPatch((1, 2.5), 14, 1, boxstyle="round,pad=0.1",
                           edgecolor='#cc99ff', facecolor='#faf5ff', linewidth=2)
    ax.add_patch(l6_box)
    ax.text(8, 3, 'LAYER 6: Response Formatting', ha='center', fontsize=12, fontweight='bold')
    ax.text(6, 2.7, '📝 NL Summary', ha='center', fontsize=9)
    ax.text(10, 2.7, '📦 JSON Assembly', ha='center', fontsize=9)
    
    # Data Storage (bottom)
    storage_box = FancyBboxPatch((1, 0.5), 6, 1.5, boxstyle="round,pad=0.1",
                                edgecolor='#64748b', facecolor='#f1f5f9', linewidth=2)
    ax.add_patch(storage_box)
    ax.text(4, 1.7, 'DATA STORAGE', ha='center', fontsize=11, fontweight='bold', color='#334155')
    ax.text(4, 1.3, '📁 Parquet Files', ha='center', fontsize=9)
    ax.text(4, 1, '🗄️ DuckDB', ha='center', fontsize=9)
    ax.text(4, 0.7, '💾 Cache (Redis)', ha='center', fontsize=9)
    
    # LLM Services (bottom right)
    llm_box = FancyBboxPatch((9, 0.5), 6, 1.5, boxstyle="round,pad=0.1",
                            edgecolor='#7c3aed', facecolor='#f5f3ff', linewidth=2)
    ax.add_patch(llm_box)
    ax.text(12, 1.7, 'LLM SERVICES', ha='center', fontsize=11, fontweight='bold', color='#5b21b6')
    ax.text(12, 1.3, '🤖 Gemini 2.5 Flash', ha='center', fontsize=9)
    ax.text(12, 1, '🧠 DeepSeek', ha='center', fontsize=9)
    ax.text(12, 0.7, '✨ GPT-4o (Fallback)', ha='center', fontsize=9)
    
    # Arrows showing data flow
    arrow_color = PRIMARY_COLOR
    
    # Frontend -> L1
    ax.add_patch(FancyArrowPatch((8, 11.5), (8, 11), arrowstyle='->', mutation_scale=25, linewidth=3, color=arrow_color))
    
    # L1 -> L2
    ax.add_patch(FancyArrowPatch((8, 10), (8, 9.5), arrowstyle='->', mutation_scale=25, linewidth=3, color=arrow_color))
    
    # L2 -> L3
    ax.add_patch(FancyArrowPatch((8, 8.5), (8, 8), arrowstyle='->', mutation_scale=25, linewidth=3, color=arrow_color))
    
    # L3 -> L4
    ax.add_patch(FancyArrowPatch((8, 7), (8, 6.5), arrowstyle='->', mutation_scale=25, linewidth=3, color=arrow_color))
    
    # L4 -> L5
    ax.add_patch(FancyArrowPatch((8, 5.5), (8, 5), arrowstyle='->', mutation_scale=25, linewidth=3, color=arrow_color))
    
    # L5 -> L6
    ax.add_patch(FancyArrowPatch((8, 4), (8, 3.5), arrowstyle='->', mutation_scale=25, linewidth=3, color=arrow_color))
    
    # L6 -> Frontend (return)
    ax.add_patch(FancyArrowPatch((9.5, 3.5), (9.5, 11.5), arrowstyle='->', mutation_scale=25, linewidth=2, 
                                color=SUCCESS_COLOR, linestyle='--'))
    ax.text(10, 7.5, 'Response', rotation=90, va='center', fontsize=10, color=SUCCESS_COLOR, fontweight='bold')
    
    # L3 <-> Storage
    ax.add_patch(FancyArrowPatch((4, 2), (4, 7), arrowstyle='<->', mutation_scale=20, linewidth=2, color='#64748b'))
    
    # L2 <-> LLM
    ax.add_patch(FancyArrowPatch((12, 2), (12, 8.5), arrowstyle='<->', mutation_scale=20, linewidth=2, color='#7c3aed'))
    
    # L4 <-> LLM
    ax.add_patch(FancyArrowPatch((12, 2), (12, 5.5), arrowstyle='<->', mutation_scale=20, linewidth=2, color='#7c3aed'))
    
    # Timing annotations
    ax.text(0.3, 10.5, '~5ms', fontsize=9, color='#dc2626', fontweight='bold')
    ax.text(0.3, 9, '~5-800ms', fontsize=9, color='#dc2626', fontweight='bold')
    ax.text(0.3, 7.5, '~50-200ms', fontsize=9, color='#dc2626', fontweight='bold')
    ax.text(0.3, 6, '~500-2000ms', fontsize=9, color='#dc2626', fontweight='bold')
    ax.text(0.3, 4.5, '~100-300ms', fontsize=9, color='#dc2626', fontweight='bold')
    ax.text(0.3, 3, '~10ms', fontsize=9, color='#dc2626', fontweight='bold')
    
    # Total time
    total_box = FancyBboxPatch((5.5, 13.8), 5, 0.4, boxstyle="round,pad=0.05",
                              edgecolor=SUCCESS_COLOR, facecolor='#d1fae5', linewidth=2)
    ax.add_patch(total_box)
    ax.text(8, 14, 'Total: 0.7 - 3.5 seconds', ha='center', fontsize=11, fontweight='bold', color='#065f46')
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'overall_system_architecture.png', dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    print("✅ Created: overall_system_architecture.png")

def create_technology_stack_diagram():
    """Create technology stack visualization"""
    fig, ax = plt.subplots(figsize=(14, 10))
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 12)
    ax.axis('off')
    
    ax.text(7, 11.5, 'PCA Agent Technology Stack', ha='center', fontsize=20, fontweight='bold')
    
    # Frontend Stack
    frontend = FancyBboxPatch((0.5, 9), 4, 2, boxstyle="round,pad=0.1",
                             edgecolor='#3b82f6', facecolor='#dbeafe', linewidth=2)
    ax.add_patch(frontend)
    ax.text(2.5, 10.5, 'FRONTEND', ha='center', fontsize=13, fontweight='bold', color='#1e40af')
    ax.text(2.5, 10.1, '⚛️ React 18', ha='center', fontsize=10)
    ax.text(2.5, 9.7, '📊 Recharts', ha='center', fontsize=10)
    ax.text(2.5, 9.3, '🎨 TailwindCSS', ha='center', fontsize=10)
    
    # Backend Stack
    backend = FancyBboxPatch((5, 9), 4, 2, boxstyle="round,pad=0.1",
                            edgecolor='#10b981', facecolor='#d1fae5', linewidth=2)
    ax.add_patch(backend)
    ax.text(7, 10.5, 'BACKEND', ha='center', fontsize=13, fontweight='bold', color='#065f46')
    ax.text(7, 10.1, '🐍 Python 3.12', ha='center', fontsize=10)
    ax.text(7, 9.7, '⚡ FastAPI', ha='center', fontsize=10)
    ax.text(7, 9.3, '🔄 Async/Await', ha='center', fontsize=10)
    
    # AI/LLM Stack
    ai = FancyBboxPatch((9.5, 9), 4, 2, boxstyle="round,pad=0.1",
                       edgecolor='#8b5cf6', facecolor='#ede9fe', linewidth=2)
    ax.add_patch(ai)
    ax.text(11.5, 10.5, 'AI/LLM', ha='center', fontsize=13, fontweight='bold', color='#5b21b6')
    ax.text(11.5, 10.1, '🤖 Gemini 2.5', ha='center', fontsize=10)
    ax.text(11.5, 9.7, '🧠 DeepSeek', ha='center', fontsize=10)
    ax.text(11.5, 9.3, '✨ GPT-4o', ha='center', fontsize=10)
    
    # Database Stack
    database = FancyBboxPatch((0.5, 6), 4, 2, boxstyle="round,pad=0.1",
                             edgecolor='#f59e0b', facecolor='#fef3c7', linewidth=2)
    ax.add_patch(database)
    ax.text(2.5, 7.5, 'DATABASE', ha='center', fontsize=13, fontweight='bold', color='#92400e')
    ax.text(2.5, 7.1, '🗄️ DuckDB', ha='center', fontsize=10)
    ax.text(2.5, 6.7, '📊 Parquet', ha='center', fontsize=10)
    ax.text(2.5, 6.3, '💾 Redis Cache', ha='center', fontsize=10)
    
    # Data Processing
    processing = FancyBboxPatch((5, 6), 4, 2, boxstyle="round,pad=0.1",
                               edgecolor='#06b6d4', facecolor='#cffafe', linewidth=2)
    ax.add_patch(processing)
    ax.text(7, 7.5, 'DATA PROCESSING', ha='center', fontsize=13, fontweight='bold', color='#164e63')
    ax.text(7, 7.1, '🐻‍❄️ Polars', ha='center', fontsize=10)
    ax.text(7, 6.7, '🐼 Pandas', ha='center', fontsize=10)
    ax.text(7, 6.3, '📈 NumPy', ha='center', fontsize=10)
    
    # Visualization
    viz = FancyBboxPatch((9.5, 6), 4, 2, boxstyle="round,pad=0.1",
                        edgecolor='#ec4899', facecolor='#fce7f3', linewidth=2)
    ax.add_patch(viz)
    ax.text(11.5, 7.5, 'VISUALIZATION', ha='center', fontsize=13, fontweight='bold', color='#9f1239')
    ax.text(11.5, 7.1, '📊 Plotly', ha='center', fontsize=10)
    ax.text(11.5, 6.7, '📈 Matplotlib', ha='center', fontsize=10)
    ax.text(11.5, 6.3, '🎨 Seaborn', ha='center', fontsize=10)
    
    # Infrastructure
    infra = FancyBboxPatch((2.5, 3), 9, 2, boxstyle="round,pad=0.1",
                          edgecolor='#64748b', facecolor='#f1f5f9', linewidth=2)
    ax.add_patch(infra)
    ax.text(7, 4.5, 'INFRASTRUCTURE & DEPLOYMENT', ha='center', fontsize=13, fontweight='bold', color='#334155')
    ax.text(4, 4, '🐳 Docker', ha='center', fontsize=10)
    ax.text(6, 4, '☸️ Kubernetes', ha='center', fontsize=10)
    ax.text(8, 4, '☁️ AWS/GCP', ha='center', fontsize=10)
    ax.text(10, 4, '📊 Prometheus', ha='center', fontsize=10)
    ax.text(4, 3.5, '🔍 Grafana', ha='center', fontsize=10)
    ax.text(6, 3.5, '📝 Logging', ha='center', fontsize=10)
    ax.text(8, 3.5, '🔐 Secrets', ha='center', fontsize=10)
    ax.text(10, 3.5, '⚖️ Load Balancer', ha='center', fontsize=10)
    
    # Development Tools
    dev = FancyBboxPatch((2.5, 0.5), 9, 2, boxstyle="round,pad=0.1",
                        edgecolor='#14b8a6', facecolor='#ccfbf1', linewidth=2)
    ax.add_patch(dev)
    ax.text(7, 2, 'DEVELOPMENT & TESTING', ha='center', fontsize=13, fontweight='bold', color='#134e4a')
    ax.text(4, 1.5, '🧪 Pytest', ha='center', fontsize=10)
    ax.text(6, 1.5, '📝 Black', ha='center', fontsize=10)
    ax.text(8, 1.5, '🔍 Mypy', ha='center', fontsize=10)
    ax.text(10, 1.5, '📋 Ruff', ha='center', fontsize=10)
    ax.text(4, 1, '🔄 Git', ha='center', fontsize=10)
    ax.text(6, 1, '🚀 CI/CD', ha='center', fontsize=10)
    ax.text(8, 1, '📊 Coverage', ha='center', fontsize=10)
    ax.text(10, 1, '🐛 Debugging', ha='center', fontsize=10)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'technology_stack.png', dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    print("✅ Created: technology_stack.png")

def create_data_flow_detailed():
    """Create detailed data flow diagram"""
    fig, ax = plt.subplots(figsize=(14, 10))
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 12)
    ax.axis('off')
    
    ax.text(7, 11.5, 'Detailed Data Flow: User Query to Response', 
            ha='center', fontsize=18, fontweight='bold')
    
    # User query
    user_box = FancyBboxPatch((5, 10), 4, 0.8, boxstyle="round,pad=0.1",
                             edgecolor=PRIMARY_COLOR, facecolor='white', linewidth=2)
    ax.add_patch(user_box)
    ax.text(7, 10.4, '👤 "What is my CPA?"', ha='center', fontsize=11, fontweight='bold')
    
    # Step 1
    step1 = FancyBboxPatch((0.5, 8.5), 3, 1, boxstyle="round,pad=0.05",
                          edgecolor='#ff9999', facecolor='#ffe5e5', linewidth=2)
    ax.add_patch(step1)
    ax.text(2, 9.3, '1. Validate', ha='center', fontsize=10, fontweight='bold')
    ax.text(2, 9, 'Check JWT', ha='center', fontsize=8)
    ax.text(2, 8.7, '5ms', ha='center', fontsize=8, color='#dc2626')
    
    # Step 2
    step2 = FancyBboxPatch((4, 8.5), 3, 1, boxstyle="round,pad=0.05",
                          edgecolor='#ffcc99', facecolor='#fff4e6', linewidth=2)
    ax.add_patch(step2)
    ax.text(5.5, 9.3, '2. Understand', ha='center', fontsize=10, fontweight='bold')
    ax.text(5.5, 9, 'Parse intent', ha='center', fontsize=8)
    ax.text(5.5, 8.7, '5ms', ha='center', fontsize=8, color='#dc2626')
    
    # Step 3
    step3 = FancyBboxPatch((7.5, 8.5), 3, 1, boxstyle="round,pad=0.05",
                          edgecolor='#ffff99', facecolor='#fffbeb', linewidth=2)
    ax.add_patch(step3)
    ax.text(9, 9.3, '3. Query', ha='center', fontsize=10, fontweight='bold')
    ax.text(9, 9, 'Generate SQL', ha='center', fontsize=8)
    ax.text(9, 8.7, '100ms', ha='center', fontsize=8, color='#dc2626')
    
    # Step 4
    step4 = FancyBboxPatch((11, 8.5), 3, 1, boxstyle="round,pad=0.05",
                          edgecolor='#99ff99', facecolor='#ecfdf5', linewidth=2)
    ax.add_patch(step4)
    ax.text(12.5, 9.3, '4. Analyze', ha='center', fontsize=10, fontweight='bold')
    ax.text(12.5, 9, 'Find insights', ha='center', fontsize=8)
    ax.text(12.5, 8.7, '1000ms', ha='center', fontsize=8, color='#dc2626')
    
    # Step 5
    step5 = FancyBboxPatch((2.5, 6.5), 3, 1, boxstyle="round,pad=0.05",
                          edgecolor='#99ccff', facecolor='#eff6ff', linewidth=2)
    ax.add_patch(step5)
    ax.text(4, 7.3, '5. Visualize', ha='center', fontsize=10, fontweight='bold')
    ax.text(4, 7, 'Create charts', ha='center', fontsize=8)
    ax.text(4, 6.7, '200ms', ha='center', fontsize=8, color='#dc2626')
    
    # Step 6
    step6 = FancyBboxPatch((6, 6.5), 3, 1, boxstyle="round,pad=0.05",
                          edgecolor='#cc99ff', facecolor='#faf5ff', linewidth=2)
    ax.add_patch(step6)
    ax.text(7.5, 7.3, '6. Format', ha='center', fontsize=10, fontweight='bold')
    ax.text(7.5, 7, 'Build JSON', ha='center', fontsize=8)
    ax.text(7.5, 6.7, '10ms', ha='center', fontsize=8, color='#dc2626')
    
    # Final response
    response_box = FancyBboxPatch((5, 4.5), 4, 1.5, boxstyle="round,pad=0.1",
                                 edgecolor=SUCCESS_COLOR, facecolor='#d1fae5', linewidth=3)
    ax.add_patch(response_box)
    ax.text(7, 5.7, '✅ RESPONSE', ha='center', fontsize=12, fontweight='bold', color='#065f46')
    ax.text(7, 5.3, '"Your CPA is $12.50"', ha='center', fontsize=10)
    ax.text(7, 5, '+ Chart + Insights', ha='center', fontsize=9)
    ax.text(7, 4.7, 'Total: 1.32 seconds', ha='center', fontsize=9, fontweight='bold')
    
    # Arrows
    ax.add_patch(FancyArrowPatch((7, 10), (2, 9.5), arrowstyle='->', mutation_scale=15, linewidth=2, color=PRIMARY_COLOR))
    ax.add_patch(FancyArrowPatch((3.5, 9), (4, 9), arrowstyle='->', mutation_scale=15, linewidth=2, color=PRIMARY_COLOR))
    ax.add_patch(FancyArrowPatch((7, 9), (7.5, 9), arrowstyle='->', mutation_scale=15, linewidth=2, color=PRIMARY_COLOR))
    ax.add_patch(FancyArrowPatch((10.5, 9), (11, 9), arrowstyle='->', mutation_scale=15, linewidth=2, color=PRIMARY_COLOR))
    ax.add_patch(FancyArrowPatch((12.5, 8.5), (4, 7.5), arrowstyle='->', mutation_scale=15, linewidth=2, color=PRIMARY_COLOR))
    ax.add_patch(FancyArrowPatch((5.5, 7), (6, 7), arrowstyle='->', mutation_scale=15, linewidth=2, color=PRIMARY_COLOR))
    ax.add_patch(FancyArrowPatch((7.5, 6.5), (7, 6), arrowstyle='->', mutation_scale=15, linewidth=2, color=SUCCESS_COLOR))
    
    # Data transformations
    ax.text(2.5, 9.8, 'HTTP Request', ha='center', fontsize=8, style='italic', color='#6b7280')
    ax.text(5.5, 9.8, 'Intent: METRIC', ha='center', fontsize=8, style='italic', color='#6b7280')
    ax.text(9, 9.8, 'SQL Query', ha='center', fontsize=8, style='italic', color='#6b7280')
    ax.text(12.5, 9.8, 'DataFrame', ha='center', fontsize=8, style='italic', color='#6b7280')
    ax.text(4, 7.8, 'Insights[]', ha='center', fontsize=8, style='italic', color='#6b7280')
    ax.text(7.5, 7.8, 'Chart + Text', ha='center', fontsize=8, style='italic', color='#6b7280')
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'data_flow_detailed.png', dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    print("✅ Created: data_flow_detailed.png")

def main():
    print("=" * 60)
    print("Creating Additional Architecture Diagrams")
    print("=" * 60)
    print()
    
    create_overall_system_architecture()
    create_technology_stack_diagram()
    create_data_flow_detailed()
    
    print()
    print("=" * 60)
    print(f"✅ All architecture diagrams created in: {OUTPUT_DIR}")
    print("=" * 60)

if __name__ == "__main__":
    main()
