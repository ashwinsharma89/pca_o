#!/usr/bin/env python3
"""
Create additional diagrams for comprehensive documentation
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, Circle, Rectangle
from pathlib import Path
import numpy as np

OUTPUT_DIR = Path("guide/diagrams")
OUTPUT_DIR.mkdir(exist_ok=True)

PRIMARY_COLOR = '#2563eb'
SUCCESS_COLOR = '#10b981'
WARNING_COLOR = '#f59e0b'
ERROR_COLOR = '#ef4444'
GRAY_COLOR = '#6b7280'

def create_authentication_flow():
    """JWT authentication flow diagram"""
    fig, ax = plt.subplots(figsize=(12, 8))
    ax.set_xlim(0, 12)
    ax.set_ylim(0, 10)
    ax.axis('off')
    
    ax.text(6, 9.5, 'Authentication Flow (JWT)', ha='center', fontsize=18, fontweight='bold')
    
    # Steps
    steps = [
        {'y': 8, 'text': '1. User logs in with credentials', 'color': '#e3f2fd'},
        {'y': 7, 'text': '2. Server validates credentials', 'color': '#fff3e0'},
        {'y': 6, 'text': '3. Server generates JWT token', 'color': '#e8f5e9'},
        {'y': 5, 'text': '4. Token sent to user', 'color': '#fce4ec'},
        {'y': 4, 'text': '5. User includes token in requests', 'color': '#f3e5f5'},
        {'y': 3, 'text': '6. Server validates token signature', 'color': '#e0f2f1'},
        {'y': 2, 'text': '7. Request processed', 'color': '#e8eaf6'},
    ]
    
    for step in steps:
        box = FancyBboxPatch((1, step['y']-0.3), 10, 0.6, boxstyle="round,pad=0.1",
                            edgecolor=PRIMARY_COLOR, facecolor=step['color'], linewidth=2)
        ax.add_patch(box)
        ax.text(6, step['y'], step['text'], ha='center', va='center', fontsize=11)
        
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'authentication_flow.png', dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    print("✅ Created: authentication_flow.png")

def create_rate_limiting_diagram():
    """Rate limiting visualization"""
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.set_xlim(0, 12)
    ax.set_ylim(0, 8)
    ax.axis('off')
    
    ax.text(6, 7.5, 'Rate Limiting: 60 Requests Per Minute', ha='center', fontsize=16, fontweight='bold')
    
    # Timeline
    for i in range(60):
        x = 1 + (i % 10) * 1
        y = 6 - (i // 10) * 0.8
        color = SUCCESS_COLOR if i < 55 else WARNING_COLOR if i < 59 else ERROR_COLOR
        circle = Circle((x, y), 0.15, color=color, alpha=0.7)
        ax.add_patch(circle)
    
    # Legend
    ax.text(2, 1.5, '● Allowed (1-55)', fontsize=10, color=SUCCESS_COLOR)
    ax.text(5, 1.5, '● Warning (56-59)', fontsize=10, color=WARNING_COLOR)
    ax.text(8, 1.5, '● Blocked (60+)', fontsize=10, color=ERROR_COLOR)
    
    ax.text(6, 0.5, 'Request #60 triggers: HTTP 429 Too Many Requests', 
            ha='center', fontsize=11, style='italic', 
            bbox=dict(boxstyle='round,pad=0.5', facecolor='#fee', edgecolor=ERROR_COLOR))
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'rate_limiting.png', dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    print("✅ Created: rate_limiting.png")

def create_sql_generation_flow():
    """SQL generation process"""
    fig, ax = plt.subplots(figsize=(12, 10))
    ax.set_xlim(0, 12)
    ax.set_ylim(0, 12)
    ax.axis('off')
    
    ax.text(6, 11.5, 'SQL Generation Process', ha='center', fontsize=18, fontweight='bold')
    
    # Natural language input
    box1 = FancyBboxPatch((2, 10), 8, 0.8, boxstyle="round,pad=0.1",
                         edgecolor=PRIMARY_COLOR, facecolor='#e3f2fd', linewidth=2)
    ax.add_patch(box1)
    ax.text(6, 10.4, 'Natural Language: "Show me top 5 campaigns by ROAS"', 
            ha='center', fontsize=11, fontweight='bold')
    
    # Arrow
    arrow = FancyArrowPatch((6, 10), (6, 9.2), arrowstyle='->', mutation_scale=20, linewidth=2, color=PRIMARY_COLOR)
    ax.add_patch(arrow)
    
    # Intent extraction
    box2 = FancyBboxPatch((2, 8.2), 8, 1.8, boxstyle="round,pad=0.1",
                         edgecolor='#10b981', facecolor='#d1fae5', linewidth=2)
    ax.add_patch(box2)
    ax.text(6, 9.5, 'Intent Extraction', ha='center', fontsize=12, fontweight='bold', color='#065f46')
    ax.text(6, 9.1, '• Metric: ROAS', ha='left', fontsize=10)
    ax.text(6, 8.8, '• Dimension: Campaign', ha='left', fontsize=10)
    ax.text(6, 8.5, '• Limit: 5', ha='left', fontsize=10)
    
    # Arrow
    arrow = FancyArrowPatch((6, 8.2), (6, 7.4), arrowstyle='->', mutation_scale=20, linewidth=2, color=PRIMARY_COLOR)
    ax.add_patch(arrow)
    
    # SQL template
    box3 = FancyBboxPatch((1.5, 5.4), 9, 1.8, boxstyle="round,pad=0.1",
                         edgecolor='#f59e0b', facecolor='#fef3c7', linewidth=2)
    ax.add_patch(box3)
    ax.text(6, 6.8, 'Generated SQL', ha='center', fontsize=12, fontweight='bold', color='#92400e')
    sql_text = """SELECT campaign_name, 
       ROUND(SUM(revenue) / NULLIF(SUM(spend), 0), 2) as roas
FROM campaigns
GROUP BY campaign_name
ORDER BY roas DESC
LIMIT 5"""
    ax.text(6, 6.2, sql_text, ha='center', fontsize=8, family='monospace')
    
    # Arrow
    arrow = FancyArrowPatch((6, 5.4), (6, 4.6), arrowstyle='->', mutation_scale=20, linewidth=2, color=PRIMARY_COLOR)
    ax.add_patch(arrow)
    
    # Execution
    box4 = FancyBboxPatch((2, 3.6), 8, 0.8, boxstyle="round,pad=0.1",
                         edgecolor='#8b5cf6', facecolor='#ede9fe', linewidth=2)
    ax.add_patch(box4)
    ax.text(6, 4, 'Execute in DuckDB (~50ms)', ha='center', fontsize=11, fontweight='bold')
    
    # Arrow
    arrow = FancyArrowPatch((6, 3.6), (6, 2.8), arrowstyle='->', mutation_scale=20, linewidth=2, color=PRIMARY_COLOR)
    ax.add_patch(arrow)
    
    # Results
    box5 = FancyBboxPatch((2, 1.8), 8, 0.8, boxstyle="round,pad=0.1",
                         edgecolor=SUCCESS_COLOR, facecolor='#d1fae5', linewidth=2)
    ax.add_patch(box5)
    ax.text(6, 2.2, 'Results: DataFrame with 5 campaigns', ha='center', fontsize=11, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'sql_generation_flow.png', dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    print("✅ Created: sql_generation_flow.png")

def create_agent_orchestration():
    """Multi-agent orchestration diagram"""
    fig, ax = plt.subplots(figsize=(14, 10))
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 12)
    ax.axis('off')
    
    ax.text(7, 11.5, 'Layer 4: Multi-Agent Orchestration', ha='center', fontsize=18, fontweight='bold')
    
    # Orchestrator (center)
    orch_box = FancyBboxPatch((5.5, 5.5), 3, 1.5, boxstyle="round,pad=0.1",
                             edgecolor=PRIMARY_COLOR, facecolor='#dbeafe', linewidth=3)
    ax.add_patch(orch_box)
    ax.text(7, 6.5, 'ORCHESTRATOR', ha='center', fontsize=13, fontweight='bold', color=PRIMARY_COLOR)
    ax.text(7, 6, 'Coordinates all agents', ha='center', fontsize=9, style='italic')
    
    # Agents around orchestrator
    agents = [
        {'x': 2, 'y': 9, 'name': 'Reasoning\nAgent', 'task': 'Find patterns', 'color': '#dcfce7'},
        {'x': 12, 'y': 9, 'name': 'B2B\nSpecialist', 'task': 'Industry context', 'color': '#fef3c7'},
        {'x': 2, 'y': 3, 'name': 'Trend\nAnalyzer', 'task': 'Time series', 'color': '#fce7f3'},
        {'x': 12, 'y': 3, 'name': 'Anomaly\nDetector', 'task': 'Find outliers', 'color': '#fee2e2'},
    ]
    
    for agent in agents:
        box = FancyBboxPatch((agent['x']-0.8, agent['y']-0.6), 1.6, 1.2, boxstyle="round,pad=0.1",
                            edgecolor='#374151', facecolor=agent['color'], linewidth=2)
        ax.add_patch(box)
        ax.text(agent['x'], agent['y']+0.2, agent['name'], ha='center', fontsize=10, fontweight='bold')
        ax.text(agent['x'], agent['y']-0.3, agent['task'], ha='center', fontsize=8, style='italic')
        
        # Arrows to/from orchestrator
        arrow1 = FancyArrowPatch((agent['x'], agent['y']-0.6), (7, 6.5 if agent['y'] > 6 else 5.5),
                                arrowstyle='->', mutation_scale=15, linewidth=1.5, color=GRAY_COLOR)
        ax.add_patch(arrow1)
        arrow2 = FancyArrowPatch((7, 6.5 if agent['y'] > 6 else 5.5), (agent['x'], agent['y']-0.6),
                                arrowstyle='->', mutation_scale=15, linewidth=1.5, color=PRIMARY_COLOR)
        ax.add_patch(arrow2)
    
    # Final output
    output_box = FancyBboxPatch((4.5, 0.5), 5, 0.8, boxstyle="round,pad=0.1",
                               edgecolor=SUCCESS_COLOR, facecolor='#d1fae5', linewidth=2)
    ax.add_patch(output_box)
    ax.text(7, 0.9, 'Combined Insights + Recommendations', ha='center', fontsize=11, fontweight='bold')
    
    arrow = FancyArrowPatch((7, 5.5), (7, 1.3), arrowstyle='->', mutation_scale=20, linewidth=2, color=SUCCESS_COLOR)
    ax.add_patch(arrow)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'agent_orchestration.png', dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    print("✅ Created: agent_orchestration.png")

def create_caching_strategy():
    """Caching layers diagram"""
    fig, ax = plt.subplots(figsize=(12, 8))
    ax.set_xlim(0, 12)
    ax.set_ylim(0, 10)
    ax.axis('off')
    
    ax.text(6, 9.5, 'Multi-Layer Caching Strategy', ha='center', fontsize=18, fontweight='bold')
    
    # Cache layers
    caches = [
        {'y': 8, 'name': 'Semantic Cache', 'hit': '40%', 'save': '800ms', 'color': '#dcfce7'},
        {'y': 6.5, 'name': 'Query Result Cache', 'hit': '30%', 'save': '200ms', 'color': '#dbeafe'},
        {'y': 5, 'name': 'LLM Response Cache', 'hit': '20%', 'save': '500ms', 'color': '#fef3c7'},
    ]
    
    for cache in caches:
        box = FancyBboxPatch((2, cache['y']-0.5), 8, 1, boxstyle="round,pad=0.1",
                            edgecolor='#374151', facecolor=cache['color'], linewidth=2)
        ax.add_patch(box)
        ax.text(6, cache['y']+0.2, cache['name'], ha='center', fontsize=12, fontweight='bold')
        ax.text(6, cache['y']-0.2, f"Hit Rate: {cache['hit']} | Saves: {cache['hit']}", 
                ha='center', fontsize=10)
    
    # Total savings
    total_box = FancyBboxPatch((3, 2.5), 6, 1, boxstyle="round,pad=0.1",
                              edgecolor=SUCCESS_COLOR, facecolor='#d1fae5', linewidth=3)
    ax.add_patch(total_box)
    ax.text(6, 3.2, 'Total Cache Hit Rate: 90%', ha='center', fontsize=13, fontweight='bold', color='#065f46')
    ax.text(6, 2.8, 'Average Response Time: 0.3s (vs 2.5s without cache)', 
            ha='center', fontsize=10, style='italic')
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'caching_strategy.png', dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    print("✅ Created: caching_strategy.png")

def create_error_handling():
    """Error handling flow"""
    fig, ax = plt.subplots(figsize=(12, 10))
    ax.set_xlim(0, 12)
    ax.set_ylim(0, 12)
    ax.axis('off')
    
    ax.text(6, 11.5, 'Error Handling & Fallback Strategy', ha='center', fontsize=18, fontweight='bold')
    
    # Primary attempt
    box1 = FancyBboxPatch((3, 9.5), 6, 0.8, boxstyle="round,pad=0.1",
                         edgecolor=PRIMARY_COLOR, facecolor='#dbeafe', linewidth=2)
    ax.add_patch(box1)
    ax.text(6, 9.9, 'Try: Gemini 2.5 Flash (Free)', ha='center', fontsize=11, fontweight='bold')
    
    # Decision diamond
    diamond1 = mpatches.FancyBboxPatch((5.2, 8), 1.6, 1, boxstyle="round,pad=0.1",
                                      edgecolor='#374151', facecolor='#fef3c7', linewidth=2)
    ax.add_patch(diamond1)
    ax.text(6, 8.5, 'Success?', ha='center', fontsize=10, fontweight='bold')
    
    # Success path
    arrow_success = FancyArrowPatch((7, 8.5), (9, 8.5), arrowstyle='->', mutation_scale=20, linewidth=2, color=SUCCESS_COLOR)
    ax.add_patch(arrow_success)
    ax.text(8, 8.8, 'YES', fontsize=9, color=SUCCESS_COLOR, fontweight='bold')
    
    success_box = FancyBboxPatch((9, 8.2), 2, 0.6, boxstyle="round,pad=0.1",
                                edgecolor=SUCCESS_COLOR, facecolor='#d1fae5', linewidth=2)
    ax.add_patch(success_box)
    ax.text(10, 8.5, 'Return Result', ha='center', fontsize=10, fontweight='bold')
    
    # Fallback 1
    arrow_fail1 = FancyArrowPatch((6, 8), (6, 7.2), arrowstyle='->', mutation_scale=20, linewidth=2, color=ERROR_COLOR)
    ax.add_patch(arrow_fail1)
    ax.text(6.3, 7.6, 'NO', fontsize=9, color=ERROR_COLOR, fontweight='bold')
    
    box2 = FancyBboxPatch((3, 6.4), 6, 0.8, boxstyle="round,pad=0.1",
                         edgecolor='#f59e0b', facecolor='#fef3c7', linewidth=2)
    ax.add_patch(box2)
    ax.text(6, 6.8, 'Fallback 1: DeepSeek (Free)', ha='center', fontsize=11, fontweight='bold')
    
    # Decision diamond 2
    diamond2 = mpatches.FancyBboxPatch((5.2, 5), 1.6, 1, boxstyle="round,pad=0.1",
                                      edgecolor='#374151', facecolor='#fef3c7', linewidth=2)
    ax.add_patch(diamond2)
    ax.text(6, 5.5, 'Success?', ha='center', fontsize=10, fontweight='bold')
    
    # Success path 2
    arrow_success2 = FancyArrowPatch((7, 5.5), (9, 5.5), arrowstyle='->', mutation_scale=20, linewidth=2, color=SUCCESS_COLOR)
    ax.add_patch(arrow_success2)
    
    # Fallback 2
    arrow_fail2 = FancyArrowPatch((6, 5), (6, 4.2), arrowstyle='->', mutation_scale=20, linewidth=2, color=ERROR_COLOR)
    ax.add_patch(arrow_fail2)
    
    box3 = FancyBboxPatch((3, 3.4), 6, 0.8, boxstyle="round,pad=0.1",
                         edgecolor='#ef4444', facecolor='#fee2e2', linewidth=2)
    ax.add_patch(box3)
    ax.text(6, 3.8, 'Fallback 2: GPT-4o (Paid)', ha='center', fontsize=11, fontweight='bold')
    
    # Final decision
    diamond3 = mpatches.FancyBboxPatch((5.2, 2), 1.6, 1, boxstyle="round,pad=0.1",
                                      edgecolor='#374151', facecolor='#fef3c7', linewidth=2)
    ax.add_patch(diamond3)
    ax.text(6, 2.5, 'Success?', ha='center', fontsize=10, fontweight='bold')
    
    # Success path 3
    arrow_success3 = FancyArrowPatch((7, 2.5), (9, 2.5), arrowstyle='->', mutation_scale=20, linewidth=2, color=SUCCESS_COLOR)
    ax.add_patch(arrow_success3)
    
    # Final error
    arrow_fail3 = FancyArrowPatch((6, 2), (6, 1.2), arrowstyle='->', mutation_scale=20, linewidth=2, color=ERROR_COLOR)
    ax.add_patch(arrow_fail3)
    
    error_box = FancyBboxPatch((3, 0.4), 6, 0.8, boxstyle="round,pad=0.1",
                              edgecolor=ERROR_COLOR, facecolor='#fee2e2', linewidth=2)
    ax.add_patch(error_box)
    ax.text(6, 0.8, 'Return Error + Suggestion', ha='center', fontsize=11, fontweight='bold', color='#991b1b')
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'error_handling.png', dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    print("✅ Created: error_handling.png")

def create_performance_comparison():
    """Performance comparison chart"""
    fig, ax = plt.subplots(figsize=(10, 6))
    
    categories = ['Bulletproof\nPath', 'LLM Path\n(Cached)', 'LLM Path\n(Uncached)', 'Traditional\nAnalyst']
    times = [0.005, 0.3, 2.5, 14400]  # in seconds
    colors_list = [SUCCESS_COLOR, '#60a5fa', WARNING_COLOR, ERROR_COLOR]
    
    bars = ax.barh(categories, times, color=colors_list, edgecolor='black', linewidth=1.5)
    
    # Add value labels
    for i, (bar, time) in enumerate(zip(bars, times)):
        if time < 10:
            label = f'{time*1000:.0f}ms'
        elif time < 3600:
            label = f'{time:.1f}s'
        else:
            label = f'{time/3600:.1f}h'
        ax.text(time, bar.get_y() + bar.get_height()/2, f'  {label}', 
                va='center', fontsize=11, fontweight='bold')
    
    ax.set_xlabel('Response Time (log scale)', fontsize=12, fontweight='bold')
    ax.set_title('Performance Comparison: PCA Agent vs Traditional Analytics', 
                fontsize=14, fontweight='bold', pad=20)
    ax.set_xscale('log')
    ax.grid(axis='x', alpha=0.3, linestyle='--')
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'performance_comparison.png', dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    print("✅ Created: performance_comparison.png")

def main():
    print("=" * 60)
    print("Generating Additional Diagrams")
    print("=" * 60)
    print()
    
    create_authentication_flow()
    create_rate_limiting_diagram()
    create_sql_generation_flow()
    create_agent_orchestration()
    create_caching_strategy()
    create_error_handling()
    create_performance_comparison()
    
    print()
    print("=" * 60)
    print(f"✅ All additional diagrams created in: {OUTPUT_DIR}")
    print("=" * 60)

if __name__ == "__main__":
    main()
