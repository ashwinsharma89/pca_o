#!/usr/bin/env python3
"""
Generate diagram images for PCA Agent documentation
Creates PNG images for all architecture diagrams
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
from pathlib import Path

# Configuration
OUTPUT_DIR = Path("guide/diagrams")
OUTPUT_DIR.mkdir(exist_ok=True)

# Color scheme
PRIMARY_COLOR = '#2563eb'
SECONDARY_COLOR = '#f3f4f6'
TEXT_COLOR = '#1f2937'
BORDER_COLOR = '#e5e7eb'

def create_six_layer_architecture():
    """Create the 6-layer architecture diagram"""
    fig, ax = plt.subplots(figsize=(12, 10))
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 12)
    ax.axis('off')
    
    # Title
    ax.text(5, 11.5, 'PCA Agent: Six-Layer Architecture', 
            ha='center', va='top', fontsize=20, fontweight='bold', color=TEXT_COLOR)
    
    # Layer definitions
    layers = [
        {'name': 'Layer 1: API Gateway', 'time': '~5ms', 'color': '#ff9999', 'y': 9.5},
        {'name': 'Layer 2: Query Understanding', 'time': '~5-800ms', 'color': '#ffcc99', 'y': 8},
        {'name': 'Layer 3: Data Retrieval', 'time': '~50-200ms', 'color': '#ffff99', 'y': 6.5},
        {'name': 'Layer 4: Analysis', 'time': '~500-2000ms', 'color': '#99ff99', 'y': 5},
        {'name': 'Layer 5: Visualization', 'time': '~100-300ms', 'color': '#99ccff', 'y': 3.5},
        {'name': 'Layer 6: Response Formatting', 'time': '~10ms', 'color': '#cc99ff', 'y': 2},
    ]
    
    # Draw user input
    user_box = FancyBboxPatch((3.5, 10.5), 3, 0.6, boxstyle="round,pad=0.1", 
                               edgecolor=PRIMARY_COLOR, facecolor='white', linewidth=2)
    ax.add_patch(user_box)
    ax.text(5, 10.8, '👤 User Question', ha='center', va='center', fontsize=12, fontweight='bold')
    
    # Draw layers
    for i, layer in enumerate(layers):
        # Layer box
        box = FancyBboxPatch((1, layer['y']), 8, 1.2, boxstyle="round,pad=0.05",
                            edgecolor=TEXT_COLOR, facecolor=layer['color'], linewidth=2)
        ax.add_patch(box)
        
        # Layer text
        ax.text(5, layer['y'] + 0.8, layer['name'], ha='center', va='center', 
                fontsize=13, fontweight='bold', color=TEXT_COLOR)
        ax.text(5, layer['y'] + 0.4, layer['time'], ha='center', va='center',
                fontsize=10, color=TEXT_COLOR)
        
        # Arrow to next layer
        if i < len(layers) - 1:
            arrow = FancyArrowPatch((5, layer['y']), (5, layers[i+1]['y'] + 1.2),
                                   arrowstyle='->', mutation_scale=20, linewidth=2,
                                   color=PRIMARY_COLOR)
            ax.add_patch(arrow)
    
    # Arrow from user to first layer
    arrow = FancyArrowPatch((5, 10.5), (5, layers[0]['y'] + 1.2),
                           arrowstyle='->', mutation_scale=20, linewidth=2,
                           color=PRIMARY_COLOR)
    ax.add_patch(arrow)
    
    # Draw final output
    output_box = FancyBboxPatch((3.5, 0.5), 3, 0.6, boxstyle="round,pad=0.1",
                               edgecolor=PRIMARY_COLOR, facecolor='white', linewidth=2)
    ax.add_patch(output_box)
    ax.text(5, 0.8, '📊 Final Response', ha='center', va='center', fontsize=12, fontweight='bold')
    
    # Arrow to output
    arrow = FancyArrowPatch((5, layers[-1]['y']), (5, 1.1),
                           arrowstyle='->', mutation_scale=20, linewidth=2,
                           color=PRIMARY_COLOR)
    ax.add_patch(arrow)
    
    # Total time
    ax.text(5, 0.2, 'Total Time: 0.7-3.5 seconds', ha='center', va='center',
            fontsize=11, fontweight='bold', color=PRIMARY_COLOR)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'six_layer_architecture.png', dpi=300, bbox_inches='tight', 
                facecolor='white', edgecolor='none')
    plt.close()
    print("✅ Created: six_layer_architecture.png")

def create_request_flow():
    """Create request flow diagram"""
    fig, ax = plt.subplots(figsize=(14, 8))
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 10)
    ax.axis('off')
    
    # Title
    ax.text(7, 9.5, 'Complete Request Flow Example', 
            ha='center', va='top', fontsize=18, fontweight='bold', color=TEXT_COLOR)
    ax.text(7, 9, 'Question: "Compare last 2 weeks performance"', 
            ha='center', va='top', fontsize=12, style='italic', color=TEXT_COLOR)
    
    # Steps
    steps = [
        {'x': 1, 'y': 7, 'name': 'Layer 1\nAPI Gateway', 'time': '5ms', 'action': 'Validate token\nCheck rate limit'},
        {'x': 3.5, 'y': 7, 'name': 'Layer 2\nQuery Understanding', 'time': '5ms', 'action': 'Pattern match:\n"last 2 weeks"'},
        {'x': 6, 'y': 7, 'name': 'Layer 3\nData Retrieval', 'time': '100ms', 'action': 'Execute SQL\nReturn data'},
        {'x': 8.5, 'y': 7, 'name': 'Layer 4\nAnalysis', 'time': '1000ms', 'action': 'Find patterns\nGenerate insights'},
        {'x': 11, 'y': 7, 'name': 'Layer 5\nVisualization', 'time': '200ms', 'action': 'Create bar chart\nFormat data'},
        {'x': 13, 'y': 7, 'name': 'Layer 6\nResponse', 'time': '10ms', 'action': 'Format JSON\nSend response'},
    ]
    
    colors = ['#ff9999', '#ffcc99', '#ffff99', '#99ff99', '#99ccff', '#cc99ff']
    
    for i, step in enumerate(steps):
        # Box
        box = FancyBboxPatch((step['x'] - 0.6, step['y'] - 0.8), 1.2, 1.6,
                            boxstyle="round,pad=0.05", edgecolor=TEXT_COLOR,
                            facecolor=colors[i], linewidth=2)
        ax.add_patch(box)
        
        # Text
        ax.text(step['x'], step['y'] + 0.5, step['name'], ha='center', va='center',
                fontsize=9, fontweight='bold', color=TEXT_COLOR)
        ax.text(step['x'], step['y'] + 0.1, step['time'], ha='center', va='center',
                fontsize=8, color=PRIMARY_COLOR, fontweight='bold')
        ax.text(step['x'], step['y'] - 0.4, step['action'], ha='center', va='center',
                fontsize=7, color=TEXT_COLOR)
        
        # Arrow
        if i < len(steps) - 1:
            arrow = FancyArrowPatch((step['x'] + 0.6, step['y']), (steps[i+1]['x'] - 0.6, steps[i+1]['y']),
                                   arrowstyle='->', mutation_scale=15, linewidth=2, color=PRIMARY_COLOR)
            ax.add_patch(arrow)
    
    # Total time box
    total_box = FancyBboxPatch((5.5, 5), 3, 0.6, boxstyle="round,pad=0.1",
                              edgecolor=PRIMARY_COLOR, facecolor=SECONDARY_COLOR, linewidth=2)
    ax.add_patch(total_box)
    ax.text(7, 5.3, 'Total Time: 1.32 seconds', ha='center', va='center',
            fontsize=12, fontweight='bold', color=PRIMARY_COLOR)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'request_flow.png', dpi=300, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.close()
    print("✅ Created: request_flow.png")

def create_layer_communication():
    """Create layer communication diagram"""
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.set_xlim(0, 13)
    ax.set_ylim(0, 4)
    ax.axis('off')
    
    # Title
    ax.text(6.5, 3.5, 'How Layers Communicate', 
            ha='center', va='top', fontsize=18, fontweight='bold', color=TEXT_COLOR)
    
    # Layers
    layers = [
        {'x': 1, 'name': 'Layer 1', 'output': 'Validated\nRequest'},
        {'x': 3, 'name': 'Layer 2', 'output': 'Query\nAnalysis'},
        {'x': 5, 'name': 'Layer 3', 'output': 'Raw\nData'},
        {'x': 7, 'name': 'Layer 4', 'output': 'Insights'},
        {'x': 9, 'name': 'Layer 5', 'output': 'Charts'},
        {'x': 11, 'name': 'Layer 6', 'output': 'JSON\nResponse'},
    ]
    
    colors = ['#ff9999', '#ffcc99', '#ffff99', '#99ff99', '#99ccff', '#cc99ff']
    
    for i, layer in enumerate(layers):
        # Layer box
        box = FancyBboxPatch((layer['x'] - 0.4, 1.5), 0.8, 0.8,
                            boxstyle="round,pad=0.05", edgecolor=TEXT_COLOR,
                            facecolor=colors[i], linewidth=2)
        ax.add_patch(box)
        ax.text(layer['x'], 1.9, layer['name'], ha='center', va='center',
                fontsize=9, fontweight='bold', color=TEXT_COLOR)
        
        # Output label
        if i < len(layers) - 1:
            ax.text(layer['x'] + 1, 2.5, layer['output'], ha='center', va='center',
                    fontsize=8, color=TEXT_COLOR, style='italic')
            
            # Arrow
            arrow = FancyArrowPatch((layer['x'] + 0.4, 1.9), (layers[i+1]['x'] - 0.4, 1.9),
                                   arrowstyle='->', mutation_scale=15, linewidth=2, color=PRIMARY_COLOR)
            ax.add_patch(arrow)
    
    # Key principle
    ax.text(6.5, 0.5, 'Key Principle: Each layer only knows about its immediate neighbors',
            ha='center', va='center', fontsize=11, style='italic', color=TEXT_COLOR,
            bbox=dict(boxstyle='round,pad=0.5', facecolor=SECONDARY_COLOR, edgecolor=BORDER_COLOR))
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'layer_communication.png', dpi=300, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.close()
    print("✅ Created: layer_communication.png")

def create_bulletproof_vs_llm():
    """Create diagram showing two processing paths"""
    fig, ax = plt.subplots(figsize=(12, 8))
    ax.set_xlim(0, 12)
    ax.set_ylim(0, 10)
    ax.axis('off')
    
    # Title
    ax.text(6, 9.5, 'Layer 2: Two Processing Paths', 
            ha='center', va='top', fontsize=18, fontweight='bold', color=TEXT_COLOR)
    
    # Input
    input_box = FancyBboxPatch((4.5, 8), 3, 0.6, boxstyle="round,pad=0.1",
                              edgecolor=PRIMARY_COLOR, facecolor='white', linewidth=2)
    ax.add_patch(input_box)
    ax.text(6, 8.3, 'User Question', ha='center', va='center', fontsize=12, fontweight='bold')
    
    # Decision diamond
    diamond = mpatches.FancyBboxPatch((5.2, 6.5), 1.6, 1, boxstyle="round,pad=0.1",
                                     edgecolor=TEXT_COLOR, facecolor='#ffffcc', linewidth=2,
                                     transform=ax.transData)
    ax.add_patch(diamond)
    ax.text(6, 7, 'Pattern\nMatch?', ha='center', va='center', fontsize=10, fontweight='bold')
    
    # Bulletproof path (left)
    bp_box = FancyBboxPatch((0.5, 4), 4, 2, boxstyle="round,pad=0.1",
                           edgecolor='#2ecc71', facecolor='#d5f4e6', linewidth=3)
    ax.add_patch(bp_box)
    ax.text(2.5, 5.5, '✓ BULLETPROOF PATH', ha='center', va='top',
            fontsize=12, fontweight='bold', color='#27ae60')
    ax.text(2.5, 5.1, 'Fast: ~5ms', ha='center', va='top', fontsize=10, color='#27ae60')
    ax.text(2.5, 4.7, '• Pre-built SQL templates', ha='left', va='top', fontsize=9)
    ax.text(2.5, 4.4, '• Pattern recognition', ha='left', va='top', fontsize=9)
    ax.text(2.5, 4.1, '• Zero LLM cost', ha='left', va='top', fontsize=9)
    
    # LLM path (right)
    llm_box = FancyBboxPatch((7.5, 4), 4, 2, boxstyle="round,pad=0.1",
                            edgecolor='#e74c3c', facecolor='#fadbd8', linewidth=3)
    ax.add_patch(llm_box)
    ax.text(9.5, 5.5, '⚡ LLM PATH', ha='center', va='top',
            fontsize=12, fontweight='bold', color='#c0392b')
    ax.text(9.5, 5.1, 'Flexible: ~800ms', ha='center', va='top', fontsize=10, color='#c0392b')
    ax.text(9.5, 4.7, '• AI-powered analysis', ha='left', va='top', fontsize=9)
    ax.text(9.5, 4.4, '• Handles complex queries', ha='left', va='top', fontsize=9)
    ax.text(9.5, 4.1, '• Dynamic SQL generation', ha='left', va='top', fontsize=9)
    
    # Arrows
    arrow1 = FancyArrowPatch((5.5, 6.5), (2.5, 6),
                            arrowstyle='->', mutation_scale=20, linewidth=2, color='#27ae60')
    ax.add_patch(arrow1)
    ax.text(3.5, 6.5, 'YES', ha='center', va='bottom', fontsize=9, color='#27ae60', fontweight='bold')
    
    arrow2 = FancyArrowPatch((6.5, 6.5), (9.5, 6),
                            arrowstyle='->', mutation_scale=20, linewidth=2, color='#c0392b')
    ax.add_patch(arrow2)
    ax.text(8.5, 6.5, 'NO', ha='center', va='bottom', fontsize=9, color='#c0392b', fontweight='bold')
    
    # Output
    output_box = FancyBboxPatch((4.5, 2.5), 3, 0.6, boxstyle="round,pad=0.1",
                               edgecolor=PRIMARY_COLOR, facecolor='white', linewidth=2)
    ax.add_patch(output_box)
    ax.text(6, 2.8, 'SQL Query + Analysis', ha='center', va='center', fontsize=12, fontweight='bold')
    
    # Arrows to output
    arrow3 = FancyArrowPatch((2.5, 4), (5.5, 3.1),
                            arrowstyle='->', mutation_scale=20, linewidth=2, color=PRIMARY_COLOR)
    ax.add_patch(arrow3)
    arrow4 = FancyArrowPatch((9.5, 4), (6.5, 3.1),
                            arrowstyle='->', mutation_scale=20, linewidth=2, color=PRIMARY_COLOR)
    ax.add_patch(arrow4)
    
    # Stats
    ax.text(6, 1.5, '80% of queries use Bulletproof Path | 20% use LLM Path',
            ha='center', va='center', fontsize=10, style='italic', color=TEXT_COLOR,
            bbox=dict(boxstyle='round,pad=0.5', facecolor=SECONDARY_COLOR, edgecolor=BORDER_COLOR))
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'bulletproof_vs_llm.png', dpi=300, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.close()
    print("✅ Created: bulletproof_vs_llm.png")

def main():
    print("=" * 60)
    print("Generating Documentation Diagrams")
    print("=" * 60)
    print()
    
    create_six_layer_architecture()
    create_request_flow()
    create_layer_communication()
    create_bulletproof_vs_llm()
    
    print()
    print("=" * 60)
    print(f"✅ All diagrams created in: {OUTPUT_DIR}")
    print("=" * 60)

if __name__ == "__main__":
    main()
