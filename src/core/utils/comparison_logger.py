"""
Logging infrastructure for RAG vs Standard executive summary comparison.
Tracks quality, performance, and cost metrics for offline analysis.
"""

import json
import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import pandas as pd


class ComparisonLogger:
    """Logger for tracking RAG vs Standard summary comparisons."""
    
    def __init__(self, log_dir: str = "logs/rag_comparison"):
        """Initialize comparison logger.
        
        Args:
            log_dir: Directory to store comparison logs
        """
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories
        self.json_dir = self.log_dir / "json"
        self.csv_dir = self.log_dir / "csv"
        self.json_dir.mkdir(exist_ok=True)
        self.csv_dir.mkdir(exist_ok=True)
        
        # CSV file paths
        self.metrics_csv = self.csv_dir / "comparison_metrics.csv"
        self.feedback_csv = self.csv_dir / "user_feedback.csv"
        
        # Initialize CSV files if they don't exist
        self._init_csv_files()
    
    def _init_csv_files(self):
        """Initialize CSV files with headers."""
        # Metrics CSV
        if not self.metrics_csv.exists():
            metrics_headers = [
                'timestamp', 'session_id', 'campaign_id',
                'standard_tokens_input', 'standard_tokens_output', 'standard_cost', 'standard_latency',
                'rag_tokens_input', 'rag_tokens_output', 'rag_cost', 'rag_latency',
                'token_increase_pct', 'cost_increase_pct', 'latency_increase_pct',
                'rag_sources_count', 'rag_sources_list'
            ]
            with open(self.metrics_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=metrics_headers)
                writer.writeheader()
        
        # Feedback CSV
        if not self.feedback_csv.exists():
            feedback_headers = [
                'timestamp', 'session_id', 'campaign_id',
                'user_preference', 'quality_rating', 'usefulness_rating',
                'comments', 'preferred_method'
            ]
            with open(self.feedback_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=feedback_headers)
                writer.writeheader()
    
    def log_comparison(self, 
                      session_id: str,
                      campaign_id: str,
                      standard_result: Dict[str, Any],
                      rag_result: Dict[str, Any],
                      metadata: Optional[Dict[str, Any]] = None) -> str:
        """Log a comparison between standard and RAG summaries.
        
        Args:
            session_id: Unique session identifier
            campaign_id: Campaign identifier
            standard_result: Standard summary result with metrics
            rag_result: RAG summary result with metrics
            metadata: Additional metadata
            
        Returns:
            Path to the saved JSON log file
        """
        timestamp = datetime.now()
        
        # Calculate percentage increases
        token_increase = self._calc_percentage_increase(
            standard_result.get('tokens_input', 0),
            rag_result.get('tokens_input', 0)
        )
        cost_increase = self._calc_percentage_increase(
            standard_result.get('cost', 0),
            rag_result.get('cost', 0)
        )
        latency_increase = self._calc_percentage_increase(
            standard_result.get('latency', 0),
            rag_result.get('latency', 0)
        )
        
        # Prepare comparison data
        comparison_data = {
            'timestamp': timestamp.isoformat(),
            'session_id': session_id,
            'campaign_id': campaign_id,
            'standard': {
                'summary_brief': standard_result.get('summary_brief', ''),
                'summary_detailed': standard_result.get('summary_detailed', ''),
                'tokens_input': standard_result.get('tokens_input', 0),
                'tokens_output': standard_result.get('tokens_output', 0),
                'cost': standard_result.get('cost', 0.0),
                'latency': standard_result.get('latency', 0.0),
                'model': standard_result.get('model', 'unknown')
            },
            'rag': {
                'summary_brief': rag_result.get('summary_brief', ''),
                'summary_detailed': rag_result.get('summary_detailed', ''),
                'tokens_input': rag_result.get('tokens_input', 0),
                'tokens_output': rag_result.get('tokens_output', 0),
                'cost': rag_result.get('cost', 0.0),
                'latency': rag_result.get('latency', 0.0),
                'model': rag_result.get('model', 'unknown'),
                'knowledge_sources': rag_result.get('knowledge_sources', []),
                'retrieval_count': len(rag_result.get('knowledge_sources', []))
            },
            'comparison': {
                'token_increase_pct': token_increase,
                'cost_increase_pct': cost_increase,
                'latency_increase_pct': latency_increase
            },
            'metadata': metadata or {}
        }
        
        # Save to JSON
        json_filename = f"{timestamp.strftime('%Y%m%d_%H%M%S')}_{session_id[:8]}.json"
        json_path = self.json_dir / json_filename
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(comparison_data, f, indent=2, ensure_ascii=False)
        
        # Append to CSV
        csv_row = {
            'timestamp': timestamp.isoformat(),
            'session_id': session_id,
            'campaign_id': campaign_id,
            'standard_tokens_input': standard_result.get('tokens_input', 0),
            'standard_tokens_output': standard_result.get('tokens_output', 0),
            'standard_cost': standard_result.get('cost', 0.0),
            'standard_latency': standard_result.get('latency', 0.0),
            'rag_tokens_input': rag_result.get('tokens_input', 0),
            'rag_tokens_output': rag_result.get('tokens_output', 0),
            'rag_cost': rag_result.get('cost', 0.0),
            'rag_latency': rag_result.get('latency', 0.0),
            'token_increase_pct': token_increase,
            'cost_increase_pct': cost_increase,
            'latency_increase_pct': latency_increase,
            'rag_sources_count': len(rag_result.get('knowledge_sources', [])),
            'rag_sources_list': json.dumps(rag_result.get('knowledge_sources', []))
        }
        
        with open(self.metrics_csv, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=csv_row.keys())
            writer.writerow(csv_row)
        
        return str(json_path)
    
    def log_feedback(self,
                    session_id: str,
                    campaign_id: str,
                    user_preference: str,
                    quality_rating: Optional[int] = None,
                    usefulness_rating: Optional[int] = None,
                    comments: Optional[str] = None):
        """Log user feedback on comparison.
        
        Args:
            session_id: Unique session identifier
            campaign_id: Campaign identifier
            user_preference: 'standard', 'rag', or 'same'
            quality_rating: 1-5 rating for quality
            usefulness_rating: 1-5 rating for usefulness
            comments: Optional user comments
        """
        timestamp = datetime.now()
        
        feedback_row = {
            'timestamp': timestamp.isoformat(),
            'session_id': session_id,
            'campaign_id': campaign_id,
            'user_preference': user_preference,
            'quality_rating': quality_rating or '',
            'usefulness_rating': usefulness_rating or '',
            'comments': comments or '',
            'preferred_method': user_preference
        }
        
        with open(self.feedback_csv, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=feedback_row.keys())
            writer.writerow(feedback_row)
    
    def get_summary_stats(self) -> Dict[str, Any]:
        """Get summary statistics from logged comparisons.
        
        Returns:
            Dictionary with summary statistics
        """
        try:
            df = pd.read_csv(self.metrics_csv)
            
            if df.empty:
                return {'message': 'No comparison data available'}
            
            stats = {
                'total_comparisons': len(df),
                'avg_token_increase_pct': df['token_increase_pct'].mean(),
                'avg_cost_increase_pct': df['cost_increase_pct'].mean(),
                'avg_latency_increase_pct': df['latency_increase_pct'].mean(),
                'avg_standard_cost': df['standard_cost'].mean(),
                'avg_rag_cost': df['rag_cost'].mean(),
                'avg_standard_latency': df['standard_latency'].mean(),
                'avg_rag_latency': df['rag_latency'].mean(),
                'avg_rag_sources': df['rag_sources_count'].mean()
            }
            
            # Add feedback stats if available
            if self.feedback_csv.exists():
                feedback_df = pd.read_csv(self.feedback_csv)
                if not feedback_df.empty:
                    stats['total_feedback'] = len(feedback_df)
                    stats['rag_preference_pct'] = (
                        (feedback_df['user_preference'] == 'rag').sum() / len(feedback_df) * 100
                    )
                    stats['standard_preference_pct'] = (
                        (feedback_df['user_preference'] == 'standard').sum() / len(feedback_df) * 100
                    )
                    if 'quality_rating' in feedback_df.columns:
                        stats['avg_quality_rating'] = feedback_df['quality_rating'].mean()
                    if 'usefulness_rating' in feedback_df.columns:
                        stats['avg_usefulness_rating'] = feedback_df['usefulness_rating'].mean()
            
            return stats
        
        except Exception as e:
            return {'error': str(e)}
    
    def export_analysis_report(self, output_path: Optional[str] = None) -> str:
        """Export comprehensive analysis report.
        
        Args:
            output_path: Optional custom output path
            
        Returns:
            Path to the generated report
        """
        if output_path is None:
            output_path = self.log_dir / f"analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        
        stats = self.get_summary_stats()
        
        report = f"""# RAG vs Standard Executive Summary Analysis Report

**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Summary Statistics

- **Total Comparisons:** {stats.get('total_comparisons', 0)}
- **Average Token Increase:** {stats.get('avg_token_increase_pct', 0):.2f}%
- **Average Cost Increase:** {stats.get('avg_cost_increase_pct', 0):.2f}%
- **Average Latency Increase:** {stats.get('avg_latency_increase_pct', 0):.2f}%

## Performance Metrics

### Standard Method
- **Average Cost:** ${stats.get('avg_standard_cost', 0):.4f}
- **Average Latency:** {stats.get('avg_standard_latency', 0):.2f}s

### RAG Method
- **Average Cost:** ${stats.get('avg_rag_cost', 0):.4f}
- **Average Latency:** {stats.get('avg_rag_latency', 0):.2f}s
- **Average Knowledge Sources:** {stats.get('avg_rag_sources', 0):.1f}

## User Feedback

- **Total Feedback Responses:** {stats.get('total_feedback', 0)}
- **RAG Preference:** {stats.get('rag_preference_pct', 0):.1f}%
- **Standard Preference:** {stats.get('standard_preference_pct', 0):.1f}%
- **Average Quality Rating:** {stats.get('avg_quality_rating', 0):.2f}/5
- **Average Usefulness Rating:** {stats.get('avg_usefulness_rating', 0):.2f}/5

## Recommendation

"""
        
        # Add recommendation based on stats
        if stats.get('total_comparisons', 0) >= 10:
            rag_pref = stats.get('rag_preference_pct', 0)
            cost_increase = stats.get('avg_cost_increase_pct', 0)
            
            if rag_pref > 60 and cost_increase < 100:
                report += "✅ **RECOMMEND RAG INTEGRATION** - Strong user preference with acceptable cost increase.\n"
            elif rag_pref > 40 and cost_increase < 50:
                report += "⚠️ **CONSIDER RAG INTEGRATION** - Moderate user preference with low cost increase.\n"
            else:
                report += "❌ **DO NOT INTEGRATE YET** - Insufficient user preference or high cost increase.\n"
        else:
            report += "⏳ **INSUFFICIENT DATA** - Collect more comparisons before making a decision.\n"
        
        report += f"\n## Data Files\n\n- Metrics CSV: `{self.metrics_csv}`\n- Feedback CSV: `{self.feedback_csv}`\n- JSON Logs: `{self.json_dir}`\n"
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(report)
        
        return str(output_path)
    
    @staticmethod
    def _calc_percentage_increase(baseline: float, new_value: float) -> float:
        """Calculate percentage increase."""
        if baseline == 0:
            return 0.0
        return ((new_value - baseline) / baseline) * 100
