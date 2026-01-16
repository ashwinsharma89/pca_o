"""
Feedback Loop System for PCA Agent
Collects user feedback to improve AI recommendations
"""
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
from loguru import logger


class FeedbackSystem:
    """System to collect and analyze user feedback on insights and recommendations."""
    
    def __init__(self, feedback_dir: str = "./data/feedback"):
        """Initialize feedback system."""
        self.feedback_dir = Path(feedback_dir)
        self.feedback_dir.mkdir(parents=True, exist_ok=True)
        self.feedback_file = self.feedback_dir / "feedback_log.jsonl"
        self.analytics_file = self.feedback_dir / "feedback_analytics.json"
        logger.info(f"Initialized FeedbackSystem at {self.feedback_dir}")
    
    def record_insight_feedback(
        self,
        insight_id: str,
        insight_text: str,
        category: str,
        rating: int,
        comment: Optional[str] = None,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Record feedback on an AI-generated insight.
        
        Args:
            insight_id: Unique identifier for the insight
            insight_text: The actual insight text
            category: Category of insight (Funnel, ROAS, Audience, etc.)
            rating: 1-5 star rating
            comment: Optional user comment
            user_id: Optional user identifier
            session_id: Optional session identifier
            
        Returns:
            Feedback record
        """
        feedback = {
            "type": "insight",
            "insight_id": insight_id,
            "insight_text": insight_text,
            "category": category,
            "rating": rating,
            "comment": comment,
            "user_id": user_id,
            "session_id": session_id,
            "timestamp": datetime.now().isoformat(),
            "version": "1.0"
        }
        
        self._save_feedback(feedback)
        logger.info(f"Recorded insight feedback: {insight_id} - Rating: {rating}/5")
        
        return feedback
    
    def record_recommendation_feedback(
        self,
        recommendation_id: str,
        recommendation_text: str,
        priority: str,
        implemented: bool,
        effectiveness_rating: Optional[int] = None,
        actual_impact: Optional[str] = None,
        comment: Optional[str] = None,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Record feedback on a recommendation.
        
        Args:
            recommendation_id: Unique identifier
            recommendation_text: The recommendation
            priority: Priority level
            implemented: Whether user implemented it
            effectiveness_rating: 1-5 rating if implemented
            actual_impact: Actual results if implemented
            comment: User comment
            user_id: User identifier
            session_id: Session identifier
            
        Returns:
            Feedback record
        """
        feedback = {
            "type": "recommendation",
            "recommendation_id": recommendation_id,
            "recommendation_text": recommendation_text,
            "priority": priority,
            "implemented": implemented,
            "effectiveness_rating": effectiveness_rating,
            "actual_impact": actual_impact,
            "comment": comment,
            "user_id": user_id,
            "session_id": session_id,
            "timestamp": datetime.now().isoformat(),
            "version": "1.0"
        }
        
        self._save_feedback(feedback)
        logger.info(f"Recorded recommendation feedback: {recommendation_id} - Implemented: {implemented}")
        
        return feedback
    
    def record_analysis_feedback(
        self,
        session_id: str,
        overall_rating: int,
        usefulness_rating: int,
        accuracy_rating: int,
        actionability_rating: int,
        most_valuable: Optional[str] = None,
        least_valuable: Optional[str] = None,
        missing_insights: Optional[str] = None,
        comment: Optional[str] = None,
        user_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Record overall feedback on the analysis.
        
        Args:
            session_id: Analysis session ID
            overall_rating: Overall satisfaction (1-5)
            usefulness_rating: How useful (1-5)
            accuracy_rating: How accurate (1-5)
            actionability_rating: How actionable (1-5)
            most_valuable: What was most valuable
            least_valuable: What was least valuable
            missing_insights: What insights were missing
            comment: General comment
            user_id: User identifier
            
        Returns:
            Feedback record
        """
        feedback = {
            "type": "analysis",
            "session_id": session_id,
            "overall_rating": overall_rating,
            "usefulness_rating": usefulness_rating,
            "accuracy_rating": accuracy_rating,
            "actionability_rating": actionability_rating,
            "most_valuable": most_valuable,
            "least_valuable": least_valuable,
            "missing_insights": missing_insights,
            "comment": comment,
            "user_id": user_id,
            "timestamp": datetime.now().isoformat(),
            "version": "1.0"
        }
        
        self._save_feedback(feedback)
        logger.info(f"Recorded analysis feedback: Session {session_id} - Rating: {overall_rating}/5")
        
        return feedback
    
    def get_feedback_analytics(self) -> Dict[str, Any]:
        """
        Generate analytics from collected feedback.
        
        Returns:
            Analytics summary
        """
        if not self.feedback_file.exists():
            return {"message": "No feedback data available"}
        
        # Load all feedback
        feedbacks = []
        with open(self.feedback_file, 'r') as f:
            for line in f:
                feedbacks.append(json.loads(line))
        
        if not feedbacks:
            return {"message": "No feedback data available"}
        
        df = pd.DataFrame(feedbacks)
        
        analytics = {
            "total_feedback_count": len(feedbacks),
            "feedback_by_type": df['type'].value_counts().to_dict(),
            "timestamp": datetime.now().isoformat()
        }
        
        # Insight analytics
        insight_feedback = df[df['type'] == 'insight']
        if len(insight_feedback) > 0:
            analytics["insights"] = {
                "total_count": len(insight_feedback),
                "average_rating": float(insight_feedback['rating'].mean()),
                "rating_distribution": insight_feedback['rating'].value_counts().to_dict(),
                "by_category": insight_feedback.groupby('category')['rating'].agg(['count', 'mean']).to_dict('index'),
                "top_rated_categories": insight_feedback.groupby('category')['rating'].mean().nlargest(3).to_dict(),
                "low_rated_categories": insight_feedback.groupby('category')['rating'].mean().nsmallest(3).to_dict()
            }
        
        # Recommendation analytics
        rec_feedback = df[df['type'] == 'recommendation']
        if len(rec_feedback) > 0:
            analytics["recommendations"] = {
                "total_count": len(rec_feedback),
                "implementation_rate": float((rec_feedback['implemented'].sum() / len(rec_feedback)) * 100),
                "implemented_count": int(rec_feedback['implemented'].sum()),
                "not_implemented_count": int((~rec_feedback['implemented']).sum()),
                "by_priority": rec_feedback.groupby('priority')['implemented'].agg(['count', 'sum']).to_dict('index')
            }
            
            # Effectiveness of implemented recommendations
            implemented = rec_feedback[rec_feedback['implemented'] == True]
            if len(implemented) > 0 and 'effectiveness_rating' in implemented.columns:
                implemented_rated = implemented[implemented['effectiveness_rating'].notna()]
                if len(implemented_rated) > 0:
                    analytics["recommendations"]["effectiveness"] = {
                        "average_rating": float(implemented_rated['effectiveness_rating'].mean()),
                        "rating_distribution": implemented_rated['effectiveness_rating'].value_counts().to_dict()
                    }
        
        # Analysis analytics
        analysis_feedback = df[df['type'] == 'analysis']
        if len(analysis_feedback) > 0:
            analytics["analysis"] = {
                "total_count": len(analysis_feedback),
                "average_overall_rating": float(analysis_feedback['overall_rating'].mean()),
                "average_usefulness": float(analysis_feedback['usefulness_rating'].mean()),
                "average_accuracy": float(analysis_feedback['accuracy_rating'].mean()),
                "average_actionability": float(analysis_feedback['actionability_rating'].mean()),
                "rating_trends": {
                    "overall": analysis_feedback['overall_rating'].value_counts().to_dict(),
                    "usefulness": analysis_feedback['usefulness_rating'].value_counts().to_dict(),
                    "accuracy": analysis_feedback['accuracy_rating'].value_counts().to_dict(),
                    "actionability": analysis_feedback['actionability_rating'].value_counts().to_dict()
                }
            }
        
        # Save analytics
        with open(self.analytics_file, 'w') as f:
            json.dump(analytics, f, indent=2)
        
        return analytics
    
    def get_improvement_suggestions(self) -> List[Dict[str, str]]:
        """
        Generate improvement suggestions based on feedback.
        
        Returns:
            List of improvement suggestions
        """
        analytics = self.get_feedback_analytics()
        suggestions = []
        
        if "insights" in analytics:
            # Low-rated categories
            if "low_rated_categories" in analytics["insights"]:
                for category, rating in analytics["insights"]["low_rated_categories"].items():
                    if rating < 3.5:
                        suggestions.append({
                            "area": "Insights",
                            "category": category,
                            "issue": f"Low average rating of {rating:.2f}/5",
                            "suggestion": f"Improve {category} insights by adding more specific data points and actionable recommendations"
                        })
        
        if "recommendations" in analytics:
            # Low implementation rate
            impl_rate = analytics["recommendations"].get("implementation_rate", 0)
            if impl_rate < 50:
                suggestions.append({
                    "area": "Recommendations",
                    "category": "Implementation",
                    "issue": f"Only {impl_rate:.1f}% implementation rate",
                    "suggestion": "Make recommendations more specific, actionable, and easier to implement. Include step-by-step guides."
                })
        
        if "analysis" in analytics:
            # Low accuracy rating
            accuracy = analytics["analysis"].get("average_accuracy", 0)
            if accuracy < 3.5:
                suggestions.append({
                    "area": "Analysis",
                    "category": "Accuracy",
                    "issue": f"Low accuracy rating of {accuracy:.2f}/5",
                    "suggestion": "Improve data validation and cross-check insights against industry benchmarks"
                })
            
            # Low actionability
            actionability = analytics["analysis"].get("average_actionability", 0)
            if actionability < 3.5:
                suggestions.append({
                    "area": "Analysis",
                    "category": "Actionability",
                    "issue": f"Low actionability rating of {actionability:.2f}/5",
                    "suggestion": "Provide more specific, step-by-step action items with expected timelines and ROI"
                })
        
        return suggestions
    
    def export_feedback(self, format: str = "csv") -> str:
        """
        Export feedback data.
        
        Args:
            format: Export format (csv, json, excel)
            
        Returns:
            Path to exported file
        """
        if not self.feedback_file.exists():
            return None
        
        # Load feedback
        feedbacks = []
        with open(self.feedback_file, 'r') as f:
            for line in f:
                feedbacks.append(json.loads(line))
        
        df = pd.DataFrame(feedbacks)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if format == "csv":
            export_path = self.feedback_dir / f"feedback_export_{timestamp}.csv"
            df.to_csv(export_path, index=False)
        elif format == "excel":
            export_path = self.feedback_dir / f"feedback_export_{timestamp}.xlsx"
            df.to_excel(export_path, index=False)
        else:  # json
            export_path = self.feedback_dir / f"feedback_export_{timestamp}.json"
            df.to_json(export_path, orient='records', indent=2)
        
        logger.info(f"Exported feedback to {export_path}")
        return str(export_path)
    
    def _save_feedback(self, feedback: Dict[str, Any]):
        """Save feedback to JSONL file."""
        with open(self.feedback_file, 'a') as f:
            f.write(json.dumps(feedback) + '\n')


class FeedbackPromptImprover:
    """Uses feedback to improve AI prompts over time."""
    
    def __init__(self, feedback_system: FeedbackSystem):
        """Initialize with feedback system."""
        self.feedback_system = feedback_system
    
    def get_enhanced_prompt_context(self, category: str) -> str:
        """
        Get additional context for prompts based on feedback.
        
        Args:
            category: Category of insight/recommendation
            
        Returns:
            Additional prompt context
        """
        analytics = self.feedback_system.get_feedback_analytics()
        
        context = ""
        
        # Add context based on low-rated categories
        if "insights" in analytics and "low_rated_categories" in analytics["insights"]:
            if category in analytics["insights"]["low_rated_categories"]:
                rating = analytics["insights"]["low_rated_categories"][category]
                context += f"\nNote: {category} insights have received lower ratings ({rating:.2f}/5). "
                context += "Ensure insights are highly specific, data-driven, and include clear action items. "
        
        # Add context based on high-rated categories
        if "insights" in analytics and "top_rated_categories" in analytics["insights"]:
            if category in analytics["insights"]["top_rated_categories"]:
                context += f"\n{category} insights have been well-received. Continue this approach. "
        
        return context
    
    def get_recommendation_improvements(self) -> Dict[str, str]:
        """
        Get improvements for recommendation generation.
        
        Returns:
            Dictionary of improvement guidelines
        """
        analytics = self.feedback_system.get_feedback_analytics()
        improvements = {}
        
        if "recommendations" in analytics:
            impl_rate = analytics["recommendations"].get("implementation_rate", 0)
            
            if impl_rate < 50:
                improvements["specificity"] = "Make recommendations more specific with exact numbers and steps"
                improvements["feasibility"] = "Ensure recommendations are realistic and achievable"
                improvements["timeline"] = "Provide clear timelines (immediate, 1 week, 1 month)"
            
            # Check priority-specific feedback
            if "by_priority" in analytics["recommendations"]:
                for priority, stats in analytics["recommendations"]["by_priority"].items():
                    impl_count = stats.get('sum', 0)
                    total_count = stats.get('count', 1)
                    priority_rate = (impl_count / total_count) * 100
                    
                    if priority_rate < 40:
                        improvements[f"{priority}_priority"] = f"{priority} priority recommendations have low implementation rate. Make them more compelling and easier to execute."
        
        return improvements
