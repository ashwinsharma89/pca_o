"""
Campaign Success Predictor - Quick Win Module
Predicts campaign success probability before launch
"""
import pandas as pd
import numpy as np
from typing import Dict, List
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import LabelEncoder
import joblib
from loguru import logger


class CampaignSuccessPredictor:
    """
    Predict 0-100% probability of campaign success
    Quick win implementation - 2-3 weeks to deploy
    """
    
    def __init__(self):
        self.model = None
        self.label_encoders = {}
        self.feature_names = [
            'budget', 'duration', 'channel_count', 'audience_size',
            'creative_type', 'objective', 'timing_score',
            'historical_avg_roas', 'historical_campaign_count',
            'month', 'quarter', 'day_of_week'
        ]
        self.feature_importance = {}
        self.model_metrics = {}
    
    def train(
        self,
        historical_campaigns: pd.DataFrame,
        success_threshold: Dict = None
    ) -> Dict:
        """
        Train model on historical campaigns
        
        Args:
            historical_campaigns: DataFrame with campaign data
            success_threshold: Dict defining success criteria
                e.g., {'roas': 3.0, 'cpa': 75}
                
        Returns:
            Dict with training metrics
        """
        logger.info(f"Training model on {len(historical_campaigns)} historical campaigns")
        
        # Define success if not provided
        if success_threshold is None:
            success_threshold = {'roas': 3.0}
        
        # Create success label
        historical_campaigns['success'] = self._define_success(
            historical_campaigns,
            success_threshold
        )
        
        # Feature engineering
        X = self._engineer_features(historical_campaigns)
        y = historical_campaigns['success'].astype(int)
        
        # Train-test split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        # Train Random Forest
        self.model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            min_samples_split=5,
            min_samples_leaf=2,
            random_state=42,
            class_weight='balanced'
        )
        
        self.model.fit(X_train, y_train)
        
        # Evaluate
        train_score = self.model.score(X_train, y_train)
        test_score = self.model.score(X_test, y_test)
        
        # Cross-validation
        cv_scores = cross_val_score(
            self.model, X_train, y_train, cv=5, scoring='accuracy'
        )
        
        # Feature importance
        self.feature_importance = dict(zip(
            self.feature_names,
            self.model.feature_importances_
        ))
        
        # Store metrics
        self.model_metrics = {
            'train_accuracy': round(train_score, 3),
            'test_accuracy': round(test_score, 3),
            'cv_mean_accuracy': round(cv_scores.mean(), 3),
            'cv_std_accuracy': round(cv_scores.std(), 3),
            'training_samples': len(X_train),
            'test_samples': len(X_test),
            'success_rate': round(y.mean(), 3)
        }
        
        logger.info(f"Model trained - Test Accuracy: {test_score:.3f}")
        
        return self.model_metrics
    
    def _define_success(
        self,
        campaigns: pd.DataFrame,
        threshold: Dict
    ) -> pd.Series:
        """
        Define campaign success based on thresholds
        """
        success = pd.Series([True] * len(campaigns), index=campaigns.index)
        
        for metric, value in threshold.items():
            if metric in campaigns.columns:
                if metric in ['roas', 'roi']:
                    success &= (campaigns[metric] >= value)
                elif metric in ['cpa', 'cpc', 'cpm']:
                    success &= (campaigns[metric] <= value)
        
        return success
    
    def _engineer_features(self, campaigns: pd.DataFrame) -> pd.DataFrame:
        """
        Engineer features for ML model
        """
        features = pd.DataFrame()
        
        # Numeric features
        features['budget'] = campaigns['budget']
        features['duration'] = campaigns['duration']
        features['audience_size'] = campaigns['audience_size']
        
        # Channel count
        if 'channels' in campaigns.columns:
            features['channel_count'] = campaigns['channels'].apply(
                lambda x: len(x.split(',')) if isinstance(x, str) else 1
            )
        else:
            features['channel_count'] = 1
        
        # Categorical features (encode)
        categorical_features = ['creative_type', 'objective']
        for feature in categorical_features:
            if feature in campaigns.columns:
                if feature not in self.label_encoders:
                    self.label_encoders[feature] = LabelEncoder()
                    features[feature] = self.label_encoders[feature].fit_transform(
                        campaigns[feature].fillna('unknown')
                    )
                else:
                    features[feature] = self.label_encoders[feature].transform(
                        campaigns[feature].fillna('unknown')
                    )
            else:
                features[feature] = 0
        
        # Timing features
        if 'start_date' in campaigns.columns:
            start_dates = pd.to_datetime(campaigns['start_date'])
            features['month'] = start_dates.dt.month
            features['quarter'] = start_dates.dt.quarter
            features['day_of_week'] = start_dates.dt.dayofweek
            features['timing_score'] = self._calculate_timing_score(start_dates)
        else:
            features['month'] = 1
            features['quarter'] = 1
            features['day_of_week'] = 0
            features['timing_score'] = 50
        
        # Historical performance
        if 'advertiser_id' in campaigns.columns:
            features['historical_avg_roas'] = campaigns.groupby('advertiser_id')['roas'].transform('mean')
            features['historical_campaign_count'] = campaigns.groupby('advertiser_id').cumcount()
        else:
            features['historical_avg_roas'] = campaigns.get('roas', 3.0)
            features['historical_campaign_count'] = 0
        
        # Fill any missing values
        features = features.fillna(0)
        
        return features[self.feature_names]
    
    def _calculate_timing_score(self, dates: pd.Series) -> pd.Series:
        """
        Calculate timing score (0-100) based on seasonality
        """
        scores = pd.Series([50] * len(dates), index=dates.index)
        
        # Higher scores for good timing
        month = dates.dt.month
        
        # Q4 (Oct-Dec) - Holiday season
        scores[month.isin([10, 11, 12])] = 80
        
        # Q1 (Jan-Mar) - New Year, Tax season
        scores[month.isin([1, 2, 3])] = 70
        
        # Q2 (Apr-Jun) - Spring
        scores[month.isin([4, 5, 6])] = 60
        
        # Q3 (Jul-Sep) - Summer (typically slower)
        scores[month.isin([7, 8, 9])] = 50
        
        # Avoid Fridays and weekends for B2B
        day_of_week = dates.dt.dayofweek
        scores[day_of_week.isin([4, 5, 6])] -= 10
        
        return scores.clip(0, 100)
    
    def predict_success_probability(
        self,
        campaign_plan: Dict
    ) -> Dict:
        """
        Predict success probability for a campaign plan
        
        Args:
            campaign_plan: Dict with campaign parameters
            
        Returns:
            Dict with prediction and insights
        """
        if self.model is None:
            raise ValueError("Model not trained. Call train() first.")
        
        logger.info(f"Predicting success for campaign: {campaign_plan.get('name', 'Unknown')}")
        
        # Convert to DataFrame for feature engineering
        campaign_df = pd.DataFrame([campaign_plan])
        
        # Engineer features
        features = self._engineer_features(campaign_df)
        
        # Predict probability
        prob = self.model.predict_proba(features)[0][1] * 100
        
        # Get feature contributions
        feature_values = features.iloc[0].to_dict()
        
        # Generate insights
        insights = self._generate_insights(
            prob,
            feature_values,
            campaign_plan
        )
        
        # Get top drivers
        top_drivers = self._identify_key_drivers(feature_values)
        
        # Generate recommendations
        recommendations = self._generate_recommendations(
            prob,
            feature_values,
            campaign_plan
        )
        
        return {
            'campaign_name': campaign_plan.get('name', 'Unknown'),
            'success_probability': round(prob, 1),
            'confidence_level': self._calculate_confidence(prob),
            'risk_level': self._assess_risk(prob),
            'key_drivers': top_drivers,
            'insights': insights,
            'recommendations': recommendations,
            'feature_values': feature_values
        }
    
    def _calculate_confidence(self, probability: float) -> str:
        """
        Calculate confidence level in prediction
        """
        # Confidence is higher when probability is extreme (very high or very low)
        distance_from_50 = abs(probability - 50)
        
        if distance_from_50 > 30:
            return 'high'
        elif distance_from_50 > 15:
            return 'medium'
        else:
            return 'low'
    
    def _assess_risk(self, probability: float) -> str:
        """
        Assess risk level
        """
        if probability >= 70:
            return 'low'
        elif probability >= 50:
            return 'medium'
        else:
            return 'high'
    
    def _identify_key_drivers(self, feature_values: Dict) -> List[Dict]:
        """
        Identify key drivers of prediction
        """
        drivers = []
        
        # Sort features by importance
        sorted_features = sorted(
            self.feature_importance.items(),
            key=lambda x: x[1],
            reverse=True
        )
        
        # Top 3 drivers
        for feature, importance in sorted_features[:3]:
            value = feature_values.get(feature, 0)
            
            drivers.append({
                'feature': feature,
                'importance': round(importance, 3),
                'value': value,
                'impact': self._interpret_feature_impact(feature, value)
            })
        
        return drivers
    
    def _interpret_feature_impact(self, feature: str, value: float) -> str:
        """
        Interpret feature impact on success
        """
        interpretations = {
            'budget': f"Budget of ${value:,.0f}",
            'duration': f"{int(value)} day campaign",
            'channel_count': f"{int(value)} channels",
            'audience_size': f"{int(value):,} audience size",
            'timing_score': f"Timing score: {int(value)}/100",
            'historical_avg_roas': f"Historical ROAS: {value:.2f}"
        }
        
        return interpretations.get(feature, f"{feature}: {value}")
    
    def _generate_insights(
        self,
        probability: float,
        features: Dict,
        campaign_plan: Dict
    ) -> List[str]:
        """
        Generate human-readable insights
        """
        insights = []
        
        # Overall assessment
        if probability >= 70:
            insights.append(f"✅ High success probability ({probability:.1f}%) - Strong campaign setup")
        elif probability >= 50:
            insights.append(f"⚠️ Medium success probability ({probability:.1f}%) - Some optimization needed")
        else:
            insights.append(f"❌ Low success probability ({probability:.1f}%) - Significant changes recommended")
        
        # Budget insights
        budget = features.get('budget', 0)
        if budget < 50000:
            insights.append("💰 Budget below recommended minimum - consider increasing for better results")
        elif budget > 500000:
            insights.append("💰 Large budget - ensure proper pacing and monitoring")
        
        # Channel insights
        channel_count = features.get('channel_count', 1)
        if channel_count == 1:
            insights.append("📱 Single channel - consider multi-channel approach for better reach")
        elif channel_count > 4:
            insights.append("📱 Many channels - ensure sufficient budget per channel")
        
        # Timing insights
        timing_score = features.get('timing_score', 50)
        if timing_score >= 70:
            insights.append("📅 Excellent timing - launching in high-performing period")
        elif timing_score < 50:
            insights.append("📅 Suboptimal timing - consider delaying to better period")
        
        # Historical performance
        historical_roas = features.get('historical_avg_roas', 0)
        if historical_roas >= 4.0:
            insights.append(f"📊 Strong historical performance (ROAS: {historical_roas:.2f}) - good foundation")
        elif historical_roas < 2.0:
            insights.append(f"📊 Historical performance below target - apply learnings from past campaigns")
        
        return insights
    
    def _generate_recommendations(
        self,
        probability: float,
        features: Dict,
        campaign_plan: Dict
    ) -> List[Dict]:
        """
        Generate actionable recommendations
        """
        recommendations = []
        
        if probability < 50:
            recommendations.append({
                'priority': 'high',
                'action': 'review_and_revise',
                'message': 'Campaign has low success probability - recommend comprehensive review',
                'expected_impact': 'Increase success probability by 20-30%'
            })
        
        # Budget recommendations
        budget = features.get('budget', 0)
        if budget < 50000:
            recommendations.append({
                'priority': 'high',
                'action': 'increase_budget',
                'message': 'Increase budget to at least $50,000 for meaningful results',
                'expected_impact': 'Improve success probability by 15-20%'
            })
        
        # Channel recommendations
        channel_count = features.get('channel_count', 1)
        if channel_count == 1 and budget > 100000:
            recommendations.append({
                'priority': 'medium',
                'action': 'add_channels',
                'message': 'Budget supports multi-channel approach - consider adding 1-2 channels',
                'expected_impact': 'Increase reach and reduce risk'
            })
        
        # Timing recommendations
        timing_score = features.get('timing_score', 50)
        if timing_score < 50:
            recommendations.append({
                'priority': 'medium',
                'action': 'adjust_timing',
                'message': 'Consider launching in Q4 or Q1 for better performance',
                'expected_impact': 'Improve success probability by 10-15%'
            })
        
        # Historical performance recommendations
        historical_roas = features.get('historical_avg_roas', 0)
        if historical_roas < 2.0:
            recommendations.append({
                'priority': 'high',
                'action': 'apply_learnings',
                'message': 'Review past campaigns and apply successful tactics',
                'expected_impact': 'Avoid repeating past mistakes'
            })
        
        return recommendations
    
    def save_model(self, filepath: str):
        """Save trained model to disk"""
        joblib.dump({
            'model': self.model,
            'label_encoders': self.label_encoders,
            'feature_importance': self.feature_importance,
            'model_metrics': self.model_metrics
        }, filepath)
        logger.info(f"Model saved to {filepath}")
    
    def load_model(self, filepath: str):
        """Load trained model from disk"""
        data = joblib.load(filepath)
        self.model = data['model']
        self.label_encoders = data['label_encoders']
        self.feature_importance = data['feature_importance']
        self.model_metrics = data['model_metrics']
        logger.info(f"Model loaded from {filepath}")


if __name__ == "__main__":
    # Example usage
    
    # Create sample historical data
    np.random.seed(42)
    n_campaigns = 100
    
    historical_campaigns = pd.DataFrame({
        'budget': np.random.uniform(50000, 500000, n_campaigns),
        'duration': np.random.randint(14, 60, n_campaigns),
        'audience_size': np.random.randint(50000, 1000000, n_campaigns),
        'channels': np.random.choice(['Meta', 'Google', 'Meta,Google', 'Meta,Google,LinkedIn'], n_campaigns),
        'creative_type': np.random.choice(['video', 'image', 'carousel'], n_campaigns),
        'objective': np.random.choice(['awareness', 'conversion', 'engagement'], n_campaigns),
        'start_date': pd.date_range('2023-01-01', periods=n_campaigns, freq='W'),
        'roas': np.random.uniform(1.5, 6.0, n_campaigns),
        'cpa': np.random.uniform(30, 120, n_campaigns),
        'advertiser_id': np.random.choice(['ADV_001', 'ADV_002', 'ADV_003'], n_campaigns)
    })
    
    # Initialize and train
    predictor = CampaignSuccessPredictor()
    metrics = predictor.train(historical_campaigns)
    
    print("\n=== Model Training Results ===")
    print(f"Test Accuracy: {metrics['test_accuracy']}")
    print(f"CV Mean Accuracy: {metrics['cv_mean_accuracy']} ± {metrics['cv_std_accuracy']}")
    
    # Predict for new campaign
    new_campaign = {
        'name': 'Q4_Holiday_Campaign',
        'budget': 250000,
        'duration': 30,
        'audience_size': 500000,
        'channels': 'Meta,Google',
        'creative_type': 'video',
        'objective': 'conversion',
        'start_date': '2024-11-01',
        'advertiser_id': 'ADV_001',
        'roas': 4.0  # Historical average
    }
    
    prediction = predictor.predict_success_probability(new_campaign)
    
    print("\n=== Campaign Success Prediction ===")
    print(f"Campaign: {prediction['campaign_name']}")
    print(f"Success Probability: {prediction['success_probability']}%")
    print(f"Confidence: {prediction['confidence_level']}")
    print(f"Risk Level: {prediction['risk_level']}")
    print(f"\nKey Drivers:")
    for driver in prediction['key_drivers']:
        print(f"  - {driver['feature']}: {driver['impact']} (importance: {driver['importance']})")
    print(f"\nInsights:")
    for insight in prediction['insights']:
        print(f"  {insight}")
    print(f"\nRecommendations:")
    for rec in prediction['recommendations']:
        print(f"  [{rec['priority']}] {rec['message']}")
