"""
Ground Truth Test Set for RAG Retrieval Quality

Defines test queries with known relevant documents for evaluating retrieval quality.
"""

from typing import List, Dict, Any


# Ground truth test set for PCA knowledge base
# Each entry has:
# - query: User question
# - relevant_docs: List of document IDs/chunks that should be retrieved
# - category: Type of query (optimization, troubleshooting, best_practices, etc.)

GROUND_TRUTH_TEST_SET: List[Dict[str, Any]] = [
    # ========================================================================
    # OPTIMIZATION QUERIES
    # ========================================================================
    {
        "query": "How do I reduce my Facebook ad CPC?",
        "relevant_docs": [
            "facebook_cpc_optimization",
            "facebook_bidding_strategies",
            "facebook_audience_targeting",
            "facebook_ad_relevance_score"
        ],
        "category": "optimization",
        "platform": "facebook",
        "difficulty": "easy"
    },
    {
        "query": "What are the best practices for Google Ads Quality Score?",
        "relevant_docs": [
            "google_quality_score_guide",
            "google_landing_page_optimization",
            "google_ad_relevance",
            "google_expected_ctr"
        ],
        "category": "best_practices",
        "platform": "google",
        "difficulty": "medium"
    },
    {
        "query": "How to fix creative fatigue in social media campaigns?",
        "relevant_docs": [
            "creative_fatigue_detection",
            "creative_refresh_strategies",
            "facebook_ad_frequency",
            "social_creative_best_practices"
        ],
        "category": "troubleshooting",
        "platform": "social",
        "difficulty": "medium"
    },
    
    # ========================================================================
    # AUDIENCE TARGETING QUERIES
    # ========================================================================
    {
        "query": "How do I expand my audience without losing performance?",
        "relevant_docs": [
            "audience_expansion_strategies",
            "lookalike_audience_best_practices",
            "audience_saturation_detection",
            "facebook_audience_network"
        ],
        "category": "optimization",
        "platform": "facebook",
        "difficulty": "hard"
    },
    {
        "query": "What is audience saturation and how do I detect it?",
        "relevant_docs": [
            "audience_saturation_detection",
            "frequency_capping_strategies",
            "reach_vs_frequency_analysis",
            "audience_fatigue_signals"
        ],
        "category": "troubleshooting",
        "platform": "social",
        "difficulty": "medium"
    },
    
    # ========================================================================
    # BIDDING & BUDGET QUERIES
    # ========================================================================
    {
        "query": "Should I use manual or automated bidding for conversions?",
        "relevant_docs": [
            "bidding_strategy_comparison",
            "automated_bidding_best_practices",
            "manual_bidding_use_cases",
            "conversion_optimization_strategies"
        ],
        "category": "best_practices",
        "platform": "google",
        "difficulty": "medium"
    },
    {
        "query": "How to allocate budget across multiple campaigns?",
        "relevant_docs": [
            "budget_allocation_strategies",
            "campaign_budget_optimization",
            "roas_based_budgeting",
            "performance_based_allocation"
        ],
        "category": "optimization",
        "platform": "all",
        "difficulty": "hard"
    },
    
    # ========================================================================
    # PERFORMANCE TROUBLESHOOTING
    # ========================================================================
    {
        "query": "Why is my CTR declining over time?",
        "relevant_docs": [
            "ctr_decline_causes",
            "creative_fatigue_detection",
            "ad_relevance_optimization",
            "audience_saturation_detection"
        ],
        "category": "troubleshooting",
        "platform": "all",
        "difficulty": "easy"
    },
    {
        "query": "My CPA is increasing but spend is stable, what's wrong?",
        "relevant_docs": [
            "cpa_increase_diagnosis",
            "conversion_rate_optimization",
            "landing_page_optimization",
            "audience_quality_issues"
        ],
        "category": "troubleshooting",
        "platform": "all",
        "difficulty": "medium"
    },
    {
        "query": "How do I improve my conversion rate?",
        "relevant_docs": [
            "conversion_rate_optimization",
            "landing_page_best_practices",
            "call_to_action_optimization",
            "funnel_optimization_strategies"
        ],
        "category": "optimization",
        "platform": "all",
        "difficulty": "medium"
    },
    
    # ========================================================================
    # PLATFORM-SPECIFIC QUERIES
    # ========================================================================
    {
        "query": "What are the best ad formats for Instagram?",
        "relevant_docs": [
            "instagram_ad_formats_guide",
            "instagram_stories_best_practices",
            "instagram_reels_advertising",
            "instagram_creative_guidelines"
        ],
        "category": "best_practices",
        "platform": "instagram",
        "difficulty": "easy"
    },
    {
        "query": "How to optimize for Google Shopping campaigns?",
        "relevant_docs": [
            "google_shopping_optimization",
            "product_feed_best_practices",
            "shopping_bid_strategies",
            "google_merchant_center_guide"
        ],
        "category": "optimization",
        "platform": "google",
        "difficulty": "hard"
    },
    {
        "query": "What is the difference between Advantage+ and manual campaigns on Meta?",
        "relevant_docs": [
            "meta_advantage_plus_guide",
            "automated_vs_manual_campaigns",
            "meta_campaign_types",
            "advantage_plus_best_practices"
        ],
        "category": "best_practices",
        "platform": "facebook",
        "difficulty": "medium"
    },
    
    # ========================================================================
    # METRICS & MEASUREMENT
    # ========================================================================
    {
        "query": "What is a good CTR for Facebook ads?",
        "relevant_docs": [
            "facebook_ctr_benchmarks",
            "industry_benchmarks_social",
            "ctr_by_objective",
            "performance_benchmarks_guide"
        ],
        "category": "benchmarks",
        "platform": "facebook",
        "difficulty": "easy"
    },
    {
        "query": "How do I calculate ROAS and what is a good target?",
        "relevant_docs": [
            "roas_calculation_guide",
            "roas_benchmarks_by_industry",
            "profitability_metrics",
            "ecommerce_roas_targets"
        ],
        "category": "metrics",
        "platform": "all",
        "difficulty": "easy"
    },
    {
        "query": "What metrics should I track for brand awareness campaigns?",
        "relevant_docs": [
            "brand_awareness_metrics",
            "upper_funnel_kpis",
            "reach_frequency_metrics",
            "brand_lift_measurement"
        ],
        "category": "metrics",
        "platform": "all",
        "difficulty": "medium"
    },
    
    # ========================================================================
    # ADVANCED STRATEGIES
    # ========================================================================
    {
        "query": "How to implement dayparting for better performance?",
        "relevant_docs": [
            "dayparting_strategies",
            "ad_scheduling_best_practices",
            "time_of_day_optimization",
            "hourly_performance_analysis"
        ],
        "category": "optimization",
        "platform": "all",
        "difficulty": "hard"
    },
    {
        "query": "What is the best way to test new creatives?",
        "relevant_docs": [
            "creative_testing_methodology",
            "ab_testing_best_practices",
            "statistical_significance_guide",
            "creative_rotation_strategies"
        ],
        "category": "best_practices",
        "platform": "all",
        "difficulty": "medium"
    },
    {
        "query": "How do I use lookalike audiences effectively?",
        "relevant_docs": [
            "lookalike_audience_guide",
            "seed_audience_best_practices",
            "lookalike_percentage_optimization",
            "facebook_lookalike_strategies"
        ],
        "category": "best_practices",
        "platform": "facebook",
        "difficulty": "medium"
    },
    
    # ========================================================================
    # SEASONAL & TIMING
    # ========================================================================
    {
        "query": "How should I prepare for Q4 holiday season?",
        "relevant_docs": [
            "q4_preparation_guide",
            "holiday_campaign_strategies",
            "seasonal_budget_planning",
            "black_friday_optimization"
        ],
        "category": "planning",
        "platform": "all",
        "difficulty": "hard"
    },
    {
        "query": "When is the best time to launch a new campaign?",
        "relevant_docs": [
            "campaign_launch_timing",
            "learning_phase_optimization",
            "seasonal_trends_guide",
            "campaign_planning_calendar"
        ],
        "category": "planning",
        "platform": "all",
        "difficulty": "medium"
    }
]


def get_test_set_by_category(category: str) -> List[Dict[str, Any]]:
    """Filter test set by category."""
    return [q for q in GROUND_TRUTH_TEST_SET if q['category'] == category]


def get_test_set_by_platform(platform: str) -> List[Dict[str, Any]]:
    """Filter test set by platform."""
    return [q for q in GROUND_TRUTH_TEST_SET if q['platform'] == platform or q['platform'] == 'all']


def get_test_set_by_difficulty(difficulty: str) -> List[Dict[str, Any]]:
    """Filter test set by difficulty."""
    return [q for q in GROUND_TRUTH_TEST_SET if q['difficulty'] == difficulty]


def get_test_set_stats() -> Dict[str, Any]:
    """Get statistics about the test set."""
    categories = {}
    platforms = {}
    difficulties = {}
    
    for query in GROUND_TRUTH_TEST_SET:
        cat = query['category']
        plat = query['platform']
        diff = query['difficulty']
        
        categories[cat] = categories.get(cat, 0) + 1
        platforms[plat] = platforms.get(plat, 0) + 1
        difficulties[diff] = difficulties.get(diff, 0) + 1
    
    return {
        'total_queries': len(GROUND_TRUTH_TEST_SET),
        'categories': categories,
        'platforms': platforms,
        'difficulties': difficulties,
        'avg_relevant_docs_per_query': sum(len(q['relevant_docs']) for q in GROUND_TRUTH_TEST_SET) / len(GROUND_TRUTH_TEST_SET)
    }
