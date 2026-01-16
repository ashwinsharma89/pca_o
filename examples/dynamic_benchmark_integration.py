"""
Dynamic Benchmark Engine Integration Example
Demonstrates context-aware benchmarking with industry, region, and objective adjustments
"""

import pandas as pd
from src.knowledge.benchmark_engine import DynamicBenchmarkEngine


def example_b2b_saas_google_search():
    """Example: B2B SaaS campaign on Google Search"""
    
    print("=" * 70)
    print("Example 1: B2B SaaS - Google Search (North America, Conversion)")
    print("=" * 70)
    
    # Initialize benchmark engine
    engine = DynamicBenchmarkEngine()
    
    # Get contextual benchmarks
    benchmarks = engine.get_contextual_benchmarks(
        channel='google_search',
        business_model='B2B',
        industry='SaaS',
        objective='conversion',
        region='North America'
    )
    
    print(f"\n📊 Context: {benchmarks['context']}")
    print(f"🌍 Region: {benchmarks['region']}")
    print(f"🎯 Objective: {benchmarks['objective']}")
    print(f"📚 Source: {benchmarks['benchmark_source']}")
    
    print(f"\n💡 Interpretation:")
    print(f"   {benchmarks['interpretation_guidance']}")
    
    print(f"\n📈 Benchmarks:")
    for metric, ranges in benchmarks['benchmarks'].items():
        print(f"\n  {metric.upper()}:")
        for level, value in ranges.items():
            if metric in ['ctr', 'conv_rate', 'quality_score', 'impression_share']:
                print(f"    {level}: {value:.3f}")
            else:
                print(f"    {level}: ${value:.2f}")
    
    # Compare actual performance
    actual_metrics = {
        'ctr': 0.045,
        'cpc': 5.5,
        'conv_rate': 0.06,
        'quality_score': 7.5
    }
    
    comparison = engine.compare_to_benchmarks(actual_metrics, benchmarks)
    
    print(f"\n🎯 Performance Comparison:")
    print(f"   Overall Score: {comparison['overall_score']:.1f}/100")
    print(f"   Assessment: {comparison['overall_assessment']}")
    
    print(f"\n📊 Metric-by-Metric:")
    for metric, comp in comparison['comparisons'].items():
        print(f"\n  {metric.upper()}:")
        print(f"    Actual: {comp['actual']:.3f}")
        print(f"    Assessment: {comp['assessment'].upper()}")
        print(f"    {comp['message']}")


def example_b2c_ecommerce_meta():
    """Example: B2C E-commerce campaign on Meta"""
    
    print("\n\n" + "=" * 70)
    print("Example 2: B2C E-commerce - Meta (Europe, Awareness)")
    print("=" * 70)
    
    engine = DynamicBenchmarkEngine()
    
    # Get contextual benchmarks with regional adjustment
    benchmarks = engine.get_contextual_benchmarks(
        channel='meta',
        business_model='B2C',
        industry='E-commerce',
        objective='awareness',
        region='Europe'
    )
    
    print(f"\n📊 Context: {benchmarks['context']}")
    print(f"🌍 Region: {benchmarks['region']} (Regional adjustments applied)")
    print(f"🎯 Objective: {benchmarks['objective']} (Objective adjustments applied)")
    
    print(f"\n💡 Interpretation:")
    print(f"   {benchmarks['interpretation_guidance']}")
    
    print(f"\n📈 Adjusted Benchmarks:")
    for metric, ranges in benchmarks['benchmarks'].items():
        print(f"\n  {metric.upper()}:")
        for level, value in ranges.items():
            if metric in ['ctr', 'conv_rate', 'roas', 'frequency']:
                print(f"    {level}: {value:.3f}")
            else:
                print(f"    {level}: ${value:.2f}")
    
    # Compare actual performance
    actual_metrics = {
        'ctr': 0.008,  # Lower CTR for awareness campaign
        'cpc': 1.1,
        'conv_rate': 0.012,  # Lower conversion for awareness
        'frequency': 3.2
    }
    
    comparison = engine.compare_to_benchmarks(actual_metrics, benchmarks)
    
    print(f"\n🎯 Performance Comparison:")
    print(f"   Overall Score: {comparison['overall_score']:.1f}/100")
    print(f"   Assessment: {comparison['overall_assessment']}")
    
    for metric, comp in comparison['comparisons'].items():
        print(f"\n  {metric.upper()}: {comp['actual']:.3f} - {comp['assessment'].upper()}")


def example_b2b_linkedin_financial():
    """Example: B2B Financial Services on LinkedIn"""
    
    print("\n\n" + "=" * 70)
    print("Example 3: B2B Financial Services - LinkedIn (Asia Pacific)")
    print("=" * 70)
    
    engine = DynamicBenchmarkEngine()
    
    # Get contextual benchmarks
    benchmarks = engine.get_contextual_benchmarks(
        channel='linkedin',
        business_model='B2B',
        industry='Financial Services',
        objective='lead_generation',
        region='Asia Pacific'
    )
    
    print(f"\n📊 Context: {benchmarks['context']}")
    print(f"🌍 Region: {benchmarks['region']}")
    
    print(f"\n📈 Benchmarks (with regional adjustments):")
    for metric, ranges in benchmarks['benchmarks'].items():
        print(f"\n  {metric.upper()}:")
        for level, value in ranges.items():
            if metric in ['ctr', 'conv_rate', 'lead_quality_rate']:
                print(f"    {level}: {value:.4f}")
            else:
                print(f"    {level}: ${value:.2f}")
    
    # Compare actual performance
    actual_metrics = {
        'ctr': 0.006,
        'cpc': 12.0,
        'conv_rate': 0.038,
        'lead_quality_rate': 0.35
    }
    
    comparison = engine.compare_to_benchmarks(actual_metrics, benchmarks)
    
    print(f"\n🎯 Performance Comparison:")
    print(f"   Overall Score: {comparison['overall_score']:.1f}/100")
    print(f"   Assessment: {comparison['overall_assessment']}")
    
    print(f"\n📊 Detailed Comparison:")
    for metric, comp in comparison['comparisons'].items():
        status_emoji = {
            'excellent': '🟢',
            'good': '🟡',
            'average': '🟠',
            'needs_work': '🔴'
        }.get(comp['assessment'], '⚪')
        
        print(f"\n  {status_emoji} {metric.upper()}:")
        print(f"     Actual: {comp['actual']:.4f}")
        print(f"     {comp['message']}")


def example_multi_region_comparison():
    """Example: Compare benchmarks across regions"""
    
    print("\n\n" + "=" * 70)
    print("Example 4: Multi-Region Benchmark Comparison")
    print("=" * 70)
    
    engine = DynamicBenchmarkEngine()
    
    regions = ['North America', 'Europe', 'Asia Pacific', 'Latin America']
    
    print(f"\n📊 B2B SaaS - Google Search - Conversion Objective")
    print(f"\nCPC Benchmarks by Region:")
    print(f"{'Region':<20} {'Excellent':<12} {'Good':<12} {'Acceptable':<12}")
    print("-" * 60)
    
    for region in regions:
        benchmarks = engine.get_contextual_benchmarks(
            channel='google_search',
            business_model='B2B',
            industry='SaaS',
            objective='conversion',
            region=region
        )
        
        cpc_ranges = benchmarks['benchmarks'].get('cpc', {})
        print(f"{region:<20} ${cpc_ranges.get('excellent', 0):<11.2f} ${cpc_ranges.get('good', 0):<11.2f} ${cpc_ranges.get('acceptable', 0):<11.2f}")
    
    print(f"\n\nCTR Benchmarks by Region:")
    print(f"{'Region':<20} {'Excellent':<12} {'Good':<12} {'Average':<12}")
    print("-" * 60)
    
    for region in regions:
        benchmarks = engine.get_contextual_benchmarks(
            channel='google_search',
            business_model='B2B',
            industry='SaaS',
            objective='conversion',
            region=region
        )
        
        ctr_ranges = benchmarks['benchmarks'].get('ctr', {})
        print(f"{region:<20} {ctr_ranges.get('excellent', 0):<11.3f} {ctr_ranges.get('good', 0):<11.3f} {ctr_ranges.get('average', 0):<11.3f}")


def example_objective_comparison():
    """Example: Compare benchmarks across objectives"""
    
    print("\n\n" + "=" * 70)
    print("Example 5: Objective-Based Benchmark Comparison")
    print("=" * 70)
    
    engine = DynamicBenchmarkEngine()
    
    objectives = ['awareness', 'consideration', 'conversion', 'lead_generation']
    
    print(f"\n📊 B2C E-commerce - Meta - North America")
    print(f"\nCTR Benchmarks by Objective:")
    print(f"{'Objective':<20} {'Excellent':<12} {'Good':<12} {'Average':<12}")
    print("-" * 60)
    
    for objective in objectives:
        benchmarks = engine.get_contextual_benchmarks(
            channel='meta',
            business_model='B2C',
            industry='E-commerce',
            objective=objective,
            region='North America'
        )
        
        ctr_ranges = benchmarks['benchmarks'].get('ctr', {})
        print(f"{objective.title():<20} {ctr_ranges.get('excellent', 0):<11.4f} {ctr_ranges.get('good', 0):<11.4f} {ctr_ranges.get('average', 0):<11.4f}")
    
    print(f"\n💡 Note: Awareness campaigns have lower CTR expectations due to broader targeting")


def example_available_contexts():
    """Example: Show available benchmark contexts"""
    
    print("\n\n" + "=" * 70)
    print("Example 6: Available Benchmark Contexts")
    print("=" * 70)
    
    engine = DynamicBenchmarkEngine()
    contexts = engine.get_available_contexts()
    
    print(f"\n📊 Available Channels:")
    for channel in contexts['channels']:
        print(f"   • {channel}")
    
    print(f"\n💼 Business Models:")
    for model in contexts['business_models']:
        print(f"   • {model}")
    
    print(f"\n🏭 Industries:")
    for industry in contexts['industries']:
        print(f"   • {industry}")
    
    print(f"\n🌍 Regions:")
    for region in contexts['regions']:
        print(f"   • {region}")


def example_programmatic_benchmarks():
    """Example: Programmatic/DV360 benchmarks"""
    
    print("\n\n" + "=" * 70)
    print("Example 7: Programmatic (DV360) Benchmarks")
    print("=" * 70)
    
    engine = DynamicBenchmarkEngine()
    
    benchmarks = engine.get_contextual_benchmarks(
        channel='dv360',
        business_model='B2C',
        industry='E-commerce',
        objective='awareness'
    )
    
    print(f"\n📊 Context: {benchmarks['context']}")
    
    print(f"\n📈 Programmatic Quality Benchmarks:")
    for metric, ranges in benchmarks['benchmarks'].items():
        print(f"\n  {metric.upper().replace('_', ' ')}:")
        for level, value in ranges.items():
            if metric in ['viewability', 'brand_safety', 'ivt_rate']:
                print(f"    {level}: {value:.2%}")
            elif metric == 'ctr':
                print(f"    {level}: {value:.3f}")
            else:
                print(f"    {level}: ${value:.2f}")
    
    # Compare actual performance
    actual_metrics = {
        'viewability': 0.72,
        'brand_safety': 0.96,
        'ctr': 0.012,
        'ivt_rate': 0.015
    }
    
    comparison = engine.compare_to_benchmarks(actual_metrics, benchmarks)
    
    print(f"\n🎯 Performance Assessment:")
    print(f"   Overall Score: {comparison['overall_score']:.1f}/100")
    print(f"   {comparison['overall_assessment']}")


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("Dynamic Benchmark Engine - Integration Examples")
    print("=" * 70)
    
    # Run all examples
    example_b2b_saas_google_search()
    example_b2c_ecommerce_meta()
    example_b2b_linkedin_financial()
    example_multi_region_comparison()
    example_objective_comparison()
    example_available_contexts()
    example_programmatic_benchmarks()
    
    print("\n\n" + "=" * 70)
    print("Examples Complete!")
    print("=" * 70)
    print("\n💡 Key Takeaways:")
    print("   • Benchmarks adapt to channel, business model, and industry")
    print("   • Regional adjustments account for geographic differences")
    print("   • Objective adjustments set appropriate expectations")
    print("   • Performance assessment provides actionable insights")
    print("=" * 70)
