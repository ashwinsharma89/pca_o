"""
Examples of using the robust DataLoader with error handling
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils import DataLoader, fetch_data, safe_load_csv
from loguru import logger

# Configure logger
logger.remove()
logger.add(sys.stdout, format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>")


def example_1_basic_csv_loading():
    """Example 1: Basic CSV loading with error handling"""
    print("\n" + "="*80)
    print("EXAMPLE 1: Basic CSV Loading")
    print("="*80)
    
    loader = DataLoader()
    
    # Try to load a file
    file_path = "data/sitevisit_fixed.csv"
    
    df, error = loader.load_csv(file_path)
    
    if error:
        logger.error(f"Failed to load data: {error}")
        return None
    else:
        logger.success(f"✓ Loaded {len(df):,} rows with {len(df.columns)} columns")
        logger.info(f"✓ Columns: {df.columns.tolist()[:5]}...")
        return df


def example_2_handle_missing_file():
    """Example 2: Gracefully handle missing file"""
    print("\n" + "="*80)
    print("EXAMPLE 2: Handle Missing File")
    print("="*80)
    
    loader = DataLoader()
    
    # Try to load non-existent file
    file_path = "data/nonexistent_file.csv"
    
    df, error = loader.load_csv(file_path)
    
    if error:
        logger.warning(f"Expected error: {error}")
        logger.info("✓ Error was caught and handled gracefully")
        return None
    else:
        logger.success("Data loaded")
        return df


def example_3_auto_fix_column_names():
    """Example 3: Automatic column name fixing"""
    print("\n" + "="*80)
    print("EXAMPLE 3: Auto-Fix Column Names")
    print("="*80)
    
    loader = DataLoader()
    
    # Load file with spaces in column names
    file_path = r"C:\Users\asharm08\OneDrive - dentsu\Desktop\AI_Agent\Data\Sitevisit.csv"
    
    if not os.path.exists(file_path):
        logger.warning(f"File not found: {file_path}")
        return None
    
    # With auto-fix (default)
    df, error = loader.load_csv(file_path, fix_column_names=True)
    
    if error:
        logger.error(f"Error: {error}")
        return None
    else:
        logger.success("✓ Column names automatically fixed!")
        logger.info(f"✓ Sample columns: {df.columns.tolist()[:5]}")
        return df


def example_4_simple_wrapper():
    """Example 4: Using the simple wrapper function"""
    print("\n" + "="*80)
    print("EXAMPLE 4: Simple Wrapper Function")
    print("="*80)
    
    # Using safe_load_csv (returns None on error, no error message)
    df = safe_load_csv("data/sitevisit_fixed.csv")
    
    if df is not None:
        logger.success(f"✓ Loaded {len(df)} rows")
        return df
    else:
        logger.warning("Failed to load data (returned None)")
        return None


def example_5_generic_file_loader():
    """Example 5: Generic file loader (auto-detects type)"""
    print("\n" + "="*80)
    print("EXAMPLE 5: Generic File Loader")
    print("="*80)
    
    # Works with any supported file type
    file_path = "data/sitevisit_fixed.csv"
    
    df, error = fetch_data(file_path)
    
    if error:
        logger.error(f"Error: {error}")
        return None
    else:
        logger.success(f"✓ Auto-detected and loaded CSV file")
        logger.info(f"✓ {len(df)} rows loaded")
        return df


def example_6_validation_options():
    """Example 6: Validation options"""
    print("\n" + "="*80)
    print("EXAMPLE 6: Validation Options")
    print("="*80)
    
    loader = DataLoader()
    
    file_path = "data/sitevisit_fixed.csv"
    
    # With validation (default)
    df, error = loader.load_csv(file_path, validate=True)
    
    if error:
        logger.error(f"Validation failed: {error}")
        return None
    else:
        logger.success("✓ Data loaded and validated")
        logger.info(f"  - Rows: {len(df):,}")
        logger.info(f"  - Columns: {len(df.columns)}")
        logger.info(f"  - Memory: {df.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB")
        return df


def example_7_encoding_handling():
    """Example 7: Automatic encoding detection"""
    print("\n" + "="*80)
    print("EXAMPLE 7: Automatic Encoding Detection")
    print("="*80)
    
    loader = DataLoader()
    
    file_path = r"C:\Users\asharm08\OneDrive - dentsu\Desktop\AI_Agent\Data\Sitevisit.csv"
    
    if not os.path.exists(file_path):
        logger.warning(f"File not found: {file_path}")
        return None
    
    # Loader tries multiple encodings automatically
    df, error = loader.load_csv(file_path)
    
    if error:
        logger.error(f"Error: {error}")
        return None
    else:
        logger.success("✓ File loaded with automatic encoding detection")
        return df


def example_8_integration_with_qa_engine():
    """Example 8: Integration with Q&A Engine"""
    print("\n" + "="*80)
    print("EXAMPLE 8: Integration with Q&A Engine")
    print("="*80)
    
    from src.query_engine import NaturalLanguageQueryEngine
    
    # Load data with error handling
    df, error = fetch_data("data/sitevisit_fixed.csv")
    
    if error:
        logger.error(f"Cannot initialize Q&A engine: {error}")
        return None
    
    # Initialize Q&A engine
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        logger.warning("OpenAI API key not found")
        return None
    
    try:
        engine = NaturalLanguageQueryEngine(api_key)
        engine.load_data(df)
        logger.success("✓ Q&A Engine initialized with loaded data")
        
        # Ask a question
        result = engine.ask("What is the total spend?")
        logger.info(f"✓ Answer: {result['answer'][:100]}...")
        
        return engine
    except Exception as e:
        logger.error(f"Error initializing engine: {e}")
        return None


def main():
    """Run all examples"""
    print("\n" + "="*80)
    print("🚀 DATA LOADER EXAMPLES - WITH ERROR HANDLING")
    print("="*80)
    
    # Run examples
    examples = [
        example_1_basic_csv_loading,
        example_2_handle_missing_file,
        example_3_auto_fix_column_names,
        example_4_simple_wrapper,
        example_5_generic_file_loader,
        example_6_validation_options,
        example_7_encoding_handling,
        example_8_integration_with_qa_engine
    ]
    
    results = {}
    
    for example in examples:
        try:
            result = example()
            results[example.__name__] = "✅ Success" if result is not None else "⚠️ No data"
        except Exception as e:
            results[example.__name__] = f"❌ Error: {str(e)}"
    
    # Summary
    print("\n" + "="*80)
    print("📊 SUMMARY")
    print("="*80)
    
    for name, status in results.items():
        print(f"{status} - {name}")
    
    print("\n" + "="*80)
    print("✅ All examples completed!")
    print("="*80)


if __name__ == "__main__":
    main()
