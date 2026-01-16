
import sys
import os
import logging

# Add project root to path
sys.path.append(os.getcwd())

from src.core.utils import setup_logger
try:
    from src.platform.query_engine.nl_to_sql import NaturalLanguageQueryEngine
    print("✅ Successfully imported NaturalLanguageQueryEngine")
    
    # Try to verify BulletproofQueries usage
    # We can check if the class is available in the module's namespace if implicitly available, 
    # but the NameError was inside a method. 
    # Just importing the module successfully usually means top-level imports are fine.
    # To be sure, let's check if we can inspect the module.
    import src.platform.query_engine.nl_to_sql as nl_module
    if hasattr(nl_module, 'BulletproofQueries'):
        print("✅ BulletproofQueries is present in nl_to_sql namespace")
    else:
        print("❌ BulletproofQueries NOT found in nl_to_sql namespace (verification might be tricky if not exported)")

except ImportError as e:
    print(f"❌ ImportError: {e}")
except NameError as e:
    print(f"❌ NameError: {e}")
except Exception as e:
    print(f"❌ Unexpected Error: {e}")
