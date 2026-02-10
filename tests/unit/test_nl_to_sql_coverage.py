
import pytest
import pandas as pd
import os
import sys
from unittest.mock import patch, MagicMock, Mock
from types import SimpleNamespace
from src.platform.query_engine.nl_to_sql import NaturalLanguageQueryEngine

# Create a mock duckdb module at module level
mock_duckdb = MagicMock()
mock_duckdb.connect.return_value = MagicMock()
mock_duckdb.connect.return_value.register = MagicMock() 

# Mock google.generativeai
mock_genai_sys = MagicMock()

@pytest.fixture(autouse=True)
def env_setup():
    """Setup basic environment for engine tests."""
    with patch.dict(os.environ, {
        "OPENAI_API_KEY": "sk-test-openai",
        "ANTHROPIC_API_KEY": "sk-ant-test-anthropic",
        "GOOGLE_API_KEY": "test-gemini-key",
        "GROQ_API_KEY": "gsk_test",
        "DEEPSEEK_API_KEY": "sk-test-deepseek"
    }):
        with patch.dict(sys.modules, {'duckdb': mock_duckdb, 'google.generativeai': mock_genai_sys}):
             yield

@pytest.fixture
def engine():
    """Create engine instance with mocked clients and required components."""
    # We patch 'src.platform.query_engine.nl_to_sql.genai' as well to be safe if it was already imported
    with patch('src.platform.query_engine.nl_to_sql.OpenAI'), \
         patch('src.platform.query_engine.nl_to_sql.create_anthropic_client'), \
         patch('src.platform.query_engine.nl_to_sql.genai', create=True) as mock_genai_module, \
         patch('src.platform.query_engine.nl_to_sql.Groq'), \
         patch('src.platform.query_engine.nl_to_sql.SemanticCache') as mock_cache:
        
        mock_cache.return_value.get.return_value = None
        
        eng = NaturalLanguageQueryEngine(api_key="sk-test-openai")
        
        eng.schema_info = {
            'columns': ['spend', 'clicks', 'date', 'platform', 'campaign', 'revenue'], 
            'table_name': 'campaigns'
        }
        eng.conn = mock_duckdb.connect.return_value
        
        eng.multi_table_manager = MagicMock()
        eng.sql_retriever = MagicMock()
        eng.available_models = [('openai', 'gpt-4o'), ('claude', 'sonnet')]
        eng.prompt_builder = MagicMock()
        eng.schema_manager = MagicMock()
        eng.executor = MagicMock()
        
        # Ensure sql_helper is mocked correctly
        eng.sql_helper = MagicMock()
        eng.sql_helper.build_context.return_value = "SQL Context"
        # IMPORTANT: Fix for test_template_fallback
        eng.sql_helper.get_last_context_package.return_value = {}
        
        eng.schema_manager.extract_schema.return_value = eng.schema_info
        eng.schema_manager.get_schema_for_prompt.return_value = "Schema Description"
        
        mock_res = MagicMock()
        mock_res.height = 1
        eng.executor.execute.return_value = (mock_res, None)
        eng.executor.format_results.return_value = [{"res": 1}]
        eng.executor.get_result_summary.return_value = "Summary"
        
        eng.prompt_builder.build.return_value = "Built Prompt"
        eng.openai_client = MagicMock()
        eng.groq_client = MagicMock()
        eng.deepseek_client = MagicMock()
        eng.template_generator = MagicMock()
        eng.mock_genai = mock_genai_module # Attach for test access

        return eng

class TestNLQueryEngineMaster:
    """Master test suite for nl_to_sql.py aiming for 100% coverage."""

    def test_init_all_scenarios(self):
        """Test variations of initialization."""
        with patch.dict(os.environ, {}, clear=True):
             eng = NaturalLanguageQueryEngine(api_key="dummy")
             assert eng is not None

    def test_load_data_flow(self, engine):
        """Test load_data."""
        df = pd.DataFrame({'spend': [100], 'date': ['2024-01-01']})
        
        with patch('src.platform.query_engine.safe_query.SafeQueryExecutor'):
             engine.load_data(df, "test_table")
             assert engine.schema_info is not None

        with patch('src.platform.query_engine.nl_to_sql.os.path.exists', return_value=True):
            with patch('src.platform.query_engine.nl_to_sql.pd.read_parquet', return_value=df):
                 with patch('src.platform.query_engine.safe_query.SafeQueryExecutor'):
                    engine.load_parquet_data("dummy.parquet", "p_table")
                    assert engine.conn.register.called

    @pytest.mark.parametrize("provider, model", [
        ('openai', 'gpt-4o'),
        ('claude', 'sonnet'),
        ('gemini', 'gemini-pro'),
        ('groq', 'llama3-70b'),
        ('deepseek', 'deepseek-coder')
    ])
    def test_generate_sql_providers(self, engine, provider, model):
        """Test generate_sql with all supported providers."""
        engine.available_models = [(provider, model)]
        
        # Bypass sanitize
        engine._sanitize_sql = MagicMock(return_value="SELECT * FROM campaigns")
        
        # Setup mocks for each provider
        if provider == 'openai':
            engine.openai_client.chat.completions.create.return_value = SimpleNamespace(
                choices=[SimpleNamespace(message=SimpleNamespace(content="SELECT * FROM campaigns"))],
                usage=SimpleNamespace(total_tokens=100)
            )
        elif provider == 'claude':
            engine.anthropic_client = MagicMock()
            engine.anthropic_client.messages.create.return_value = SimpleNamespace(
                content=[SimpleNamespace(text="SELECT * FROM campaigns")]
            )
        elif provider == 'gemini':
            # Explicitly inject genai into module to handle conditional import logic
            import src.platform.query_engine.nl_to_sql as nl_module
            nl_module.genai = engine.mock_genai
            nl_module.GEMINI_AVAILABLE = True
            
            mock_model = MagicMock()
            mock_model.generate_content.return_value = SimpleNamespace(
                text="SELECT * FROM campaigns",
                usage_metadata=SimpleNamespace(prompt_token_count=10, candidates_token_count=10)
            )
            engine.mock_genai.GenerativeModel.return_value = mock_model
             
        elif provider == 'groq':
             engine.groq_client.chat.completions.create.return_value = SimpleNamespace(
                choices=[SimpleNamespace(message=SimpleNamespace(content="SELECT * FROM campaigns"))],
                usage=SimpleNamespace(total_tokens=100)
            )
        elif provider == 'deepseek':
             engine.deepseek_client.chat.completions.create.return_value = SimpleNamespace(
                choices=[SimpleNamespace(message=SimpleNamespace(content="SELECT * FROM campaigns"))]
            )

        # Common setup
        mock_analysis = {
            'intent': SimpleNamespace(value="analytics"),
            'complexity': SimpleNamespace(value="simple"),
            'entities': []
        }
        
        with patch('src.platform.query_engine.marketing_context.get_marketing_context_for_nl_to_sql', return_value="ctx"):
            with patch('src.platform.query_engine.hybrid_retrieval.analyze_question', return_value=mock_analysis):
                # Chain prompt builder
                engine.prompt_builder.set_schema.return_value = engine.prompt_builder
                engine.prompt_builder.set_marketing_context.return_value = engine.prompt_builder
                engine.prompt_builder.set_sql_context.return_value = engine.prompt_builder
                engine.prompt_builder.set_rag_examples.return_value = engine.prompt_builder
                engine.prompt_builder.set_hybrid_analysis.return_value = engine.prompt_builder
                engine.prompt_builder.set_query_analysis.return_value = engine.prompt_builder
                engine.prompt_builder.set_reference_date.return_value = engine.prompt_builder
                engine.prompt_builder.set_examples.return_value = engine.prompt_builder
                
                sql = engine.generate_sql("show campaigns")
                assert "SELECT" in sql

    def test_generate_answer_insights(self, engine):
        """Test _generate_answer logic."""
        df = pd.DataFrame({'spend': [100.0], 'conversions': [5]})
        results_polars = MagicMock()
        results_polars.is_empty.return_value = False
        results_polars.to_pandas.return_value = df
        
        answer = engine._generate_answer("What are the insights?", results_polars)
        # Verify it returns the default success response for data
        assert "Query executed successfully" in answer

    def test_extract_sample_context(self, engine):
        """Test _extract_sample_context logic."""
        df = pd.DataFrame({'conversions': [100], 'spend': [1000]})
        context = engine._extract_sample_context(df)
        assert "Rows: 1" in context
        assert "Total Conversions: 100" in context
        assert "Total Spend: $1.0K" in context
        
        # Test empty
        assert "No data" in engine._extract_sample_context(None)

    def test_marketing_rules(self, engine):
        """Test marketing rules."""
        bad_sql = "SELECT AVG(CTR) FROM t"
        corrected = "SELECT SUM(clicks)/NULLIF(SUM(imps),0) FROM t"
        
        engine.openai_client.chat.completions.create.return_value = SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content=corrected))],
            usage=SimpleNamespace(total_tokens=10)
        )
        res = engine._self_correct_sql(bad_sql, ["Rule 1"], "q")
        assert res.strip() == corrected

    def test_sanitize_sql(self, engine):
        """Test sanitization."""
        engine.schema_info = {'columns': ['spend']}
        sql = 'SELECT ""Total Spent" FROM t'
        assert "spend" in engine._sanitize_sql(sql)

    def test_answer_question(self, engine):
        """Test answer_question flow."""
        mock_res = MagicMock()
        mock_res.is_empty.return_value = False
        engine.executor.execute.return_value = (mock_res, None)
        
        with patch.object(engine, 'generate_sql', return_value="SELECT 1"):
             with patch.object(engine, '_generate_answer', return_value="Ans"):
                  res = engine.answer_question("q")
                  assert res['success'] is True
                  
    def test_template_fallback(self, engine):
        """Test template fallback when generate_sql fails."""
        engine.template_generator.generate_all_templates.return_value = {}
        # Make sure helper is present
        engine.sql_helper.get_last_context_package.return_value = {}
        
        with patch.object(engine, 'generate_sql', side_effect=Exception("LLM Fail")):
             res = engine.ask("show campaigns")
             assert res['success'] is False

    def test_bulletproof_query_success(self, engine):
        """Test bulletproof query path."""
        with patch('src.platform.query_engine.nl_to_sql.BulletproofQueries') as mock_bp:
            mock_bp.get_sql_for_question.return_value = "SELECT * FROM campaigns"
            
            # Use global mock_duckdb which is injected via sys.modules
            # Configure it to return our mock df
            mock_df = MagicMock()
            mock_df.__len__.return_value = 10
            # mock_duckdb.connect() -> conn context -> execute -> df()
            mock_duckdb.connect.return_value.__enter__.return_value.execute.return_value.df.return_value = mock_df
            
            res = engine.ask("show campaigns")
            assert res['success'] is True
            assert res['source'] == "bulletproof_template"

    def test_template_first_success(self, engine):
        """Test template-first approach success."""
        # Ensure bulletproof returns None
        with patch('src.platform.query_engine.nl_to_sql.BulletproofQueries.get_sql_for_question', return_value=None):
            # Setup template generator
            mock_template = MagicMock()
            mock_template.patterns = ["campaigns"]
            mock_template.sql = "SELECT * FROM all_campaigns"
            mock_template.name = "campaign_overview"
            
            engine.template_generator.generate_all_templates.return_value = {"campaign_overview": mock_template}
            
            # Mock execute_query -> non-empty results
            mock_res = MagicMock()
            mock_res.is_empty.return_value = False
            engine.executor.execute.return_value = (mock_res, None)
            
            res = engine.ask("show campaigns")
            assert res['success'] is True
            assert "Template" in res['model_used']

    def test_deepseek_fallback(self, engine):
        """Test DeepSeek fallback when primary returns empty."""
        with patch('src.platform.query_engine.nl_to_sql.BulletproofQueries.get_sql_for_question', return_value=None):
            engine.template_generator = None # Disable templates
            
            # Setup available models: OpenAI first, DeepSeek second
            engine.available_models = [('openai', 'gpt-4o'), ('deepseek', 'deepseek-chat')]
            
            # Mock generate_sql to work
            with patch.object(engine, 'generate_sql', return_value="SELECT 1"):
                 # Mock execute_query side effect: First empty, Second (fallback) full
                 mock_empty = MagicMock()
                 mock_empty.is_empty.return_value = True
                 
                 mock_full = MagicMock()
                 mock_full.is_empty.return_value = False
                 
                 # execute_query is called:
                 # 1. Primary execution -> Empty
                 # 2. Fallback execution -> Full
                 engine.executor.execute.side_effect = [(mock_empty, None), (mock_full, None)]
                 
                 res = engine.ask("show campaigns")
                 
                 # It should have tried fallback
                 assert res['success'] is True
                 # Verify execute was called twice
                 assert engine.executor.execute.call_count == 2
