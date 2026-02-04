
import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import MagicMock, patch
from src.platform.ingestion.pipeline import ParquetSink, IngestionPipeline, ingest_file, ingest_dataframe

class TestParquetSink:
    def test_init(self, tmp_path):
        sink = ParquetSink(base_path=tmp_path)
        assert sink.base_path == tmp_path
        assert tmp_path.exists()

    def test_write_empty(self, tmp_path):
        sink = ParquetSink(base_path=tmp_path)
        assert sink.write(pd.DataFrame()) == 0

    def test_write_single(self, tmp_path):
        sink = ParquetSink(base_path=tmp_path, partition_by=None)
        df = pd.DataFrame({'a': [1, 2], 'date': ['2024-01-01', '2024-01-02']})
        rows = sink.write(df)
        assert rows == 2
        assert len(list(tmp_path.glob("*.parquet"))) == 1

    def test_write_partitioned_date(self, tmp_path):
        sink = ParquetSink(base_path=tmp_path, partition_by='date')
        df = pd.DataFrame({
            'a': [1, 2],
            'date': [pd.Timestamp('2024-01-01'), pd.Timestamp('2024-02-01')]
        })
        rows = sink.write(df)
        assert rows == 2
        assert (tmp_path / "year=2024" / "month=01").exists()

    def test_write_partitioned_fallback(self, tmp_path):
        # Hit branch where partition_by is in df but NOT 'date'
        # ParquetSink.write calls _write_partitioned if partition_by in df.
        # _write_partitioned returns 0 if partition_by != 'date'.
        sink = ParquetSink(base_path=tmp_path, partition_by='platform')
        df = pd.DataFrame({'platform': ['FB'], 'spend': [100]})
        rows = sink.write(df)
        assert rows == 0 # Current implementation only supports date partitioning

    def test_write_partitioned_no_valid_dates(self, tmp_path):
        sink = ParquetSink(base_path=tmp_path, partition_by='date')
        df = pd.DataFrame({'a': [1], 'date': [None]})
        rows = sink.write(df)
        assert rows == 0

class TestIngestionPipeline:
    @pytest.fixture
    def mock_adapter(self):
        adapter = MagicMock()
        adapter.source_type = "mock"
        adapter.read_chunks.return_value = [
            pd.DataFrame({'spend': [100.0], 'date': [pd.Timestamp('2024-01-01')], 'platform': ['FB']}),
            pd.DataFrame({'spend': [-10.0], 'date': [pd.Timestamp('2024-01-02')]}) # Warning: spend < 0
        ]
        return adapter

    def test_pipeline_run(self, mock_adapter, tmp_path):
        sink = ParquetSink(base_path=tmp_path)
        progress_calls = []
        def on_progress(w, r): progress_calls.append((w, r))
        
        pipeline = IngestionPipeline(
            adapter=mock_adapter,
            sink=sink,
            on_progress=on_progress
        )
        stats = pipeline.run()
        
        assert stats['rows_read'] == 2
        assert stats['rows_written'] >= 1
        assert len(stats['validation_warnings']) > 0
        assert len(progress_calls) == 2

    def test_pipeline_validated_empty(self, tmp_path):
        adapter = MagicMock()
        adapter.read_chunks.return_value = [pd.DataFrame({'spend': [100]})] # Missing date -> fails validation
        pipeline = IngestionPipeline(adapter=adapter, sink=ParquetSink(base_path=tmp_path))
        # Note: IngestionPipeline._process_chunk calls validator.validate(normalized)
        # If validated is empty, it skips sink.write.
        # We need a chunk that is not empty but results in an empty validated DF.
        with patch.object(pipeline.validator, 'validate', return_value=(pd.DataFrame(), MagicMock(valid_rows=0))):
             pipeline._process_chunk(pd.DataFrame({'a':[1]}))
             assert pipeline.stats['rows_written'] == 0

    def test_pipeline_failure(self, mock_adapter):
        mock_adapter.read_chunks.side_effect = Exception("Read error")
        pipeline = IngestionPipeline(adapter=mock_adapter)
        with pytest.raises(Exception, match="Read error"):
            pipeline.run()
        assert "Read error" in pipeline.stats['error']

def test_ingest_file_conv(tmp_path):
    with patch('src.platform.ingestion.pipeline.get_adapter') as mock_get:
        mock_adapter = MagicMock()
        mock_adapter.read_chunks.return_value = [pd.DataFrame({'spend': [100.0], 'date': [pd.Timestamp('2024-01-01')], 'platform': ['FB']})]
        mock_get.return_value = mock_adapter
        
        stats = ingest_file("dummy.csv", output_dir=str(tmp_path))
        assert stats['rows_written'] == 1

def test_ingest_dataframe_conv(tmp_path):
    df = pd.DataFrame({'spend': [100.0], 'date': [pd.Timestamp('2024-01-01')], 'platform': ['FB']})
    stats = ingest_dataframe(df, output_dir=str(tmp_path))
    assert stats['rows_written'] == 1
    assert stats['rows_read'] == 1
