# Knowledge Base Setup Guide

## Overview

The PCA Agent can now learn from external sources (URLs, YouTube videos, PDFs) to enhance its reasoning and provide better insights.

## Installation

### Required Packages

```bash
# Core dependencies (already installed)
pip install openai anthropic

# Knowledge ingestion dependencies
pip install youtube-transcript-api PyPDF2 beautifulsoup4 requests

# Optional: For better text processing
pip install langchain
```

### Quick Install

```bash
pip install youtube-transcript-api PyPDF2 beautifulsoup4 requests langchain
```

## Features

### 1. **Learn from Web URLs** 🌐
- Extract content from articles, blogs, documentation
- Automatically removes navigation, ads, and clutter
- Processes and chunks content for efficient retrieval

**Example:**
```python
from src.knowledge import EnhancedReasoningEngine

engine = EnhancedReasoningEngine()
result = engine.learn_from_url("https://blog.google/products/ads/performance-max-tips/")
```

### 2. **Learn from YouTube Videos** 🎥
- Extracts video transcripts/captions
- Includes timestamps for reference
- Works with any video that has captions

**Example:**
```python
result = engine.learn_from_youtube("https://www.youtube.com/watch?v=VIDEO_ID")
```

### 3. **Learn from PDF Documents** 📄
- Extracts text from PDF files
- Maintains page structure
- Processes research papers, guides, reports

**Example:**
```python
result = engine.learn_from_pdf("marketing_guide.pdf")
```

## How It Works

### Information Flow

```
External Source → Extract Content → Chunk & Process → Store in Memory
                                                              ↓
User Query → Retrieve Relevant Chunks → Combine with Data → LLM Reasoning → Enhanced Insights
```

### Reasoning Layer Integration

When you ask a question or request analysis:

1. **Query Analysis**: Your question is processed
2. **Context Retrieval**: Relevant knowledge chunks are retrieved from learned sources
3. **Data Integration**: Campaign data context is added
4. **Enhanced Reasoning**: LLM combines:
   - Your campaign performance data
   - Best practices from learned sources
   - Built-in marketing expertise
5. **Actionable Output**: Recommendations backed by both data and knowledge

## Usage Examples

### Example 1: Learn Industry Best Practices

```python
from src.knowledge import EnhancedReasoningEngine

# Initialize
engine = EnhancedReasoningEngine()

# Learn from Google Ads blog
engine.learn_from_url("https://blog.google/products/ads/performance-max-tips/")

# Learn from Meta Business blog
engine.learn_from_url("https://www.facebook.com/business/news/insights")

# Analyze with knowledge
result = engine.analyze_with_knowledge(
    query="How can I improve my Performance Max campaigns?",
    data_context="Current ROAS: 3.2x, Spend: $50k, Platform: Google Ads"
)

print(result['response'])
```

### Example 2: Learn from YouTube Tutorials

```python
# Learn from video tutorial
engine.learn_from_youtube("https://www.youtube.com/watch?v=TUTORIAL_ID")

# Get knowledge-enhanced insights
result = engine.get_knowledge_enhanced_insights(
    campaign_summary="ROAS: 2.8x, CTR: 1.5%, CPA: $45",
    topic="audience targeting optimization"
)
```

### Example 3: Learn from Research Papers

```python
# Learn from PDF whitepaper
engine.learn_from_pdf("digital_marketing_benchmarks_2024.pdf")

# Analyze with benchmarks
result = engine.analyze_with_knowledge(
    query="How does my performance compare to industry benchmarks?",
    data_context="ROAS: 4.2x, CTR: 2.3%, Industry: E-commerce"
)
```

## Streamlit UI

A dedicated Knowledge Base page is available in the Streamlit app:

**Location:** `streamlit_apps/pages/4_📚_Knowledge_Base.py`

**Features:**
- Upload URLs, YouTube videos, and PDFs
- View knowledge base status
- Test knowledge-enhanced reasoning
- Clear knowledge base

**Access:** Navigate to "📚 Knowledge Base" in the sidebar

## Use Cases

### 1. **Platform Updates & Best Practices**
Learn from official platform blogs and documentation:
- Google Ads blog posts
- Meta Business updates
- LinkedIn Marketing solutions
- Platform-specific optimization guides

### 2. **Industry Research & Benchmarks**
Learn from research and reports:
- Industry benchmark reports
- Marketing research papers
- Case studies
- Performance studies

### 3. **Training & Tutorials**
Learn from educational content:
- YouTube tutorials
- Webinars and conference talks
- Expert presentations
- How-to guides

### 4. **Competitive Intelligence**
Learn from public sources:
- Industry trend reports
- Market analysis
- Best practice guides
- Success stories

## Best Practices

### What to Upload ✅

- ✅ Official platform documentation and updates
- ✅ Industry research and benchmark reports
- ✅ Marketing best practice guides
- ✅ Case studies and success stories
- ✅ Educational tutorials and webinars
- ✅ Expert presentations and talks

### What NOT to Upload ❌

- See FUNNEL_ANALYTICS_SOURCES.md for funnel-specific content
- See CHANNEL_SOURCES.md for platform-specific guidance
- See VISUALIZATION_SOURCES.md for reporting and visualization

## NEW: Semantic Retrieval (RAG)

- **Vector store**: FAISS (L2-normalized inner-product index) located at `data/vector_store/faiss.index`
- **Metadata**: Stored at `data/vector_store/metadata.json`
- **Embeddings**: OpenAI `text-embedding-3-small` (update in `VectorStoreConfig` if needed)
- **Builder**: `VectorStoreBuilder` (see `src/knowledge/vector_store.py`)
  1. Run `python scripts/auto_ingest_knowledge.py` after new sources are ingested
  2. The script now persists `data/knowledge_base.json` and rebuilds the FAISS index automatically
  3. If you need a manual rebuild, import the builder and call `build_from_documents()` with the saved knowledge base
- **Retriever**: `VectorRetriever` automatically loads inside `EnhancedReasoningEngine`
  - If the FAISS index is missing, the engine falls back to keyword retrieval and logs a warning
- **Usage in reasoning**: `_get_external_context()` first queries FAISS, then appends snippets with source/title/URL into the LLM prompt
- **Troubleshooting**:
  - Missing files -> rerun ingestion or point `VectorStoreConfig` to the correct paths
  - OpenAI errors -> ensure `OPENAI_API_KEY` is set and the embedding model is available
  - Versioning -> delete `data/vector_store/*` before rebuilding if dimensions change
- **Advanced options**:
  - `HybridRetriever` blends FAISS vectors with BM25 keyword scores using Reciprocal Rank Fusion (RRF). Adjust weights via `vector_weight`, `keyword_weight`, and `rrf_k` if needed.
  - Cohere reranking (model `rerank-english-v3.0`) is enabled automatically when `COHERE_API_KEY` is set. Install deps with `pip install rank-bm25 cohere`.
  - Manual vector rebuilds: run `python scripts/auto_ingest_knowledge.py` or call `VectorStoreBuilder().build_from_documents(json.load(open('data/knowledge_base.json')))`.

This workflow enables true Retrieval-Augmented Generation across the full knowledge base.

### Tips for Better Results

1. **Quality over Quantity**: Focus on high-quality, authoritative sources
2. **Keep it Current**: Upload recent content (last 6-12 months)
3. **Diverse Sources**: Mix different types (URLs, videos, PDFs)
4. **Relevant Topics**: Upload content related to your campaigns
5. **Clear Knowledge**: Periodically clear outdated knowledge

## Technical Details

### Text Processing

- **Chunk Size**: 1000 characters
- **Chunk Overlap**: 200 characters
- **Retrieval**: Top 3-5 most relevant chunks
- **Matching**: Keyword-based (can be enhanced with embeddings)

### Storage

- **In-Memory**: Knowledge stored in session state
- **Persistence**: Cleared on app restart (can be enhanced with database)
- **Privacy**: All processing is local, no external storage

### Performance

- **URL Extraction**: ~2-5 seconds
- **YouTube Transcript**: ~1-3 seconds
- **PDF Processing**: ~5-10 seconds (depends on size)
- **Query with Knowledge**: ~3-8 seconds (depends on LLM)

## Advanced Features (Future Enhancements)

### 1. **Vector Embeddings**
```python
# Use embeddings for semantic search
from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import FAISS

# Create vector store
embeddings = OpenAIEmbeddings()
vectorstore = FAISS.from_texts(chunks, embeddings)

# Semantic search
results = vectorstore.similarity_search(query, k=5)
```

### 2. **Persistent Storage**
```python
# Save knowledge to database
import sqlite3

# Store chunks with metadata
db.execute("INSERT INTO knowledge (source, chunk, metadata) VALUES (?, ?, ?)")
```

### 3. **Knowledge Graph**
```python
# Build relationships between concepts
from langchain.graphs import KnowledgeGraph

kg = KnowledgeGraph()
kg.add_entity("Performance Max", type="campaign_type")
kg.add_relation("Performance Max", "uses", "Smart Bidding")
```

## Troubleshooting

### Common Issues

**1. "youtube-transcript-api not installed"**
```bash
pip install youtube-transcript-api
```

**2. "Video has no captions"**
- Video must have captions/subtitles enabled
- Try videos with auto-generated captions
- Check if captions are available in your language

**3. "Could not extract from URL"**
- Verify URL is accessible
- Some sites block automated access
- Try different URLs or use PDF export

**4. "PDF extraction failed"**
- Ensure PDF contains text (not scanned images)
- Try re-saving PDF with text layer
- Use OCR for scanned documents

**5. "API key not found"**
- Set OPENAI_API_KEY or ANTHROPIC_API_KEY in .env file
- Verify key is valid and has credits

## API Reference

### KnowledgeIngestion

```python
from src.knowledge import KnowledgeIngestion

ki = KnowledgeIngestion(chunk_size=1000, chunk_overlap=200)

# Ingest from URL
result = ki.ingest_from_url(url)

# Ingest from YouTube
result = ki.ingest_from_youtube(video_url, languages=['en'])

# Ingest from PDF
result = ki.ingest_from_pdf(pdf_path)

# Get context for query
context = ki.get_context_for_query(query, max_chunks=5)

# Get summary
summary = ki.get_knowledge_summary()

# Clear knowledge
ki.clear_knowledge_base()
```

### EnhancedReasoningEngine

```python
from src.knowledge import EnhancedReasoningEngine

engine = EnhancedReasoningEngine(use_anthropic=False)

# Learn from sources
engine.learn_from_url(url)
engine.learn_from_youtube(video_url)
engine.learn_from_pdf(pdf_path)

# Analyze with knowledge
result = engine.analyze_with_knowledge(
    query="Your question",
    data_context="Campaign data context",
    use_knowledge=True
)

# Get enhanced insights
result = engine.get_knowledge_enhanced_insights(
    campaign_summary="Performance summary",
    topic="optimization focus"
)

# Get status
status = engine.get_knowledge_status()

# Clear knowledge
engine.clear_knowledge()
```

## Examples in Practice

### Scenario 1: Optimizing Performance Max Campaigns

```python
# Learn from Google's official guide
engine.learn_from_url("https://support.google.com/google-ads/answer/10724817")

# Learn from YouTube tutorial
engine.learn_from_youtube("https://www.youtube.com/watch?v=PMAX_TUTORIAL")

# Analyze your campaign
result = engine.analyze_with_knowledge(
    query="How can I improve my Performance Max ROAS?",
    data_context="""
    Current Performance:
    - ROAS: 3.2x
    - Spend: $50,000/month
    - Conversions: 850
    - Asset Groups: 3
    - Product Feed: Connected
    """
)

print(result['response'])
# Output will include recommendations based on:
# 1. Your campaign data
# 2. Google's official best practices
# 3. Tutorial insights
# 4. Built-in expertise
```

### Scenario 2: Understanding Industry Benchmarks

```python
# Learn from benchmark report
engine.learn_from_pdf("ecommerce_benchmarks_2024.pdf")

# Compare your performance
result = engine.analyze_with_knowledge(
    query="How does my e-commerce performance compare to industry benchmarks?",
    data_context="""
    My Performance:
    - Industry: E-commerce (Fashion)
    - ROAS: 4.5x
    - CTR: 2.1%
    - Conversion Rate: 3.2%
    - AOV: $85
    """
)

# Get insights with benchmark comparisons
```

## Support

For issues or questions:
1. Check this documentation
2. Review error messages in logs
3. Verify package installations
4. Check API key configuration
5. Refer to package documentation:
   - [youtube-transcript-api](https://github.com/jdepoix/youtube-transcript-api)
   - [PyPDF2](https://pypdf2.readthedocs.io/)
   - [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)

## Next Steps

1. Install required packages
2. Configure API keys in `.env`
3. Start the Streamlit app
4. Navigate to "📚 Knowledge Base"
5. Upload your first knowledge source
6. Test enhanced reasoning!

---

**Note**: This feature enhances the PCA Agent's capabilities by allowing it to learn from external sources. The knowledge is used to provide more comprehensive and up-to-date recommendations alongside your campaign data analysis.
