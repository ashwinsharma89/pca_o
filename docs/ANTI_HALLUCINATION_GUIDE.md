# Anti-Hallucination & Content Validation Guide

## Overview

The PCA Agent knowledge system now includes robust validation and anti-hallucination measures to ensure only reliable, validated information is used in reasoning.

## Key Principles

### 🎯 Core Rules

1. **Never Make Up Information**: If uncertain, explicitly state it or skip
2. **Validate Before Use**: All content is validated for quality and reliability
3. **Cite Sources**: Always attribute information to its source
4. **Acknowledge Gaps**: Prefer "I don't have enough information" over guessing
5. **Skip Unclear Content**: If information can't be validated, it's not used

## Content Validation System

### Automatic Quality Checks

When ingesting content from URLs, YouTube, or PDFs, the system automatically:

#### 1. **Minimum Length Check**
- Content must be at least 100 characters
- Prevents ingesting empty or trivial content

#### 2. **Word Diversity Analysis**
- Checks for repetitive content (spam indicator)
- Requires at least 30% unique words
- **Warning**: "Low word diversity - possible spam"

#### 3. **Word Length Analysis**
- Checks average word length
- Detects navigation/menu text (very short words)
- **Warning**: "May be navigation/menu text"

#### 4. **Special Character Ratio**
- Detects garbled or corrupted text
- Flags content with >30% special characters
- **Warning**: "Possible garbled text"

#### 5. **Sentence Structure**
- Analyzes sentence length and structure
- Detects formatting issues
- **Warning**: "Very short/long sentences"

#### 6. **Error Detection**
- Checks for common error pages:
  - "404 Not Found"
  - "Access Denied"
  - "JavaScript Required"
  - "Cookies Required"
- **Rejects** content with error indicators

### Quality Scoring (0-100)

Each piece of content receives a quality score:

- **70-100**: ✅ Excellent - High confidence
- **50-69**: ℹ️ Good - Moderate confidence
- **30-49**: ⚠️ Fair - Use with caution
- **0-29**: ❌ Rejected - Not used

**Scoring Factors:**
- Base score: 100
- Penalties for quality issues: -10 to -25 each
- Bonuses for substantive content: +10 to +20
- **Minimum threshold**: 30 (below this = rejected)

## LLM Anti-Hallucination Instructions

### System Prompt Rules

The reasoning engine includes explicit anti-hallucination instructions:

```
IMPORTANT RULES:
1. NEVER make up information or hallucinate facts
2. If uncertain, explicitly state "I'm not certain" or skip it
3. Only use information that can be validated from provided sources
4. Cite sources when referencing external knowledge
5. If information is unclear or contradictory, acknowledge uncertainty
6. Prefer saying "I don't have enough information" over guessing
7. Distinguish between:
   - Facts from sources (cite them)
   - Data-driven insights (reference the data)
   - General best practices (label as such)
   - Your analysis (clearly mark as interpretation)
```

### Response Format Requirements

The LLM is instructed to:

✅ **DO:**
- "Based on the data..."
- "According to [source name]..."
- "The source suggests..."
- "I'm not certain about..."
- "The source doesn't provide clear information on..."

❌ **DON'T:**
- Make definitive claims without source attribution
- Fill gaps with assumptions
- Ignore contradictions
- Present guesses as facts

## Usage Examples

### Example 1: Validated Content

**Input URL**: High-quality Google Ads blog post

**Validation Result:**
```
✅ Quality Score: 85/100 - Excellent
📊 Extracted 12 chunks
No warnings
```

**AI Response:**
```
According to Google's Performance Max guide (source: blog.google.com), 
you should have 3-5 asset groups for optimal performance. Your current 
setup with 2 asset groups is below the recommended range.

Based on your data showing ROAS of 3.2x, this aligns with the guide's 
suggestion that proper asset group structure can improve ROAS by 15-20%.

Recommendation: Add 1-2 more asset groups with different creative angles.
```

### Example 2: Low-Quality Content Rejected

**Input URL**: Page with mostly navigation/ads

**Validation Result:**
```
❌ Quality Score: 25/100
⚠️ Warnings:
- Low word diversity - possible spam
- Very short average word length - may be navigation text
- Quality score too low (25/100)

Error: Content validation failed
```

**Result**: Content is **not ingested** - prevents hallucination from poor sources

### Example 3: Uncertain Information

**Query**: "What's the best bidding strategy for my campaign?"

**AI Response (with anti-hallucination):**
```
Based on your data showing ROAS of 3.2x and stable performance, 
Target ROAS bidding could be appropriate.

However, the sources I have don't provide specific guidance for your 
exact scenario (e-commerce, $50k budget, 3.2x ROAS). 

General best practices suggest:
- Target ROAS works well for campaigns with consistent conversion data
- You have sufficient conversion volume (850/month) for automated bidding

I'm not certain about the optimal target ROAS value for your specific 
case without more industry-specific benchmarks. Consider starting 
conservatively at your current 3.2x and gradually increasing.
```

## Validation in Practice

### When Ingesting Content

```python
from src.knowledge import EnhancedReasoningEngine

engine = EnhancedReasoningEngine()

# Attempt to learn from URL
result = engine.learn_from_url("https://example.com/article")

if result['success']:
    print(f"Quality Score: {result['quality_score']}/100")
    
    # Check for warnings
    validation = result.get('validation', {})
    if validation.get('warnings'):
        print("Warnings:")
        for warning in validation['warnings']:
            print(f"  - {warning}")
else:
    print(f"Rejected: {result['error']}")
```

### When Analyzing with Knowledge

```python
# Analyze with validated knowledge only
result = engine.analyze_with_knowledge(
    query="How to improve ROAS?",
    data_context="Current ROAS: 3.2x",
    use_knowledge=True  # Only uses validated sources
)

# Response will include source citations and uncertainty acknowledgments
print(result['response'])
```

## Best Practices for Users

### 1. **Choose Reliable Sources**

✅ **Good Sources:**
- Official platform blogs (Google Ads, Meta Business)
- Reputable industry publications
- Academic research papers
- Well-known marketing experts

❌ **Avoid:**
- Spam sites
- Low-quality content farms
- Outdated information
- Unverified claims

### 2. **Review Quality Scores**

Always check the quality score when ingesting:
- **85+**: Excellent - High confidence
- **70-84**: Good - Reliable
- **50-69**: Fair - Use with caution
- **<50**: Consider finding better sources

### 3. **Monitor Validation Warnings**

Pay attention to warnings:
- "Low word diversity" → May be repetitive/spam
- "Navigation text" → May not be substantive
- "Garbled text" → Extraction issues

### 4. **Cross-Reference Information**

For critical decisions:
- Use multiple sources
- Look for consensus
- Verify with official documentation

### 5. **Question AI Responses**

When reviewing AI responses:
- Look for source citations
- Check for uncertainty acknowledgments
- Verify claims against original sources
- Be skeptical of uncited claims

## Technical Implementation

### Validation Pipeline

```
URL/YouTube/PDF
    ↓
Extract Content
    ↓
Quality Checks:
  - Length check
  - Word diversity
  - Special characters
  - Error detection
  - Sentence structure
    ↓
Calculate Quality Score
    ↓
Score >= 30? → YES → Store in Knowledge Base
              → NO  → Reject with reason
    ↓
LLM Reasoning (with anti-hallucination rules)
    ↓
Validated Response with Citations
```

### Code Example: Custom Validation

```python
from src.knowledge import KnowledgeIngestion

# Initialize with custom thresholds
ki = KnowledgeIngestion(
    chunk_size=1000,
    chunk_overlap=200,
    min_content_length=200  # Stricter minimum
)

# Ingest with validation
result = ki.ingest_from_url(url)

# Check validation details
if result['success']:
    validation = result['validation']
    print(f"Quality: {validation['quality_score']}/100")
    print(f"Warnings: {len(validation['warnings'])}")
    
    # Custom decision based on quality
    if validation['quality_score'] < 60:
        print("⚠️ Low quality - consider finding better source")
```

## Monitoring and Feedback

### In Streamlit UI

The Knowledge Base page shows:
- ✅ Quality scores for each source
- ⚠️ Validation warnings
- 📊 Content preview
- 🗑️ Easy removal of low-quality sources

### In Code

```python
# Get knowledge base summary
status = engine.get_knowledge_status()
print(f"Total sources: {status['total_documents']}")
print(f"Total chunks: {status['total_chunks']}")

# Review individual sources
for item in knowledge_items:
    if item.get('quality_score', 0) < 60:
        print(f"⚠️ Low quality: {item['source']}")
```

## Troubleshooting

### "Content validation failed"

**Possible reasons:**
1. Content too short (< 100 chars)
2. Quality score too low (< 30)
3. Error page detected
4. Excessive repetition/spam

**Solutions:**
- Try a different URL
- Check if page loaded correctly
- Verify source is substantive content
- Use PDF export if web extraction fails

### "Quality score is low"

**Possible reasons:**
1. Navigation/menu text extracted
2. Garbled or corrupted text
3. Very short sentences
4. Low word diversity

**Solutions:**
- Try different extraction method
- Use PDF version if available
- Manually verify content quality
- Consider alternative sources

### AI response lacks detail

**Possible reasons:**
1. Limited validated knowledge
2. Sources don't cover the topic
3. AI following anti-hallucination rules

**Solutions:**
- Add more relevant sources
- Use higher-quality sources
- Provide more specific data context
- This is actually GOOD - prevents hallucination!

## Summary

The anti-hallucination system ensures:

✅ **Only validated content is used**
- Automatic quality checks
- Minimum quality threshold
- Error detection

✅ **AI is honest about limitations**
- Cites sources
- Acknowledges uncertainty
- Skips unclear information

✅ **Users have transparency**
- Quality scores visible
- Validation warnings shown
- Source attribution clear

✅ **Continuous improvement**
- Monitor quality scores
- Remove low-quality sources
- Add better sources over time

**Result**: Reliable, trustworthy insights backed by validated sources! 🎯
