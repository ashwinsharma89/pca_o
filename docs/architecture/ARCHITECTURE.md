## PCA Agent - System Architecture

### Overview
PCA Agent is an AI-powered system for automated post-campaign analysis across multiple advertising platforms. It uses Vision Language Models (VLMs) to extract data from dashboard screenshots and Large Language Models (LLMs) for agentic reasoning and insight generation.

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                     Frontend Layer                               │
│  ┌──────────────────┐         ┌──────────────────┐             │
│  │  Streamlit UI    │         │  External Apps   │             │
│  └────────┬─────────┘         └────────┬─────────┘             │
└───────────┼──────────────────────────────┼──────────────────────┘
            │                              │
            └──────────────┬───────────────┘
                           │
┌──────────────────────────┴───────────────────────────────────────┐
│                     API Gateway (FastAPI)                         │
│  - Campaign Management                                            │
│  - Snapshot Upload                                                │
│  - Analysis Orchestration                                         │
│  - Report Download                                                │
└────────────────────────────┬──────────────────────────────────────┘
                             │
┌────────────────────────────┴──────────────────────────────────────┐
│              Orchestration Layer (LangGraph)                       │
│  ┌──────────────────────────────────────────────────────────┐    │
│  │  Workflow State Machine                                   │    │
│  │  1. Vision Extraction → 2. Data Normalization →          │    │
│  │  3. Reasoning Analysis → 4. Visualization Generation →   │    │
│  │  5. Report Assembly                                       │    │
│  └──────────────────────────────────────────────────────────┘    │
└────────────────────────────┬──────────────────────────────────────┘
                             │
┌────────────────────────────┴──────────────────────────────────────┐
│                     Agent Layer                                    │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐         │
│  │ Vision   │  │ Extract  │  │ Reasoning│  │ Visual   │         │
│  │ Agent    │  │ Agent    │  │ Agent    │  │ Agent    │         │
│  │ (VLM)    │  │          │  │ (LLM)    │  │          │         │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘         │
│       │             │             │             │                 │
│  ┌────┴─────────────┴─────────────┴─────────────┴─────┐         │
│  │              Report Agent (PPT Generation)          │         │
│  └─────────────────────────────────────────────────────┘         │
└────────────────────────────┬──────────────────────────────────────┘
                             │
┌────────────────────────────┴──────────────────────────────────────┐
│                     External Services                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐         │
│  │ OpenAI   │  │ Anthropic│  │ LangSmith│  │ Storage  │         │
│  │ GPT-4V   │  │ Claude   │  │ (Trace)  │  │ (S3/FS)  │         │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘         │
└───────────────────────────────────────────────────────────────────┘
```

### Component Details

#### 1. Vision Agent
**Purpose**: Extract data from dashboard screenshots using Vision Language Models

**Capabilities**:
- Platform detection (auto-identify Google Ads, Meta Ads, etc.)
- Metric extraction (numbers, percentages, currency values)
- Chart data extraction (line graphs, bar charts, pie charts)
- Table parsing (campaign breakdowns, performance tables)
- Metadata extraction (date ranges, campaign names)

**Models Used**:
- OpenAI GPT-4 Vision Preview
- Anthropic Claude Sonnet 4 (vision capabilities)

**Input**: Dashboard screenshot (PNG, JPG, PDF)
**Output**: `PlatformSnapshot` with extracted metrics, charts, tables

#### 2. Extraction Agent
**Purpose**: Normalize and validate data across platforms

**Capabilities**:
- Metric normalization (standardize across platforms)
- Data validation (detect anomalies, inconsistencies)
- Derived metric calculation (CTR, CPC, CPA, ROAS)
- Cross-platform aggregation

**Input**: List of `PlatformSnapshot` with extracted data
**Output**: List of `NormalizedMetric` in unified schema

#### 3. Reasoning Agent
**Purpose**: Generate insights using agentic AI reasoning

**Capabilities**:
- Channel performance analysis (per-platform insights)
- Cross-channel analysis (synergies, attribution)
- Achievement detection (identify top performers)
- Recommendation generation (actionable insights)
- Performance scoring (0-100 scale)

**Models Used**:
- OpenAI GPT-4 Turbo
- Anthropic Claude Sonnet 4

**Input**: Campaign with normalized metrics
**Output**: Insights, achievements, recommendations

#### 4. Visualization Agent
**Purpose**: Generate charts and infographics

**Capabilities**:
- Cross-channel comparison charts
- Spend distribution pie charts
- ROAS comparison bar charts
- Efficiency scatter plots
- Performance radar charts
- Conversion funnel visualizations

**Libraries Used**:
- Plotly (interactive charts)
- Matplotlib (static charts)
- Seaborn (statistical visualizations)

**Input**: Campaign data and channel performances
**Output**: PNG images and visualization metadata

#### 5. Report Agent
**Purpose**: Assemble PowerPoint reports

**Capabilities**:
- Template-based report generation
- Slide creation (title, summary, channel details, insights)
- Chart embedding
- Table generation
- Brand customization (colors, logos)

**Libraries Used**:
- python-pptx (PowerPoint generation)

**Input**: `ConsolidatedReport` with all data
**Output**: PowerPoint file (.pptx)

### Data Flow

```
1. User uploads dashboard snapshots
   ↓
2. Vision Agent extracts data from each snapshot
   ↓
3. Extraction Agent normalizes data across platforms
   ↓
4. Reasoning Agent analyzes and generates insights
   ↓
5. Visualization Agent creates charts
   ↓
6. Report Agent assembles PowerPoint report
   ↓
7. User downloads report
```

### Data Models

#### Platform Models
- `PlatformType`: Enum of supported platforms
- `MetricType`: Standardized metric types
- `ExtractedMetric`: Raw metric from snapshot
- `NormalizedMetric`: Standardized metric
- `PlatformSnapshot`: Snapshot with extracted data

#### Campaign Models
- `Campaign`: Campaign analysis request
- `CampaignObjective`: Campaign goals
- `DateRange`: Campaign date range
- `ChannelPerformance`: Per-channel analysis
- `CrossChannelInsight`: Cross-channel insights
- `Achievement`: Key achievements
- `ConsolidatedReport`: Complete report data

### API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/campaigns` | Create new campaign |
| GET | `/api/campaigns/{id}` | Get campaign details |
| POST | `/api/campaigns/{id}/snapshots` | Upload snapshots |
| GET | `/api/campaigns/{id}/snapshots` | List snapshots |
| POST | `/api/campaigns/{id}/analyze` | Start analysis |
| GET | `/api/campaigns/{id}/status` | Check status |
| GET | `/api/campaigns/{id}/data` | Get extracted data (JSON) |
| GET | `/api/campaigns/{id}/report` | Download report (PPTX) |
| DELETE | `/api/campaigns/{id}` | Delete campaign |

### Technology Stack

**AI/ML**:
- OpenAI GPT-4 Vision, GPT-4 Turbo
- Anthropic Claude Sonnet 4
- LangChain (agent framework)
- LangGraph (workflow orchestration)
- LangSmith (observability)

**Backend**:
- FastAPI (API framework)
- Python 3.11+
- Pydantic (data validation)
- Loguru (logging)

**Data Processing**:
- OpenCV (image processing)
- Pillow (image manipulation)
- Tesseract OCR (text extraction)
- Pandas (data manipulation)

**Visualization**:
- Plotly (interactive charts)
- Matplotlib (static charts)
- Seaborn (statistical plots)

**Report Generation**:
- python-pptx (PowerPoint)
- ReportLab (PDF)

**Frontend**:
- Streamlit (demo dashboard)

### Scalability Considerations

**Current Implementation** (MVP):
- In-memory storage (campaigns_db dict)
- Synchronous processing
- Single-instance deployment

**Production Recommendations**:
1. **Database**: Replace in-memory storage with PostgreSQL
2. **Task Queue**: Use Celery + Redis for async processing
3. **Caching**: Redis for intermediate results
4. **Storage**: S3/Azure Blob for snapshots and reports
5. **Load Balancing**: Multiple API instances behind load balancer
6. **Monitoring**: Prometheus + Grafana for metrics
7. **Error Tracking**: Sentry for error monitoring

### Security Considerations

1. **API Keys**: Store in environment variables, never commit
2. **File Upload**: Validate file types and sizes
3. **Authentication**: Add JWT-based auth for production
4. **Rate Limiting**: Implement rate limiting on API endpoints
5. **Data Privacy**: Encrypt sensitive campaign data
6. **CORS**: Configure appropriate CORS policies

### Performance Optimization

1. **Parallel Processing**: Process multiple snapshots concurrently
2. **Caching**: Cache VLM responses for identical images
3. **Batch Processing**: Batch API calls to reduce latency
4. **Image Optimization**: Resize/compress images before VLM calls
5. **Lazy Loading**: Load visualizations only when needed

### Error Handling

1. **Graceful Degradation**: Continue processing if one snapshot fails
2. **Retry Logic**: Retry failed API calls with exponential backoff
3. **Validation**: Validate data at each stage
4. **Logging**: Comprehensive logging for debugging
5. **User Feedback**: Clear error messages to users

### Future Enhancements

1. **Real-time Dashboards**: Live campaign monitoring
2. **Automated Scheduling**: Periodic report generation
3. **Custom Templates**: User-defined report templates
4. **Multi-language**: Support for non-English dashboards
5. **Advanced Attribution**: Multi-touch attribution modeling
6. **Predictive Analytics**: Forecast future performance
7. **A/B Testing**: Compare campaign variations
8. **Budget Optimizer**: AI-powered budget allocation recommendations
