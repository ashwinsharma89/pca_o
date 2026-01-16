---
markmap:
  colorFreezeLevel: 2
---

# ⏱️ Layer Latency Topology

## L1: Perception (<100ms)
- **User Interface** (React)
  - *Rendering*: 16ms (60fps)
  - *Network*: ~50ms
- **Feedback Loop**
  - *Optimistic Updates*: Immediate

## L2: Gateway (<50ms)
- **API (FastAPI)**
  - *Validation*: 5ms
  - *Auth Check*: 10ms
  - *Serialization*: 5ms

## L3: Orchestration (200ms)
- **Routing**
  - *Intent Analysis*: 150ms
  - *Decision Tree*: 50ms

## L4: Deep Reasoning (2s - 15s)
- **Agent Thinking**
  - *LLM Token Generation*: 30-50 tokens/sec
  - *Multi-step Chain*: 3-5 hops
- **Tool Execution**
  - *DB Query*: 100ms
  - *External API*: 500ms+

## L5: Data Fetch (50ms)
- **Knowledge Graph**
  - *Cypher Query*: 20-50ms
- **Vector Search**
  - *ANN Search*: 10-30ms
