# Appendix A: Glossary

[← Back to Index](file:///Users/ashwin/Desktop/pca_agent%20copy/guide/INDEX.md)

---

## AI/ML Terms

### **Agent**
An autonomous software component that can make decisions and take actions to achieve a goal. In the PCA system, agents analyze data, generate insights, and create visualizations.

### **LLM (Large Language Model)**
An AI model trained on vast amounts of text data that can understand and generate human language. Examples: GPT-4, Gemini, Claude.

### **Intent Classification**
The process of determining what a user wants to do based on their question. Example: "Compare last week to this week" → Intent: COMPARISON

### **Entity Extraction**
Identifying specific pieces of information in a question. Example: "What is my CPA for Facebook?" → Entities: {metric: "CPA", platform: "Facebook"}

### **RAG (Retrieval-Augmented Generation)**
A technique where an LLM retrieves relevant information from a knowledge base before generating an answer, making responses more accurate and grounded in facts.

### **Semantic Cache**
A cache that understands meaning, not just exact matches. "What is my spend?" and "How much did I spend?" would hit the same cache entry.

### **Few-Shot Learning**
Providing an LLM with a few examples of what you want before asking it to perform a task. Improves accuracy significantly.

### **Prompt Engineering**
The art of crafting the perfect instructions for an LLM to get the best results.

---

## Marketing Analytics Terms

### **CPA (Cost Per Acquisition)**
How much you spend to get one conversion. Formula: `Total Spend / Total Conversions`

### **ROAS (Return on Ad Spend)**
How much revenue you generate for every dollar spent. Formula: `Total Revenue / Total Spend`

### **CTR (Click-Through Rate)**
Percentage of people who click your ad after seeing it. Formula: `(Clicks / Impressions) × 100`

### **CPC (Cost Per Click)**
How much you pay for each click on your ad. Formula: `Total Spend / Total Clicks`

### **Conversion**
A desired action taken by a user (purchase, sign-up, download, etc.)

### **Impression**
One instance of your ad being shown to a user.

### **MQL (Marketing Qualified Lead)**
A lead that marketing has determined is likely to become a customer (B2B term).

### **SQL (Sales Qualified Lead)**
A lead that sales has accepted and is actively pursuing (B2B term).

### **Funnel**
The stages a customer goes through from awareness to purchase.

### **Attribution**
Determining which marketing touchpoints deserve credit for a conversion.

---

## Technical Terms

### **API (Application Programming Interface)**
A way for different software systems to talk to each other. The PCA Agent exposes an API that the frontend calls.

### **Endpoint**
A specific URL path that performs a specific function. Example: `/api/v1/campaigns/chat`

### **HTTP Request**
A message sent from a client (browser) to a server asking for something.

### **JWT (JSON Web Token)**
A secure way to transmit information between parties. Used for authentication.

### **Middleware**
Code that runs between receiving a request and executing the main logic. Used for security, logging, etc.

### **Router**
A component that directs incoming requests to the appropriate handler based on the URL path.

### **CSRF (Cross-Site Request Forgery)**
A type of attack where a malicious site tricks your browser into making requests to another site. CSRF tokens prevent this.

### **Rate Limiting**
Restricting how many requests a user can make in a given time period to prevent abuse.

### **SQL (Structured Query Language)**
A language for querying databases. Example: `SELECT * FROM campaigns WHERE spend > 1000`

### **DuckDB**
An in-memory analytical database optimized for fast queries on large datasets.

### **Parquet**
A columnar file format that's highly compressed and optimized for analytics.

### **DataFrame**
A table-like data structure (rows and columns) used in Pandas and Polars.

### **CTE (Common Table Expression)**
A temporary named result set in SQL, used to make complex queries more readable.

### **Polars**
A fast DataFrame library for Python, similar to Pandas but optimized for performance.

### **FastAPI**
A modern Python web framework for building APIs quickly.

### **Mermaid**
A text-based diagramming tool that creates flowcharts, sequence diagrams, etc.

---

## Architecture Terms

### **Layer**
A distinct level in the system architecture, each with a specific responsibility.

### **Separation of Concerns**
Design principle where each component has one clear job and doesn't mix responsibilities.

### **Orchestration**
Coordinating multiple agents or services to work together toward a goal.

### **Bulletproof Query**
A pre-built, tested SQL template for common questions. Fast and reliable.

### **Template**
A reusable pattern with placeholders that can be filled in. Example: SQL templates.

### **Fallback**
A backup plan when the primary approach fails. Example: If Gemini fails, try GPT-4.

---

[← Back to Index](file:///Users/ashwin/Desktop/pca_agent%20copy/guide/INDEX.md)
