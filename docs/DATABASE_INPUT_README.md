# 🗄️ Database Input Feature

Connect directly to your database to analyze campaign data without CSV exports!

## ✨ Features

- **Multiple Database Support**: PostgreSQL, MySQL, SQLite, SQL Server
- **Secure Connections**: Password-protected connections with session management
- **Table Browser**: View all tables and their schemas
- **Custom Queries**: Write SQL queries for complex data filtering
- **Row Limits**: Control data volume with configurable row limits
- **Auto-Normalization**: Automatic column name mapping for analysis

---

## 🚀 Quick Start

### 1. Install Database Drivers

```bash
# PostgreSQL
pip install psycopg2-binary

# MySQL
pip install pymysql

# SQL Server
pip install pyodbc

# SQLite (included in Python)
```

### 2. Launch Streamlit App

```bash
streamlit run streamlit_app_hitl.py
```

### 3. Select Database Input

1. Choose **"🗄️ Database"** from input methods
2. Select your database type
3. Enter connection credentials
4. Click **"🔌 Connect"**

---

## 📊 Supported Databases

### PostgreSQL

```
Database Type: PostgreSQL
Host: localhost
Port: 5432
Database: campaign_data
Username: your_username
Password: your_password
```

### MySQL

```
Database Type: MySQL
Host: localhost
Port: 3306
Database: campaign_data
Username: your_username
Password: your_password
```

### SQLite

```
Database Type: SQLite
File Path: data/campaigns.db
```

### SQL Server

```
Database Type: SQL Server
Host: localhost
Port: 1433
Database: campaign_data
Username: your_username
Password: your_password
```

---

## 🔍 Usage Examples

### Example 1: Load Entire Table

1. Connect to database
2. Select **"📋 Select Table"**
3. Choose table from dropdown
4. Set row limit (e.g., 10,000)
5. Click **"📥 Load Data from Table"**

### Example 2: Custom SQL Query

1. Connect to database
2. Select **"✍️ Custom Query"**
3. Enter SQL query:
   ```sql
   SELECT * FROM campaigns 
   WHERE date >= '2024-01-01' 
   AND platform IN ('Meta', 'Google')
   AND spend > 1000
   ORDER BY date DESC
   ```
4. Click **"▶️ Execute Query"**

### Example 3: View Table Schema

1. Connect to database
2. Select table
3. Check **"Show Table Schema"**
4. View column names, types, and nullability

---

## 🛠️ Database Connector API

### Programmatic Usage

```python
from src.data.database_connector import DatabaseConnector

# Connect to PostgreSQL
connector = DatabaseConnector()
connector.connect(
    db_type='postgresql',
    host='localhost',
    port=5432,
    database='campaign_data',
    username='user',
    password='pass'
)

# Get tables
tables = connector.get_tables()
print(f"Found {len(tables)} tables")

# Load table
df = connector.load_table('campaigns', limit=1000)

# Execute custom query
query = "SELECT * FROM campaigns WHERE roas > 2.0"
df = connector.execute_query(query)

# Close connection
connector.close()
```

### Quick Functions

```python
from src.data.database_connector import load_from_database

# One-liner to load data
df = load_from_database(
    query="SELECT * FROM campaigns WHERE date >= '2024-01-01'",
    db_type='postgresql',
    host='localhost',
    database='campaign_data',
    username='user',
    password='pass'
)
```

---

## 🔒 Security Best Practices

### 1. Use Environment Variables

```python
import os
from dotenv import load_dotenv

load_dotenv()

connector.connect(
    db_type='postgresql',
    host=os.getenv('DB_HOST'),
    database=os.getenv('DB_NAME'),
    username=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)
```

### 2. Create `.env` File

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=campaign_data
DB_USER=your_username
DB_PASSWORD=your_password
```

### 3. Add to `.gitignore`

```
.env
*.db
```

---

## 📋 Required Database Schema

The database connector works with any schema, but for optimal analysis, include these columns:

### Recommended Columns

| Column | Type | Description |
|--------|------|-------------|
| `Campaign_Name` | VARCHAR | Campaign identifier |
| `Platform` | VARCHAR | Ad platform (Meta, Google, etc.) |
| `Date` | DATE | Campaign date |
| `Spend` | DECIMAL | Ad spend |
| `Impressions` | INTEGER | Ad impressions |
| `Clicks` | INTEGER | Ad clicks |
| `Conversions` | INTEGER | Conversions |
| `Revenue` | DECIMAL | Revenue generated |
| `ROAS` | DECIMAL | Return on ad spend |
| `CTR` | DECIMAL | Click-through rate |
| `CPA` | DECIMAL | Cost per acquisition |

### Example Table Creation

```sql
CREATE TABLE campaigns (
    id SERIAL PRIMARY KEY,
    campaign_name VARCHAR(255),
    platform VARCHAR(50),
    date DATE,
    spend DECIMAL(10,2),
    impressions INTEGER,
    clicks INTEGER,
    conversions INTEGER,
    revenue DECIMAL(10,2),
    roas DECIMAL(5,2),
    ctr DECIMAL(5,4),
    cpa DECIMAL(10,2)
);
```

---

## 🐛 Troubleshooting

### Connection Failed

**Issue**: "Connection failed: could not connect to server"

**Solutions**:
- Verify database is running
- Check host and port are correct
- Ensure firewall allows connections
- Verify credentials are correct

### Driver Not Found

**Issue**: "No module named 'psycopg2'"

**Solution**:
```bash
pip install psycopg2-binary
```

### Table Not Found

**Issue**: "Table 'campaigns' doesn't exist"

**Solutions**:
- Check table name spelling
- Verify you're connected to correct database
- Use "Show Table Schema" to browse available tables

### Query Timeout

**Issue**: Query takes too long

**Solutions**:
- Add row limit
- Optimize query with WHERE clauses
- Add indexes to database
- Use date range filters

---

## 📈 Performance Tips

### 1. Use Row Limits

```sql
-- Instead of loading all data
SELECT * FROM campaigns

-- Use limits for faster loading
SELECT * FROM campaigns LIMIT 10000
```

### 2. Filter by Date

```sql
-- Load recent data only
SELECT * FROM campaigns 
WHERE date >= CURRENT_DATE - INTERVAL '90 days'
```

### 3. Select Specific Columns

```sql
-- Instead of SELECT *
SELECT campaign_name, platform, date, spend, roas, conversions
FROM campaigns
WHERE date >= '2024-01-01'
```

### 4. Use Indexes

```sql
-- Add indexes for faster queries
CREATE INDEX idx_campaigns_date ON campaigns(date);
CREATE INDEX idx_campaigns_platform ON campaigns(platform);
```

---

## 🔄 Connection Management

### Session State

The database connection is stored in Streamlit session state:

```python
# Check if connected
if st.session_state.get('db_connected', False):
    connector = st.session_state.db_connector
    # Use connector
```

### Reconnect

If connection is lost:
1. Click **"🔌 Reconnect"** button
2. Or refresh the page and reconnect

### Close Connection

Connections are automatically closed when:
- Streamlit session ends
- Page is refreshed
- User disconnects manually

---

## 📚 Additional Resources

### Database Drivers

- **PostgreSQL**: https://www.psycopg.org/
- **MySQL**: https://github.com/PyMySQL/PyMySQL
- **SQL Server**: https://github.com/mkleehammer/pyodbc
- **SQLAlchemy**: https://www.sqlalchemy.org/

### SQL Tutorials

- **PostgreSQL**: https://www.postgresql.org/docs/
- **MySQL**: https://dev.mysql.com/doc/
- **SQL Server**: https://docs.microsoft.com/en-us/sql/

---

## ✅ Feature Checklist

- [x] PostgreSQL support
- [x] MySQL support
- [x] SQLite support
- [x] SQL Server support
- [x] Table browser
- [x] Schema viewer
- [x] Custom queries
- [x] Row limits
- [x] Connection management
- [x] Error handling
- [x] Data normalization
- [x] Streamlit UI integration

---

## 🎯 Next Steps

1. **Test Connection**: Try connecting to your database
2. **Browse Tables**: Explore available tables
3. **Load Data**: Load campaign data
4. **Analyze**: Run AI analysis on database data
5. **Compare**: Test RAG-enhanced summaries

**Happy analyzing!** 🚀
