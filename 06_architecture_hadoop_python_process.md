# 📘 Study Notes — 8. Architecture + 9. Hadoop + 10. Python + 11. Project & Process

---

# 8. Architecture & Data Flow

### Q111. Describe your overall data architecture.

**Template answer for a typical Azure Databricks project:**

```
Sources → Ingestion → Storage → Processing → Serving → Consumption
```

```
[On-prem SQL]  ─┐
[REST APIs]    ─┤  ADF   →  ADLS Gen2  →  Databricks  →  Delta Tables  →  Power BI
[SFTP/Files]   ─┤  (Copy)    (Raw)        (Bronze→      (Gold/Silver)     (Dashboards)
[Kafka]        ─┘                          Silver→Gold)
```

**Key talking points:**
- **Sources:** SQL Server, Oracle, REST APIs, flat files, Kafka streams
- **Ingestion:** ADF Copy Activity (batch), Event Hub (streaming)
- **Storage:** ADLS Gen2 — raw zone, curated zone, consumption zone
- **Processing:** Databricks — Bronze (raw), Silver (cleaned), Gold (aggregated)
- **Governance:** Unity Catalog — access control, data lineage, audit
- **Orchestration:** ADF triggers + Databricks Workflows
- **Serving:** Gold tables → Power BI, Synapse SQL pool, APIs
- **Security:** Service Principal, Secret Scopes, RBAC

---

### Q112. How does data come in and get consumed?

**Data ingestion (in):**
| Source Type | Method |
|-------------|--------|
| Relational DB (SQL Server, Oracle) | ADF Copy Activity with Self-hosted IR |
| REST APIs | ADF Web Activity / Custom Python |
| Files (CSV, JSON, Excel) | ADF Copy from SFTP/Blob to ADLS |
| Streaming (Kafka, Event Hub) | Databricks Structured Streaming |
| Cloud services (Salesforce, SAP) | ADF connectors or Fivetran |

**Data consumption (out):**
| Consumer | Method |
|----------|--------|
| **Power BI** | Direct Query or Import from Gold Delta tables |
| **SQL analysts** | Databricks SQL Warehouse |
| **Data scientists** | Databricks notebooks (read from Silver/Gold) |
| **APIs** | Azure Function reading from Delta / SQL |
| **Other teams** | Delta Sharing (cross-org data sharing) |

---

### Q113. What is data lineage?

**Data lineage** tracks the **origin, movement, and transformation** of data through the entire pipeline.

**What it answers:**
- Where did this data come from? (origin)
- What transformations were applied? (processing)
- What downstream tables/reports depend on this data? (impact)

**In Databricks Unity Catalog:**
- **Automatic lineage** — tracks table-to-table and column-to-column lineage
- **Visible in Unity Catalog UI** — graphical lineage view
- Tracks lineage from SQL, Python, R notebooks

**Why it matters:**
- **Impact analysis** — if source changes, what breaks downstream?
- **Debugging** — trace data issues back to source
- **Compliance** — regulatory requirements (GDPR, SOX) require data traceability
- **Documentation** — auto-generated data documentation

---

### Q114. External data sources → pull/copy to Delta Lake.

**Methods:**
| Method | Use case |
|--------|----------|
| **ADF Copy Activity** | Bulk copy from 100+ connectors (SQL, S3, SFTP, APIs) |
| **Databricks Auto Loader** | Incrementally load new files from cloud storage |
| **COPY INTO** | SQL command to load files into Delta table |
| **Custom Python (requests)** | REST API ingestion |
| **Fivetran / Airbyte** | SaaS connectors (Salesforce, HubSpot, etc.) |

```sql
-- COPY INTO (incremental file loading)
COPY INTO my_delta_table
FROM 'abfss://container@storage.dfs.core.windows.net/raw/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');
```

```python
# Auto Loader (recommended for production)
df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", "/checkpoints/schema") \
    .load("abfss://container@storage.dfs.core.windows.net/raw/")

df.writeStream.format("delta") \
    .option("checkpointLocation", "/checkpoints/my_table") \
    .outputMode("append") \
    .start("/mnt/delta/my_table")
```

---

### Q115. What is Big Data?

**Big Data** = datasets too large or complex for traditional database tools to process.

**5 V's of Big Data:**
| V | Meaning | Example |
|---|---------|---------|
| **Volume** | Massive amount of data | Petabytes of log data |
| **Velocity** | Speed of data generation | Real-time sensor data, social media |
| **Variety** | Different formats | Structured (SQL), semi-structured (JSON), unstructured (images) |
| **Veracity** | Data quality/accuracy | Noisy, incomplete, inconsistent data |
| **Value** | Business insights from data | Predictive analytics, recommendations |

**Technologies:** Hadoop, Spark, Databricks, Kafka, ADLS, Delta Lake

---

### Q116. Steps to ingest on-premises data to ADLS.

1. **Install Self-hosted Integration Runtime** on an on-prem machine
2. **Register IR** with Azure Data Factory
3. **Create Linked Service** in ADF:
   - Source: On-prem SQL Server (via Self-hosted IR)
   - Sink: ADLS Gen2 (via Azure IR)
4. **Create datasets** for source and sink
5. **Create pipeline** with Copy Activity
6. **Configure schedule trigger** (daily/hourly)
7. **Test and monitor** via ADF Monitor tab

```
[On-prem SQL Server] → [Self-hosted IR] → [ADF Pipeline] → [Azure IR] → [ADLS Gen2]
                         (on-prem machine)                   (cloud)      (cloud storage)
```

---

### Q117. How to pull data from MySQL to Hadoop.

**Using Apache Sqoop:**
```bash
sqoop import \
    --connect jdbc:mysql://hostname:3306/database \
    --username user --password pass \
    --table customers \
    --target-dir /user/hadoop/customers \
    --as-parquetfile \
    --num-mappers 4
```

**Alternatives:**
- **Spark JDBC:** `spark.read.format("jdbc").option("url", "jdbc:mysql://...").load()`
- **ADF + HDFS connector:** Copy Activity with HDFS linked service
- **Kafka Connect:** CDC-based real-time ingestion

---

---

# 9. Hadoop Ecosystem

### Q118. Core components of Hadoop.

| Component | Purpose |
|-----------|---------|
| **HDFS** | Distributed file system — stores data across cluster nodes |
| **MapReduce** | Distributed processing framework (map → shuffle → reduce) |
| **YARN** | Resource manager — allocates CPU/memory to applications |
| **Hadoop Common** | Utilities and libraries shared by other modules |

**Extended ecosystem:**
| Tool | Purpose |
|------|---------|
| **Hive** | SQL on Hadoop (translates SQL → MapReduce/Spark) |
| **HBase** | NoSQL column-family database on HDFS |
| **Sqoop** | Import/export between RDBMS ↔ HDFS |
| **Pig** | Scripting language for data transformation |
| **Flume** | Log/event data collection into HDFS |
| **Oozie** | Workflow scheduler for Hadoop jobs |
| **Zookeeper** | Distributed coordination service |

---

### Q119. NameNode vs DataNode.

| Aspect | NameNode | DataNode |
|--------|----------|----------|
| **Role** | Master — manages metadata | Worker — stores actual data |
| **Stores** | File system tree, block locations, permissions | Data blocks (default 128 MB each) |
| **Count** | 1 per cluster (+ standby for HA) | Many (10s to 1000s) |
| **Failure impact** | Cluster is unusable (single point of failure) | Data is replicated — cluster continues |
| **Memory** | High (metadata in RAM) | Moderate |

```
NameNode (metadata):
  /data/sales.csv → Block1 (DN1, DN3), Block2 (DN2, DN4), Block3 (DN1, DN5)

DataNodes: Actually store the 128 MB blocks
  DN1: [Block1, Block3]
  DN2: [Block2]
  DN3: [Block1]  (replica)
  DN4: [Block2]  (replica)
  DN5: [Block3]  (replica)
```

---

### Q120. Hive vs HBase.

| Aspect | Hive | HBase |
|--------|------|-------|
| **Type** | Data warehouse (SQL) | NoSQL database |
| **Query language** | HiveQL (SQL-like) | Java API, HBase Shell |
| **Use case** | Batch analytics, reporting | Real-time random read/write |
| **Latency** | High (minutes for queries) | Low (milliseconds) |
| **Data model** | Tables with rows & columns (schema) | Column-family (schema-less) |
| **Storage** | HDFS files | HDFS (HFiles) |
| **ACID** | Limited (Hive 3+) | Yes (row-level) |
| **Best for** | ETL, analytics, BI | Real-time lookups, IoT, time-series |

> **Simple answer:** "Hive is for analytics (batch SQL on HDFS), HBase is for real-time random access (NoSQL on HDFS)."

---

### Q121. Create Hive table partitioned by year & month + load + query.

```sql
-- Create partitioned table
CREATE TABLE insurance_payments (
    payment_id INT,
    customer_id INT,
    amount DECIMAL(10,2),
    payment_date DATE
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET;

-- Load data (static partitioning)
INSERT INTO insurance_payments PARTITION (year=2024, month=3)
SELECT payment_id, customer_id, amount, payment_date
FROM raw_payments
WHERE YEAR(payment_date) = 2024 AND MONTH(payment_date) = 3;

-- OR dynamic partitioning
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO insurance_payments PARTITION (year, month)
SELECT payment_id, customer_id, amount, payment_date,
    YEAR(payment_date) AS year, MONTH(payment_date) AS month
FROM raw_payments;

-- Query March 2024
SELECT * FROM insurance_payments WHERE year = 2024 AND month = 3;
```

---

---

# 10. Python

### Q122–124. Find occurrences of values in a Python list.

**Beginner approach:**
```python
my_list = ["apple", "banana", "apple", "cherry", "banana", "apple"]

# Manual counting
counts = {}
for item in my_list:
    if item in counts:
        counts[item] += 1
    else:
        counts[item] = 1
print(counts)
# {'apple': 3, 'banana': 2, 'cherry': 1}
```

**Pythonic approach:**
```python
from collections import Counter

my_list = ["apple", "banana", "apple", "cherry", "banana", "apple"]
counts = Counter(my_list)
print(counts)  # Counter({'apple': 3, 'banana': 2, 'cherry': 1})
print(counts.most_common(2))  # [('apple', 3), ('banana', 2)]
```

**For numbers:**
```python
num_list = [1, 2, 3, 1, 2, 1, 4, 5, 2]

# Beginner approach
counts = {}
for num in num_list:
    if num in counts:
        counts[num] += 1
    else:
        counts[num] = 1
print(counts)  # {1: 3, 2: 3, 3: 1, 4: 1, 5: 1}

# Using .get() (slightly cleaner)
counts = {}
for num in num_list:
    counts[num] = counts.get(num, 0) + 1

# Pythonic
from collections import Counter
counts = Counter(num_list)
```

---

---

# 11. Project Walkthrough & Process

### Q125–129. Project intro & end-to-end walkthrough.

**Template answer (customize to your project):**

> "I'm working on a data engineering project for [Client/Domain]. The data flows from [sources] through our pipeline into a lakehouse architecture on Azure."

**Structure your answer:**
1. **Project context:** Client, domain (financial, retail, healthcare), data volume
2. **Sources:** SQL Server, SAP, REST APIs, flat files
3. **Ingestion:** ADF pipelines triggered by schedule/events
4. **Processing:** Databricks notebooks — Bronze → Silver → Gold
5. **Key transformations:** Deduplication, SCD Type 2, aggregations, joins with reference data
6. **Storage:** ADLS Gen2 with Delta Lake format
7. **Serving:** Power BI dashboards, Synapse for ad-hoc queries
8. **Governance:** Unity Catalog, RBAC, data lineage
9. **Your role:** Developed X pipelines, optimized Y, migrated from Z

> **Interview tip:** Pick ONE pipeline you know inside out — from source to dashboard. Be ready to deep-dive on any part.

---

### Q130. What is Agile?

**Agile** is an iterative software development methodology focused on delivering small, incremental changes in short cycles (**sprints**).

**Key practices in your data engineering context:**
| Practice | How you use it |
|----------|---------------|
| **Sprint** | 2-week cycles — deliver pipeline features |
| **Daily standup** | 15 min — blockers, progress, plan |
| **Sprint planning** | Prioritize pipeline tasks for the sprint |
| **Retrospective** | What went well, what to improve |
| **User stories** | "As a business analyst, I need sales data refreshed daily" |
| **Kanban/Jira** | Track tasks: To Do → In Progress → Code Review → Done |

---

### Q131. End-to-end delivery process.

```
Requirement → Design → Develop → Test → Deploy → Monitor
```

1. **Requirement:** Business analyst raises a user story / requirement in Jira
2. **Design:** Data engineer reviews source, designs pipeline (mapping document)
3. **Develop:** Build ADF pipeline + Databricks notebooks in dev environment
4. **Unit test:** Validate logic with sample data
5. **Code review:** Peer review via Git pull request
6. **Deploy to QA:** CI/CD (Azure DevOps) promotes to QA environment
7. **SIT (System Integration Testing):** End-to-end testing with real data
8. **UAT (User Acceptance Testing):** Business users validate output
9. **Deploy to Prod:** Approved release pushed to production
10. **Hypercare:** Monitor for 1–2 weeks post-deployment

---

### Q132. Testing — UAT & SIT.

| Test | Who | What | When |
|------|-----|------|------|
| **Unit Testing** | Developer | Test individual transformations, logic | During development |
| **SIT** | Dev + QA team | End-to-end pipeline with integrated systems | After dev complete |
| **UAT** | Business users | Validate data accuracy, business rules | Before production |

**What I test:**
- Row counts match between source and target
- Data quality (nulls, duplicates, ranges)
- SCD logic (correct history tracking)
- Incremental load (no data loss, no duplicates)
- Error handling (bad data, missing files)

---

### Q133. How deployment is done.

**CI/CD pipeline (Azure DevOps):**

```
Git (main branch) → Build Pipeline → Release Pipeline → Deploy to Dev/QA/Prod
```

**Components deployed:**
| Component | How |
|-----------|-----|
| **ADF pipelines** | ARM templates exported → deployed via Azure DevOps |
| **Databricks notebooks** | Repos (Git integration) or Databricks CLI / REST API |
| **Delta tables (DDL)** | SQL scripts executed via Databricks SQL |
| **Cluster configs** | Terraform / ARM templates |

**Promotion path:** Dev → QA → Prod (separate workspaces, same Git repo with branches)

---

### Q134. Hypercare — what monitoring you do.

**Hypercare** = intensive monitoring period (1–2 weeks) after production deployment.

**What to monitor:**
| Check | How |
|-------|-----|
| **Pipeline success/failure** | ADF Monitor, Databricks job alerts |
| **Data freshness** | Check `MAX(load_timestamp)` — is data arriving on time? |
| **Row counts** | Compare source vs target daily |
| **Data quality** | Null percentages, duplicate checks, range validation |
| **Performance** | Job duration — is it within SLA? |
| **Business validation** | Power BI reports showing correct data |
| **Error logs** | Check for warnings, retries, data issues |

**Columns to mark:** `load_status`, `load_timestamp`, `record_count`, `error_count` — tracked in a monitoring/audit table.

---

### Q135–138. Migration projects.

**Unity Catalog migration (Q135):**
- Migrate from Hive Metastore to Unity Catalog
- Steps: Create catalog → create schemas → migrate tables (`CTAS` or `ALTER TABLE SET OWNER`)
- Update all notebooks to use 3-level namespace (`catalog.schema.table`)
- Reconfigure access control (GRANT/REVOKE)

**Alteryx to Databricks migration (Q137):**
1. **Inventory:** List all Alteryx workflows, dependencies, schedules
2. **Prioritize:** Critical workflows first, batch by complexity
3. **Convert:** Translate Alteryx visual logic to PySpark/SQL notebooks
4. **Test:** Validate output matches between Alteryx and Databricks
5. **Parallel run:** Run both systems simultaneously for 2 weeks
6. **Cutover:** Decommission Alteryx workflows after validation
7. **AI tools (Q138):** Use Copilot/ChatGPT to translate Alteryx XML → PySpark code, accelerate conversion

---

### Q139. How to use Gen AI tools in day-to-day project?

| Use case | Tool |
|----------|------|
| **Code generation** | GitHub Copilot — auto-complete PySpark/SQL |
| **Code review** | ChatGPT — review logic, suggest optimizations |
| **Documentation** | Generate docstrings, README, pipeline documentation |
| **Debugging** | Paste error → get explanation + fix |
| **SQL optimization** | Paste slow query → get optimized version |
| **Learning** | Explain complex Spark internals, Delta Lake features |
| **Data quality rules** | Generate validation checks from business requirements |

> **Answer:** "I use GitHub Copilot for PySpark code generation, ChatGPT for debugging complex Spark errors, and for generating documentation. It speeds up development by ~30% but I always review and test AI-generated code."
