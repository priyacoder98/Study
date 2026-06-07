# 📘 Study Notes — 3. Databricks & Delta Lake

---

## Delta Lake Properties

### Q36. What are the properties of Delta Lake?

Delta Lake is an **open-source storage layer** that brings reliability to data lakes.

**Key properties:**
| Property | Description |
|----------|-------------|
| **ACID transactions** | Atomic, Consistent, Isolated, Durable — ensures data integrity |
| **Schema enforcement** | Rejects writes that don't match the table schema |
| **Schema evolution** | Supports adding new columns via `mergeSchema` |
| **Time travel** | Query previous versions of data using `VERSION AS OF` |
| **Unified batch & streaming** | Same table supports both batch reads/writes and streaming |
| **DML support** | `UPDATE`, `DELETE`, `MERGE` on data lake files (unlike plain Parquet) |
| **Transaction log** | `_delta_log/` directory tracks every change as JSON commits |
| **Scalable metadata** | Handles billions of files efficiently |

> **Interview tip:** "Delta Lake gives us data warehouse reliability on top of data lake storage — ACID transactions, time travel, and MERGE/UPDATE/DELETE that plain Parquet can't do."

---

### Q37. What are ACID properties of Delta Lake?

| Property | Meaning | Delta Lake Implementation |
|----------|---------|--------------------------|
| **Atomicity** | All or nothing — transaction fully completes or fully rolls back | Each commit to `_delta_log` is atomic |
| **Consistency** | Data moves from one valid state to another | Schema enforcement ensures only valid data is written |
| **Isolation** | Concurrent transactions don't interfere | Optimistic concurrency control; serializable isolation |
| **Durability** | Committed data is never lost | Data stored on durable cloud storage (ADLS/S3) + transaction log |

```python
# ACID in action: MERGE is atomic
deltaTable.alias("target").merge(
    updates.alias("source"),
    "target.id = source.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
# Either ALL updates succeed, or NONE do
```

---

### Q38. How do you use Delta tables?

```python
# Create Delta table
df.write.format("delta").save("/mnt/delta/my_table")

# OR register as a managed table
df.write.format("delta").saveAsTable("my_database.my_table")

# Read
df = spark.read.format("delta").load("/mnt/delta/my_table")
# OR
df = spark.table("my_database.my_table")

# DML Operations
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/delta/my_table")

# UPDATE
deltaTable.update(
    condition="id = 1",
    set={"name": "'Updated Name'"}
)

# DELETE
deltaTable.delete("status = 'inactive'")

# MERGE (upsert)
deltaTable.alias("t").merge(
    source_df.alias("s"), "t.id = s.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

---

### Q39. Delta Lifecycle vs Normal Pipeline/Notebooks.

| Aspect | Normal Pipeline/Notebooks | Delta Live Tables (DLT) |
|--------|--------------------------|------------------------|
| **Management** | Manual — you write ETL logic, error handling, scheduling | Declarative — you define *what*, Databricks handles *how* |
| **Data quality** | Manual validation checks | Built-in `@dlt.expect` constraints |
| **Dependency** | Manual ordering of notebooks | Automatic dependency resolution |
| **Error handling** | Custom try/catch | Automatic retry, quarantine bad records |
| **Monitoring** | Custom logging | Built-in pipeline UI & lineage |

```python
# DLT example (declarative)
import dlt

@dlt.table
def cleaned_orders():
    return spark.read.format("delta").load("/raw/orders") \
        .filter(col("amount") > 0)

@dlt.table
@dlt.expect("valid_amount", "amount > 0")  # quality constraint
def gold_summary():
    return dlt.read("cleaned_orders").groupBy("product").sum("amount")
```

---

### Q40. Delta Lake vs Delta Warehouse.

| Aspect | Delta Lake | Data Warehouse (e.g., Synapse, Snowflake) |
|--------|-----------|------------------------------------------|
| **Storage** | Files on cloud storage (ADLS/S3) + Delta format | Proprietary managed storage |
| **Format** | Open source (Parquet + transaction log) | Proprietary |
| **Cost** | Pay for storage + compute separately | Often bundled (compute expensive) |
| **Data types** | Structured, semi-structured, unstructured | Primarily structured |
| **Processing** | Spark-based (batch + streaming) | SQL-based |
| **ACID** | Yes | Yes |
| **Schema** | Schema-on-read + enforcement | Schema-on-write |
| **Use case** | Data lakehouse — unified analytics | Traditional BI / reporting |

> **Databricks Lakehouse** = Delta Lake + Photon engine = warehouse performance on lakehouse architecture.

---

## Unity Catalog & Hive

### Q41. What is Unity Catalog?

**Unity Catalog** is Databricks' **unified governance solution** for all data and AI assets.

**Key features:**
- **Centralized governance** — one place to manage access across all workspaces
- **3-level namespace** — `catalog.schema.table` (vs Hive's 2-level: `database.table`)
- **Fine-grained access control** — table, column, and row-level security
- **Data lineage** — automatic tracking of data flow across tables & notebooks
- **Audit logging** — who accessed what, when
- **Cross-workspace sharing** — share data across Databricks workspaces

```sql
-- Unity Catalog namespace
SELECT * FROM my_catalog.my_schema.my_table;

-- Grant access
GRANT SELECT ON TABLE my_catalog.my_schema.my_table TO `user@company.com`;
```

---

### Q42. Unity Catalog vs Hive Metastore.

| Aspect | Hive Metastore | Unity Catalog |
|--------|---------------|---------------|
| **Namespace** | 2-level: `database.table` | 3-level: `catalog.schema.table` |
| **Scope** | Per-workspace | Cross-workspace |
| **Access control** | Table-level only | Table, column, row-level |
| **Governance** | Basic | Centralized — RBAC, audit, lineage |
| **Data sharing** | Not supported natively | Delta Sharing (open protocol) |
| **Supported assets** | Tables, views | Tables, views, models, functions, volumes |
| **Lineage** | Not built-in | Automatic lineage tracking |

> **Migration path:** Most orgs are migrating from Hive Metastore → Unity Catalog for better governance. This is a common project topic in interviews.

---

### Q43. Why use Hive Metastore?

- **Central metadata store** — stores table definitions, schemas, partition info, storage locations
- **SQL interface** — enables SQL queries on data lake files via `spark.sql()`
- **Schema management** — defines structure for raw files (schema-on-read)
- **Compatibility** — works with Spark, Presto, Trino, Hive, and other engines
- **Partition pruning** — metastore tracks partition info for query optimization

> **When someone asks "why Hive over Unity Catalog":** Hive is simpler, doesn't require Databricks premium, and is sufficient for single-workspace, basic use cases.

---

## Clusters & Jobs

### Q44. How do you manage clusters in Databricks?

**Cluster types:**
| Type | Use Case | Lifecycle |
|------|----------|-----------|
| **All-Purpose** | Interactive development, notebooks | Long-running, manually started/stopped |
| **Job Cluster** | Automated production jobs | Auto-created at job start, auto-terminated after |

**Management best practices:**
- **Auto-scaling:** Set min/max workers — Databricks adds/removes nodes based on load
- **Auto-termination:** Set idle timeout (e.g., 30 min) to avoid cost waste
- **Cluster policies:** Admins define allowed configurations (instance types, max nodes)
- **Spot instances:** Use spot/preemptible VMs for cost savings (with fallback to on-demand)
- **Pool:** Pre-warm instances for faster cluster start times

```json
// Cluster config example
{
  "num_workers": 4,
  "spark_version": "13.3.x-scala2.12",
  "node_type_id": "Standard_DS3_v2",
  "autoscale": { "min_workers": 2, "max_workers": 8 },
  "autotermination_minutes": 30
}
```

---

### Q45. Types of Databricks clusters.

1. **All-Purpose Clusters** — interactive, shared, for development/exploration
2. **Job Clusters** — ephemeral, created per job run, cost-efficient for production
3. **SQL Warehouses** — optimized for SQL queries (Databricks SQL / BI tools)
4. **Single-Node Cluster** — no workers, for lightweight tasks / ML experiments

---

### Q46–47. Job clusters and scheduling.

**Databricks Jobs** = scheduled or triggered execution of notebooks/JARs/Python scripts.

```
Job → Task 1 (Notebook A) → Task 2 (Notebook B) → Task 3 (Python script)
              ↓ dependency        ↓ dependency
```

**Scheduling options:**
- **Cron-based:** `0 0 * * *` (daily at midnight)
- **Manual trigger:** run on-demand
- **API trigger:** via REST API
- **File arrival / event-based:** triggered by new files (via ADF or Databricks)

**Job cluster benefit:** Created fresh for each run → no idle costs, clean environment.

---

### Q48. Why Spark vs Databricks?

| Aspect | Apache Spark (OSS) | Databricks |
|--------|-------------------|------------|
| **Setup** | Manual cluster setup & management | Managed — click to create |
| **Optimization** | Manual tuning | Auto-optimization (Photon, adaptive query) |
| **Collaboration** | No built-in notebooks | Collaborative notebooks with versioning |
| **Delta Lake** | Manual integration | Native, first-class support |
| **Governance** | Manual (Ranger, etc.) | Unity Catalog built-in |
| **Job orchestration** | External (Airflow, etc.) | Built-in Workflows |
| **Cost** | Free (but infra cost) | Premium pricing + infra |
| **MLflow** | Separate setup | Built-in & managed |

> **Answer:** "Databricks is a managed Spark platform that removes infrastructure overhead, adds collaborative notebooks, native Delta Lake, Unity Catalog for governance, and the Photon engine for performance — so we can focus on data engineering rather than cluster management."

---

## OPTIMIZE, VACUUM, Z-Ordering, Time Travel

### Q49. What is the VACUUM command?

**VACUUM** removes old data files that are no longer referenced by the Delta transaction log (i.e., files from previous versions after updates/deletes).

```sql
-- Remove files older than 7 days (default retention)
VACUUM my_table;

-- Remove files older than 24 hours
VACUUM my_table RETAIN 24 HOURS;

-- Dry run (see what would be deleted)
VACUUM my_table DRY RUN;
```

**Key points:**
- Default retention: **7 days** (`168 hours`)
- After VACUUM, you **cannot time travel** to versions older than the retention period
- Does NOT delete the transaction log — only unreferenced data files
- Run regularly to manage storage costs

> ⚠️ **Never set retention below 7 days** in production — concurrent readers may fail.

---

### Q50. What is Z-ordering and how does it help?

**Z-ordering** is a technique to **co-locate related data** in the same set of files, optimizing data skipping for queries that filter on specific columns.

```sql
OPTIMIZE my_table ZORDER BY (country, date);
```

**How it works:**
- Rearranges data so rows with similar Z-ordered column values are stored in the same files
- Delta tracks **min/max statistics** per file
- Queries can **skip entire files** when filtering on Z-ordered columns

**When to use:**
- Columns frequently used in `WHERE` clauses
- High-cardinality columns (e.g., `customer_id`, `date`)
- **Max 2–4 columns** — effectiveness decreases with more columns

**Before Z-ordering:**
```
File 1: country=[India, USA, UK, India, Japan]
File 2: country=[USA, India, UK, Japan, India]
→ Query WHERE country='India' must read ALL files
```

**After Z-ordering:**
```
File 1: country=[India, India, India, India]
File 2: country=[USA, UK, Japan, Japan]
→ Query WHERE country='India' reads ONLY File 1
```

---

### Q51. What does OPTIMIZE command do?

**OPTIMIZE** compacts small files into larger, more efficient files (solves the **small files problem**).

```sql
OPTIMIZE my_table;                          -- compact all files
OPTIMIZE my_table WHERE date = '2024-03-01'; -- compact specific partition
OPTIMIZE my_table ZORDER BY (customer_id);   -- compact + Z-order
```

**Why it matters:**
- Many small files = excessive metadata overhead + slow reads
- OPTIMIZE creates ~1 GB files (optimal for Spark)
- Combines nicely with Z-ordering for query performance

---

### Q52. 100-column table, 99th column is partition — will Z-ordering be effective?

**Short answer:** Z-ordering is **independent of the partition column**. You Z-order **within** each partition.

**But the real issue:** If the 99th column is the partition column:
- Partitioning on a non-selective or high-cardinality column creates **too many small partitions**
- Z-ordering within tiny partitions has limited benefit since there are few files to optimize

**Recommendation:**
- Partition on **low-cardinality** columns (date, region, country)
- Z-order on **high-cardinality filter** columns (customer_id, product_id)
- If the 99th column has too many unique values → **don't partition, just Z-order**

---

### Q53. Time travel in Databricks.

**Time travel** lets you query **previous versions** of a Delta table.

```sql
-- Query by version number
SELECT * FROM my_table VERSION AS OF 5;

-- Query by timestamp
SELECT * FROM my_table TIMESTAMP AS OF '2024-03-01 10:00:00';

-- View table history (all changes)
DESCRIBE HISTORY my_table;
```

**PySpark:**
```python
df = spark.read.format("delta").option("versionAsOf", 5).load("/path/to/table")
df = spark.read.format("delta").option("timestampAsOf", "2024-03-01").load("/path")
```

**Use cases:**
- **Audit:** What did the data look like yesterday?
- **Rollback:** Restore after accidental deletes/updates
- **Debugging:** Compare current vs previous version
- **Reproducibility:** ML experiments on exact historical data

> ⚠️ Time travel only works within VACUUM retention period (default 7 days).

---

## Isolation & Comparison

### Q54. What is isolation level in Databricks?

**Isolation level** determines how concurrent transactions see each other's changes.

Delta Lake uses **Serializable isolation** (strictest) with **optimistic concurrency control:**

| Concept | Behavior |
|---------|----------|
| **WriteSerializable** (default) | Writers see a consistent snapshot; concurrent writes to different partitions succeed; same-partition conflicts fail |
| **Serializable** | Strictest — all transactions appear to execute serially |
| **Optimistic concurrency** | No locks — transactions proceed, conflicts detected at commit time |

**Conflict resolution:**
- If two transactions modify the **same files** → one succeeds, other fails with `ConcurrentModificationException`
- The failed transaction can be **retried** — it will see the latest committed data

---

### Q55. What is hash comparison in Databricks?

**Hash comparison** is a technique to **detect changes between source and target** efficiently — used in incremental/CDC pipelines.

```python
from pyspark.sql.functions import md5, concat_ws

# Create hash of key columns
source_df = source_df.withColumn("row_hash",
    md5(concat_ws("||", col("name"), col("salary"), col("dept")))
)

target_df = target_df.withColumn("row_hash",
    md5(concat_ws("||", col("name"), col("salary"), col("dept")))
)

# Compare: find changed/new rows
changed = source_df.join(target_df, on="id", how="left") \
    .filter(source_df.row_hash != target_df.row_hash)
```

**Use case:** Instead of comparing every column, compare a single hash — much faster for wide tables.

---

## Notebooks

### Q56–58. Notebook orchestration in Databricks.

**Call one notebook from another:**
```python
# %run executes in SAME context (shares variables)
# %run /path/to/other_notebook

# dbutils.notebook.run() — runs in SEPARATE context
result = dbutils.notebook.run("/path/to/other_notebook", timeout_seconds=300)
```

**Pass output of one notebook to another:**
```python
# In Notebook A (child): return a value
dbutils.notebook.exit("success: 1000 rows processed")

# In Notebook B (parent): capture the return value
result = dbutils.notebook.run("/path/to/notebook_a", 300)
print(result)  # "success: 1000 rows processed"
```

**Pass dynamic inputs to notebooks:**
```python
# Parent notebook: pass parameters as a dict
result = dbutils.notebook.run(
    "/path/to/child_notebook",
    timeout_seconds=300,
    arguments={"date": "2024-03-01", "env": "production"}
)

# Child notebook: read parameters
date = dbutils.widgets.get("date")        # "2024-03-01"
env = dbutils.widgets.get("env")          # "production"
```

**In ADF:** Use the Databricks Notebook activity with **Base Parameters** to pass dynamic values from the ADF pipeline.
