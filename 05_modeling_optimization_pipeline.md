# 📘 Study Notes — 5. Data Warehousing & Modeling + 6. Optimization + 7. Pipeline Design

---

# 5. Data Warehousing & Modeling

## Schema Design

### Q77. Data Warehouse vs Data Lake.

| Aspect | Data Warehouse | Data Lake |
|--------|---------------|-----------|
| **Data type** | Structured only | Structured, semi-structured, unstructured |
| **Schema** | Schema-on-write (defined before loading) | Schema-on-read (applied when reading) |
| **Processing** | ETL | ELT |
| **Users** | Business analysts, BI tools | Data engineers, data scientists |
| **Cost** | Expensive (compute + storage bundled) | Cheap (cloud object storage) |
| **Performance** | Optimized for SQL queries | Optimized for large-scale processing |
| **Quality** | High — curated, clean data | Raw — may contain duplicates/errors |
| **Examples** | Snowflake, Synapse, Redshift | ADLS, S3, GCS |

> **Data Lakehouse** (Databricks) = combines both — lake storage + warehouse features (ACID, SQL, governance via Delta Lake).

---

### Q78. Snowflake Schema vs Star Schema.

| Aspect | Star Schema | Snowflake Schema |
|--------|------------|-----------------|
| **Structure** | Fact table + denormalized dimensions | Fact table + normalized dimensions (sub-dimensions) |
| **Joins** | Fewer (fact → dimension) | More (fact → dim → sub-dim) |
| **Redundancy** | Higher (denormalized) | Lower (normalized) |
| **Query performance** | Faster (fewer joins) | Slower (more joins) |
| **Storage** | More (duplicate data) | Less (normalized) |
| **Complexity** | Simpler | More complex |
| **Best for** | BI/reporting (fast queries) | Storage efficiency, complex relationships |

```
STAR:                          SNOWFLAKE:
   [Date Dim]                     [Date Dim]
       |                              |
[Product Dim]─[FACT]─[Store Dim]  [Product Dim]─[FACT]─[Store Dim]
       |                              |                    |
   [Customer Dim]               [Category]           [City]─[Country]
```

---

### Q79. Fact table vs Dimension table.

| Aspect | Fact Table | Dimension Table |
|--------|-----------|----------------|
| **Contains** | Measurable business metrics | Descriptive attributes |
| **Examples** | Sales amount, quantity, revenue | Product name, customer info, date |
| **Size** | Very large (millions/billions of rows) | Smaller (thousands/millions) |
| **Keys** | Foreign keys to dimensions + measures | Primary key + attributes |
| **Changes** | Grows continuously (new transactions) | Changes slowly (SCD) |
| **Grain** | One row per event/transaction | One row per entity |

```sql
-- Fact table: sales_fact
-- order_id | product_id (FK) | customer_id (FK) | date_id (FK) | amount | qty

-- Dimension table: product_dim
-- product_id (PK) | product_name | category | brand | price
```

---

## SCD (Slowly Changing Dimensions)

### Q80. What is SCD? Types?

**SCD** = strategies for handling changes in dimension table attributes over time.

| Type | Strategy | History? | Example |
|------|----------|----------|---------|
| **Type 0** | Retain original | No changes ever | Date of birth |
| **Type 1** | Overwrite | No — loses history | Fix a typo in name |
| **Type 2** | Add new row | Yes — full history | Track address changes |
| **Type 3** | Add new column | Partial — only previous value | `current_city`, `previous_city` |
| **Type 4** | History table | Yes — in separate table | Mini-dimension |
| **Type 6** | Hybrid (1+2+3) | Yes — combined approach | Current + historical flags |

**SCD Type 2 (most commonly asked):**
```
| id | name  | city    | start_date | end_date   | is_current |
|----|-------|---------|------------|------------|------------|
| 1  | Alice | Mumbai  | 2020-01-01 | 2023-06-30 | false      |
| 1  | Alice | Delhi   | 2023-07-01 | 9999-12-31 | true       |
```

**Housekeeping columns** (Q82): `start_date`, `end_date`, `is_current` (or `effective_date`, `expiry_date`, `active_flag`)

---

### Q81. SCD Scenario: Change UK → United Kingdom without overwriting.

**Answer: SCD Type 2** — because we want to preserve the history (old value = UK, new value = United Kingdom).

**Steps:**
1. Close the existing record: set `end_date = today`, `is_current = false`
2. Insert a new record with `country = 'United Kingdom'`, `start_date = today`, `end_date = 9999-12-31`, `is_current = true`

```sql
-- Step 1: Close old record
UPDATE country_dim
SET end_date = CURRENT_DATE(), is_current = false
WHERE id = 2 AND is_current = true;

-- Step 2: Insert new record
INSERT INTO country_dim VALUES (2, 'United Kingdom', CURRENT_DATE(), '9999-12-31', true);
```

**Result:**
```
| id | country        | start_date | end_date   | is_current |
|----|----------------|------------|------------|------------|
| 2  | UK             | 2020-01-01 | 2024-03-15 | false      |
| 2  | United Kingdom | 2024-03-15 | 9999-12-31 | true       |
```

---

### Q83. Implement SCD Type 2 in SQL (using MERGE).

```sql
MERGE INTO customer_dim AS target
USING staging_customers AS source
ON target.customer_id = source.customer_id AND target.is_current = true

-- When a match is found AND data has changed → close old record
WHEN MATCHED AND (target.city != source.city OR target.phone != source.phone) THEN
    UPDATE SET target.end_date = CURRENT_DATE(), target.is_current = false

-- When no match → new customer → insert
WHEN NOT MATCHED THEN
    INSERT (customer_id, name, city, phone, start_date, end_date, is_current)
    VALUES (source.customer_id, source.name, source.city, source.phone,
            CURRENT_DATE(), '9999-12-31', true);

-- Insert updated records as new rows (second pass)
INSERT INTO customer_dim
SELECT s.customer_id, s.name, s.city, s.phone, CURRENT_DATE(), '9999-12-31', true
FROM staging_customers s
JOIN customer_dim t ON s.customer_id = t.customer_id
WHERE t.is_current = false AND t.end_date = CURRENT_DATE();
```

---

## Medallion Architecture

### Q84–85. Medallion architecture.

A **multi-layered data architecture** pattern used in lakehouses:

```
  [Bronze]          →         [Silver]         →         [Gold]
  Raw / landing            Cleansed / conformed         Aggregated / business
  As-is from source        Deduplicated, typed          Ready for BI/ML
  Tables                   Tables                       Tables / Views
```

| Layer | Purpose | Format | Example |
|-------|---------|--------|---------|
| **Bronze** | Raw ingestion — exact copy from source | Delta (append-only) | Raw JSON from API |
| **Silver** | Cleaned, deduplicated, standardized | Delta (merge/overwrite) | Joined, typed, filtered |
| **Gold** | Business-level aggregates, KPIs | Delta + Views | Revenue by region, daily active users |

**Transformations (Bronze → Silver):**
- Data type casting, null handling
- Deduplication
- Schema enforcement
- Join reference/lookup tables
- Filter invalid records

**Validation (after Gold loading):**
- Row count checks (source vs target)
- Null percentage checks on key columns
- Aggregate validation (sum of amounts matches)
- Business rule checks (e.g., no negative revenue)
- Data freshness checks (latest record timestamp)

---

## File Formats

### Q86. Parquet vs CSV/JSON.

| Aspect | Parquet | CSV | JSON |
|--------|---------|-----|------|
| **Format** | Columnar binary | Row-based text | Row-based text |
| **Compression** | Excellent (Snappy, gzip) | Poor | Poor |
| **Read performance** | Fast (column pruning, predicate pushdown) | Slow (read all columns) | Slow |
| **Schema** | Embedded in file | No (inferred) | Partial (inferred) |
| **Nested data** | Supported | Not supported | Native |
| **Human readable** | No | Yes | Yes |
| **File size** | Small (compressed) | Large | Large |
| **Best for** | Analytics, big data | Simple data exchange | APIs, semi-structured |

> **Rule of thumb:** Always store data as **Parquet** (or **Delta** = Parquet + transaction log) for analytics workloads. Use CSV/JSON only for ingestion from external sources.

---

---

# 6. Optimization & Performance Tuning

## Joins & Skew

### Q87. What is broadcast join? When to use it?

**Broadcast join** sends the **smaller table to all worker nodes**, avoiding shuffle of the large table.

```python
from pyspark.sql.functions import broadcast

# Small table (< 10 MB by default) broadcasted to all nodes
result = large_df.join(broadcast(small_df), on="key")
```

**When to use:**
- One table is **small** (< 10 MB default, configurable up to ~1 GB)
- Avoid shuffle on the large table
- Lookup/reference table joins

**Config:**
```python
# Default broadcast threshold: 10 MB
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)

# Disable auto-broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
```

> **How it works:** Small table is collected to driver → sent to every executor → join happens locally (no shuffle).

---

### Q88. What is salting (for data skew)?

**Data skew** = one partition has disproportionately more data than others → one task runs for hours while others finish in seconds.

**Salting** adds a random prefix/suffix to the skewed key to distribute data evenly across partitions.

```python
import random
from pyspark.sql.functions import concat, lit, col, floor, rand

# Step 1: Add salt to the skewed (large) table
num_salts = 10
large_df = large_df.withColumn("salt", (rand() * num_salts).cast("int"))
large_df = large_df.withColumn("salted_key", concat(col("join_key"), lit("_"), col("salt")))

# Step 2: Explode the small table to match all salt values
from pyspark.sql.functions import explode, array
small_df = small_df.withColumn("salt", explode(array([lit(i) for i in range(num_salts)])))
small_df = small_df.withColumn("salted_key", concat(col("join_key"), lit("_"), col("salt")))

# Step 3: Join on salted key
result = large_df.join(small_df, on="salted_key")
```

---

### Q89. Broadcast join vs salting.

| Aspect | Broadcast Join | Salting |
|--------|---------------|--------|
| **When to use** | One table is small (< 1 GB) | Both tables are large, but join key is skewed |
| **How it works** | Send small table to all nodes | Add random prefix to distribute skewed key |
| **Shuffle** | No shuffle (small table broadcasted) | Shuffle happens but is more balanced |
| **Limitation** | Small table must fit in memory | Increases data size (replication) |
| **Complexity** | Simple — just use `broadcast()` | More complex — salt + explode logic |

> **Try broadcast first** — if the small table fits, it's simpler and faster. Use salting only when both tables are large.

---

### Q90. How to identify data skew in production?

1. **Spark UI → Stages → Task Duration:** If one task takes 10x longer than others → skew
2. **Spark UI → SQL → Exchange:** Look for uneven partition sizes
3. **Symptoms:** Job stuck at 99% with one task running
4. **Code check:**
```python
# Check distribution of join key
df.groupBy("join_key").count().orderBy(col("count").desc()).show()
```

**Fixes:** Broadcast join, salting, adaptive query execution (AQE), repartitioning.

---

## Partitioning

### Q91. What's partitioning? How to decide column?

**Partitioning** = splitting data into subdirectories based on column values. Queries filtering on the partition column skip irrelevant directories (**partition pruning**).

```python
df.write.partitionBy("year", "month").format("delta").save("/path")
# Creates: /path/year=2024/month=01/, /path/year=2024/month=02/, ...
```

**How to choose partition column:**
| Criteria | Good Partition Column | Bad Partition Column |
|----------|----------------------|---------------------|
| **Cardinality** | Low (date, region, country) | High (customer_id — millions of partitions) |
| **Query pattern** | Frequently filtered on | Rarely used in WHERE |
| **Distribution** | Even data per partition | Heavily skewed |
| **Size** | Each partition ≥ 1 GB ideal | < 1 MB = small files problem |

> **Rule of thumb:** Partition when table is > 1 TB. Each partition should have at least 1 GB of data. Date columns are the most common choice.

---

### Q92. How to increase number of partitions in PySpark?

```python
# repartition() — full shuffle, can increase OR decrease
df = df.repartition(200)                    # 200 partitions
df = df.repartition(200, "join_key")        # 200 partitions, hash by join_key

# coalesce() — NO shuffle, can only DECREASE (merges partitions)
df = df.coalesce(10)                        # reduce to 10 partitions

# Config: default shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", 200)  # default for groupBy/join
```

| Method | Shuffle? | Increase? | Decrease? | Use case |
|--------|----------|-----------|-----------|----------|
| `repartition(N)` | Yes | ✅ | ✅ | Rebalance for joins/processing |
| `coalesce(N)` | No | ❌ | ✅ | Reduce partitions before write |
| `spark.sql.shuffle.partitions` | — | ✅ | ✅ | Default for all shuffles |

---

## Shuffle & Small Files

### Q93. How to optimize too many shuffles?

1. **Broadcast join** — eliminate shuffle for small table joins
2. **Pre-partition data** — partition by join key to avoid shuffle at join time
3. **Reduce shuffle partitions:** `spark.conf.set("spark.sql.shuffle.partitions", "auto")` (AQE)
4. **Use `coalesce` after wide operations** to reduce output partitions
5. **Avoid unnecessary `orderBy`** — it triggers a global shuffle
6. **Cache/persist** intermediate DataFrames if reused multiple times
7. **Enable AQE (Adaptive Query Execution):**
```python
spark.conf.set("spark.sql.adaptive.enabled", True)           # auto-optimize at runtime
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)  # auto-merge small partitions
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)  # auto-handle skew
```

---

### Q94. How to reduce the small files problem?

**Small files problem:** Too many tiny files → excessive metadata overhead, slow reads.

**Solutions:**
1. **`OPTIMIZE`** — compacts small files into ~1 GB files
   ```sql
   OPTIMIZE my_table;
   OPTIMIZE my_table WHERE date = '2024-03-01';
   ```
2. **Auto-optimize (Databricks):**
   ```sql
   ALTER TABLE my_table SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');
   ALTER TABLE my_table SET TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true');
   ```
3. **`coalesce(N)` before writing** — reduce output files
4. **Repartition before write** — control file count
5. **`maxRecordsPerFile`:**
   ```python
   df.write.option("maxRecordsPerFile", 1000000).parquet("path")
   ```

---

## General Optimization

### Q95–96. Optimization techniques summary.

| Technique | What it does |
|-----------|-------------|
| **Broadcast join** | Send small table to all nodes, avoid shuffle |
| **Z-ordering** | Co-locate related data for data skipping |
| **Partitioning** | Directory-level data pruning |
| **Caching** | Keep frequently accessed data in memory |
| **Predicate pushdown** | Push filters to storage layer (read less data) |
| **Column pruning** | Read only needed columns (Parquet advantage) |
| **AQE** | Runtime optimization — auto-coalesce, auto-skew handling |
| **OPTIMIZE** | Compact small files |
| **Salting** | Distribute skewed join keys |
| **Coalesce** | Reduce partitions without shuffle |
| **Delta data skipping** | Min/max stats per file → skip irrelevant files |

---

### Q97. Cluster configuration — 1 TB data, how many workers?

**Rule of thumb:**
- Each worker has ~60 GB usable memory (for Standard_DS3_v2)
- Spark needs ~3x the data size in memory for transformations
- 1 TB data × 3 = 3 TB memory needed
- 3000 GB ÷ 60 GB/worker ≈ **50 workers**

**But it depends on:**
| Factor | Impact |
|--------|--------|
| **Workload type** | Simple reads need fewer; complex joins/aggregations need more |
| **Data format** | Parquet = compressed, actual memory use is less |
| **Partitioning** | Well-partitioned data needs less memory |
| **Instance type** | Memory-optimized (r-series) vs general purpose (d-series) |

**Practical approach:**
1. Start with **8–16 workers** with auto-scaling up to 32
2. Monitor Spark UI — if tasks are spilling to disk, add more workers
3. If GC time is high → workers need more memory

---

---

# 7. Pipeline Design & Debugging

## Pipeline Failures

### Q98–99. Pipeline failing / failing intermittently — how to fix?

**Systematic debugging approach:**

**Step 1: Identify the failure**
- Check ADF Monitor / Databricks job logs for error messages
- Look at which activity/task failed

**Step 2: Common failure causes & fixes:**

| Cause | Symptoms | Fix |
|-------|----------|-----|
| **OOM (Out of Memory)** | `java.lang.OutOfMemoryError` | Increase driver/executor memory, optimize joins |
| **Data skew** | One task takes 100x longer | Broadcast join, salting, AQE |
| **Schema change** | `AnalysisException` — column not found | Enable `mergeSchema`, add validation |
| **Network timeout** | Intermittent connection errors | Increase retry count, check connectivity |
| **Corrupt data** | `BadRecordException` | Use `mode("PERMISSIVE")`, add error handling |
| **Concurrent writes** | `ConcurrentModificationException` | Add retry logic, use isolation levels |
| **Resource contention** | Intermittent slow performance | Schedule jobs to avoid overlap, use separate clusters |

**For intermittent failures specifically:**
- Usually caused by transient issues: network timeouts, cluster preemption, resource contention
- **Fix:** Add retry policies (3 retries with exponential backoff)
- Enable **spot instance fallback** to on-demand
- Check if another job is competing for the same cluster

---

### Q100. Debug pipeline taking longer than expected (10 min → 40 min).

1. **Check Spark UI → Stages:** Which stage is slow?
2. **Check data volume:** Did source data grow? (10x more rows)
3. **Check for data skew:** One partition much larger than others?
4. **Check shuffle:** Too many shuffle partitions? Large shuffle write?
5. **Check cluster resources:** Are nodes fully utilized? Spilling to disk?
6. **Check for cartesian joins:** Accidental cross join?
7. **Compare execution plans:** `df.explain(True)` vs previous runs

---

### Q101. Debug pipeline stuck due to memory or join issues.

**Memory issues:**
```python
# Increase memory
spark.conf.set("spark.executor.memory", "16g")
spark.conf.set("spark.driver.memory", "8g")

# Reduce memory pressure
spark.conf.set("spark.sql.shuffle.partitions", 400)  # more partitions = less per partition
```

**Join issues:**
- **Cartesian join** → add proper join condition
- **Skewed join** → use broadcast or salting
- **Too many small partitions** → repartition before join

---

### Q102. How to identify issues in production PySpark jobs.

1. **Spark UI** — stages, tasks, shuffle metrics, storage
2. **Driver/executor logs** — search for ERROR/WARN
3. **Ganglia/Metrics** — cluster-level CPU, memory, disk I/O
4. **Databricks Job UI** — run duration trends, cluster utilization
5. **Event log** — `spark.eventLog.enabled=true` for historical analysis
6. **Code profiling** — add timing logs, row counts between transformations

---

## Incremental & Data Loading

### Q103. Incremental load — how to handle?

**Approaches:**

**1. Watermark / High-water mark:**
```python
# Track max timestamp from last run
last_loaded = spark.sql("SELECT MAX(updated_at) FROM target_table").collect()[0][0]

# Load only new/changed records
new_data = spark.read.format("jdbc") \
    .option("query", f"SELECT * FROM source WHERE updated_at > '{last_loaded}'") \
    .load()
```

**2. Delta MERGE (Upsert):**
```python
deltaTable.alias("t").merge(
    new_data.alias("s"), "t.id = s.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

**3. Change Data Capture (CDC):**
- Use Debezium/Kafka to capture INSERT/UPDATE/DELETE from source DB
- Apply changes to Delta table via MERGE

**4. ADF Copy Activity:**
- Use watermark column with ADF's incremental copy pattern (Lookup + Copy)

---

### Q104. Import MySQL → HDFS using Sqoop (incremental).

```bash
sqoop import \
    --connect jdbc:mysql://host:3306/mydb \
    --username user --password pass \
    --table customer \
    --target-dir /user/hive/warehouse/customer \
    --incremental lastmodified \
    --check-column last_updated \
    --last-value "2024-01-01 00:00:00" \
    --merge-key customer_id \
    --as-parquetfile
```

| Incremental Mode | Use Case |
|-----------------|----------|
| `--incremental append` | Only new rows (INSERT-only sources) |
| `--incremental lastmodified` | New + updated rows (uses timestamp column) |

---

## Retry & Recovery

### Q105. Retry mechanisms for pipeline failures.

| Level | How |
|-------|-----|
| **ADF Activity retry** | Set retries (1–3) with interval (e.g., 30s) on each activity |
| **ADF Failure path** | Route to error-handling activity (email, log, fallback) |
| **Databricks job retry** | Configure max retries in Databricks Workflow |
| **Code-level retry** | Python `try/except` with retry decorator |
| **Idempotent design** | Use MERGE (not INSERT) — safe to rerun without duplicates |

```python
# Python retry decorator
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=60))
def load_data():
    df = spark.read.format("jdbc").options(...).load()
    return df
```

---

### Q106. Pipeline failed 2 days, data missing — what automation?

1. **Detect gaps** — query target table for missing dates
2. **Parameterized backfill** — pipeline accepts date parameter, processes specific date
3. **Tumbling window trigger** — ADF natively supports backfill (reruns missed windows)
4. **Idempotent writes** — use MERGE + partition overwrite so reruns are safe
5. **Alerting** — set up alerts on pipeline failures to catch gaps early

```python
# Backfill script
missing_dates = ["2024-03-14", "2024-03-15"]
for date in missing_dates:
    dbutils.notebook.run("/pipelines/daily_load", 3600, {"process_date": date})
```

---

### Q107. What is checkpointing? Why use it?

**Checkpointing** saves the state of a streaming query or RDD computation to reliable storage, so it can **recover from failures** without reprocessing everything.

**Structured Streaming:**
```python
df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/mnt/checkpoints/my_stream") \
    .start("/mnt/output/")
```

**Why:**
- **Fault tolerance** — restart from last checkpoint, not from scratch
- **Exactly-once processing** — avoid duplicate records on restart
- **Progress tracking** — records which offsets have been processed (Kafka, Event Hub)

> **Rule:** Always set `checkpointLocation` for production streaming jobs. Without it, a restart reprocesses ALL data.

---

## Data Quality

### Q108–109. Quality checks in projects / preliminary checks before merging.

| Check | What to validate | How |
|-------|-----------------|-----|
| **Row count** | Source rows match target rows | `source.count() == target.count()` |
| **Null checks** | Key columns not null | `df.filter(col("id").isNull()).count() == 0` |
| **Duplicate check** | No duplicate primary keys | `df.groupBy("id").count().filter("count > 1")` |
| **Schema validation** | Expected columns present & correct types | Compare `df.schema` against expected |
| **Freshness** | Data is recent | `MAX(updated_at)` within expected window |
| **Range checks** | Values within business bounds | `df.filter(col("amount") < 0).count() == 0` |
| **Referential integrity** | FK values exist in reference table | Left anti-join with dimension table |
| **Completeness** | No missing dates/periods | Check for gaps in date sequence |

```python
# Pre-merge validation
assert new_data.count() > 0, "No data to process"
assert new_data.filter(col("id").isNull()).count() == 0, "Null IDs found"

dupes = new_data.groupBy("id").count().filter("count > 1").count()
assert dupes == 0, f"Found {dupes} duplicate IDs"
```

---

### Q110. FMCG client, 200M records/day, duplicates & partial matches — pipeline design.

**Architecture:**

```
ADLS (raw/landing) → Bronze (raw copy) → Silver (deduplicated, cleaned) → Gold (aggregated)
```

**Key design decisions:**
1. **Ingestion:** ADF Copy Activity → ADLS landing zone (partitioned by date)
2. **Bronze:** Append raw data as-is to Delta table (no transforms)
3. **Deduplication (Silver):**
   ```python
   # Exact duplicates
   df = df.dropDuplicates(["order_id", "product_id", "timestamp"])

   # Partial matches (fuzzy) — use hash comparison
   df = df.withColumn("row_hash", md5(concat_ws("||", *key_columns)))
   ```
4. **MERGE for upsert:** Handle reprocessing/partial matches
5. **Optimization for 200M records:**
   - Partition by `date`
   - Z-order by `product_id` or `store_id`
   - Use Delta `OPTIMIZE` after writes
   - Auto-scaling cluster (min 8, max 32 workers)
6. **Data quality checks** before Gold layer
7. **Monitoring & alerting** on row counts, nulls, processing time
