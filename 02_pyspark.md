# 📘 Study Notes — 2. PySpark / Spark

---

## Fundamentals

### Q20. Why PySpark over Python?

| Aspect | Python (Pandas) | PySpark |
|--------|----------------|---------|
| **Data size** | Single machine RAM (GBs) | Distributed cluster (TBs/PBs) |
| **Processing** | Single-threaded | Parallel across nodes |
| **Fault tolerance** | None — crash = data lost | RDD lineage — auto-recovers |
| **Scalability** | Vertical (bigger machine) | Horizontal (add more nodes) |
| **Lazy evaluation** | Eager (immediate) | Lazy (optimized execution plan) |
| **Integration** | Limited | Hive, HDFS, Kafka, Delta, ADLS |

**Key talking points:**
1. **Scalability** — handles massive datasets across clusters
2. **Parallel processing** — splits data into partitions, processes simultaneously
3. **Fault tolerance** — if a node fails, Spark recomputes from lineage (DAG)
4. **Ecosystem** — integrates with Hadoop, Hive, Kafka, Delta Lake, cloud storage
5. **Catalyst optimizer** — auto-optimizes query execution plans

> **Interview tip:** "I use Pandas for quick EDA on small datasets, but PySpark for production pipelines handling millions/billions of rows."

---

### Q21. What is lazy evaluation in PySpark?

**Lazy evaluation** means Spark **does not execute transformations immediately**. It builds a **Directed Acyclic Graph (DAG)** of operations and only executes when an **action** is triggered.

**Transformations (lazy):** `select`, `filter`, `groupBy`, `join`, `withColumn` — just add to the plan  
**Actions (trigger execution):** `show()`, `collect()`, `count()`, `write()`, `save()` — execute the plan

```python
# Nothing happens here — just building the DAG
df2 = df.filter(col("age") > 25)        # transformation
df3 = df2.select("name", "salary")       # transformation
df4 = df3.groupBy("dept").sum("salary")  # transformation

# NOW Spark executes the entire chain, optimized
df4.show()  # action — triggers execution
```

**Why it matters:**
- Spark's **Catalyst optimizer** can rearrange & optimize the entire plan before execution
- Predicate pushdown — filters pushed early to reduce data scanned
- Avoids unnecessary intermediate computations

> **30-word answer for interviews:** "PySpark delays execution of transformations until an action is called. This lets the Catalyst optimizer build and optimize the entire execution plan before running it."

---

### Q22. Wide vs narrow transformations.

| Aspect | Narrow | Wide |
|--------|--------|------|
| **Data movement** | No shuffle — data stays on same partition | Shuffle required — data moves across partitions |
| **Dependency** | Each output partition depends on **one** input partition | Each output partition depends on **multiple** input partitions |
| **Performance** | Fast | Expensive (network I/O) |
| **Examples** | `map`, `filter`, `select`, `withColumn` | `groupBy`, `join`, `orderBy`, `repartition`, `distinct` |

```
Narrow: [Partition 1] → [Partition 1']     (1:1 mapping)
Wide:   [Partition 1] ─┐
        [Partition 2] ─┼─→ [New Partition 1']   (shuffle/exchange)
        [Partition 3] ─┘
```

**Why this matters:** Wide transformations trigger **shuffle** — the most expensive operation in Spark. Minimize shuffles by:
- Using broadcast joins for small tables
- Pre-partitioning data by join keys
- Using `coalesce()` instead of `repartition()` when reducing partitions

---

### Q23. How to create a Spark Session?

`SparkSession` is the **single entry point** to all Spark functionality (replaced `SparkContext`, `SQLContext`, `HiveContext` from Spark 1.x).

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .enableHiveSupport() \
    .getOrCreate()
```

**Key methods:**
| Method | Purpose |
|--------|---------|
| `.appName("name")` | Set application name (visible in Spark UI) |
| `.master("local[*]")` | Set cluster manager (local, yarn, mesos, k8s) |
| `.config(key, value)` | Set Spark config properties |
| `.enableHiveSupport()` | Enable Hive metastore access |
| `.getOrCreate()` | Get existing session or create new one |

> **In Databricks:** SparkSession is pre-created as `spark` — no need to create it manually.

---

## Read / Write Operations

### Q24–27. Spark Read/Write Syntax

**Reading files:**
```python
# CSV
df = spark.read.csv("path/to/file.csv", header=True, inferSchema=True)
# or
df = spark.read.format("csv").option("header", "true").load("path")

# JSON
df = spark.read.json("path/to/file.json")

# Parquet
df = spark.read.parquet("path/to/file.parquet")

# Delta
df = spark.read.format("delta").load("path/to/delta_table")

# From ADLS
df = spark.read.csv("abfss://container@storageaccount.dfs.core.windows.net/path/file.csv")
```

**Writing files:**
```python
# Parquet
df.write.parquet("path/output", mode="overwrite")

# Delta
df.write.format("delta").mode("overwrite").save("path/output")

# CSV
df.write.csv("path/output", header=True, mode="append")

# Partitioned write
df.write.partitionBy("year", "month").parquet("path/output")
```

**Write modes:**
| Mode | Behavior |
|------|----------|
| `overwrite` | Replace existing data |
| `append` | Add to existing data |
| `ignore` | Skip if data exists |
| `errorifexists` | Throw error if data exists (default) |

**Reading from ADLS (password-protected / secret scope):**
```python
# Set up credentials
spark.conf.set(
    "fs.azure.account.key.<storage>.dfs.core.windows.net",
    dbutils.secrets.get(scope="my-scope", key="storage-key")
)

# Then read normally
df = spark.read.csv("abfss://container@storage.dfs.core.windows.net/path")
```

---

## DataFrame Operations

### Q28. Add a new column with a constant value.

```python
from pyspark.sql.functions import lit

df = df.withColumn("country", lit("India"))
df = df.withColumn("status", lit(1))
df = df.withColumn("is_active", lit(True))
```

---

### Q29. `display()`, `dropna/fillna`, `filter()` syntax.

```python
# Display (Databricks-specific)
display(df)
df.show()          # generic Spark
df.show(20, False) # 20 rows, don't truncate

# Drop nulls
df.dropna()                              # drop rows with ANY null
df.dropna(how="all")                     # drop only if ALL columns null
df.dropna(subset=["name", "salary"])     # drop if null in specific columns

# Fill nulls
df.fillna(0)                             # fill all numeric nulls with 0
df.fillna({"salary": 0, "name": "N/A"}) # column-specific fills

# Filter
df.filter(col("salary") > 50000)
df.filter((col("dept") == "HR") & (col("salary") > 50000))
df.where("salary > 50000")              # SQL-style string filter
```

---

### Q30. Conditional column — `age_group` (0–18: kids, 19–60: adult, 61+: senior).

```python
from pyspark.sql.functions import when, col

df = spark.createDataFrame([("Alice", 15), ("Bob", 35), ("Charlie", 70)], ["name", "age"])

df = df.withColumn("age_group",
    when(col("age") <= 18, "kids")
    .when(col("age") <= 60, "adult")
    .otherwise("senior")
)
df.show()
# +-------+---+---------+
# |   name|age|age_group|
# +-------+---+---------+
# |  Alice| 15|     kids|
# |    Bob| 35|    adult|
# |Charlie| 70|   senior|
# +-------+---+---------+
```

---

### Q31. Nth highest salary in PySpark.

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, col

N = 3  # 3rd highest

window = Window.orderBy(col("salary").desc())
result = df.withColumn("rank", dense_rank().over(window)) \
           .filter(col("rank") == N) \
           .select("salary").distinct()
result.show()
```

---

## PySpark GroupBy & Aggregation

### Q32. Employee data → `groupBy().agg(count, avg, max)`.

```python
from pyspark.sql.functions import count, avg, max

result = df.groupBy("department").agg(
    count("id").alias("total_employees"),
    avg("salary").alias("avg_salary"),
    max("salary").alias("highest_salary")
)
result.show()
```

---

### Q33. Total salary per dept + rank (with and without window functions).

**With window function:**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, dense_rank

# Total salary per dept
dept_total = df.groupBy("dept").agg(sum("salary").alias("total_salary"))

# Rank by salary within dept
w = Window.partitionBy("dept").orderBy(col("salary").desc())
df_ranked = df.withColumn("salary_rank", dense_rank().over(w))
```

**Without window function (rank alternative):**
```python
# Self-join approach: count how many salaries are higher
from pyspark.sql.functions import countDistinct

df_rank = df.alias("a").join(
    df.alias("b"),
    (col("a.dept") == col("b.dept")) & (col("a.salary") <= col("b.salary"))
).groupBy("a.dept", "a.name", "a.salary") \
 .agg(countDistinct("b.salary").alias("salary_rank"))
```

---

## Schema Handling

### Q34. Table had 5 columns, now 7 — how will Spark handle it?

**Default behavior:** Spark uses schema-on-read.
- **If `inferSchema=True`:** Spark reads the new file, infers 7 columns — works fine
- **If schema is explicitly defined (5 cols):** The 2 new columns are **ignored/dropped**
- **If reading from Delta table:** Delta enforces schema — new columns will be **rejected** unless:

**Schema evolution in Delta:**
```python
# Enable schema evolution (merge new columns automatically)
df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save("path/to/delta_table")

# Or set globally
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
```

> **Interview tip:** Mention `mergeSchema` for Delta and discuss how your pipeline handles schema changes — alerts, validation, auto-merge strategies.

---

### Q35. How do you handle schema drift in pipelines?

**Schema drift** = source schema changes unexpectedly (new columns, renamed columns, type changes).

**Strategies:**
1. **Schema validation at ingestion** — compare incoming schema against expected; alert on mismatch
2. **Delta Lake `mergeSchema`** — automatically accommodate new columns
3. **Schema registry** — maintain a central schema definition (e.g., Unity Catalog)
4. **ADF schema mapping** — ADF's Copy Activity has drift handling (auto-map new columns)
5. **Defensive coding** — use `try/except`, check column existence before transformations

```python
# Defensive check
if "new_column" in df.columns:
    df = df.withColumn("new_column", col("new_column").cast("string"))
else:
    df = df.withColumn("new_column", lit(None).cast("string"))
```

**In ADF:** Enable "Allow schema drift" in data flow sources — maps unknown columns automatically.
