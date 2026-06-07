# 📘 Study Notes — 12. Scenario-Based Coding Problems

---

## SQL Coding Problems

### Q140. Employee + Department → total employees, avg salary, highest salary.

**Tables:**
```
Employee: employee_id, employee_name, department_id, salary, hire_date
Department: department_id, department_name, location
```

**Solution:**
```sql
SELECT
    d.department_name,
    COUNT(e.employee_id) AS total_employees,
    ROUND(AVG(e.salary), 2) AS avg_salary,
    MAX(e.salary) AS highest_salary
FROM employee e
JOIN department d ON e.department_id = d.department_id
GROUP BY d.department_name;
```

**Result:**
```
| department_name | total_employees | avg_salary | highest_salary |
|----------------|-----------------|------------|----------------|
| HR             | 2               | 60000.00   | 70000          |
| Marketing      | 1               | 60000.00   | 60000          |
| Finance        | 1               | 45000.00   | 45000          |
```

---

### Q141. Moving average on sales data.

**Table:** `sales(sale_date, amount)`

**Solution:**
```sql
SELECT
    sale_date,
    amount,
    ROUND(AVG(amount) OVER (
        ORDER BY sale_date
        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    ), 2) AS moving_avg
FROM sales;
```

**Explanation:**
- For each row, average the current row + 1 row before + 1 row after
- First row: only current + next (2 values)
- Last row: only previous + current (2 values)
- Middle rows: 3 values

**Result:**
```
| sale_date   | amount | moving_avg |
|-------------|--------|------------|
| 2025-11-01  | 100    | 125.00     |  ← (100+150)/2
| 2025-11-02  | 150    | 150.00     |  ← (100+150+200)/3
| 2025-11-03  | 200    | 200.00     |  ← (150+200+250)/3
| 2025-11-04  | 250    | 250.00     |  ← (200+250+300)/3
| 2025-11-05  | 300    | 275.00     |  ← (250+300)/2
```

---

### Q142. Pivot: `(entity_id, subjects, marks)` → `(entity_id, Maths, English)`.

**Input:**
```
| entity_id | subjects | marks |
|-----------|----------|-------|
| En_01     | Maths    | 80    |
| En_01     | English  | 78    |
```

**Solution (standard SQL):**
```sql
SELECT
    entity_id,
    MAX(CASE WHEN subjects = 'Maths' THEN marks END) AS Maths,
    MAX(CASE WHEN subjects = 'English' THEN marks END) AS English
FROM student_marks
GROUP BY entity_id;
```

**Solution (Databricks PIVOT):**
```sql
SELECT * FROM student_marks
PIVOT (MAX(marks) FOR subjects IN ('Maths', 'English'));
```

**Solution (PySpark):**
```python
df.groupBy("entity_id").pivot("subjects").agg(max("marks")).show()
```

**Output:**
```
| entity_id | Maths | English |
|-----------|-------|---------|
| En_01     | 80    | 78      |
```

---

### Q143. Airline table → add `cheapest` and `fastest` columns.

**Table:** `flights(airline, date, travel_duration, price)`

**Solution:**
```sql
SELECT
    airline,
    date,
    travel_duration,
    price,
    CASE WHEN price = MIN(price) OVER (PARTITION BY date) THEN 'Yes' ELSE 'No' END AS cheapest,
    CASE WHEN travel_duration = MIN(travel_duration) OVER (PARTITION BY date) THEN 'Yes' ELSE 'No' END AS fastest
FROM flights;
```

**PySpark:**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import min, when, col

w = Window.partitionBy("date")

df = df.withColumn("cheapest",
    when(col("price") == min("price").over(w), "Yes").otherwise("No")
).withColumn("fastest",
    when(col("travel_duration") == min("travel_duration").over(w), "Yes").otherwise("No")
)
```

---

## PySpark Coding Problems

### Q144. Join `customer.csv` + `order.csv` → total amount per customer.

**Data:**
```
customer.csv: customer_id, name
order.csv: order_id, customer_id, amount
```

**Solution:**
```python
# Read CSVs
customers = spark.read.csv("customer.csv", header=True, inferSchema=True)
orders = spark.read.csv("order.csv", header=True, inferSchema=True)

# Join and aggregate
result = customers.join(orders, on="customer_id") \
    .groupBy("customer_id", "name") \
    .agg(sum("amount").alias("total_amount"))

result.show()
```

**SQL equivalent:**
```sql
SELECT c.customer_id, c.name, SUM(o.amount) AS total_amount
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name;
```

---

### Q145. Employee data → `groupBy().agg(count, avg, max)`.

```python
from pyspark.sql.functions import count, avg, max

data = [
    (1, "Alice", "HR", 4000),
    (2, "Bob", "IT", 6000),
    (3, "Cathy", "IT", 5500),
    (4, "David", "HR", 4500),
    (5, "Eva", "Finance", 7000)
]
columns = ["id", "name", "department", "salary"]
df = spark.createDataFrame(data, columns)

result = df.groupBy("department").agg(
    count("id").alias("total_employees"),
    avg("salary").alias("avg_salary"),
    max("salary").alias("highest_salary")
)
result.show()
```

**Output:**
```
+----------+---------------+----------+--------------+
|department|total_employees|avg_salary|highest_salary|
+----------+---------------+----------+--------------+
|        HR|              2|    4250.0|          4500|
|        IT|              2|    5750.0|          6000|
|   Finance|              1|    7000.0|          7000|
+----------+---------------+----------+--------------+
```

---

### Q146. User activity → first and last action per user per day, ignore duplicates.

**Data:**
```
user_id | timestamp           | action
A       | 2023-03-20 11:30:00 | login
A       | 2023-03-20 09:30:00 | adding_to_cart
A       | 2023-03-20 11:30:00 | login  (duplicate — ignore)
```

**Solution:**
```python
from pyspark.sql.functions import col, to_date, min, max, first, last
from pyspark.sql.window import Window

# Remove duplicate actions per user per day
df = df.dropDuplicates(["user_id", "timestamp", "action"])

# Extract date
df = df.withColumn("date", to_date("timestamp"))

# Window ordered by timestamp
w = Window.partitionBy("user_id", "date").orderBy("timestamp")
w_desc = Window.partitionBy("user_id", "date").orderBy(col("timestamp").desc())

# Get first and last action
from pyspark.sql.functions import row_number

df_first = df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1) \
    .select("user_id", "date", col("action").alias("first_action"), col("timestamp").alias("first_time"))

df_last = df.withColumn("rn", row_number().over(w_desc)).filter(col("rn") == 1) \
    .select("user_id", "date", col("action").alias("last_action"), col("timestamp").alias("last_time"))

result = df_first.join(df_last, on=["user_id", "date"])
result.show()
```

**Simpler approach using `agg`:**
```python
from pyspark.sql.functions import struct, min as spark_min, max as spark_max

# Get first action (by earliest timestamp) and last action (by latest timestamp)
result = df.dropDuplicates(["user_id", "timestamp", "action"]) \
    .withColumn("date", to_date("timestamp")) \
    .groupBy("user_id", "date") \
    .agg(
        spark_min(struct("timestamp", "action")).alias("first"),
        spark_max(struct("timestamp", "action")).alias("last")
    ) \
    .select("user_id", "date",
            col("first.action").alias("first_action"),
            col("last.action").alias("last_action"))

result.show()
```

---

### Q147. Dept/salary → total per dept + rank (with and without window functions).

**Data:**
```python
data = [("HR", 30, 5000), ("HR", 28, 4500), ("IT", 35, 7000), ("IT", 32, 6500), ("Finance", 40, 8000)]
df = spark.createDataFrame(data, ["dept", "age", "salary"])
```

**With window function:**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, dense_rank, col

# Total salary per department
dept_window = Window.partitionBy("dept")
rank_window = Window.partitionBy("dept").orderBy(col("salary").desc())

result = df.withColumn("dept_total_salary", sum("salary").over(dept_window)) \
           .withColumn("salary_rank", dense_rank().over(rank_window))
result.show()
```

**Without window function:**
```python
# Total salary per department using groupBy + join back
dept_totals = df.groupBy("dept").agg(sum("salary").alias("dept_total_salary"))
result = df.join(dept_totals, on="dept")

# Rank without window: self-join approach
from pyspark.sql.functions import countDistinct

df_a = df.alias("a")
df_b = df.alias("b")

ranked = df_a.join(
    df_b,
    (col("a.dept") == col("b.dept")) & (col("a.salary") <= col("b.salary"))
).groupBy(col("a.dept"), col("a.age"), col("a.salary")) \
 .agg(countDistinct(col("b.salary")).alias("salary_rank"))

ranked.show()
```

---

### Q148. Create DataFrame → add conditional `age_group` column.

```python
from pyspark.sql.functions import when, col

data = [("Alice", 15), ("Bob", 35), ("Charlie", 70), ("David", 8), ("Eva", 62)]
df = spark.createDataFrame(data, ["name", "age"])

df = df.withColumn("age_group",
    when(col("age") <= 18, "kids")
    .when(col("age") <= 60, "adult")
    .otherwise("senior")
)

df.show()
```

**Output:**
```
+-------+---+---------+
|   name|age|age_group|
+-------+---+---------+
|  Alice| 15|     kids|
|    Bob| 35|    adult|
|Charlie| 70|   senior|
|  David|  8|     kids|
|    Eva| 62|   senior|
+-------+---+---------+
```

---

### Q149. Read CSV from HDFS → filter `amount > 1000` → write Parquet.

```python
# Read CSV from HDFS
df = spark.read.csv("hdfs:///data/customers.csv", header=True, inferSchema=True)

# Filter
filtered = df.filter(col("amount") > 1000)

# Write to HDFS in Parquet format
filtered.write.parquet("hdfs:///output/high_value_customers", mode="overwrite")

# Verify
spark.read.parquet("hdfs:///output/high_value_customers").show()
```

---

## Scenario / Design Problems

### Q150. 200M records/day — duplicates & partial matches — build pipeline.

**Architecture:**
```
Source (ADLS) → Bronze (raw) → Silver (deduplicated) → Gold (aggregated)
```

**Detailed approach:**

**Step 1: Ingestion (Bronze)**
```python
# Auto Loader for incremental file ingestion
raw_stream = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", "/checkpoints/schema/") \
    .load("abfss://container@storage/raw/fmcg/")

raw_stream.writeStream.format("delta") \
    .option("checkpointLocation", "/checkpoints/bronze/") \
    .outputMode("append") \
    .start("/delta/bronze/fmcg_data")
```

**Step 2: Deduplication (Silver)**
```python
bronze_df = spark.read.format("delta").load("/delta/bronze/fmcg_data")

# Exact deduplication
deduped = bronze_df.dropDuplicates(["order_id", "product_id", "timestamp"])

# Partial match detection — hash comparison
from pyspark.sql.functions import md5, concat_ws
deduped = deduped.withColumn("row_hash",
    md5(concat_ws("||", col("customer_name"), col("product_id"), col("amount")))
)

# MERGE into Silver (upsert — handles reprocessing)
silver_table = DeltaTable.forPath(spark, "/delta/silver/fmcg_data")
silver_table.alias("t").merge(
    deduped.alias("s"),
    "t.order_id = s.order_id AND t.product_id = s.product_id"
).whenMatchedUpdate(condition="t.row_hash != s.row_hash",
    set={"*": "s.*"}
).whenNotMatchedInsertAll() \
 .execute()
```

**Step 3: Optimization**
- Partition Silver table by `date`
- Z-order by `product_id` or `store_id`
- Auto-optimize enabled
- Cluster: 16-32 workers with auto-scaling

**Step 4: Quality checks**
```python
assert silver_df.filter(col("order_id").isNull()).count() == 0
assert silver_df.groupBy("order_id").count().filter("count > 1").count() == 0
```

---

### Q151. PySpark job runs 4 hrs for weekly report — how to optimise?

**Investigation steps:**
1. **Check Spark UI** — identify the slowest stage
2. **Check data volume** — has it grown significantly?
3. **Check for data skew** — one partition much larger?

**Optimization strategies:**

| Strategy | How |
|----------|-----|
| **Broadcast join** | If joining with small reference tables, broadcast them |
| **Partition pruning** | Filter on partition columns early |
| **Column pruning** | `select` only needed columns (don't `SELECT *`) |
| **Cache** | Persist intermediate DataFrames used multiple times |
| **Repartition** | Repartition by join key before joining |
| **Z-order source tables** | Speed up reads with data skipping |
| **AQE** | Enable Adaptive Query Execution |
| **Right-size cluster** | More workers with memory-optimized instances |
| **Incremental processing** | Only process new/changed data, not full table |
| **Predicate pushdown** | Push filters to Delta/Parquet before reading full data |

```python
# Example optimizations
spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.shuffle.partitions", "auto")

# Cache frequently joined table
reference_df = spark.read.format("delta").load("/path/to/ref").cache()

# Select only needed columns early
df = df.select("order_id", "product_id", "amount", "date") \
       .filter(col("date") >= "2024-03-01")

# Broadcast small table
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_ref), on="product_id")
```

---

### Q152. SCD Scenario — change country name without overwriting.

**(Covered in detail in Q81 above)**

**Answer: SCD Type 2** — preserves history.

```sql
-- Close existing record
UPDATE country_dim
SET end_date = CURRENT_DATE, is_current = FALSE
WHERE country_id = 2 AND is_current = TRUE;

-- Insert new record
INSERT INTO country_dim (country_id, country_name, start_date, end_date, is_current)
VALUES (2, 'United Kingdom', CURRENT_DATE, '9999-12-31', TRUE);
```

**Delta Lake implementation (MERGE):**
```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "/delta/country_dim")

# Close old record
delta_table.update(
    condition="country_id = 2 AND is_current = true",
    set={"end_date": "current_date()", "is_current": "false"}
)

# Insert new record
new_row = spark.createDataFrame([
    (2, "United Kingdom", "2024-03-15", "9999-12-31", True)
], ["country_id", "country_name", "start_date", "end_date", "is_current"])

new_row.write.format("delta").mode("append").save("/delta/country_dim")
```

**Result:**
```
| country_id | country_name   | start_date | end_date   | is_current |
|------------|----------------|------------|------------|------------|
| 2          | UK             | 2020-01-01 | 2024-03-15 | false      |
| 2          | United Kingdom | 2024-03-15 | 9999-12-31 | true       |
```

History is preserved — you can query `WHERE is_current = true` for latest, or query by date range for historical values.
