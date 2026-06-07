# 📘 Study Notes — 1. SQL

---

## Joins

### Q1. Explain SQL joins.

A **join** combines rows from two or more tables based on a related column.

| Join Type | Returns |
|-----------|---------|
| **INNER JOIN** | Only matching rows from both tables |
| **LEFT JOIN** | All rows from left + matching from right (NULL if no match) |
| **RIGHT JOIN** | All rows from right + matching from left |
| **FULL OUTER JOIN** | All rows from both tables (NULL where no match) |
| **CROSS JOIN** | Cartesian product — every row × every row |
| **SELF JOIN** | Table joined with itself (e.g., manager–employee hierarchy) |

```sql
-- Example: INNER JOIN
SELECT e.name, d.department_name
FROM employee e
INNER JOIN department d ON e.dept_id = d.dept_id;

-- LEFT JOIN (all employees, even without a department)
SELECT e.name, d.department_name
FROM employee e
LEFT JOIN department d ON e.dept_id = d.dept_id;
```

> **Interview tip:** Always mention the use case. LEFT JOIN is common when you need all records from one table regardless of matches.

---

### Q2. What is a semi-join?

A **semi-join** returns rows from the left table where a match exists in the right table — but **does not return columns from the right table**. It's like an `EXISTS` or `IN` subquery.

```sql
-- Semi-join using EXISTS
SELECT e.name, e.salary
FROM employee e
WHERE EXISTS (
    SELECT 1 FROM department d
    WHERE d.dept_id = e.dept_id AND d.location = 'Mumbai'
);

-- Equivalent using IN
SELECT name, salary FROM employee
WHERE dept_id IN (SELECT dept_id FROM department WHERE location = 'Mumbai');
```

**Anti-join** is the opposite — returns rows with **no match** (`NOT EXISTS`).

> **In PySpark:** `df1.join(df2, on="key", how="left_semi")` / `"left_anti"`

---

## Window Functions

### Q3. Explain and use window functions.

Window functions perform calculations **across a set of rows related to the current row**, without collapsing them (unlike `GROUP BY`).

**Syntax:**
```sql
function_name() OVER (
    [PARTITION BY col1]
    [ORDER BY col2]
    [ROWS/RANGE frame_clause]
)
```

**Categories:**

| Category | Functions |
|----------|-----------|
| **Ranking** | `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `NTILE()` |
| **Aggregate** | `SUM()`, `AVG()`, `COUNT()`, `MIN()`, `MAX()` |
| **Value** | `LAG()`, `LEAD()`, `FIRST_VALUE()`, `LAST_VALUE()` |

```sql
-- Running total of salary per department
SELECT name, department, salary,
    SUM(salary) OVER (PARTITION BY department ORDER BY salary) AS running_total
FROM employee;

-- Previous month's sales
SELECT month, sales,
    LAG(sales, 1) OVER (ORDER BY month) AS prev_month_sales
FROM monthly_sales;
```

---

### Q4. ROW_NUMBER vs RANK vs DENSE_RANK

All are ranking functions, but differ in how they handle **ties**:

| Data (salary) | ROW_NUMBER | RANK | DENSE_RANK |
|---------------|-----------|------|------------|
| 100 | 1 | 1 | 1 |
| 200 | 2 | 2 | 2 |
| 200 | 3 | 2 | 2 |
| 300 | 4 | **4** | **3** |

- **ROW_NUMBER:** Always unique, arbitrary for ties (1, 2, 3, 4)
- **RANK:** Same rank for ties, **skips** next (1, 2, 2, **4**)
- **DENSE_RANK:** Same rank for ties, **no skip** (1, 2, 2, **3**)

**When to use:**
- `ROW_NUMBER` → deduplication (pick 1 row per group)
- `RANK` → competitive ranking (who came 1st, 2nd...)
- `DENSE_RANK` → Nth highest salary (no gaps in ranking)

```sql
-- Second highest salary using DENSE_RANK
WITH ranked AS (
    SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS rnk
    FROM employee
)
SELECT DISTINCT salary FROM ranked WHERE rnk = 2;
```

---

### Q5. Why should we use window functions? Give examples.

**Why:**
- Perform calculations **without collapsing rows** (unlike GROUP BY)
- Access data from other rows (LAG/LEAD) — useful for time-series
- Running totals, moving averages, rankings in a single query
- Avoids expensive self-joins or correlated subqueries

**Examples:**
- Rank employees by salary within each department
- Calculate month-over-month growth
- Running total of revenue
- Identify first/last purchase per customer
- Moving average for trend analysis

---

### Q6. Window functions vs aggregate functions — explain with table.

| Aspect | Aggregate Functions | Window Functions |
|--------|-------------------|------------------|
| **Rows returned** | One row per group | All original rows preserved |
| **Requires GROUP BY** | Yes | No |
| **Syntax** | `SUM(sal) ... GROUP BY dept` | `SUM(sal) OVER (PARTITION BY dept)` |
| **Access to row detail** | Lost after grouping | Retained |

**Example with table:**

| name | dept | salary |
|------|------|--------|
| Alice | HR | 50000 |
| Bob | HR | 60000 |
| Charlie | IT | 70000 |

```sql
-- Aggregate: 1 row per dept
SELECT dept, SUM(salary) FROM employee GROUP BY dept;
-- Result: HR=110000, IT=70000

-- Window: all rows preserved + total added
SELECT name, dept, salary,
    SUM(salary) OVER (PARTITION BY dept) AS dept_total
FROM employee;
-- Result: Alice|HR|50000|110000, Bob|HR|60000|110000, Charlie|IT|70000|70000
```

---

## Aggregations & GroupBy

### Q7. GroupBy — explain and use.

`GROUP BY` groups rows sharing a value and applies aggregate functions to each group.

```sql
SELECT department, COUNT(*) AS emp_count, AVG(salary) AS avg_sal
FROM employee
GROUP BY department
HAVING AVG(salary) > 50000;  -- filter on aggregated result
```

**Key rules:**
- Every non-aggregated column in SELECT must be in GROUP BY
- `WHERE` filters **before** grouping, `HAVING` filters **after**
- Common aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`

---

### Q8. Employee + Department → total employees, avg salary, highest salary.

```sql
SELECT d.department_name,
    COUNT(e.employee_id) AS total_employees,
    AVG(e.salary) AS avg_salary,
    MAX(e.salary) AS highest_salary
FROM employee e
JOIN department d ON e.department_id = d.department_id
GROUP BY d.department_name;
```

---

### Q9. Moving average — `ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING`.

A moving average smooths data by averaging a sliding window of rows.

```sql
SELECT sale_date, amount,
    AVG(amount) OVER (
        ORDER BY sale_date
        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    ) AS moving_avg
FROM sales;
```

| sale_date | amount | moving_avg |
|-----------|--------|------------|
| 2025-11-01 | 100 | (100+150)/2 = **125** |
| 2025-11-02 | 150 | (100+150+200)/3 = **150** |
| 2025-11-03 | 200 | (150+200+250)/3 = **200** |

**Frame options:**
- `ROWS BETWEEN N PRECEDING AND N FOLLOWING` — fixed row count
- `RANGE BETWEEN ...` — based on value range
- `UNBOUNDED PRECEDING` — from start to current row (running total)

---

## Duplicates

### Q10. Find duplicate values without using DISTINCT.

**Method 1: GROUP BY + HAVING**
```sql
SELECT name, COUNT(*) AS cnt
FROM employee
GROUP BY name
HAVING COUNT(*) > 1;
```

**Method 2: ROW_NUMBER (find & keep one)**
```sql
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY name ORDER BY id) AS rn
    FROM employee
)
SELECT * FROM ranked WHERE rn > 1;  -- these are duplicates
```

---

### Q11. How to drop/filter out duplicates?

```sql
-- Keep first occurrence, delete rest
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY name, dept ORDER BY id) AS rn
    FROM employee
)
DELETE FROM ranked WHERE rn > 1;

-- Or create a clean table
SELECT DISTINCT * FROM employee;
```

**In PySpark:**
```python
df.dropDuplicates()                    # all columns
df.dropDuplicates(["name", "dept"])    # specific columns
```

---

### Q12. Delete duplicates from a Delta table using CTE.

```sql
WITH duplicates AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY name, dept ORDER BY id) AS rn
    FROM my_delta_table
)
MERGE INTO my_delta_table AS target
USING (SELECT id FROM duplicates WHERE rn > 1) AS source
ON target.id = source.id
WHEN MATCHED THEN DELETE;
```

**Alternative — overwrite with deduplicated data:**
```sql
CREATE OR REPLACE TABLE my_delta_table AS
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY name, dept ORDER BY id) AS rn
    FROM my_delta_table
) WHERE rn = 1;
```

---

## Ranking & Nth Value

### Q13. Fetch the second highest sales per product.

```sql
WITH ranked AS (
    SELECT product, sales,
        DENSE_RANK() OVER (PARTITION BY product ORDER BY sales DESC) AS rnk
    FROM sales_table
)
SELECT product, sales FROM ranked WHERE rnk = 2;
```

> Use `DENSE_RANK` not `RANK` — if two products tie for 1st, `RANK` would skip 2nd.

---

### Q14. Nth highest salary in SQL and PySpark.

**SQL:**
```sql
-- Using DENSE_RANK
WITH ranked AS (
    SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS rnk
    FROM employee
)
SELECT DISTINCT salary FROM ranked WHERE rnk = N;

-- Using LIMIT/OFFSET (simpler but less flexible)
SELECT DISTINCT salary FROM employee ORDER BY salary DESC LIMIT 1 OFFSET N-1;
```

**PySpark:**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank

w = Window.orderBy(col("salary").desc())
df.withColumn("rnk", dense_rank().over(w)) \
  .filter(col("rnk") == N) \
  .select("salary").distinct()
```

---

## Column Operations

### Q15. How to remove/rename a column?

**SQL:**
```sql
ALTER TABLE employee DROP COLUMN middle_name;
ALTER TABLE employee RENAME COLUMN fname TO first_name;
```

**PySpark:**
```python
df = df.drop("middle_name")
df = df.withColumnRenamed("fname", "first_name")
```

---

### Q16. Bucket/bin sales values (e.g. 25 → `20–30`).

```sql
SELECT *,
    CONCAT(FLOOR(sale / 10) * 10, '-', FLOOR(sale / 10) * 10 + 10) AS sales_bucket
FROM sales_table;

-- Using CASE (explicit buckets)
SELECT *,
    CASE
        WHEN sale BETWEEN 0 AND 10 THEN '0-10'
        WHEN sale BETWEEN 11 AND 20 THEN '10-20'
        WHEN sale BETWEEN 21 AND 30 THEN '20-30'
        ELSE '30+'
    END AS sales_bucket
FROM sales_table;
```

**PySpark (scalable):**
```python
from pyspark.sql.functions import floor, concat, lit
df.withColumn("bucket_start", floor(col("sale") / 10) * 10) \
  .withColumn("sales_bucket", concat(col("bucket_start"), lit("-"), col("bucket_start") + 10))
```

---

## CTE & Temp Tables

### Q17. CTE vs Temp Table.

| Aspect | CTE (`WITH ... AS`) | Temp Table |
|--------|---------------------|------------|
| **Scope** | Single query only | Entire session |
| **Storage** | Not materialized (inline) | Physically stored in tempdb |
| **Reusability** | Cannot reuse across queries | Can reuse multiple times |
| **Indexing** | Cannot index | Can add indexes |
| **Performance** | Re-evaluated each reference | Computed once, read many |
| **Use case** | Readability, recursive queries | Large intermediate results |

```sql
-- CTE
WITH dept_stats AS (
    SELECT dept, AVG(salary) AS avg_sal FROM employee GROUP BY dept
)
SELECT * FROM dept_stats WHERE avg_sal > 50000;

-- Temp Table
CREATE TEMPORARY TABLE dept_stats AS
SELECT dept, AVG(salary) AS avg_sal FROM employee GROUP BY dept;

SELECT * FROM dept_stats WHERE avg_sal > 50000;  -- can reuse
```

---

## Pivot

### Q18. Pivot: `(entity_id, subjects, marks)` → `(entity_id, Maths, English)`.

```sql
SELECT entity_id,
    MAX(CASE WHEN subjects = 'Maths' THEN marks END) AS Maths,
    MAX(CASE WHEN subjects = 'English' THEN marks END) AS English
FROM student_marks
GROUP BY entity_id;
```

**In Databricks SQL:**
```sql
SELECT * FROM student_marks
PIVOT (MAX(marks) FOR subjects IN ('Maths', 'English'));
```

**PySpark:**
```python
df.groupBy("entity_id").pivot("subjects").agg(max("marks"))
```

---

## ETL vs ELT

### Q19. ETL vs ELT — what's the difference?

| Aspect | ETL | ELT |
|--------|-----|-----|
| **Full form** | Extract → Transform → Load | Extract → Load → Transform |
| **Where transformation happens** | Staging server / ETL tool | Inside the target system (DW/lakehouse) |
| **Best for** | On-prem, structured data | Cloud, big data, data lakes |
| **Tools** | Informatica, Talend, SSIS | Databricks, Snowflake, BigQuery |
| **Speed** | Slower (transform before load) | Faster (raw data loaded first) |
| **Data availability** | Only transformed data in target | Raw + transformed data available |
| **Scalability** | Limited by ETL server | Leverages cloud compute |

> **Modern trend:** ELT is dominant in cloud architectures because cloud warehouses/lakehouses have powerful compute for transformations. Databricks + ADF follows an ELT pattern.
