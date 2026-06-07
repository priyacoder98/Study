# 📘 Study Notes — 4. Azure — ADF, ADLS & Services

---

## ADF Basics

### Q59. How do you use ADF?

**Azure Data Factory (ADF)** is a cloud-based **ETL/ELT orchestration** service. It doesn't do heavy compute — it **orchestrates** data movement and triggers transformations in Databricks, Synapse, etc.

**Core components:**

| Component | Purpose |
|-----------|---------|
| **Pipeline** | A logical grouping of activities (workflow) |
| **Activity** | A single step — copy data, run notebook, execute SQL |
| **Dataset** | Points to the data (source/sink) |
| **Linked Service** | Connection string to a data store (ADLS, SQL, Databricks) |
| **Trigger** | Defines when a pipeline runs |
| **Integration Runtime** | The compute used to execute activities |

**Typical flow:**
```
Trigger → Pipeline → Copy Activity (source → ADLS) → Databricks Notebook → Sink (Delta Table)
```

---

### Q60. Why ADF, not Databricks Workflow?

| Aspect | ADF | Databricks Workflow |
|--------|-----|---------------------|
| **Orchestration** | Full — connects 100+ data sources natively | Limited to Databricks tasks |
| **Data movement** | Built-in Copy Activity (optimized) | Requires custom code |
| **Non-Spark tasks** | SQL, Web, Azure Functions, Logic Apps | Only Spark notebooks/JARs/Python |
| **Monitoring** | Rich UI for pipeline runs, alerts | Job-level monitoring |
| **Cost** | Pay-per-pipeline-run | Pay-per-compute (DBU) |
| **Hybrid** | Connects on-prem via Self-hosted IR | Cloud-only |
| **When to use** | Multi-service orchestration, data movement | Pure Spark workloads, ML pipelines |

> **Answer:** "We use ADF for orchestration — it pulls data from diverse sources (SQL Server, APIs, SFTP) into ADLS, then triggers Databricks notebooks for transformations. ADF handles what it does best (data movement, scheduling, monitoring), and Databricks handles compute."

---

### Q61. Different types of activities in ADF.

| Category | Activities | Examples |
|----------|-----------|----------|
| **Data Movement** | Copy Activity | Copy from SQL Server → ADLS |
| **Data Transformation** | Data Flow, Databricks Notebook, HDInsight, Stored Procedure | Transform data in Spark/SQL |
| **Control** | If Condition, ForEach, Until, Switch, Wait | Branching & looping logic |
| **Lookup** | Lookup, Get Metadata | Read config tables, check file existence |
| **Web** | Web Activity, Webhook | Call REST APIs |
| **Execute** | Execute Pipeline | Call child pipelines |

---

### Q62. What is Integration Runtime (IR) in ADF?

**Integration Runtime** is the **compute infrastructure** used by ADF to execute activities.

| IR Type | Where it runs | Use Case |
|---------|---------------|----------|
| **Azure IR** | Azure cloud (auto-managed) | Cloud-to-cloud data movement |
| **Self-hosted IR** | On-prem machine or Azure VM | On-prem ↔ cloud (SQL Server, file shares) |
| **Azure-SSIS IR** | Azure-managed SSIS | Run legacy SSIS packages in cloud |

```
On-prem SQL Server → [Self-hosted IR] → ADF Pipeline → [Azure IR] → ADLS
```

> **Key point:** Self-hosted IR is essential for **hybrid scenarios** — accessing on-premises data behind firewalls.

---

## Triggers & Scheduling

### Q63. Types of triggers in ADF.

| Trigger Type | When it fires |
|-------------|---------------|
| **Schedule Trigger** | Fixed schedule (cron — daily, hourly, weekly) |
| **Tumbling Window** | Fixed-size, non-overlapping time intervals; supports backfill & dependencies |
| **Event-based (Storage)** | When a file is created/deleted in Blob Storage/ADLS |
| **Custom Event** | From Event Grid (custom event types) |
| **Manual** | On-demand via UI, API, or PowerShell |

```json
// Schedule trigger: every day at 6 AM UTC
{
  "type": "ScheduleTrigger",
  "recurrence": {
    "frequency": "Day",
    "interval": 1,
    "startTime": "2024-01-01T06:00:00Z"
  }
}
```

---

### Q64. What are event-based triggers?

**Event-based triggers** fire when a **storage event** occurs — typically a file arriving in Blob Storage or ADLS.

**How it works:**
1. File lands in ADLS container
2. Azure Event Grid detects the event
3. ADF trigger fires → starts the pipeline

**Configuration:**
- **Storage account** → subscribe to Blob Created / Blob Deleted events
- **Filter:** by folder path, file name prefix/suffix (e.g., `*.csv`)

**Use case:** "When a new CSV file arrives in `/raw/sales/`, automatically trigger the ingestion pipeline."

```json
{
  "type": "BlobEventsTrigger",
  "typeProperties": {
    "blobPathBeginsWith": "/raw/sales/",
    "blobPathEndsWith": ".csv",
    "events": ["Microsoft.Storage.BlobCreated"]
  }
}
```

---

### Q65. Different kinds of ADF schedulers.

Same as trigger types (Q63). Additionally:
- **Tumbling Window** is unique to ADF — great for **backfill** scenarios (reprocessing past windows)
- Tumbling windows can have **dependencies** — window N waits for window N-1 to complete

---

### Q66. How to schedule jobs in Databricks.

**Option 1: Databricks Workflows (native)**
- Create a Job → add tasks (notebooks, JARs, Python)
- Set schedule (cron) or trigger manually
- Define dependencies between tasks

**Option 2: ADF orchestration**
- ADF Schedule Trigger → Pipeline → Databricks Notebook Activity
- Better for multi-service pipelines

**Option 3: External tools**
- Apache Airflow with Databricks operator
- Azure DevOps pipelines

---

## Parameterization & Debugging

### Q67. How to parameterize in ADF.

**Levels of parameterization:**

| Level | How |
|-------|-----|
| **Pipeline parameters** | Defined on pipeline, passed at runtime |
| **Global parameters** | Shared across all pipelines in a factory |
| **Variables** | Scoped to a pipeline, mutable during execution |
| **Expressions** | Dynamic content using `@pipeline().parameters.myParam` |
| **Linked Service params** | Dynamic connection strings |

```json
// Expression examples in ADF
"@pipeline().parameters.fileName"
"@concat('raw/', pipeline().parameters.date, '/data.csv')"
"@formatDateTime(utcNow(), 'yyyy-MM-dd')"
"@activity('LookupConfig').output.firstRow.tableName"
```

**Common pattern:**
```
Lookup Activity (read config table) → ForEach → Copy/Databricks (parameterized)
```

---

### Q68. How to debug ADF pipelines.

1. **Debug mode** — click "Debug" in ADF Studio → runs pipeline with breakpoints
2. **Monitor tab** — view all pipeline runs, activity-level status, duration, errors
3. **Activity output** — click failed activity → view detailed error message & stack trace
4. **Rerun from failed** — rerun pipeline from the failed activity (skip successful ones)
5. **Alerts** — set up Azure Monitor alerts for pipeline failures
6. **Diagnostic logs** — send to Log Analytics for deep analysis

**Debugging tips:**
- Check **Input/Output** tabs on each activity for actual data passed
- For Copy Activity: check **rows read vs rows written** for data loss
- For Databricks: check the **Spark UI** linked from the activity output

---

### Q69. ADF pipeline failure — how to handle?

1. **Retry policy** — set retries (up to 3) with backoff interval on each activity
2. **Failure path** — connect a "failure" activity (e.g., send email, log to DB)
3. **Try-Catch pattern** — use Execute Pipeline with "Secure Output" + If Condition
4. **Alerts** — Azure Monitor action groups → email/SMS/Teams on failure
5. **Error logging** — Web Activity to log error details to a monitoring table
6. **Rerun from failed** — manual rerun from the failed activity

```
[Copy Activity] → success → [Databricks]
                 ↘ failure → [Send Alert Email] → [Log to Error Table]
```

---

## ADLS & Connectivity

### Q70. How to connect ADF & ADLS with Databricks.

**Step 1: ADF → ADLS (Linked Service)**
- Create ADLS Gen2 Linked Service in ADF
- Auth: Managed Identity (recommended) or Account Key

**Step 2: ADF → Databricks (Linked Service)**
- Create Databricks Linked Service
- Auth: Access Token or Managed Identity
- Specify cluster ID or new job cluster config

**Step 3: Pipeline**
```
Copy Activity (Source → ADLS) → Databricks Notebook Activity (transform) → Copy Activity (ADLS → Sink)
```

**In Databricks → ADLS:**
```python
# Mount ADLS
dbutils.fs.mount(
    source="abfss://container@storage.dfs.core.windows.net/",
    mount_point="/mnt/my_mount",
    extra_configs={"fs.azure.account.key.storage.dfs.core.windows.net":
                   dbutils.secrets.get("scope", "key")}
)

# Direct access (recommended over mount)
spark.conf.set("fs.azure.account.key.<storage>.dfs.core.windows.net",
               dbutils.secrets.get("scope", "key"))
df = spark.read.csv("abfss://container@storage.dfs.core.windows.net/path")
```

---

### Q71. How to read & write files in ADLS.

```python
# Using abfss:// protocol (recommended)
path = "abfss://container@storageaccount.dfs.core.windows.net/folder/file.csv"

# Read
df = spark.read.format("csv").option("header", True).load(path)

# Write
df.write.format("delta").mode("overwrite").save(
    "abfss://container@storageaccount.dfs.core.windows.net/output/"
)

# Using mount point
df = spark.read.csv("/mnt/my_mount/folder/file.csv")
```

---

### Q72–73. Secret Scope in Azure / Databricks.

**Secret Scope** = a secure store for credentials (keys, passwords, connection strings) that Databricks notebooks can access without hardcoding secrets.

**Two types:**
| Type | Backed by | Management |
|------|-----------|------------|
| **Azure Key Vault-backed** | Azure Key Vault | Manage secrets in Key Vault |
| **Databricks-backed** | Databricks internal store | Manage via Databricks CLI |

**Creating Azure Key Vault-backed scope:**
1. Create an Azure Key Vault → add secrets (e.g., storage account key)
2. In Databricks → `https://<workspace-url>#secrets/createScope`
3. Provide scope name, Key Vault DNS name, and Resource ID

**Using in code:**
```python
storage_key = dbutils.secrets.get(scope="my-kv-scope", key="adls-account-key")
spark.conf.set("fs.azure.account.key.mystorage.dfs.core.windows.net", storage_key)
```

> **Never hardcode credentials** — always use secret scopes.

---

## Security

### Q74. What is a Service Principal?

A **Service Principal** is an **identity for an application** in Azure Active Directory (now Entra ID). It's like a "user account" for automated processes.

**Used for:**
- ADF accessing ADLS or Databricks (non-interactive auth)
- Databricks accessing ADLS without user credentials
- CI/CD pipelines deploying to Azure resources

**Components:**
| Component | What it is |
|-----------|-----------|
| **Application (client) ID** | Unique identifier |
| **Directory (tenant) ID** | Azure AD tenant |
| **Client Secret / Certificate** | Password or certificate for auth |

**How to use with ADLS in Databricks:**
```python
spark.conf.set("fs.azure.account.auth.type.<storage>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<storage>.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<storage>.dfs.core.windows.net", "<app-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.<storage>.dfs.core.windows.net",
               dbutils.secrets.get("scope", "client-secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage>.dfs.core.windows.net",
               "https://login.microsoftonline.com/<tenant-id>/oauth2/token")
```

---

### Q75. How to implement RBAC?

**RBAC (Role-Based Access Control)** = assigning permissions based on roles, not individual users.

**In Azure:**
| Level | How |
|-------|-----|
| **ADLS** | Storage Blob Data Reader/Contributor roles via Azure IAM |
| **ADF** | Data Factory Contributor / Reader roles |
| **Databricks** | Workspace Admin / User / Viewer |
| **Unity Catalog** | GRANT/REVOKE on catalogs, schemas, tables |

**In Databricks (Unity Catalog):**
```sql
-- Create a group
-- (done in Azure AD / Databricks Admin Console)

-- Grant access
GRANT USAGE ON CATALOG production TO `data_engineers`;
GRANT SELECT ON SCHEMA production.gold TO `analysts`;
GRANT ALL PRIVILEGES ON TABLE production.gold.sales TO `data_engineers`;

-- Revoke
REVOKE SELECT ON TABLE production.gold.sales FROM `interns`;
```

---

## Azure Services (General)

### Q76. Azure services used by a data engineer.

| Service | Purpose |
|---------|---------|
| **ADLS Gen2** | Scalable data lake storage |
| **Azure Data Factory** | ETL/ELT orchestration |
| **Azure Databricks** | Spark-based data processing |
| **Azure Key Vault** | Secret/credential management |
| **Azure SQL Database** | Relational database |
| **Azure Synapse Analytics** | Data warehouse + Spark |
| **Azure Event Hub / IoT Hub** | Real-time streaming ingestion |
| **Azure DevOps** | CI/CD for pipelines & notebooks |
| **Azure Monitor / Log Analytics** | Monitoring & alerting |
| **Azure Active Directory (Entra ID)** | Identity & access management |
