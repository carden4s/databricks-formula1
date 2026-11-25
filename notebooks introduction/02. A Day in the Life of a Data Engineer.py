# Databricks notebook source
# DBTITLE 1,Title and story
# MAGIC %md
# MAGIC # A Day in the Life of a Data Engineer
# MAGIC
# MAGIC The story: **Ana** is a data engineer at **FreshMart**, an online grocery store. Every day the web app drops yesterday's orders as raw JSON files. Ana's job is to turn those messy files into clean, trusted tables that analysts and dashboards can use. This notebook walks through her day hour by hour, using the **medallion architecture**: Bronze (raw), Silver (clean), and Gold (business-ready). All the data is randomly generated.
# MAGIC
# MAGIC | Time | What Ana does | Output |
# MAGIC | --- | --- | --- |
# MAGIC | 08:00 | Sets up her workspace (schema and volume) | `de_day_demo` schema, `raw_landing` volume |
# MAGIC | 08:30 | Raw files arrive from the app (simulated) | JSON and CSV files in the volume |
# MAGIC | 09:00 | Ingests the raw data as-is into Bronze | `bronze_orders` |
# MAGIC | 10:00 | Runs data quality checks | DQ report |
# MAGIC | 11:00 | Cleans the data into Silver and quarantines bad rows | `silver_orders`, `quarantine_orders` |
# MAGIC | 12:00 | Applies late corrections with MERGE | updated `silver_orders` |
# MAGIC | 13:00 | Builds Gold tables for the business | `gold_*` tables |
# MAGIC | 14:00 | Audits changes with Delta time travel | history report |
# MAGIC | 15:00 | Optimizes and documents the tables | OPTIMIZE, comments |
# MAGIC | 16:00 | Wraps up and plans the schedule | Job plan |

# COMMAND ----------

# DBTITLE 1,08:00 Setup
# MAGIC %md
# MAGIC ## 08:00 - Coffee and setup
# MAGIC Ana starts by making sure her workspace exists: a **schema** to hold her tables and a **volume** (a folder governed by Unity Catalog) where raw files land. Using `IF NOT EXISTS` makes the step safe to re-run every day.

# COMMAND ----------

# DBTITLE 1,Create schema and landing volume
from pyspark.sql import functions as F

CATALOG = "databrick_course_ws"
SCHEMA = "de_day_demo"
VOLUME = "raw_landing"
BUSINESS_DATE = "2026-10-28"   # Ana processes yesterday's orders

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

RAW = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
print("Working in:", spark.sql("SELECT current_catalog() || '.' || current_schema()").first()[0])
print("Raw landing zone:", RAW)

# COMMAND ----------

# DBTITLE 1,08:30 Raw data arrives
# MAGIC %md
# MAGIC ## 08:30 - The raw files arrive
# MAGIC In real life the FreshMart app writes these files. Here we **simulate** them with random data: 5,000 orders, 200 customers, and 20 products.
# MAGIC
# MAGIC Real data is never perfect, so we deliberately add problems Ana has to deal with:
# MAGIC * about 2% duplicate orders (the app sometimes sends a file twice)
# MAGIC * about 1% orders with no `customer_id`
# MAGIC * about 1% orders with negative quantities
# MAGIC * about 0.5% orders with a broken timestamp (`not-a-date`)

# COMMAND ----------

# DBTITLE 1,Generate random raw data
# --- Reference data: products and customers ---
products_data = [
    (1, "Organic Bananas", "Produce", 0.89), (2, "Avocados", "Produce", 1.49),
    (3, "Baby Spinach", "Produce", 3.29), (4, "Strawberries", "Produce", 4.99),
    (5, "Whole Milk", "Dairy", 3.49), (6, "Greek Yogurt", "Dairy", 5.99),
    (7, "Cheddar Cheese", "Dairy", 6.49), (8, "Free-Range Eggs", "Dairy", 4.79),
    (9, "Sourdough Bread", "Bakery", 5.49), (10, "Croissants", "Bakery", 6.99),
    (11, "Chicken Breast", "Meat", 9.99), (12, "Ground Beef", "Meat", 7.99),
    (13, "Salmon Fillet", "Seafood", 12.99), (14, "Shrimp", "Seafood", 10.49),
    (15, "Pasta", "Pantry", 1.99), (16, "Olive Oil", "Pantry", 11.99),
    (17, "Coffee Beans", "Pantry", 13.49), (18, "Orange Juice", "Beverages", 4.29),
    (19, "Sparkling Water", "Beverages", 5.99), (20, "Dark Chocolate", "Snacks", 3.99),
]
products = spark.createDataFrame(products_data, "product_id INT, product_name STRING, category STRING, unit_price DOUBLE")

cities = ["Madrid", "Barcelona", "Valencia", "Sevilla", "Bilbao", "Malaga"]
customers = spark.range(1, 201).select(
    F.col("id").cast("int").alias("customer_id"),
    F.format_string("Customer %03d", F.col("id")).alias("customer_name"),
    F.element_at(F.array(*[F.lit(c) for c in cities]), (F.floor(F.rand(11) * len(cities)) + 1).cast("int")).alias("city"),
    F.date_sub(F.lit(BUSINESS_DATE).cast("date"), F.floor(F.rand(12) * 730).cast("int")).alias("signup_date"),
)

# --- Yesterday's orders (random, with some dirty rows) ---
N_ORDERS = 5000
orders = (
    spark.range(1, N_ORDERS + 1)
    .select(
        F.format_string("ORD-%06d", F.col("id")).alias("order_id"),
        (F.floor(F.rand(1) * 200) + 1).cast("int").alias("customer_id"),
        (F.floor(F.rand(2) * 20) + 1).cast("int").alias("product_id"),
        (F.floor(F.rand(3) * 5) + 1).cast("int").alias("quantity"),
        F.date_format(
            F.timestamp_seconds(F.unix_timestamp(F.lit(f"{BUSINESS_DATE} 00:00:00")) + F.floor(F.rand(4) * 86400)),
            "yyyy-MM-dd HH:mm:ss").alias("order_ts"),
        F.element_at(F.array(F.lit("card"), F.lit("paypal"), F.lit("cash_on_delivery")),
                     (F.floor(F.rand(5) * 3) + 1).cast("int")).alias("payment_method"),
        F.rand(6).alias("_r_status"),
        F.rand(7).alias("_r_dirty"),
    )
    .withColumn("status", F.when(F.col("_r_status") < 0.85, "completed")
                           .when(F.col("_r_status") < 0.95, "cancelled")
                           .otherwise("pending"))
    # inject data quality problems
    .withColumn("customer_id", F.when(F.col("_r_dirty") < 0.01, F.lit(None)).otherwise(F.col("customer_id")))
    .withColumn("quantity", F.when(F.col("_r_dirty").between(0.01, 0.02), -F.col("quantity")).otherwise(F.col("quantity")))
    .withColumn("order_ts", F.when(F.col("_r_dirty").between(0.0201, 0.025), F.lit("not-a-date")).otherwise(F.col("order_ts")))
    .drop("_r_status", "_r_dirty")
)
duplicates = orders.sample(fraction=0.02, seed=8)   # the app re-sent some orders
orders_raw = orders.unionByName(duplicates)

# --- "The app" writes the files into the landing volume ---
orders_raw.repartition(4).write.mode("overwrite").json(f"{RAW}/orders/{BUSINESS_DATE}")
customers.write.mode("overwrite").option("header", True).csv(f"{RAW}/customers")
products.write.mode("overwrite").option("header", True).csv(f"{RAW}/products")

print(f"Raw orders written: {orders_raw.count()} rows ({N_ORDERS} unique + duplicates)")

# COMMAND ----------

# DBTITLE 1,Peek at the raw files
# First thing Ana does: look at what actually landed
display(dbutils.fs.ls(f"{RAW}/orders/{BUSINESS_DATE}"))
display(spark.read.text(f"{RAW}/orders/{BUSINESS_DATE}").limit(5))

# COMMAND ----------

# DBTITLE 1,09:00 Bronze
# MAGIC %md
# MAGIC ## 09:00 - Bronze: load the raw data as-is
# MAGIC Bronze is a copy of the source with **no cleaning**. Every column is read as text so that nothing fails on bad values. Ana adds two audit columns: which file each row came from and when it was loaded. If something goes wrong later, she can always go back to Bronze.
# MAGIC
# MAGIC In production she would usually use **Auto Loader** to pick up only the new files each day. A plain batch read keeps this example simple.

# COMMAND ----------

# DBTITLE 1,Ingest to Bronze
orders_schema = """order_id STRING, customer_id STRING, product_id STRING, quantity STRING,
                   order_ts STRING, payment_method STRING, status STRING"""

bronze_orders = (
    spark.read.schema(orders_schema).json(f"{RAW}/orders/{BUSINESS_DATE}")
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
)
bronze_orders.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_orders")

# Reference data is small and clean, so it goes straight to Silver
(spark.read.option("header", True).schema("customer_id INT, customer_name STRING, city STRING, signup_date DATE")
     .csv(f"{RAW}/customers").write.mode("overwrite").saveAsTable("silver_customers"))
(spark.read.option("header", True).schema("product_id INT, product_name STRING, category STRING, unit_price DOUBLE")
     .csv(f"{RAW}/products").write.mode("overwrite").saveAsTable("silver_products"))

for t in ["bronze_orders", "silver_customers", "silver_products"]:
    print(f"{t}: {spark.table(t).count()} rows")
display(spark.table("bronze_orders").limit(5))

# COMMAND ----------

# DBTITLE 1,10:00 Data quality
# MAGIC %md
# MAGIC ## 10:00 - Data quality checks
# MAGIC Before trusting the data, Ana measures how dirty it is. These numbers go into her daily report. If they ever jump (for example, 30% missing customers), something is broken in the app and she warns the team before anyone builds a report on bad data.

# COMMAND ----------

# DBTITLE 1,Data quality report on Bronze
b = spark.table("bronze_orders")

dq_report = b.agg(
    F.count("*").alias("total_rows"),
    (F.count("*") - F.countDistinct("order_id")).alias("duplicate_rows"),
    F.sum(F.col("customer_id").isNull().cast("int")).alias("missing_customer_id"),
    F.sum((F.expr("try_cast(quantity AS INT)") <= 0).cast("int")).alias("non_positive_quantity"),
    F.sum(F.expr("try_to_timestamp(order_ts)").isNull().cast("int")).alias("invalid_timestamp"),
)
display(dq_report)

# COMMAND ----------

# DBTITLE 1,11:00 Silver
# MAGIC %md
# MAGIC ## 11:00 - Silver: clean, deduplicate, and enrich
# MAGIC Ana turns Bronze into a trusted table:
# MAGIC 1. **Cast** text into proper types. `try_cast` turns bad values into NULL instead of failing.
# MAGIC 2. **Deduplicate** on `order_id`.
# MAGIC 3. **Validate** each row. Bad rows are not deleted. They go to a **quarantine** table with the reason, so the app team can fix them.
# MAGIC 4. **Enrich** good rows with product info and compute `amount`.
# MAGIC 5. **Reconcile**: check that every Bronze row ends up somewhere, so no data is lost silently.

# COMMAND ----------

# DBTITLE 1,Build Silver and quarantine
typed = spark.table("bronze_orders").select(
    "order_id",
    F.expr("try_cast(customer_id AS INT)").alias("customer_id"),
    F.expr("try_cast(product_id AS INT)").alias("product_id"),
    F.expr("try_cast(quantity AS INT)").alias("quantity"),
    F.expr("try_to_timestamp(order_ts)").alias("order_ts"),
    F.col("order_ts").alias("raw_order_ts"),
    "payment_method", "status", "_ingested_at",
)

deduped = typed.dropDuplicates(["order_id"])

# Explain WHY a row is rejected (concat_ws skips NULLs, so valid rows get an empty string)
checked = deduped.withColumn("reject_reason", F.concat_ws(", ",
    F.when(F.col("customer_id").isNull(), F.lit("missing customer_id")),
    F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), F.lit("non-positive quantity")),
    F.when(F.col("order_ts").isNull(), F.lit("invalid order_ts")),
))

quarantine = checked.filter(F.col("reject_reason") != "")
valid = checked.filter(F.col("reject_reason") == "")

silver_orders = (
    valid.join(spark.table("silver_products"), "product_id")
    .select(
        "order_id", "customer_id", "product_id", "product_name", "category", "quantity", "unit_price",
        F.round(F.col("quantity") * F.col("unit_price"), 2).alias("amount"),
        "payment_method", "status", "order_ts",
        F.to_date("order_ts").alias("order_date"),
        "_ingested_at",
        F.current_timestamp().alias("_updated_at"),
    )
)
silver_orders.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_orders")
quarantine.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("quarantine_orders")

# --- Reconciliation: every Bronze row must be accounted for ---
n_bronze = spark.table("bronze_orders").count()
n_dedup = deduped.count()
n_valid = valid.count()
n_silver = spark.table("silver_orders").count()
n_quar = spark.table("quarantine_orders").count()
print(f"Bronze rows:          {n_bronze}")
print(f"  duplicates removed: {n_bronze - n_dedup}")
print(f"  quarantined:        {n_quar}")
print(f"  loaded to Silver:   {n_silver}")
assert n_silver == n_valid, "Some valid rows were lost in the product join!"
assert n_silver + n_quar == n_dedup, "Reconciliation failed!"
print("Reconciliation OK")

display(spark.table("quarantine_orders").groupBy("reject_reason").count().orderBy(F.desc("count")))

# COMMAND ----------

# DBTITLE 1,12:00 Late corrections
# MAGIC %md
# MAGIC ## 12:00 - Late corrections arrive (MERGE)
# MAGIC The customer service team sends a file: **50 orders were refunded** (their status should be `cancelled`), and **20 orders from late last night** were missing. Reloading everything would be slow, so Ana uses **MERGE** (an upsert). It updates the orders that already exist and inserts the new ones in a single operation.

# COMMAND ----------

# DBTITLE 1,MERGE corrections into Silver
silver = spark.table("silver_orders")

refunds = (silver.filter("status = 'completed'").orderBy("order_id").limit(50)
           .withColumn("status", F.lit("cancelled")))
late_orders = (silver.orderBy("order_id").limit(20)
               .withColumn("order_id", F.regexp_replace("order_id", "ORD-", "LATE-"))
               .withColumn("order_ts", F.lit(f"{BUSINESS_DATE} 23:58:00").cast("timestamp"))
               .withColumn("status", F.lit("completed")))

corrections = refunds.unionByName(late_orders).withColumn("_updated_at", F.current_timestamp())
corrections.createOrReplaceTempView("order_corrections")

merge_result = spark.sql("""
  MERGE INTO silver_orders AS t
  USING order_corrections AS s
  ON t.order_id = s.order_id
  WHEN MATCHED THEN UPDATE SET t.status = s.status, t._updated_at = s._updated_at
  WHEN NOT MATCHED THEN INSERT *
""")
display(merge_result)

# COMMAND ----------

# DBTITLE 1,13:00 Gold
# MAGIC %md
# MAGIC ## 13:00 - Gold: tables the business asks for
# MAGIC The sales team wants to know **revenue by category** and **which cities buy the most**. Gold tables are small, pre-aggregated, and ready for dashboards. Revenue only counts `completed` orders.

# COMMAND ----------

# DBTITLE 1,Gold: sales by category
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE databrick_course_ws.de_day_demo.gold_daily_sales_by_category AS
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   category,
# MAGIC   COUNT(*)                 AS orders,
# MAGIC   SUM(quantity)            AS units_sold,
# MAGIC   ROUND(SUM(amount), 2)    AS revenue,
# MAGIC   ROUND(AVG(amount), 2)    AS avg_order_value
# MAGIC FROM databrick_course_ws.de_day_demo.silver_orders
# MAGIC WHERE status = 'completed'
# MAGIC GROUP BY ALL;
# MAGIC
# MAGIC SELECT * FROM databrick_course_ws.de_day_demo.gold_daily_sales_by_category
# MAGIC ORDER BY revenue DESC;

# COMMAND ----------

# DBTITLE 1,Gold: sales by city
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE databrick_course_ws.de_day_demo.gold_sales_by_city AS
# MAGIC SELECT
# MAGIC   c.city,
# MAGIC   COUNT(DISTINCT o.customer_id) AS active_customers,
# MAGIC   COUNT(*)                      AS orders,
# MAGIC   ROUND(SUM(o.amount), 2)       AS revenue,
# MAGIC   ROUND(SUM(o.amount) / COUNT(DISTINCT o.customer_id), 2) AS revenue_per_customer
# MAGIC FROM databrick_course_ws.de_day_demo.silver_orders o
# MAGIC JOIN databrick_course_ws.de_day_demo.silver_customers c USING (customer_id)
# MAGIC WHERE o.status = 'completed'
# MAGIC GROUP BY c.city;
# MAGIC
# MAGIC SELECT * FROM databrick_course_ws.de_day_demo.gold_sales_by_city
# MAGIC ORDER BY revenue DESC;

# COMMAND ----------

# DBTITLE 1,14:00 Audit
# MAGIC %md
# MAGIC ## 14:00 - Audit with Delta time travel
# MAGIC A manager asks: *"Why did cancelled orders go up today?"* Every Delta table keeps a **history** of its changes. Ana can query the table **as it was before the MERGE** and compare it with the current version.

# COMMAND ----------

# DBTITLE 1,Table history
# MAGIC %sql
# MAGIC DESCRIBE HISTORY databrick_course_ws.de_day_demo.silver_orders

# COMMAND ----------

# DBTITLE 1,Before vs after the MERGE
history = spark.sql("DESCRIBE HISTORY silver_orders")
merge_version = history.filter("operation = 'MERGE'").agg(F.max("version")).first()[0]

def summarize(df, label):
    return df.agg(
        F.lit(label).alias("snapshot"),
        F.count("*").alias("total_orders"),
        F.count_if(F.col("status") == "cancelled").alias("cancelled_orders"),
        F.count_if(F.col("order_id").startswith("LATE-")).alias("late_orders"),
    )

before = spark.read.option("versionAsOf", merge_version - 1).table("silver_orders")
after = spark.read.option("versionAsOf", merge_version).table("silver_orders")
display(summarize(before, f"before MERGE (v{merge_version - 1})")
        .unionByName(summarize(after, f"after MERGE (v{merge_version})")))

# COMMAND ----------

# DBTITLE 1,15:00 Optimize and document
# MAGIC %md
# MAGIC ## 15:00 - Optimize and document
# MAGIC Many small writes and MERGEs leave behind lots of small files. `OPTIMIZE` compacts them so queries run faster. Ana also adds **comments** so analysts can find and understand the tables in Catalog Explorer.

# COMMAND ----------

# DBTITLE 1,Optimize and add comments
# MAGIC %sql
# MAGIC OPTIMIZE databrick_course_ws.de_day_demo.silver_orders;
# MAGIC
# MAGIC COMMENT ON TABLE databrick_course_ws.de_day_demo.silver_orders IS
# MAGIC   'Clean, deduplicated FreshMart orders enriched with product info. One row per order_id. Owner: Ana (data engineering).';
# MAGIC COMMENT ON TABLE databrick_course_ws.de_day_demo.gold_daily_sales_by_category IS
# MAGIC   'Daily revenue of completed orders per product category. Source: silver_orders.';
# MAGIC COMMENT ON TABLE databrick_course_ws.de_day_demo.gold_sales_by_city IS
# MAGIC   'Revenue and active customers per city (completed orders only). Source: silver_orders + silver_customers.';
# MAGIC
# MAGIC SELECT table_name, comment
# MAGIC FROM databrick_course_ws.information_schema.tables
# MAGIC WHERE table_schema = 'de_day_demo'
# MAGIC ORDER BY table_name;

# COMMAND ----------

# DBTITLE 1,16:00 Wrap-up
# MAGIC %md
# MAGIC ## 16:00 - Wrap-up and automation
# MAGIC Nobody wants to run this by hand every morning. Ana's next step is to **schedule this notebook as a Lakeflow Job** that runs every day at 06:00, so the data is ready before the analysts arrive. She also plans to:
# MAGIC * get an alert if the job fails or if the quarantine count jumps
# MAGIC * switch Bronze to Auto Loader so only new files are processed
# MAGIC * build a dashboard on top of the Gold tables
# MAGIC
# MAGIC **What Ana did today**
# MAGIC
# MAGIC | Layer | Table | Purpose |
# MAGIC | --- | --- | --- |
# MAGIC | Landing | `/Volumes/.../raw_landing` | Raw files exactly as the app sent them |
# MAGIC | Bronze | `bronze_orders` | Raw copy plus audit columns, never modified |
# MAGIC | Silver | `silver_orders`, `silver_customers`, `silver_products` | Typed, deduplicated, validated, enriched |
# MAGIC | Silver | `quarantine_orders` | Rejected rows with the reason |
# MAGIC | Gold | `gold_daily_sales_by_category`, `gold_sales_by_city` | Business-ready aggregates |
