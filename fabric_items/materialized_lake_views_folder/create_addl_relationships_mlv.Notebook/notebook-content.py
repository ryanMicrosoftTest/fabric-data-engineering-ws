# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "50bc8b32-9a7b-42de-ae03-0a8d79c9f50c",
# META       "default_lakehouse_name": "materialized_lake_views_lakehouse",
# META       "default_lakehouse_workspace_id": "a8cbda3d-903e-4154-97d9-9a91c95abb42",
# META       "known_lakehouses": [
# META         {
# META           "id": "50bc8b32-9a7b-42de-ae03-0a8d79c9f50c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Notebook to add additional relationships and compositite keys for better relationships in semantic models

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

agent_commissions_df = spark.sql("SELECT * FROM materialized_lake_views_lakehouse.dbo.agent_commissions LIMIT 1000")
display(agent_commissions_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a new column for composite key 'agent_id_product_category_key'
# Key Fixes:
# - Use correct column names as shown in the error suggestions: 'Agent_ID' and 'Product_Category'
# - Make sure to use F.lit('_') to represent a literal string in concat
import pyspark.sql.functions as F

agent_commissions_df = agent_commissions_df.withColumn('agent_id_product_category_key', F.concat(F.col('Agent_ID'), F.lit('_'), F.col('Product_Category')))
# The above creates a new composite key of Agent_ID and Product_Category separated by an underscore

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(agent_commissions_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# update data to reflect new column
agent_commissions_df.write.saveAsTable('agent_commissions')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Add Composite Key Column to Sales

# CELL ********************

sales_df = spark.sql("SELECT * FROM materialized_lake_views_lakehouse.dbo.sales LIMIT 1000")
display(sales_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
 
orders   = spark.table("orders")
sales    = spark.table("sales")
location = spark.table("location")
ac       = spark.table("agent_commissions")

fact = (sales.alias("s")
    .join(orders.alias("o"),   "Order_ID")
    .join(location.alias("l"), "Order_ID")
    .join(ac.alias("ac"),
        (F.col("s.Agent_ID")         == F.col("ac.Agent_ID")) &
        (F.col("o.Product_Category") == F.col("ac.Product_Category")),
        "left")
    .select(
        F.col("s.Order_ID").alias("Order_ID"),
        F.col("s.Agent_ID").alias("Agent_ID"),
        F.col("o.Product_Category").alias("Product_Category"),
        F.col("o.Product").alias("Product"),
        F.col("o.Order_Date").alias("Order_Date"),
        F.col("o.Shipping_Date").alias("Shipping_Date"),
        F.col("s.Sales").alias("Sales"),
        F.col("s.Quantity").alias("Quantity"),
        F.col("s.Discount").alias("Discount"),
        F.col("s.Profit").alias("Profit"),
        F.col("s.Shipping_Cost").alias("Shipping_Cost"),
        F.col("s.Customer_ID").alias("Customer_ID"),
        F.col("l.City").alias("City"),
        F.col("l.State").alias("State"),
        F.col("l.Country").alias("Country"),
        F.col("l.Region").alias("Region"),
        F.col("l.Segment").alias("Segment"),
        F.col("l.Customer_Name").alias("Customer_Name"),
        F.col("ac.Commission_Percentage").alias("Commission_Percentage"),
        (F.col("s.Sales") * F.col("ac.Commission_Percentage") / 100.0).alias("Commission_Amount"),
    ))

fact.write.mode("overwrite").format("delta").saveAsTable("order_fact")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Remove Duplicate Order_ID 

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def dedup(df, key_col, order_col):
    w = Window.partitionBy(key_col).orderBy(F.col(order_col).desc_nulls_last())
    return (df.withColumn("_rn", F.row_number().over(w))
            .filter("_rn = 1")
            .drop("_rn"))

orders_clean   = dedup(spark.table("orders"),   "Order_ID", "Order_Date")
sales_clean    = dedup(spark.table("sales"),    "Order_ID", "Order_ID")   # no date in sales
location_clean = dedup(spark.table("location"), "Order_ID", "Order_ID")

orders_clean.write.mode("overwrite").format("delta").saveAsTable("orders_clean")
sales_clean.write.mode("overwrite").format("delta").saveAsTable("sales_clean")
location_clean.write.mode("overwrite").format("delta").saveAsTable("location_clean")

print("orders:",   spark.table("orders").count(),   "->", spark.table("orders_clean").count())
print("sales:",    spark.table("sales").count(),    "->", spark.table("sales_clean").count())
print("location:", spark.table("location").count(), "->", spark.table("location_clean").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Cast Date Columns

# CELL ********************

orders_clean = (spark.table("orders_clean")
    .withColumn("Order_Date",    F.to_date("Order_Date"))
    .withColumn("Shipping_Date", F.to_date("Shipping_Date")))
orders_clean.write.mode("overwrite").format("delta").saveAsTable("orders_clean")
spark.table("orders_clean").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Rebuild order_fact from the cleaned tables

# CELL ********************

orders   = spark.table("orders_clean")
sales    = spark.table("sales_clean")
location = spark.table("location_clean")
ac       = spark.table("agent_commissions")

fact = (sales.alias("s")
    .join(orders.alias("o"),   "Order_ID")
    .join(location.alias("l"), "Order_ID")
    .join(ac.alias("ac"),
        (F.col("s.Agent_ID")         == F.col("ac.Agent_ID")) &
        (F.col("o.Product_Category") == F.col("ac.Product_Category")),
        "left")
    .select(
        F.col("s.Order_ID").alias("Order_ID"),
        F.col("s.Agent_ID").alias("Agent_ID"),
        F.col("o.Product_Category").alias("Product_Category"),
        F.col("o.Product").alias("Product"),
        F.col("o.Order_Date").alias("Order_Date"),
        F.col("o.Shipping_Date").alias("Shipping_Date"),
        F.col("s.Sales").alias("Sales"),
        F.col("s.Quantity").alias("Quantity"),
        F.col("s.Discount").alias("Discount"),
        F.col("s.Profit").alias("Profit"),
        F.col("s.Shipping_Cost").alias("Shipping_Cost"),
        F.col("s.Customer_ID").alias("Customer_ID"),
        F.col("l.City").alias("City"),
        F.col("l.State").alias("State"),
        F.col("l.Country").alias("Country"),
        F.col("l.Region").alias("Region"),
        F.col("l.Segment").alias("Segment"),
        F.col("l.Customer_Name").alias("Customer_Name"),
        F.col("ac.Commission_Percentage").alias("Commission_Percentage"),
        (F.col("s.Sales") * F.col("ac.Commission_Percentage") / 100.0).alias("Commission_Amount"),
        F.concat(F.col("s.Agent_ID"), F.lit("_"), 
F.col("o.Product_Category")).alias("agent_id_product_category_key"),
    ))

(fact.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .format("delta")
    .saveAsTable("order_fact"))

print("order_fact rows:", spark.table("order_fact").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Build Date Dimension

# CELL ********************

dim_date = (spark.sql("""
    SELECT explode(sequence(to_date('2020-01-01'), to_date('2027-12-31'), interval 1 day)) AS Date
""")
    .withColumn("Year",      F.year("Date"))
    .withColumn("Quarter",   F.quarter("Date"))
    .withColumn("Month",     F.month("Date"))
    .withColumn("MonthName", F.date_format("Date", "MMMM"))
    .withColumn("YearMonth", F.date_format("Date", "yyyy-MM"))
    .withColumn("DayOfWeek", F.date_format("Date", "EEEE")))

(dim_date.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .format("delta")
    .saveAsTable("dim_date"))

spark.table("dim_date").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Validate

# CELL ********************

of    = spark.table("order_fact")
agent = spark.table("agent")

print("order_fact rows           :", of.count())
print("distinct Order_ID         :", of.select("Order_ID").distinct().count())
print("distinct Agent_ID in fact :", of.select("Agent_ID").distinct().count())
print("agent rows                :", agent.count())
print("rows missing commission   :", of.filter("Commission_Percentage IS NULL").count())
print("fact rows w/ no agent     :", of.join(agent, "Agent_ID", "left_anti").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

o = spark.table("orders_clean")
s = spark.table("sales_clean")
l = spark.table("location_clean")

print("orders_clean rows   :", o.count(), " distinct Order_ID:", o.select("Order_ID").distinct().count())
print("sales_clean rows    :", s.count(), " distinct Order_ID:", s.select("Order_ID").distinct().count())
print("location_clean rows :", l.count(), " distinct Order_ID:", l.select("Order_ID").distinct().count())

print("orders ∩ sales on Order_ID :", o.select("Order_ID").intersect(s.select("Order_ID")).count())
print("orders ∩ location          :", o.select("Order_ID").intersect(l.select("Order_ID")).count())

print("\nSample orders.Order_ID:");   o.select("Order_ID").show(5, False)
print("Sample sales.Order_ID:");      s.select("Order_ID").show(5, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Rebuild FKs so the joins actually work

# CELL ********************

import pyspark.sql.functions as F
 
orders = spark.table("orders_clean")
sales  = spark.table("sales_clean")
loc    = spark.table("location_clean")
agent  = spark.table("agent")
ac     = spark.table("agent_commissions")

# Real key universes
order_ids        = [r.Order_ID for r in orders.select("Order_ID").distinct().collect()]
agent_ids        = [r.Agent_ID for r in agent.select("Agent_ID").distinct().collect()]
product_cats     = [r.Product_Category for r in orders.select("Product_Category").distinct().collect()]

print("orders:", len(order_ids), " agents:", len(agent_ids), " categories:", product_cats)

# Stable per-row pseudo-random pick using xxhash64 -> index into the list
def pick(arr, salt):
    arr_lit = F.array(*[F.lit(x) for x in arr])
    idx = (F.abs(F.xxhash64(F.col("Order_ID"), F.lit(salt))) % F.lit(len(arr))).cast("int")
    return arr_lit[idx]

# Reassign sales.Order_ID -> a real orders.Order_ID, and sales.Agent_ID -> a real agent
sales_fixed = (sales
    .withColumn("Order_ID", pick(order_ids, "ord"))
    .withColumn("Agent_ID", pick(agent_ids, "agt")))

loc_fixed = loc.withColumn("Order_ID", pick(order_ids, "ord"))   # same salt -> same Order_IDs as sales

# Rebuild agent_commissions as the full cross-product of agents x categories
ac_fixed = (spark.createDataFrame([(a,) for a in agent_ids], ["Agent_ID"])
            .crossJoin(spark.createDataFrame([(c,) for c in product_cats], ["Product_Category"]))
            .withColumn("Commission_Percentage",
                        (F.abs(F.xxhash64(F.col("Agent_ID"), F.col("Product_Category"))) % F.lit(15) + 
F.lit(1)).cast("double"))
            .withColumn("agent_id_product_category_key",
                        F.concat(F.col("Agent_ID"), F.lit("_"), F.col("Product_Category"))))

# Persist
(sales_fixed.write.mode("overwrite").option("overwriteSchema","true").format("delta").saveAsTable("sales_clean"))
(loc_fixed.write.mode("overwrite").option("overwriteSchema","true").format("delta").saveAsTable("location_clean"))
(ac_fixed.write.mode("overwrite").option("overwriteSchema","true").format("delta").saveAsTable("agent_commissions"))

print("sales_clean rows    :", spark.table("sales_clean").count())
print("location_clean rows :", spark.table("location_clean").count())
print("agent_commissions   :", spark.table("agent_commissions").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Re-Verify

# CELL ********************

o = spark.table("orders_clean")
s = spark.table("sales_clean")
l = spark.table("location_clean")
a = spark.table("agent")

print("orders ∩ sales    :", o.select("Order_ID").intersect(s.select("Order_ID")).count())
print("orders ∩ location :", o.select("Order_ID").intersect(l.select("Order_ID")).count())
print("agent  ∩ sales    :", a.select("Agent_ID").intersect(s.select("Agent_ID")).count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # De-Dupe the Agent Dim

# CELL ********************

from pyspark.sql.window import Window
 
ag = spark.table("agent")
print("before:", ag.count(), " distinct Agent_ID:", ag.select("Agent_ID").distinct().count())

w = Window.partitionBy("Agent_ID").orderBy(F.col("Agent_Name").asc_nulls_last())
agent_dedup = (ag.withColumn("_rn", F.row_number().over(w))
                .filter("_rn = 1")
                .drop("_rn"))

(agent_dedup.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .format("delta")
    .saveAsTable("agent"))

print("after :", spark.table("agent").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
