# Optimizing a Fabric Data Agent

A practical, end-to-end checklist for getting a Microsoft Fabric Data Agent to answer business questions accurately and consistently. The order matters — most failure modes come from skipping the data-prep steps and trying to fix accuracy with prompt engineering alone.

> **The single biggest lever:** the simpler the *shape* of your data, the more accurate the agent. A single denormalized table or view will outperform a normalized star schema *every time* for a Data Agent, because the agent never has to guess a join.

---

## 1. Why Data Agents struggle (and what they're good at)

| Hard for the agent | Easy for the agent |
|---|---|
| Multi-table joins, especially composite-key joins | Single-table SELECTs with WHERE / GROUP BY |
| Picking the right grain (sales line vs. order vs. customer) | Aggregations over a flat fact |
| Knowing which of two same-named columns to use (`agent.Country` vs `Customer_Country`) | Distinct, descriptively-named columns |
| Inferring intent from vague questions | Questions phrased like the few-shot examples |
| Time intelligence without a date dimension | `dim_date.Year`, `dim_date.Month` columns |

**Implication:** invest 80% of your effort in data shape, 20% in prompts.

---

## 2. Data prep checklist

Run these before connecting an agent.


### 2.1 Dedup primary keys

Every table that should be unique on a key (orders, locations, agents, etc.) must actually be unique. Source data rarely is.

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def dedup(df, key_col, order_col):
    w = Window.partitionBy(key_col).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
```

### 2.2 Cast numeric types defensively

`INT` columns overflow on `SUM()` once totals exceed ~2.1 billion. Cast to `BIGINT` (integers) or `DECIMAL(18,4)` (currency) at the source.

```python
from pyspark.sql import types as T
fact = (fact
    .withColumn("Sales",  F.col("Sales").cast(T.LongType()))
    .withColumn("Profit", F.col("Profit").cast(T.LongType()))
    .withColumn("Shipping_Cost", F.col("Shipping_Cost").cast(T.DoubleType())))
```

### 2.3 Real foreign-key integrity

Every FK in the fact must match a row in the dim. If the source data is broken, regenerate keys before joining (don't just LEFT join and accept NULLs everywhere).

### 2.4 Date columns must be real `date`/`timestamp`, not strings

```python
.withColumn("Order_Date", F.to_date("Order_Date"))
```

### 2.5 Build a `dim_date`

```python
dim_date = (spark.sql("""
    SELECT explode(sequence(to_date('2020-01-01'), to_date('2027-12-31'), interval 1 day)) AS Date
""")
    .withColumn("Year",      F.year("Date"))
    .withColumn("Quarter",   F.quarter("Date"))
    .withColumn("Month",     F.month("Date"))
    .withColumn("MonthName", F.date_format("Date", "MMMM"))
    .withColumn("YearMonth", F.date_format("Date", "yyyy-MM"))
    .withColumn("DayOfWeek", F.date_format("Date", "EEEE")))
dim_date.write.mode("overwrite").option("overwriteSchema","true").format("delta").saveAsTable("dim_date")
```

---

## 3. Build a single denormalized fact

Even if you have a clean star schema, build **one wide fact table** (or view) that pre-joins everything. This is the object the Data Agent will query.

```python
fact = (sales.alias("s")
    .join(orders.alias("o"),   "Order_ID")
    .join(location.alias("l"), "Order_ID")
    .join(ac.alias("ac"),
          (F.col("s.Agent_ID")         == F.col("ac.Agent_ID")) &
          (F.col("o.Product_Category") == F.col("ac.Product_Category")),
          "left")
    .select( ... explicit column list ... ))

(fact.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .format("delta")
    .saveAsTable("order_fact"))
```

Rules:
- **Explicit `.select(...)`** with `.alias()` on every column — prevents `[COLUMN_ALREADY_EXISTS]` errors and gives you the exact column names the agent will see.
- **Use `.option("overwriteSchema", "true")`** when the schema changes between runs.
- **Use string-form joins (`"Order_ID"`)** when both sides have the same column name; use boolean form only for composite keys, then `.drop()` the duplicates.

---

## 4. Add a "for the agent" view on top

Create a SQL view that joins the fact to its dimensions and casts numerics. The agent points only at this view.

```sql
CREATE OR ALTER VIEW dbo.vw_order_fact_enriched AS
SELECT
    f.Order_ID,
    f.Customer_ID,

    -- Dates
    f.Order_Date,
    d.Year      AS Order_Year,
    d.Quarter   AS Order_Quarter,
    d.Month     AS Order_Month,
    d.MonthName AS Order_MonthName,
    d.YearMonth AS Order_YearMonth,
    d.DayOfWeek AS Order_DayOfWeek,
    f.Shipping_Date,

    -- Product
    f.Product_Category, f.Product,

    -- Agent
    f.Agent_ID,
    a.Agent_Name,
    a.Country AS Agent_Country, a.State AS Agent_State, a.City AS Agent_City,

    -- Customer location
    f.City    AS Customer_City,
    f.State   AS Customer_State,
    f.Country AS Customer_Country,
    f.Region  AS Customer_Region,
    f.Segment AS Customer_Segment,
    f.Customer_Name,

    -- Measures (overflow-safe types)
    CAST(f.Sales    AS BIGINT)              AS Sales,
    CAST(f.Quantity AS BIGINT)              AS Quantity,
    CAST(f.Discount AS BIGINT)              AS Discount,
    CAST(f.Profit   AS BIGINT)              AS Profit,
    CAST(f.Shipping_Cost          AS DECIMAL(18,4)) AS Shipping_Cost,
    CAST(f.Commission_Percentage  AS DECIMAL(18,4)) AS Commission_Percentage,
    CAST(f.Commission_Amount      AS DECIMAL(18,4)) AS Commission_Amount
FROM dbo.order_fact f
JOIN dbo.agent    a ON a.Agent_ID = f.Agent_ID
JOIN dbo.dim_date d ON d.Date     = f.Order_Date;
GO
```

**Naming conventions matter.** The agent reads column names as documentation:
- Disambiguate same-named columns: `Agent_Country` vs `Customer_Country`.
- Pre-derive grouping columns: `Order_Year` is more obvious than `YEAR(Order_Date)`.
- Keep currency/measure columns at the bottom so the agent doesn't accidentally `SUM` an ID.

---

## 5. Semantic model setup (only if you'll also use Power BI / DAX)

If you're attaching the agent to a **semantic model** instead of a SQL endpoint, build the model as a clean star.

### Tables to include

Just three:

```
dim_date  ──1:N──►  order_fact  ◄──N:1──  agent
```

Do **not** add the source tables (`orders_clean`, `sales_clean`, `location_clean`, `agent_commissions`) — they're already merged into `order_fact` and will only confuse the agent.

### Relationships

| From (many) | To (one) | Cardinality | Cross-filter |
|---|---|---|---|
| `order_fact[Order_Date]` | `dim_date[Date]` | N : 1 | Single |
| `order_fact[Agent_ID]` | `agent[Agent_ID]` | N : 1 | Single |

- Active: ✅
- Assume referential integrity: ✅ (safe because we built the fact ourselves)
- **Never use bidirectional cross-filtering** on a star schema — causes ambiguity and slows DAX.

### Other semantic-model housekeeping

1. **Mark `dim_date` as date table** (Modeling pane → Mark as date table → `Date`).
2. **Hide keys** from report view: `Order_ID`, `Agent_ID`, `Date`.
3. **Set `Summarize by = None`** on all ID columns (`Order_ID`, `Agent_ID`, `Customer_ID`) so they don't auto-SUM.
4. **Composite relationships are not supported.** If you need one (e.g., `Agent_ID + Product_Category`), materialize a concatenated key column **upstream in the lakehouse**, not as a calculated column (Direct Lake doesn't support calculated columns).

---

## 6. Configure the Data Agent

### 6.1 Restrict the data sources

This is the single most important agent setting. **Select only the one view/table the agent should use; deselect everything else.** If the agent can see the raw tables, it will use them.

For our example: select only `vw_order_fact_enriched`. Deselect `order_fact`, `agent`, `dim_date`, and all `_clean` tables.

### 6.2 Write explicit AI instructions

A solid template:

```
ROLE
You answer business questions about sales, agents, products, and customers
by writing T-SQL queries against the Fabric Lakehouse SQL endpoint.

DATA SOURCE RULES
- ALWAYS query the view `dbo.vw_order_fact_enriched`.
- NEVER reference any other table directly, even if you remember it.
- The view is pre-joined and contains every column needed.
- Do not write JOINs.

COLUMN GUIDANCE
- Use `Order_Year`, `Order_Quarter`, `Order_Month`, `Order_MonthName`,
  `Order_YearMonth` for time grouping. Do NOT call YEAR()/MONTH()
  on Order_Date.
- `Agent_*` columns describe where the agent is located.
  `Customer_*` columns describe where the order shipped to.
- Currency columns: Sales, Profit, Commission_Amount, Shipping_Cost.
- Discount is stored as an integer (0–100), not a fraction.

QUERY RULES
- Always qualify tables with the dbo schema: `dbo.vw_order_fact_enriched`.
- Use `TOP (N)` with `ORDER BY ... DESC` for "top N" questions.
- Use `NULLIF(denominator, 0)` in any division.
- Never write `SELECT *`. Always list columns.
- Filter values are case-sensitive — use the exact spelling from the data
  ('Electronics', 'West', etc.).

RESPONSE STYLE
- Always show the SQL you ran above the results.
- If a question is ambiguous, ask one clarifying question.
- If a query returns zero rows, say so explicitly — don't fabricate data.
```

### 6.3 Add few-shot examples — choose by *shape*, not topic

Aim for 8–12 examples covering the common query shapes. Pasting 50 examples that all do the same thing wastes prompt budget.

| Shape | Example question |
|---|---|
| Single value | "What were our total sales last year?" |
| Simple GROUP BY | "Show me sales by year." |
| TOP N + ORDER BY | "Who are the top 10 agents by sales?" |
| Computed ratio with NULLIF | "What's the profit margin by product category?" |
| Two-dimensional GROUP BY | "Show me sales by region across years." |
| Self-join / window function | "What's the year-over-year sales change?" |
| `COUNT(DISTINCT ...)` | "How many unique orders and customers do we have?" |
| `CASE` in GROUP BY | "Avg discount on profitable vs unprofitable orders?" |
| Window with PARTITION BY | "Top product category in each region?" |
| Filter + TOP 1 | "Top-selling agent in the West region?" |

Every few-shot must use the view (`vw_order_fact_enriched`) — the agent learns from the examples and will copy the table name you wrote.

---

## 7. Common gotchas and fixes

| Symptom | Cause | Fix |
|---|---|---|
| `Arithmetic overflow error` on `SUM()` | Source column is `INT`; sum exceeds 2.1B | Cast to `BIGINT`/`DECIMAL` in the view (preferred) or in the fact table |
| Agent ignores the view, uses raw tables | Raw tables are still selected as data sources | Deselect every table except the view |
| Agent picks wrong column when two have similar names | Ambiguous naming | Disambiguate: `Agent_Country` vs `Customer_Country` |
| Agent writes `JOIN` even though view is pre-joined | Few-shot examples or instructions reference base tables | Update few-shots to use only the view; add explicit "no JOINs" rule |
| Time intelligence questions are wrong | No date dim, or relationship missing | Build `dim_date`, mark as date table, relate on date column |
| Filtered query returns 0 rows ("No Electronics in 2024") | Case-sensitive collation; data has `electronics` lowercase | Standardize casing in the lakehouse, or use `WHERE LOWER(col) = 'electronics'` |
| Schema mismatch on Delta `saveAsTable` | New columns added after first write | Use `.option("overwriteSchema", "true")` |
| Relationship 1:1 with bidirectional filter breaks | Duplicates appeared in the "1" side | Dedup, or change cardinality, or remove bidirectional |
| Calculated column not allowed in Direct Lake | Direct Lake disallows calc columns | Materialize the column upstream in lakehouse / SQL view |

---

## 8. Validation queries to run after every data refresh

Run these in a scratch SQL cell or notebook to catch silent regressions before they confuse the agent:

```sql
-- Fact has rows
SELECT COUNT(*) AS row_count FROM dbo.vw_order_fact_enriched;

-- No orphan agents
SELECT COUNT(*) AS missing_agent
FROM dbo.vw_order_fact_enriched
WHERE Agent_Name IS NULL;

-- No orphan dates
SELECT COUNT(*) AS missing_date_attrs
FROM dbo.vw_order_fact_enriched
WHERE Order_Year IS NULL;

-- Currency totals are sane (not zero, not absurd)
SELECT SUM(Sales) AS total_sales,
       SUM(Profit) AS total_profit,
       SUM(Commission_Amount) AS total_commission
FROM dbo.vw_order_fact_enriched;

-- Distinct counts match expectations
SELECT COUNT(DISTINCT Order_ID)    AS orders,
       COUNT(DISTINCT Customer_ID) AS customers,
       COUNT(DISTINCT Agent_ID)    AS agents
FROM dbo.vw_order_fact_enriched;
```

If any of these change unexpectedly, fix the data before users notice the agent giving wrong answers.

---

## 9. Operating model — how to maintain this

| Cadence | Action |
|---|---|
| Each data refresh | Run validation queries (section 8) |
| Weekly | Spot-check 5 random user questions from agent logs; if a wrong answer is found, add or fix a few-shot |
| When a user asks a new *shape* of question | Add it as a new few-shot if it's likely to repeat |
| When schema changes | Update the view, then update the AI instructions' "COLUMN GUIDANCE" section |
| Quarterly | Audit few-shots — remove any that are no longer accurate or relevant |

---

## 10. TL;DR

1. **One wide fact table** beats a normalized star schema for a Data Agent.
2. **Wrap it in a view** that adds dim attributes, disambiguates column names, and casts numerics to overflow-safe types.
3. **Restrict the agent to that one view** — deselect everything else.
4. **Write 8–12 few-shot examples by shape** (TOP N, ratio, time intel, etc.), all referencing the view.
5. **Spell out routing rules in instructions** ("always use the view, never JOIN").
6. **Validate after every refresh** with sanity queries.
7. **Iterate on few-shots** based on real user questions.

If you do those seven things, the agent will answer ~95% of business questions correctly without you ever touching SQL again.
