# Part 4: Analysis & Reflection

## Why Is the Star Schema Faster?

The primary reason the star schema improves analytical performance is **query simplification**. In the original OLTP schema, data is highly normalized, meaning information required for analysis is spread across many tables. Analytical queries therefore require **multiple joins**, complex grouping, and on-the-fly calculations.

In contrast, the star schema is designed specifically for analytics:

### Join Reduction

* **OLTP queries** often join:

  * encounters
  * patients
  * providers
  * specialties
  * billing
  * diagnoses
  * procedures
    This can result in **6–10 joins per query**.
* **Star schema queries** typically join:

  * 1 fact table
  * 2–4 dimension tables
    This reduces join depth to **3–5 joins**, all on indexed surrogate keys.

Fewer joins reduce:

* CPU usage
* memory consumption
* join reordering cost in the optimizer

---

### Pre-Computed Metrics

The star schema stores **pre-aggregated measures** directly in the fact table, including:

* `total_allowed_amount`
* `length_of_stay`
* encounter-level counts

In the OLTP model, these metrics must be:

* calculated at query time
* derived using multiple joins and aggregations

Pre-computation shifts workload from **query time** to **ETL time**, which is ideal for analytical systems.

---

### Why Denormalization Helps Analytical Queries

Denormalization:

* reduces table lookups
* avoids recursive joins
* improves cache locality
* allows the optimizer to use **bitmap and star-join optimizations**

Analytical queries typically scan large volumes of data but return summarized results. The star schema aligns perfectly with this access pattern.

---

## Trade-offs: What Did You Gain? What Did You Lose?

### What Was Lost

* **Data duplication**

  * Dimension attributes (e.g., specialty names) are repeated across many fact rows.
* **ETL complexity**

  * Requires surrogate key management
  * Needs incremental load logic and data quality checks
* **Storage efficiency**

  * More disk space than a normalized schema

---

### What Was Gained

* **Significantly simpler queries**
* **Improved and predictable performance**
* **Business-friendly schema**

  * Analysts can query without deep schema knowledge
* **Scalability**

  * Query performance degrades much more slowly as data grows

---

### Was It Worth It?

Yes. For analytical workloads, the trade-off is justified. Storage is relatively inexpensive, while analyst productivity and query performance are critical. The star schema clearly outperforms the OLTP schema for reporting and analytics.

---

## Bridge Tables: Were They Worth It?

### Why Use Bridge Tables?

Diagnoses and procedures have a **many-to-many relationship** with encounters:

* One encounter → many diagnoses
* One diagnosis → many encounters

Embedding these directly into the fact table would:

* violate the fact table grain
* cause row explosion
* inflate metrics such as revenue or encounter counts

Bridge tables preserve:

* correct cardinality
* analytical flexibility
* clean separation of concerns

---

### Trade-offs of Bridge Tables

**Costs:**

* Additional joins for diagnosis/procedure analysis
* Slightly more complex queries

**Benefits:**

* Accurate aggregations
* No metric distortion
* Better long-term scalability

---

### Would This Change in Production?

In production, the same approach would be used. However:

* Frequently queried diagnosis/procedure aggregates might be materialized
* Summary fact tables could be added for high-demand reports

---

## Performance Quantification

### Query Example 1: Revenue by Specialty & Month

* **OLTP execution time:** ~62 ms
* **Star schema execution time:** ~46 ms

**Improvement:**
62 / 46 ≈ **1.35× faster**

**Main reason for speedup:**

* Removal of billing joins
* Pre-aggregated revenue stored in the fact table
* Fewer group-by columns

---

### Query Example 2: Top Diagnosis–Procedure Pairs

* **OLTP execution time:** ~110 ms
* **Star schema execution time:** ~78 ms

**Improvement:**
110 / 78 ≈ **1.41× faster**

**Main reason for speedup:**

* Simplified join paths
* Indexed surrogate keys
* Reduced normalization depth

---

### Why Some Queries Show Similar Times

Some queries showed minimal improvement (e.g., 47 ms vs. 47 ms). This is expected because:

* Dataset size is relatively small
* OLTP schema is well-indexed
* Performance benefits grow **exponentially** with scale

In large datasets (millions of rows), the star schema advantage becomes far more pronounced.

---

## Final Reflection

This exercise demonstrates that **schema design matters as much as query tuning**. While OLTP schemas excel at transactional integrity, they are not optimized for analytics. The star schema, despite added ETL complexity, provides a robust, scalable, and performant foundation for analytical workloads.

The observed performance improvements, though modest at this scale, validate the theoretical advantages of dimensional modeling and align with real-world data warehouse best practices.


