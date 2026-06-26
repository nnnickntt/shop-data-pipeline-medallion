# 🚀 shop-data-pipeline-medallion

A Databricks-based PySpark pipeline executing an evolutionary data cleansing framework (Bronze to Silver) across 3 engineering paradigms: Standard, Dynamic, and OOP Framework. This project demonstrates enterprise-grade ingestion and a rigorous Quality Audit flow that quarantines corrupted data using high-performance Spark operations.

### 🛠️ Core Engineering & Optimization Highlights:
* **Engineering Evolution:** Demonstrates software maturity by moving from procedural scripts (`Standard`), to configuration decoupling (`Dynamic`), up to a reusable metadata-driven `@dataclass` architecture (`OOP Framework`).
* **Multi-Stage Quality Gates:** Rigorously evaluates raw data to isolate **Invalid formats** (Regex parsing), **Null keys**, and **Dual-Tier Duplicates** (Row-level vs. Key-level using Spark Window `row_number()`).
* **Unpivot Error:** Leverages a custom `melt()` and `collect_list()` strategy, merging multiple validation failure flags into a single, comprehensive `reason` array for full auditability.
* **Left Anti Join Isolation:** Implements a strict data-splitting strategy that quarantines all corrupted rows into a dedicated `bad_record` table, while casting records into the analytics-ready `silver` table.

---

## 📂 Repository Structure

* **📄 shop_mock.csv** - Raw shop telemetry data used for pipeline testing.
* **📄 ddl.py** - Script to provision the Databricks Catalog, Schema, and Volume for raw file landing.
* **📄 lib.py** - Centralized utility library housing the global `melt` function, dynamic reason parsing, and shared configurations.
* **🥉 [bronze](./bronze)** - **Data Ingestion Pipeline** | Demonstrates the transition from hardcoded scripts to a dynamic `@dataclass` framework with automated file metadata extraction (`_metadata`).
* **🥈 [silver](./silver)** - **Data Cleansing & Quality Audit Pipeline** | Implements multi-stage validation checking (Integrity, Format Regex, and Dual-Tier Deduplication) with an error quarantine mechanism.
