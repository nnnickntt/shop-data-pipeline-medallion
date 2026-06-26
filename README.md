# 🚀 shop-data-pipeline-medallion

A Databricks-based PySpark pipeline executing an evolutionary data cleansing framework (Bronze to Silver) across 3 engineering paradigms: Standard, Dynamic, and OOP Framework.

---

## 📂 Repository Structure

* **📄 shop_mock.csv** - Raw shop telemetry data used for pipeline testing.
* **📄 ddl.py** - Script to provision the Databricks Catalog, Schema, and Volume for raw file landing.
* **📄 lib.py** - Centralized utility library housing the global `melt` function, dynamic reason parsing, and shared configurations.
* **🥉 [bronze](./bronze)** - **Data Ingestion Pipeline** | Demonstrates the transition from hardcoded scripts to a dynamic `@dataclass` framework with automated file metadata extraction (`_metadata`).
* **🥈 [silver](./silver)** - **Data Cleansing & Quality Audit Pipeline** | Implements multi-stage validation checking (Integrity, Format Regex, and Dual-Tier Deduplication) with an error quarantine mechanism.
