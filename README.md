# 🚀 Dynamic Medallion Data Framework on Databricks

A Databricks-based PySpark pipeline executing Medallion Architecture across 3 paradigms: Manual, Dynamic, and Metadata-driven. Features automated data cleansing that isolates Invalid, Null, and Duplicate records via Left Anti Join to guarantee production-ready Silver and Gold layers.

---

## 📂 Repository Structure
* **📄 shop_mock.csv** - Raw data
* **📄 ddl.py** - Script to create Schema and Volume for storing raw files
* **📄 lib.py** - Centralized library for common functions, utilities, and configurations
* **🥉 [bronze](./bronze)** - Data Ingestion Pipeline (Contains 3 approach scripts: Standard, Dynamic, and OOP Framework)
* **🥈 [silver](./silver)** - Data Cleansing & Quality Audit Framework using Left Anti Join (Implements Multi-stage QA, Melt Error Logging, and 3-Tier Code Evolution)
* ⏳ *gold (Coming Soon)* - Data Aggregation & Business Layer
