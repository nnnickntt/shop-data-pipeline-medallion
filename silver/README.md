## 🥈 Silver Layer: Data Cleansing & Validation Pipeline

The Silver layer acts as the **Data Quality Gatekeeper** of the Medallion Architecture. It consumes raw data from the Bronze layer, applies rigorous schema validation, constraints checking, and deduplication rules, and splits the records into "Good" data (persisted for analytics) and "Bad" data (quarantined for auditing).

---

## 🚀 Key Highlights

* **Comprehensive Quality Rules:** Implements a multi-stage validation framework verifying three critical data dimensions:
  * **Data Integrity (Null Check):** Detects missing values in primary key columns (`shop_id`).
  * **Format Validation (Regex Match):** Validates strict data types (e.g., matching Integers via `^[0-9]+$` and Dates via `\d{4}-\d{2}-\d{2}$`).
  * **Dual-Tier Deduplication:** Separates and tracks both full-row duplicates (`_is_dup_row`) and key-level duplicates (`_is_dup_key`) using Spark Window functions.
* **Unified Error Quarantine (Melt Strategy):** Instead of just dropping corrupted rows, the pipeline leverages a dynamic `melt` and `collect_list` function to pivot validation flags into a single, comprehensive array of errors (`reason`), ensuring transparent traceability for data stewards.
* **3-Tier Code Evolution:** Mirrors the architectural maturity of the Bronze layer by scaling from basic procedural scripts to a highly flexible OOP framework.

---

## 📂 Implementation

Inside the Silver directory, the processing logic is evolved across 3 development methodologies:

### 1. Standard Approach (`Silver.py`)
* **Description:** A declarative, step-by-step notebook where table configurations, schemas, and columns are hardcoded. It uses explicit `.withColumn()` chains and manual unpivoting for each validation state. Ideal for initial data behavior profiling.

### 2. Dynamic Approach (`Silver_Dynamic.py`)
* **Description:** Introduces operational flexibility by storing schemas inside a central dictionary (`schema_detail`) and building dynamic PySpark validation statements programmatically. It abstracts the error-pivoting logic into a global reusable function `get_reason()`.

### 3. Framework-Driven Approach (`Silver_Framework.py`)
* **Description:** A production-grade, enterprise-ready architecture wrapping all data quality stages into a **Modular Object-Oriented (`@dataclass`)** layout.
* **Key Advantage:** Encapsulates specialized validation states into isolated class methods (e.g., `get_invalid_record`, `get_dup_record`). Pipelines for new source systems can be instantiated seamlessly by declaring a new class instance with its schema dictionary and target key inputs—minimizing redundant code down to zero.

---

## 🛠️ Technical Workflow

1. **Surrogate Key Generation:** Ingests the Bronze table and appends a unique internal key (`_sk`) using `monotonically_increasing_id()` to track records throughout the cleansing process.
2. **Parallel Validation Checks:** Extracts bad data into three independent categories: Invalid formats, Null keys, and Duplicate
