# 🚀 Dynamic Medallion Data Framework on Databricks

A PySpark data pipeline on Databricks implementing Medallion Architecture. It features 3 development approaches—Manual, Dynamic, and a Metadata-Driven Framework—that automatically isolates Invalid, Null, and Duplicate records using Left Anti Join to ensure high-quality data in Silver and Gold tables.

---

## 📂 Repository Structure & Navigation

คุณสามารถคลิกลิงก์ที่โฟลเดอร์ด้านล่างนี้ เพื่อเข้าไปดูโค้ดและรายละเอียดแบบเจาะลึกของแต่ละเลเยอร์ได้ทันทีครับ:

* **🥉 [bronze](./bronze)** - Data Ingestion Pipeline (Contains 3 approach scripts: Standard, Dynamic, and OOP Framework)
* ⏳ *silver (Coming Soon)* - Data Cleansing & Quality Audit Framework using Left Anti Join
* ⏳ *gold (Coming Soon)* - Data Aggregation & Business Layer
