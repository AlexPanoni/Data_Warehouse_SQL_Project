# Welcome to the Data Warehouse Project 👋

This repository contains the development of an end-to-end **Data Warehouse using SQL Server**, following the **Medallion Architecture (Bronze → Silver → Gold)**.  
The main goal of this project is to build a structured, scalable, and analytics-ready data environment, covering data ingestion, transformation, modeling, and documentation.

This project is part of my Data Analytics portfolio and will continue to evolve as new components are added.

---

## 📌 Project Overview

The Data Warehouse is being designed using:

- **SQL Server** as the main database engine  
- **Medallion Architecture** for data organization  
- **Bronze Layer** for raw, traceable data  
- **Silver Layer** for cleaned and standardized data  
- **Gold Layer** for business-ready data (star schema, aggregated views)

---

## 🚧 Progress So Far

### **1. Project Planning**
- Defined project scope and objectives  
- Selected SQL Server as the DWH platform  

### **2. Architecture Design**
- Designed the Medallion Architecture (Bronze, Silver, Gold)
- Created the full **Data Architecture diagram** using Draw.io  
- Defined responsibilities of each layer and types of transformations

### **3. Data Layer Specifications**
- **Bronze:** Raw data, full load, tables only  
- **Silver:** Cleaned & standardized data, full load, tables only  
- **Gold:** Business-ready layer, star schema, aggregated objects, views  

### **4. Naming Conventions**
- Using `snake_case` and English-only names  
- Bronze & Silver: `<source>_<entity>`  
- Gold: `<category>_<entity>` (e.g., `dim_customers`)  
- Primary keys use the `_id` suffix  
- Technical fields use the `dwh_` prefix  
- Stored procedures follow `load_<layer>` pattern  

### **5. Repository Setup**
- GitHub repository created  
- Initial README structure added (this file)

---

## 🗂️ Next Steps (To Be Added Later)
- Building staging tables and initial scripts  
- Implementing ETL procedures  
- Modeling fact and dimension tables  
- Creating business logic for the Gold layer  
- Adding documentation and SQL scripts  
- Preparing final reporting outputs  

---

## 💬 Feedback & Contributions
This project is part of my ongoing learning and portfolio development.  
Feel free to open issues, suggest improvements, or share feedback.

Thanks for stopping by! 🚀
