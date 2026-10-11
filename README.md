# Azure Databricks End-to-End Data Engineering Project

End-to-end data engineering pipeline built on **Azure Databricks** using the **Medallion Architecture** (Bronze → Silver → Gold).

This project demonstrates incremental data ingestion with Autoloader, data cleaning and transformation with PySpark, dimensional modeling with Slowly Changing Dimensions (SCD Type 2), and orchestration using Delta Live Tables (Lakeflow Declarative Pipelines) and Databricks Jobs.

---

## Architecture

### High-Level Architecture

![High-Level Architecture](architecture/high-level-architecture.jpg)

### Databricks Workflow Graph

![Databricks Workflow Graph](architecture/databricks-workflow-graph.png)

### DLT / Lakeflow Products Pipeline

![DLT Products Pipeline](architecture/dlt-products-pipeline.png)

---

## Tech Stack

- **Platform**: Azure Databricks
- **Processing**: Apache Spark / PySpark
- **Storage Format**: Delta Lake
- **Ingestion**: Databricks Autoloader
- **Orchestration**: Delta Live Tables (Lakeflow) + Databricks Jobs / Workflows
- **Governance**: Unity Catalog
- **Compute**: Serverless (used for DLT pipelines)
- **Language**: Python

---

## Project Structure

```text
azure-databricks-end-to-end-project/
├── architecture/
│   ├── high-level-architecture.jpg
│   ├── databricks-workflow-graph.png
│   └── dlt-products-pipeline.png
├── datasets/                  # Sample parquet files
├── notebooks/
│   ├── Bronze/                # Autoloader ingestion (raw layer)
│   ├── Silver/                # Cleaning, standardization & transformations
│   └── Gold/                  # Dimensional modeling + SCD Type 2
├── README.md
└── .gitignore
```

---

## Key Features

- Medallion Architecture (Bronze → Silver → Gold)
- Incremental ingestion using Databricks Autoloader
- Data cleaning, deduplication and standardization in Silver layer
- Dimensional modeling (Star Schema)
- Slowly Changing Dimension Type 2 implementation
- Orchestration with Delta Live Tables / Lakeflow Declarative Pipelines
- Scheduling and workflow management with Databricks Jobs
- Practical solution for Azure Free Trial compute limitations using Serverless

---

## Challenges & Solutions

### Challenge: Azure Free Trial Core Limitation
The Azure Free Trial only provides 4 cores, which is insufficient to run classic clusters for Delta Live Tables pipelines.

**Solution**:  
Switched to **Databricks Serverless compute** for the DLT / Lakeflow pipelines. This allowed the pipelines to run successfully without needing higher core quotas.

### Challenge: Reliable Incremental Ingestion
Need to ingest only new files while avoiding duplicates and reprocessing.

**Solution**:  
Used **Databricks Autoloader** (`cloudFiles`) for efficient, incremental, and scalable file ingestion into the Bronze layer.

---

## How to Run

1. Clone this repository
2. Import the notebooks into your Azure Databricks workspace
3. Upload the sample datasets from the `datasets/` folder (or point to your own data)
4. Update any storage paths / catalog / schema names according to your environment
5. Run the notebooks in order: **Bronze → Silver → Gold**
6. (Optional) Create a Databricks Job or DLT pipeline to orchestrate the full flow
7. Prefer **Serverless compute** if you are on Azure Free Trial or have limited quotas

> Note: This is a portfolio/learning project. Some paths and configurations may need adjustment depending on your Databricks workspace.

---

## Key Learnings

- Designing and implementing a full Medallion Architecture on Databricks
- Building incremental pipelines with Autoloader
- Writing clean and maintainable PySpark transformations
- Implementing SCD Type 2 correctly
- Using Delta Live Tables / Lakeflow Declarative Pipelines
- Working around real-world cloud resource limitations (Serverless)
- Structuring a data engineering project for clarity and reproducibility

---

## Author

**Abdenour LOUAHABI**  
Master’s Student | Aspiring Data Engineer  

- GitHub: [louahabi79](https://github.com/louahabi79)  
- LinkedIn: [Abdenour Louahabi](https://www.linkedin.com/in/abdenour-louahabi/)  
- Email: abdenourlouahabi23@gmail.com

---

⭐ If you found this project useful, feel free to star the repository!
