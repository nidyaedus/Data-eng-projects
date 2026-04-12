# E-Commerce Data Pipeline

This project is more than just a coding exercise; it is a learning laboratory where I have documented, step-by-step, how I transformed complex and "dirty" e-commerce data into a structured system according to modern data engineering standards (**Medallion Architecture & Star Schema**).

## What I Learned in This Project?

While building this pipeline, I focused on mastering "business logic" rather than just writing code. Here are the key challenges I encountered and my engineering solutions:

* **Preserving Data Integrity and Revenue:** The dataset contained 135,000 rows with missing `CustomerID` values. 
    * I recognized that removing these records with `dropna()` would mean losing a massive amount of "Guest Customer" data, which is vital for revenue reporting.
    * To prevent data loss and maintain financial accuracy, I modeled these transactions in the data warehouse by filling missing IDs with `-1`.
* **Handling Returns (Finding Actual Sales):** I identified that negative values in the `Quantity` column represented cancellations or returns.
    * To ensure the `fact_sales` table remained clean and provided an accurate analysis of "Gross Sales," I applied a `Quantity > 0` filter.
* **Star Schema Logic:** Instead of a bloated, flat database where everything is in a single table, I decomposed the data into 4 parts to answer the questions of *Who, What, When, and How*.
    * By isolating product descriptions into the `dim_product` table instead of repeating them in every row, I optimized storage and memory usage.
* **Connection Management and Error Handling:** I encountered the `PendingRollbackError` while loading massive amounts of data to the cloud.
    * This taught me how databases lock during incomplete transactions. I resolved this by using `engine.dispose()` to reset the connection and implemented `chunksize=10000` to load data in manageable batches without overwhelming the system.

[Image of Medallion Architecture Bronze Silver Gold]

## Architecture and Data Flow

The project follows the **Medallion Architecture** (Bronze -> Silver -> Gold) principles.

### 1. Extract & Load (Bronze Layer)
* Data was extracted from an external source (CSV) using `pandas`.
* It was loaded directly into the Neon (PostgreSQL) database as the `raw_sales` table without any modifications to ensure traceability and data lineage.

### 2. Transform (Silver Layer)
* Missing `CustomerID` values were imputed with `-1`.
* Returns and cancellations were filtered out.
* The `InvoiceDate` column was standardized into a machine-readable `DateTime` format.
* For analytical convenience, the pre-calculated $TotalAmount = Quantity * UnitPrice$ was added (Feature Engineering).

### 3. Modeled Load (Gold Layer)
The cleaned data was partitioned into a **Star Schema** for optimized analytical (BI) querying:
* **`dim_customer`**: A unique pool of customers.
* **`dim_product`**: A deduplicated product catalog (`StockCode`, `Description`, `UnitPrice`).
* **`dim_date`**: A time-intelligence dimension (`Year`, `Month`, `Day`, `IsWeekend`).
* **`fact_sales`**: The heart of the schema. It contains metrics (`Quantity`, `TotalAmount`) and links (Foreign Keys) to the dimensions.

[Image of Star Schema Data Warehouse architecture]

## Technologies Used

* **Language:** Python
* **Libraries:** Pandas (Data Manipulation), SQLAlchemy (Database Communication)
* **Database:** PostgreSQL (Neon Serverless Cloud)

