**Author:** *Priyansh Kaushik* 
**Submission:** *Dev Dolphins - PySpark Data Engineer Assignment*

##  Introduction

This project implements a scalable, real-time data pipeline to ingest and analyze financial transaction data. The system is built entirely on a cloud-native stack using AWS and Databricks. It consists of two primary mechanisms:

* **Mechanism X**: A data ingestion service that periodically fetches chunks of transaction data and lands them in a raw data store.
* **Mechanism Y**: A stream processing service that ingests the raw data, detects predefined behavioral patterns in near real-time, and stores the results.

The entire pipeline is automated using Databricks Jobs and is designed to be robust, scalable, and fault-tolerant, with a strong emphasis on data quality and state management.

---

##  Architecture

The pipeline follows a decoupled, event-driven architecture. Mechanism X and Mechanism Y operate independently, communicating via AWS S3, which acts as a data buffer. This design ensures that the ingestion process is not blocked by processing, and vice-versa.

* **Databricks (Compute Engine)**: The core of the pipeline, used for both scheduling ingestion jobs (**Mechanism X**) and running the continuous stream processing logic (**Mechanism Y**) with PySpark.
* **AWS S3 (Data Lake)**: Serves as the central storage layer.
    * `pk-transactions-raw`: The "inbox" where Mechanism X drops 10,000-record chunks of raw transaction data.
    * `pk-transactions-detections`: The "outbox" where Mechanism Y writes the final pattern detection files.
    * `pk-transactions-temp`: Used for storing the offset tracker for Mechanism X and the checkpoint location for Mechanism Y.
* **AWS RDS PostgreSQL (State Store)**: A persistent state management layer. It stores long-running aggregations needed for pattern detection, such as total transaction counts per merchant and detailed customer statistics. This ensures state is maintained correctly even if the streaming job restarts.

---

##  Technology Stack

* **Cloud Platform**: AWS
* **Compute**: Databricks
* **Language**: Python, SQL
* **Core Framework**: PySpark (Structured Streaming)
* **Data Storage**: AWS S3, AWS RDS (PostgreSQL)
* **Orchestration**: Databricks Jobs / Workflows
* **Key Libraries**: `psycopg2`, `boto3`

---

##  Setup and Configuration

To reproduce this environment, follow these steps:

1.  **AWS Setup**:
    * Create three S3 buckets as shown in the architecture: `pk-transactions-raw`, `pk-transactions-detections`, and `pk-transactions-temp`.
    * Set up an AWS RDS for PostgreSQL instance. The project uses an instance named `databricks-postgres-db`. Ensure it is publicly accessible so the Databricks workspace can connect to it.

2.  **Database Initialization**:
    * Run the `postgres_setup.ipynb` notebook once. This script connects to the RDS instance and creates the necessary tables for state management:
        * `merchant_transaction_counts`: Stores the running total of transactions for each merchant.
        * `customer_merchant_stats`: Stores detailed stats like transaction counts, average weight, and average amount for each customer-merchant pair.
        * `merchant_gender_stats`: Stores the count of unique male and female customers for each merchant.

3.  **Initial Data Load**:
    * Upload the provided `transactions.csv` and `CustomerImportance.csv` files to Databricks and save them as tables (e.g., `workspace.default.transactions`). The ingestion script (`mechanism_x_ingestion.ipynb`) reads from this source table to simulate the data feed.

4.  **Databricks Configuration**:
    * Create a Databricks cluster and ensure it has the necessary libraries (`psycopg2-binary`) installed.
    * Configure the notebooks with the correct credentials and resource names using the Databricks widgets provided in the code.

---

##  Code Deep Dive

### Mechanism X: Data Ingestion (`mechanism_x_ingestion.ipynb`)

Mechanism X is a straightforward, stateful batch job. Its sole purpose is to feed data to Mechanism Y in manageable chunks.

#### Key Logic: Offset Management

To avoid reprocessing the entire source file on every run, I implemented an offset tracking system using a simple text file stored in S3.

```python
# Helper functions to manage the offset in S3
def get_offset():
    try:
        response = s3.get_object(Bucket=s3_temp_bucket, Key=OFFSET_FILE_KEY)
        return int(response['Body'].read().decode('utf-8'))
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return 0 # If file doesn't exist, start from the beginning
        else:
            raise

def update_offset(new_offset):
    s3.put_object(Bucket=s3_temp_bucket, Key=OFFSET_FILE_KEY, Body=str(new_offset))
```

* **Why this approach?** Storing the offset externally in S3 makes the job stateless. If the job fails and is restarted, it can safely pick up from where it left off by reading this file, ensuring data is neither lost nor duplicated.

#### Key Logic: Data Chunking

I used a standard Spark SQL query with `LIMIT` and `OFFSET` to fetch the data in chunks of 10,000.

```python
current_offset = get_offset()
query = f"""
    SELECT * FROM {source_table}
    ORDER BY step
    LIMIT {CHUNK_SIZE}
    OFFSET {current_offset}
"""
spark_df = spark.sql(query)
```

* **Why this approach?** Using `ORDER BY` on a column like `step` ensures that the order of records is deterministic. This is critical for `OFFSET` to work correctly across different job runs. The resulting DataFrame is then written to the `pk-transactions-raw` S3 bucket, triggering Mechanism Y.

### Mechanism Y: Stream Processing (`mechanism_y_processor.ipynb`)

This is the heart of the pipeline, where the real-time analysis happens. It's designed around PySpark's Structured Streaming and a `foreachBatch` function.

#### Key Logic: The `foreachBatch` Processor

The entire processing logic is encapsulated within a `process_batch` function, which is applied to each micro-batch of data from the stream.

```python
def process_batch(batch_df, batch_id):
    # ... all processing logic here ...

df_stream.writeStream.foreachBatch(process_batch).start()
```

* **Why this approach?** `foreachBatch` is the most powerful tool in Structured Streaming. It provides two key benefits:
    1.  It allows you to run standard batch DataFrame operations on streaming data, which is necessary for complex logic like percentile calculations.
    2.  It enables writing data to sinks that don't have a native streaming equivalent, such as a JDBC connection to PostgreSQL.

#### Key Logic: Stateful Updates in PostgreSQL

A core design decision was to offload state management to PostgreSQL. This makes the state durable and queryable outside of Spark. I used an "UPSERT" pattern for efficient updates.

```python
# Example UPSERT for merchant counts
cursor.execute(
    """INSERT INTO merchant_transaction_counts (merchant_id, transaction_count)
       VALUES (%s, %s)
       ON CONFLICT (merchant_id)
       DO UPDATE SET transaction_count = merchant_transaction_counts.transaction_count + EXCLUDED.transaction_count;""",
    (row['merchant'], row['count'])
)
```

* **Why this approach?** The `INSERT ... ON CONFLICT DO UPDATE` command is atomic and highly efficient. It attempts to insert a new record; if a record with the same primary key already exists, it updates the existing one instead. This single command prevents race conditions and avoids the need for a separate `SELECT` then `UPDATE`/`INSERT` logic, which is less performant.

#### Key Logic: Pattern Detection

Each pattern is detected by querying the fully updated state from PostgreSQL within the same batch.

* **PatId1 (UPGRADE)**: This is the most complex pattern.
    1.  First, I query Postgres for merchants with more than 50,000 transactions.
    2.  Then, I query the full `customer_merchant_stats` table.
    3.  I use a PySpark **Window Function** to calculate the `percent_rank` for both transaction count and average weight for every customer within each merchant's group.
    4.  Finally, I filter for customers in the top 10% for transactions and bottom 10% for weight.

    ```python
    merchant_window = Window.partitionBy("merchant_id")
    percentiles_df = customer_stats_df.withColumn(
        "txn_percentile", F.percent_rank().over(merchant_window.orderBy("transaction_count"))
    ).withColumn(
        "weight_percentile", F.percent_rank().over(merchant_window.orderBy("avg_weight"))
    )
    ```

* **PatId2 (CHILD)**: This pattern is a direct filter on the `customer_merchant_stats` table.

    ```python
    patid2_detections = customer_stats_df.filter(
        (F.col("avg_txn_value") < 23) & (F.col("transaction_count") >= 80)
    )
    ```

* **PatId3 (DEI-NEEDED)**: This requires comparing male vs. female customer counts. I used a SQL `PIVOT` query (emulated with `SUM(CASE WHEN ...)` ) directly in the JDBC read to get these counts side-by-side for easy comparison.

    ```sql
    -- Executed via JDBC
    SELECT merchant_id,
           COALESCE(SUM(CASE WHEN gender = 'F' THEN customer_count END), 0) as female_customers,
           COALESCE(SUM(CASE WHEN gender = 'M' THEN customer_count END), 0) as male_customers
    FROM merchant_gender_stats
    GROUP BY merchant_id
    ```

---

##  Project Enhancements & Considerations

* **Data Quality**: The pipeline includes explicit data cleaning steps (`clean_quoted_strings`, `normalize_gender`) to handle inconsistencies in the source data. This is crucial for ensuring the accuracy of the final detections.
* **Scalability**: The architecture is highly scalable. The Databricks cluster can be resized to handle larger data volumes. The use of S3 and RDS, which are managed services, means they can scale independently without manual intervention.
* **Error Handling & Fault Tolerance**:
    * **Mechanism X** is idempotent due to the S3 offset manager.
    * **Mechanism Y** uses Spark's checkpointing mechanism (`cloudFiles.schemaLocation`). If the streaming job fails, it can restart from the exact point it left off, guaranteeing exactly-once processing semantics.
    * Database transactions ensure that state updates are atomic.
* **Maintainability**: The code is modularized into separate notebooks for ingestion and processing. Using Databricks widgets makes it easy to configure paths and credentials without hardcoding them.

---

##  Assumptions

* **Ingestion Frequency**: The assignment's "every second" requirement for Mechanism X was interpreted as a near real-time feed. I have implemented this using a Databricks Job scheduled to run **every minute**, which is a more practical and cost-effective approach for a job-based scheduler.
* **Data Source Simulation**: Instead of directly connecting to Google Drive in a loop (which can be unreliable), the source CSVs were first loaded into Databricks tables. Mechanism X then simulates the chunked feed from these stable tables, providing a more controlled and fault-tolerant ingestion process.
* **Output Batching**: The requirement to output detections "50 at a time" is handled by dynamically repartitioning the final detections DataFrame before writing it to S3. This ensures that the number of records per output file is controlled as requested.

## ScreenShots

<img width="1920" height="927" alt="image" src="https://github.com/user-attachments/assets/f082de88-85c9-49e0-b36e-76e7d497932c" />

<img width="1920" height="930" alt="image" src="https://github.com/user-attachments/assets/22b4a750-1480-4467-a13a-e4a384aac3e6" />

<img width="1920" height="927" alt="image" src="https://github.com/user-attachments/assets/4bdc3b4a-6c0e-44a4-9c32-a14dde2e5fbd" />

<img width="1920" height="931" alt="image" src="https://github.com/user-attachments/assets/42fe34cd-5f3b-4b60-983f-6f6938028fb0" />

<img width="1895" height="368" alt="image" src="https://github.com/user-attachments/assets/ef895879-3286-43ce-a568-1bc27e46779f" />

<img width="1920" height="512" alt="image" src="https://github.com/user-attachments/assets/a1e9caae-1cee-42ca-a770-9213971c3fe9" />
<img width="1920" height="933" alt="image" src="https://github.com/user-attachments/assets/d63c6e97-e6f1-4ac4-9e42-b83e23c8bfce" />





