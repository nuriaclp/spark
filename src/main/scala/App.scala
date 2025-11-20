// Spark 3.5 with Scala 3 – Mastery Exercises
// ==========================================
//
// This file contains skeletons and detailed instructions for a sequence
// of Spark 3.5 exercises using Scala 3.x.
//
// For each exercise, read the comments carefully and complete the
// corresponding object or methods.
//
// Recommended usage:
//   - Keep this file as a guide / TODO list.
//   - Implement your own solutions in separate files, OR
//   - Replace the TODOs directly here.

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._

// ------------------------------------------------------------
// 0. Project Setup & Warm-Up
// ------------------------------------------------------------
//
// Goal:
//   - Create a SparkSession.
//   - Read a CSV as a DataFrame.
//   - Inspect schema and data.
//   - Do a simple filter transformation.
//
// Dataset:
//   - CSV file: data/warmup.csv
//   - Example columns: name (String), age (Int)
//
// Tasks:
//   1. Create a SparkSession named "WarmUpApp" (local[*]).
//   2. Read data/warmup.csv with header and inferred schema.
//   3. Print schema and show first 5 rows.
//   4. Filter rows where age >= 18 and show them.

object WarmUpAppExercises {

  def main(args: Array[String]): Unit = {
    // TODO:
    // 1. Build SparkSession.
    // 2. Read CSV data/warmup.csv.
    // 3. Print schema.
    // 4. Show first 5 rows.
    // 5. Filter age >= 18 and show.
    val spark = SparkSession.builder.appName("WarmUpApp").master("local[*]").getOrCreate()
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv("C:\\Users\\WillyCheto\\Desktop\\CursoBigData\\data\\warmup.csv")
    df.printSchema()
    df.show(5)    
    println("Filtrando registros donde la edad es >= 18:")
    df.filter("age >= 18").show()
    spark.stop()
  }

}


// ------------------------------------------------------------
// 1. DataFrame Fundamentals – Online Retail Analysis
// ------------------------------------------------------------
//
// Goal:
//   - Practice core DataFrame operations:
//       * Explicit schema definition.
//       * Filtering, aggregations, sorting.
//       * New columns.
//   - Handle nulls and simple data cleaning.
//
// Dataset:
//   - CSV file: data/online_retail.csv
//   - Columns (example):
//       order_id: Long
//       customer_id: Long
//       country: String
//       product: String
//       category: String
//       unit_price: Double
//       quantity: Int
//       order_timestamp: String (ISO, e.g. 2025-01-05T10:30:00)
//
// Tasks:
//   1. Define an explicit StructType schema for the CSV.
//   2. Load data/online_retail.csv using that schema (header = true).
//   3. Clean data:
//        - Remove rows with null customer_id.
//        - Remove rows with quantity <= 0 or unit_price <= 0.
//   4. Add column total_amount = quantity * unit_price.
//   5. Answer (via DataFrame transformations + actions):
//        a) Top 10 countries by total revenue (sum total_amount).
//        b) For each country, average order value:
//             * First, compute order totals per (order_id, country).
//             * Then average those totals per country.
//        c) Top 5 products by total quantity sold globally.
//        d) Per category:
//             * total quantity sold
//             * total revenue
//             * average unit price.

object RetailAnalysisAppExercises {

  def main(args: Array[String]): Unit = {
    // TODO:
    // 1. Create SparkSession.
    // 2. Define StructType retailSchema.
    // 3. Read CSV with that schema.
    // 4. Clean data (filter conditions).
    // 5. Add total_amount column.
    // 6. Implement each query (a–d) and show results.
  }

}

// ------------------------------------------------------------
// 2. Joins & Window Functions – Customers and Orders
// ------------------------------------------------------------
//
// Goal:
//   - Practice joins between DataFrames.
//   - Use Window for ranking and running totals.
//
// Datasets:
//   - data/customers.csv:
//       customer_id: Long
//       name: String
//       email: String
//       country: String
//       signup_date: String
//
//   - data/orders.csv:
//       order_id: Long
//       customer_id: Long
//       order_timestamp: String
//       order_total: Double
//
// Tasks:
//   1. Load both CSVs with header and inferred schema.
//   2. Inner join customers and orders on customer_id.
//   3. Compute total spent per customer (sum order_total).
//   4. Add column customer_total_spent to each order row
//      (join or window-based approach).
//   5. Rank customers within each country by customer_total_spent
//      (descending) using row_number or dense_rank.
//   6. Extract top 3 customers per country based on that ranking.
//   7. For each customer, compute:
//        - order sequence number (1st, 2nd, 3rd...) by order_timestamp.
//        - running total of order_total over that ordering.

object JoinsAndWindowsExercises {

  def main(args: Array[String]): Unit = {
    // TODO:
    // 1. Create SparkSession.
    // 2. Read customers.csv and orders.csv.
    // 3. Inner join by customer_id.
    // 4. Compute total spent per customer.
    // 5. Add customer_total_spent to each order row.
    // 6. Rank customers per country and get top 3.
    // 7. Compute order_seq and running_total per customer.
  }

}

// ------------------------------------------------------------
// 3. Datasets & Typed Transformations
// ------------------------------------------------------------
//
// Goal:
//   - Use case classes with Dataset[T].
//   - Use typed operations (map, filter, groupByKey, mapGroups).
//
// Reuse:
//   - customers.csv and orders.csv from Exercise 2.
//
// Case classes to use:
//
//   case class Customer(
//     customer_id: Long,
//     name: String,
//     email: String,
//     country: String,
//     signup_date: String
//   )
//
//   case class Order(
//     order_id: Long,
//     customer_id: Long,
//     order_timestamp: String,
//     order_total: Double
//   )
//
//   case class CustomerStats(
//     customer_id: Long,
//     name: String,
//     country: String,
//     total_orders: Long,
//     total_spent: Double,
//     first_order_ts: Option[String],
//     last_order_ts: Option[String]
//   )
//
// Tasks:
//   1. Load CSVs into DataFrames, then convert to Dataset[Customer] and Dataset[Order].
//   2. Build a Dataset[CustomerStats]:
//        - For each customer, collect their orders and compute:
//             * total_orders
//             * total_spent
//             * first_order_ts (or None)
//             * last_order_ts (or None)
//        - Include customers with no orders (0, 0.0, None).
//   3. Filter CustomerStats to keep only customers with total_spent > 1000.
//   4. Map these to a human-readable String summary.

object TypedDatasetsExercises {

  case class Customer(
    customer_id: Long,
    name: String,
    email: String,
    country: String,
    signup_date: String
  )

  case class Order(
    order_id: Long,
    customer_id: Long,
    order_timestamp: String,
    order_total: Double
  )

  case class CustomerStats(
    customer_id: Long,
    name: String,
    country: String,
    total_orders: Long,
    total_spent: Double,
    first_order_ts: Option[String],
    last_order_ts: Option[String]
  )

  def main(args: Array[String]): Unit = {
    // TODO:
    // 1. Create SparkSession and import spark.implicits._.
    // 2. Load customers.csv and orders.csv; convert to Dataset[Customer] and Dataset[Order].
    // 3. Use typed operations (e.g., groupByKey + mapGroups) to compute CustomerStats.
    // 4. Filter total_spent > 1000 and map to String summaries.
  }

}

// ------------------------------------------------------------
// 4. Spark SQL & UDFs
// ------------------------------------------------------------
//
// Goal:
//   - Create temp views and query them via Spark SQL.
//   - Define and register UDFs.
//
// Dataset:
//   - Reuse the cleaned retail DataFrame from Exercise 1.
//
// UDF to implement:
//
//   def spamRisk(email: String): Int = {
//     // 0 if domain is "gmail.com" or "outlook.com"
//     // 1 if domain is "yahoo.com"
//     // 2 for any other domain or null
//   }
//
// Tasks:
//   1. Load and clean online_retail.csv (same as Exercise 1).
//   2. Add total_amount column.
//   3. Register DataFrame as temp view "retail".
//   4. SQL query 1: per country,
//        - num_orders (count distinct order_id)
//        - num_customers (count distinct customer_id)
//        - total_revenue (sum quantity * unit_price)
//   5. SQL query 2: top 5 categories per country by total_revenue,
//      using ROW_NUMBER OVER (PARTITION BY country ORDER BY total_revenue DESC).
//   6. Implement spamRisk UDF, register it as "spam_risk", and use it:
//        - with DataFrame .withColumn
//        - in a SQL query over a "customers" view.

object SqlAndUdfExercises {

  def main(args: Array[String]): Unit = {
    // TODO:
    // 1. Create SparkSession.
    // 2. Load + clean retail CSV, add total_amount.
    // 3. createOrReplaceTempView("retail").
    // 4. Implement SQL queries for metrics and top categories.
    // 5. Implement spamRisk UDF and register it.
    // 6. Apply spam_risk in DataFrame API and SQL.
  }

}

// ------------------------------------------------------------
// 5. Structured Streaming – Real-Time Word Count
// ------------------------------------------------------------
//
// Goal:
//   - Use Structured Streaming with Spark.
//   - Global and windowed aggregations over streaming text.
//
// Source:
//   - Socket text stream (e.g., nc -lk 9999), or
//   - Directory of text files.
//
// Tasks:
//   1. Build StreamingWordCountApp.
//   2. Read streaming lines:
//        - For socket: format("socket").option("host", "localhost").option("port", 9999).
//   3. Split each line into words, lowercase, filter empty.
//   4. Global running count:
//        - groupBy("word").count().
//   5. Windowed count:
//        - Add timestamp column (e.g., current_timestamp()).
//        - Use withWatermark + window(column, "10 minutes", "5 minutes").
//   6. Write to console sink with outputMode("update") and checkpointLocation.
//   7. (Optional) Also write to parquet directory.

object StreamingWordCountAppExercises {

  def main(args: Array[String]): Unit = {
    // TODO:
    // 1. Create SparkSession (with spark.sql.shuffle.partitions etc. if desired).
    // 2. Build streaming query for global word counts.
    // 3. Build second query with windowed word counts.
    // 4. Start queries and awaitTermination.
  }

}

// ------------------------------------------------------------
// 6. Performance Tuning & Partitioning
// ------------------------------------------------------------
//
// Goal:
//   - Inspect physical plans (.explain(true)).
//   - Understand repartition vs coalesce.
//   - Write partitioned output.
//   - Use caching effectively.
//
// Dataset:
//   - Any larger dataset, e.g. online_retail.csv or orders.csv,
//     with many rows.
//
// Tasks:
//   1. Define a moderately complex aggregation query, for example
//      revenue by (country, category).
//   2. Call .explain(true) and note shuffles and partition counts.
//   3. Repartition input with repartition(200), explain again.
//   4. Repartition by column (repartition(col("country"))), explain again.
//   5. Compare coalesce(4) vs repartition(4) and write comments explaining.
//   6. Write aggregation output as parquet partitioned by country, category.
//   7. Cache an intermediate DataFrame and compare timings with and without cache.

object PerformanceTuningExercises {

  def main(args: Array[String]): Unit = {
    // TODO:
    // 1. SparkSession.
    // 2. Read dataset and define complex aggregation query.
    // 3. Call explain(true) before and after repartition.
    // 4. Demonstrate coalesce vs repartition.
    // 5. Write partitioned output.
    // 6. Demonstrate caching.
  }

}

// ------------------------------------------------------------
// 7. Mini Project – Log Analytics End-to-End
// ------------------------------------------------------------
//
// Goal:
//   - Build an end-to-end analytics flow:
//       * Read JSON logs.
//       * Clean & transform.
//       * Compute metrics.
//       * Write partitioned results.
//       * Query with SQL.
//
// Dataset:
//   - Folder: data/logs/
//   - JSON fields:
//       timestamp: String (ISO)
//       user_id: Long
//       url: String
//       response_time_ms: Long
//       status_code: Int
//       country: String
//       device: String
//
// Tasks:
//   1. Read all JSON files under data/logs/ into a DataFrame.
//   2. Clean data:
//        - Filter null user_id or url.
//        - Keep only status_code in {200, 301, 302, 404, 500}.
//   3. Transform:
//        - Add is_error = status_code >= 400.
//        - Convert timestamp to proper timestamp type (to_timestamp).
//        - Add date = to_date(timestamp column).
//   4. Metrics per (date, country):
//        - total_requests
//        - unique_users
//        - avg_response_time_ms
//        - p95_response_time_ms (percentile_approx)
//        - error_rate = errors / total_requests.
//   5. For each date, find top 10 urls by number of requests
//      (and optionally by error count).
//   6. Write outputs as parquet:
//        - metrics partitioned by date, country.
//        - top URLs partitioned by date.
//   7. Register metrics as view "daily_metrics" and answer via SQL:
//        - For each country, which date had the highest error_rate?
//   8. Optional advanced: turn into Structured Streaming job
//      reading logs as they arrive.

object LogAnalyticsMiniProjectExercises {

  def main(args: Array[String]): Unit = {
    // TODO:
    // 1. Create SparkSession.
    // 2. Read JSON logs.
    // 3. Clean and transform (is_error, ts, date).
    // 4. Compute metrics per (date, country).
    // 5. Compute top 10 URLs per date.
    // 6. Write parquet outputs partitioned as requested.
    // 7. Register daily_metrics and query for worst error_rate per country.
  }

}
