from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, to_timestamp, concat, round, broadcast

# Initialize Spark Session without you 
spark = SparkSession.builder \
    .appName("Credit Card Transactions Processor") \
    .getOrCreate()

# Enable Spark Optimizations
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Input JSON files path
json_file_path = 'gs://credit-card-data-analysis/transactions/transactions_*.json'

# Define BigQuery Dataset & Table Names
BQ_PROJECT_ID = "psyched-service-442305-q1"
BQ_DATASET = "credit_card"
BQ_CARDHOLDERS_TABLE = f"{BQ_PROJECT_ID}.{BQ_DATASET}.cardholders"
BQ_TRANSACTIONS_TABLE = f"{BQ_PROJECT_ID}.{BQ_DATASET}.transactions"

# Load Static Cardholders Data from BigQuery (Select only required columns)
cardholders_df = spark.read.format("bigquery") \
    .option("table", BQ_CARDHOLDERS_TABLE) \
    .load() \
    .select("cardholder_id", "reward_points", "risk_score") \
    .cache()  # Cache because this is a static reference table

# Materialize cache
cardholders_df.count()

# Load Daily Transactions Data (JSON) with column pruning
transactions_df = spark.read.option("multiline", "true") \
    .json(json_file_path) \
    .select(
        "transaction_id",
        "cardholder_id",
        "merchant_id",
        "transaction_amount",
        "transaction_status",
        "transaction_timestamp",
        "fraud_flag",
        "merchant_name",
        "merchant_location"
    )

# Data Validations
transactions_df = transactions_df.filter(
    (col("transaction_amount") >= 0) &
    (col("transaction_status").isin("SUCCESS", "FAILED", "PENDING")) &
    col("cardholder_id").isNotNull() &
    col("merchant_id").isNotNull()
)

# Repartition by join key to optimize shuffle
transactions_df = transactions_df.repartition("cardholder_id")

# Data Transformations
transactions_df = transactions_df.withColumn(
    "transaction_category",
    when(col("transaction_amount") <= 100, lit("Low"))
    .when((col("transaction_amount") > 100) & (col("transaction_amount") <= 500), lit("Medium"))
    .otherwise(lit("High"))
).withColumn(
    "transaction_timestamp", to_timestamp(col("transaction_timestamp"))
).withColumn(
    "high_risk",
    (col("fraud_flag") == True) |
    (col("transaction_amount") > 10000) |
    (col("transaction_category") == "High")
).withColumn(
    "merchant_info", concat(col("merchant_name"), lit(" - "), col("merchant_location"))
)

# Enrich Transactions with Cardholders Data using Broadcast Join
enriched_df = transactions_df.join(
    broadcast(cardholders_df),
    on="cardholder_id",
    how="left"
)

# Update Reward Points (Earn 1 point per $10 spent)
enriched_df = enriched_df.withColumn(
    "updated_reward_points",
    col("reward_points") + round(col("transaction_amount") / 10)
)

# Calculate Fraud Risk Level
enriched_df = enriched_df.withColumn(
    "fraud_risk_level",
    when(col("high_risk") == True, lit("Critical"))
    .when((col("risk_score") > 0.3) | (col("fraud_flag") == True), lit("High"))
    .otherwise(lit("Low"))
)

# Write Final Processed Data to BigQuery
enriched_df.write.format("bigquery") \
    .option("table", BQ_TRANSACTIONS_TABLE) \
    .option("writeMethod", "direct") \
    .mode("append") \
    .save()

print(f"Successfully processed file: {json_file_path}")
print("Optimized Transactions Processing Completed!")
