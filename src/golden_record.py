from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, row_number, coalesce, when, lit,
    count as spark_count, min as spark_min, max as spark_max,
    collect_list, current_date, datediff,
    months_between, round as spark_round, slice
)
from pyspark.sql.window import Window
from utils.general_utils import read_yaml_config as ryc
from utils.logging_utils import get_logger
from utils.spark_utils import lineage_tracking

config_path = "config/config.yaml"
config = ryc(config_path)
logger = get_logger(config)


class GoldenRecordBuilder:

    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config

    @staticmethod
    def _aggregate_transactions(trx_df: DataFrame) -> DataFrame:
        """
        Aggregate transaction data per customer using normalized_email.
        """

        window_spec = Window.partitionBy("normalized_email") \
                            .orderBy(col("purchase_date").desc_nulls_last())

        # Latest transaction details (using window function to get the most recent transaction per customer)
        latest_trx = (
            trx_df.alias("t")
            .withColumn("row_num", row_number().over(window_spec))
            .filter(col("row_num") == 1)
            .select(
                col("normalized_email"),
                col("normalized_phone").alias("txn_normalized_phone"),
                col("phone").alias("txn_phone_raw"),
                col("customer_email").alias("txn_email"),
                col("transaction_id").alias("latest_transaction_id"),
                col("first_name").alias("txn_first_name"),
                col("last_name").alias("txn_last_name"),
                col("shipping_address").alias("txn_address"),
                col("city").alias("txn_city"),
                col("country").alias("txn_country")
            )
        )

        # Aggregate transactions to get counts and date ranges per customer
        trx_aggregated = (
            trx_df.groupBy("normalized_email")
            .agg(
                spark_min("purchase_date").alias("first_purchase_date"),
                spark_max("purchase_date").alias("last_purchase_date"),
                spark_count("*").alias("total_transactions"),
                slice(collect_list("transaction_id"), 1, 100)
                .alias("sample_transaction_ids")
            )
            .join(latest_trx, on="normalized_email", how="left")
        )

        return trx_aggregated

    def build_golden_record(self, crm_df: DataFrame, txn_df: DataFrame, mapping_df: DataFrame) -> DataFrame:

        logger.info("Building golden customer records")

    
        # Step 1: Aggregate transactions
        txn_agg = self._aggregate_transactions(txn_df).alias("txn")

        # Step 2: Normalize mapping column names EARLY

        mapping_clean = mapping_df.select(
            col("crm_customer_id").alias("crm_id"),
            col("transaction_id").alias("txn_id"),
            col("gold_customer_id"),
            col("match_type"),
            col("confidence_score").alias("confidence")
        ).alias("map")

        # Step 3: Join mapping → CRM 
        golden_base = (
            mapping_clean
            .join(
                crm_df.alias("crm"),
                col("map.crm_id") == col("crm.customer_id"),
                "left"
            )
        ).alias("g") 


        # Step 4: Join with transactions using NORMALIZED EMAIL
        
        golden_enriched = (
            golden_base
            .join(
                txn_agg,
                col("g.normalized_email") == col("txn.normalized_email"),
                "left"
            )
        )

        
        # Step 5: FINAL SELECT
        golden_record = golden_enriched.select(

            # Primary key - from mapping (via 'g')
            col("g.gold_customer_id"),

            # Identity - from CRM (via 'g') or Txn
            coalesce(col("g.first_name"), col("txn.txn_first_name")).alias("first_name"),
            coalesce(col("g.last_name"), col("txn.txn_last_name")).alias("last_name"),

            # Email - from CRM (via 'g')
            col("g.normalized_email").alias("email"),

            # Phone (normalized priority) - from CRM (via 'g') or Txn
            coalesce(
                col("g.normalized_phone"),
                col("txn.txn_normalized_phone")
            ).alias("phone"),

            # Raw traceability
            col("g.phone").alias("raw_crm_phone"),
            col("txn.txn_phone_raw"),

            # Address (txn preferred)
            coalesce(col("txn.txn_address"), col("g.address")).alias("address"),
            coalesce(col("txn.txn_city"), col("g.city")).alias("city"),
            coalesce(col("txn.txn_country"), col("g.country")).alias("country"),

            # Dates - from CRM (via 'g') or Txn
            col("g.registration_date"),
            col("g.last_updated"),
            col("txn.first_purchase_date"),
            col("txn.last_purchase_date"),

            # Transaction aggregates
            coalesce(col("txn.total_transactions"), lit(0)).alias("total_transactions"),
            col("txn.sample_transaction_ids"),

            # Matching metadata - from mapping (via 'g')
            col("g.match_type"),
            col("g.confidence"),

            # Match quality
            when(col("g.confidence") >= 0.8, "high")
            .when(col("g.confidence") >= 0.5, "medium")
            .otherwise("low")
            .alias("match_quality"),

            # IDs - from mapping (via 'g') and Txn
            col("g.crm_id"),
            col("g.txn_id"),
            col("txn.latest_transaction_id")
        )


        # Step 6: Derived fields
    
        golden_record = (
            golden_record
            .withColumn(
                "customer_tenure_days",
                datediff(current_date(), col("registration_date"))
            )
            .withColumn(
                "customer_tenure_months",
                spark_round(
                    months_between(current_date(), col("registration_date")), 1
                )
            )
            .withColumn(
                "is_active",
                when(
                    col("last_purchase_date").isNotNull() &
                    (datediff(current_date(), col("last_purchase_date")) <= 365),
                    True
                ).otherwise(False)
            )
            .withColumn(
                "data_quality_score",
                (
                    when(col("email").isNotNull(), 25).otherwise(0) +
                    when(col("phone").startswith("+"), 25).otherwise(10) +
                    when(col("address").isNotNull(), 20).otherwise(0) +
                    when(col("city").isNotNull(), 15).otherwise(0) +
                    when(col("country").isNotNull(), 15).otherwise(0)
                )
            )
        )

   
        # Step 7: Audit for traceability and lineage. 
   
        golden_record = lineage_tracking(golden_record, "GOLDEN_RECORD")

        logger.info("Golden record construction complete")

        return golden_record