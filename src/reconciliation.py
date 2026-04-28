from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (col, when, count as spark_count, concat_ws, 
                                   desc, broadcast, lit, row_number, round as spark_round)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from utils.logging_utils import get_logger
from utils.general_utils import read_yaml_config as ryc

config_path = ("config/config.yaml")
config = ryc(config_path)
logger = get_logger(config)

class CustomerReconciliation:
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.match_keys = self.config['reconciliation']['match_keys']
        self.confidence_scoring = self.config['reconciliation']['confidence_scoring']
        self.confidence_threshold = self.config['reconciliation']['confidence_threshold']


    def find_email_matches(self, crm_df: DataFrame, trx_df: DataFrame) -> DataFrame:
        """
        Find potential matches between CRM and Transaction data based on email addresses.
        """
        logger.info("Finding email matches between CRM and Transaction data...")

        # Perform a inner join on the email column
        matched_df = crm_df.alias("crm").join(
            broadcast(trx_df.alias("trx")), 
            col("crm.normalized_email") == col("trx.normalized_email"), 
            "inner"
        ) \
            .filter(col("crm.normalized_email").isNotNull()) \
            .select(
            col("crm.customer_id").alias("crm_customer_id"),
            col("trx.transaction_id").alias("transaction_id"),
            col("crm.normalized_email").alias("match_key"),
            lit("email").alias("match_type"),
            lit(self.confidence_scoring['email_match']).alias("confidence_score")
        )

        return matched_df
    

    def find_phone_matches(self, crm_df: DataFrame, trx_df: DataFrame) -> DataFrame:
        """
        Find potential matches between CRM and Transaction data based on phone numbers.
        """
        logger.info("Finding phone matches between CRM and Transaction data...")

        # Perform an inner join on the phone column
        matched_df = crm_df.alias("crm").join(
            broadcast(trx_df.alias("trx")), 
            col("crm.normalized_phone") == col("trx.normalized_phone"), 
            "inner"
        ) \
            .filter(col("crm.normalized_phone").isNotNull()) \
            .select(
            col("crm.customer_id").alias("crm_customer_id"),
            col("trx.transaction_id").alias("transaction_id"),
            col("crm.normalized_phone").alias("match_key"),
            lit("phone").alias("match_type"),
            lit(self.confidence_scoring['phone_match']).alias("confidence_score")
            )
        
        return matched_df


    def find_name_address_matches(self, crm_df: DataFrame, trx_df: DataFrame) -> DataFrame:
        """
        Find potential matches between CRM and Transaction data based on name and address similarity.
        """
        logger.info("Finding name and address matches between CRM and Transaction data...")

        # Perform a inner join on the name and address columns
        matched_df = crm_df.alias("crm").join(
            broadcast(trx_df.alias("trx")), 
            (col("crm.first_name") == col("trx.first_name")) &
            (col("crm.last_name") == col("trx.last_name")) &
            (col("crm.city") == col("trx.city")) &
            (col("crm.country") == col("trx.country")), 
            "inner"
        ) \
            .filter(col("crm.first_name").isNotNull() & 
                    col("crm.last_name").isNotNull()
                    ) \
            .select(
            col("crm.customer_id").alias("crm_customer_id"),
            col("trx.transaction_id").alias("transaction_id"),
            concat_ws(
                "|",
                col("crm.first_name"),
                col("crm.last_name"),
                col("crm.city")
            ).alias("match_key"),
            lit("name_address").alias("match_type"),
            lit(self.confidence_scoring['name_address_match']).alias("confidence_score")
        )

        return matched_df
    

    def reconcile(self, crm_df: DataFrame, trx_df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """
        
        Perform reconciliation between CRM and Transaction data by finding matches 
        based on email, phone, and name/address.
        """

        email_matches = self.find_email_matches(crm_df, trx_df)
        phone_matches = self.find_phone_matches(crm_df, trx_df)
        name_address_matches = self.find_name_address_matches(crm_df, trx_df)

        # Combine all matches into a single DataFrame
        all_matches = email_matches \
                    .unionByName(phone_matches) \
                    .unionByName(name_address_matches)

        # Filter matches based on confidence threshold
        all_matches = all_matches.filter(col("confidence_score") >= self.confidence_threshold)

        # Adding explicit priority for match types

        all_matches = all_matches.withColumn(
            "match_priority",
            when(col("match_type") == "email", 3)
            .when(col("match_type") == "phone", 2)
            .otherwise(1)
        )

        # Use window function to keep the highest confidence match for each matched pair
        window_spec = Window.partitionBy("crm_customer_id") \
                    .orderBy(desc("confidence_score"), desc("match_priority"))
        
        best_matches = (all_matches
        .withColumn("rank", row_number().over(window_spec))
        .filter(col("rank") == 1)
        .drop("rank", "match_priority")
        )
        
        mappings = best_matches.select(
            col("crm_customer_id"), 
            col("transaction_id"), 
            col("match_type"), 
            col("confidence_score"),
            concat_ws(
                "_",
                lit("GOLD"),
                col("crm_customer_id")
                ).alias("gold_customer_id")
        )

        unmatched = (
            crm_df.alias("crm").join(mappings.select("crm_customer_id").distinct().alias("matched"),
                                     col("crm.customer_id") == col("matched.crm_customer_id"),
                                     "left_anti")
        ) \
            .select(
            col("crm.customer_id").alias("crm_customer_id"),
            lit(None).alias("transaction_id"),
            lit(None).alias("match_type"),
            lit(0.0).cast(DoubleType()).alias("confidence_score"),
            concat_ws(
                "_",
                lit("GOLD"),
                col("crm.customer_id"),
            ).alias("gold_customer_id")
        )

        final_reconciliation = mappings.unionByName(unmatched)

        final_reconciliation.cache()

        total_count = final_reconciliation.count()
        matched_count = mappings.count()
        unmatched_count = unmatched.count()

        # Log reconciliation summary
        logger.info(f"Reconciliation completed: {matched_count} records matched, {unmatched_count} unmatched.")

        stats = (
            final_reconciliation \
            .groupBy("match_type") \
            .agg(spark_count("*").alias("count")) \
            .withColumn("percentage", spark_round(col("count") / total_count * 100, 2))
        )

        logger.info(f"Reconciliation completed: {total_count} records matched.")
        logger.info(f"Match rate: {matched_count / total_count * 100:.2f}%")

        return final_reconciliation, stats
