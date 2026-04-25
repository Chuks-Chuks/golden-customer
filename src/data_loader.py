from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import col, to_date, coalesce, desc, when, lower, regexp_replace, trim, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from utils.logging_utils import get_logger
from utils.general_utils import read_yaml_config as ryc

config_path = "config/config.yaml"
config = ryc(config_path)
logger = get_logger(config)

class DataLoader:
    """
    This is the DataLoader class responsible for loading and preprocessing data for the Golden Record Pipeline.
    """
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.crm_path = self.config["input"]["crm_path"]
        self.transaction_path = self.config["input"]["transaction_path"]

    @staticmethod
    def _normalize_email(col_name: str = "email") -> Column:
        """Normalize email to lowercase, trimmed, no spaces."""

        logger.info(f"Normalizing email column '{col_name}'")

        return when(
            col(col_name).isNotNull(),
            lower(regexp_replace(trim(col(col_name)), "\\s+", ""))
        ).otherwise(lit(None))


    @staticmethod
    def _normalize_phone(col_name: str = "phone") -> Column:
        """Normalize phone numbers removing non-digit characters and ensuring a standard format."""

        logger.info(f"Normalizing phone column '{col_name}'")
        return when(
            col(col_name).isNotNull(),
            regexp_replace(
                regexp_replace(
                    trim(col(col_name)),
                    "[()\\-\\s\\.]", ""
                ),
                "^(\\+?)(\\d{10,15})$", "+$2"
            )
        ).otherwise(lit(None))


    @staticmethod
    def _normalising_date_columns(date_column: str):
        """Normalize date columns to a standard format (yyyy-MM-dd)."""

        logger.info(f"Normalizing date column '{date_column}' to standard format 'yyyy-MM-dd'")
        return coalesce(
            to_date(col(date_column), "yyyy-MM-dd"),
            to_date(col(date_column), "yyyyMMdd")
        )
    
    @staticmethod
    def _remove_duplicates(df: DataFrame, partition_column: str, order_column: str) -> DataFrame:
        """Remove duplicates based on specified column, keeping the most recent record."""

        logger.info(f"Removing duplicates based on columns: {partition_column}")
        window_spec = Window.partitionBy(partition_column).orderBy(desc(order_column))
        return df \
            .withColumn(
                "row_num", 
                row_number().over(window_spec)
                ) \
            .filter(col("row_num") == 1) \
            .drop("row_num")

    @staticmethod
    def _lineage_tracking(df: DataFrame, source: str) -> DataFrame:
        """Add audit columns for lineage tracking."""
        return df.withColumn("source", lit(source)) \
            .withColumn("ingestion_timestamp", current_timestamp())
    
    def load_crm_data(self) -> DataFrame:
        """
        This method loads CRM data from the specified path, normalizes email and phone columns, and removes duplicates.
        Returns:
            DataFrame: A Spark DataFrame containing the loaded and preprocessed CRM data.
        """
        logger.info(f"Loading CRM data from path: {self.crm_path}")

        # Defining the schema for CRM data
        crm_schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("registration_date", StringType(), True),
            StructField("last_updated", StringType(), True)
        ])
        crm_df = self.spark.read.option("header", "true") \
            .schema(crm_schema) \
            .csv(self.crm_path)

        ## Perform data normalization and deduplication
        # Normalize email and phone columns
        crm_df = crm_df \
            .withColumn("customer_id", trim(col("customer_id"))) \
            .withColumn("first_name", trim(col("first_name"))) \
            .withColumn("last_name", trim(col("last_name"))) \
            .withColumn("email", self._normalize_email("email")) \
            .withColumn("phone", self._normalize_phone("phone")) \
            .withColumn("address", trim(col("address"))) \
            .withColumn("city", trim(col("city"))) \
            .withColumn("country", trim(col("country"))) \
            .withColumn("registration_date", self._normalising_date_columns("registration_date")) \
            .withColumn("last_updated", self._normalising_date_columns("last_updated"))
            
        # Remove duplicates
        crm_df = self._remove_duplicates(crm_df, "customer_id", "last_updated")

        # Add lineage tracking columns for CRM data
        crm_df = self._lineage_tracking(crm_df, "CRM")

        return crm_df

    def load_transaction_data(self) -> DataFrame:
        """
        This method loads transaction data from the specified path and normalizes date columns.

        Returns:
            DataFrame: A Spark DataFrame containing the loaded and preprocessed transaction data.
        """
        logger.info(f"Loading transaction data from path: {self.transaction_path}")

        # Defining the schema for transaction data
        transaction_schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("customer_email", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("shipping_address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("purchase_date", StringType(), True)
        ])
        
        transaction_df = self.spark.read.option("header", "true") \
            .schema(transaction_schema) \
            .csv(self.transaction_path)
        
        # Normalize dataframe columns and deduplicate
        transaction_df = transaction_df \
            .withColumn("transaction_id", trim(col("transaction_id"))) \
            .withColumn("customer_email", self._normalize_email("customer_email")) \
            .withColumn("first_name", trim(col("first_name"))) \
            .withColumn("last_name", trim(col("last_name"))) \
            .withColumn("phone", self._normalize_phone("phone")) \
            .withColumn("shipping_address", trim(col("shipping_address"))) \
            .withColumn("city", trim(col("city"))) \
            .withColumn("country", trim(col("country"))) \
            .withColumn("purchase_date", self._normalising_date_columns("purchase_date"))
        
        # Removing duplicates based on transaction_id and the most recent purchase_date
        transaction_df = self._remove_duplicates(transaction_df, "transaction_id", "purchase_date")

        # Add lineage tracking columns for transaction data
        transaction_df = self._lineage_tracking(transaction_df, "TRX")

        return transaction_df
    

    def load_data(self) -> tuple[DataFrame, DataFrame]:
        """
        Load both CRM and transaction data, caching them for performance and returning them as DataFrames.

        Returns:
            tuple: A tuple containing the CRM DataFrame and the transaction DataFrame.
        """
        # Load and preprocess CRM and transaction data
        logger.info("Loading and preprocessing CRM and transaction data")
        try:
            crm_df = self.load_crm_data()
            trx_df = self.load_transaction_data()
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise e
    # Cache the DataFrames for performance
        crm_df.cache()
        trx_df.cache()

        logger.info("Data loading and preprocessing completed successfully")
        logger.info(f"Loaded {crm_df.count()} of CRM records")
        logger.info(f"Loaded {trx_df.count()} of Transaction records")

        return crm_df, trx_df