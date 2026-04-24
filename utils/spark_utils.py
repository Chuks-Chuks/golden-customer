from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import col, to_date, when, lit, trim, lower, regexp_replace, coalesce
from utils.logging_utils import get_logger
from utils.general_utils import read_yaml_config as ryc

config = ryc("config.yaml")
logger = get_logger(config)

def create_spark_session(app_name: str = "GoldenCustomerRecord", 
                        master: str = "local[*]") -> SparkSession:
    """Create and configure a Spark session."""

    logger.info(f"Creating Spark session with app name '{app_name}' and master '{master}'")
    return (SparkSession.builder \
            .appName(app_name) \
            .master(master) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .getOrCreate())


def normalize_email(col_name: str = "email") -> Column:
    """Normalize email to lowercase, trimmed, no spaces."""

    logger.info(f"Normalizing email column '{col_name}'")

    return when(
        col(col_name).isNotNull(),
        lower(regexp_replace(trim(col(col_name)), "\\s+", ""))
    ).otherwise(lit(None))


def normalize_phone(col_name: str = "phone") -> Column:
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


def normalising_date_columns(dataframe: DataFrame, date_columns: list) -> DataFrame:
    """Normalize date columns to a standard format (yyyy-MM-dd)."""
    for col_name in date_columns:
        dataframe = dataframe.withColumn(
            f"{col_name}_normalized",
            coalesce(to_date(col(col_name), "yyyy-MM-dd"),
                    to_date(col(col_name), "yyyyMMdd"))
        )

        # Ensuring the date has been parsed correctly.
        try:
            error_count = dataframe.filter(col(f"{col_name}_normalized").isNull() & col(col_name).isNotNull()).count()
            if error_count > 0:
                logger.warning(f"Warning: {error_count} records in column '{col_name}' could not be parsed as dates.")
        except Exception as e:
            logger.error(f"Error occurred while parsing column '{col_name}': {e}")
    
    if len(date_columns) > 1:
        logger.info(f"Date columns normalized: {', '.join(date_columns)}")
    else:
        logger.info("Date column normalized.")

   
    return dataframe