import os
from dotenv import load_dotenv


load_dotenv()

pyspark_python = os.getenv("PYSPARK_DRIVER_PYTHON", "")
pyspark_driver_python = os.getenv("PYSPARK_DRIVER_PYTHON", "")


if pyspark_python:
    os.environ["PYSPARK_PYTHON"] = pyspark_python
if pyspark_driver_python:
    os.environ["PYSPARK_DRIVER_PYTHON"] = pyspark_driver_python

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, lit
from utils.logging_utils import get_logger
from utils.general_utils import read_yaml_config as ryc


config_path = ("config/config.yaml")
config = ryc(config_path)
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
            .config("spark.pyspark.python", pyspark_python) \
            .config("spark.pyspark.driver.python", pyspark_driver_python) \
            .getOrCreate())


def lineage_tracking(df: DataFrame, source: str) -> DataFrame:
    """Add audit columns for lineage tracking."""
    return df.withColumn("source", lit(source)) \
        .withColumn("ingestion_timestamp", current_timestamp())