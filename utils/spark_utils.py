from pyspark.sql import SparkSession
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
            .getOrCreate())
