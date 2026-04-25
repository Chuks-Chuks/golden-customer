from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, count, lit, isnull, length, regexp_replace, current_timestamp, coalesce, sum
from utils.logging_utils import get_logger
from utils.general_utils import read_yaml_config as ryc
from utils.spark_utils import create_spark_session

config_path = ("config/config.yaml")
config = ryc(config_path)
logger = get_logger(config)

class DataQualityChecker:
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.email_pattern = self.config['data_quality']['email_pattern']
        self.phone_pattern = self.config['data_quality']['phone_pattern']    

        
    def check_completeness(self, df: DataFrame, required_columns: list) -> dict[str, dict]:
        """
        Check for completeness by counting null or empty values in required columns.
        """
        logger.info("Checking data completeness...")

        total_records = df.count()
        metrics = {}

        agg_exprs = [count(when(col(column).isNotNull(), 1)).alias(f"{column}_complete_count") for column in required_columns if column in df.columns]
        agg_result = df.agg(*agg_exprs).collect()[0].asDict()

        for column in required_columns:
            if column in df.columns:
                complete_count = agg_result.get(f"{column}_complete_count", 0)
                completeness_percentage = (complete_count / total_records * 100) if total_records > 0 else 0
                metrics[column] = {
                    "complete_count": complete_count,
                    "total_records": total_records,
                    "null_count": total_records - complete_count,
                    "completeness_percentage": round(completeness_percentage, 2)
                }
            else:
                logger.warning(f"Column '{column}' not found in DataFrame. Skipping completeness check for this column.")
                metrics[column] = {
                    "complete_count": None,
                    "total_records": total_records,
                    "completeness_percentage": None
                }

        return metrics
