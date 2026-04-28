from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, count, min as spark_min, max as spark_max
from utils.logging_utils import get_logger
from utils.general_utils import read_yaml_config as ryc
from datetime import datetime
import json
import os

config_path = ("config/config.yaml")
config = ryc(config_path)
logger = get_logger(config)

class DataQualityChecker:
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.email_pattern = self.config['data_quality']['email_pattern']
        self.phone_pattern = self.config['data_quality']['phone_pattern'] 
        self.columns_to_check = self.config['data_quality']['columns_to_check']   

        
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


    def check_format_validity(self, df: DataFrame) -> dict[str, dict]:
        """
        Check for format validity in the email and phone columns by counting values that match a regex pattern.
        """
        logger.info(f"Checking format validity for email column")

        metrics = {}

        if "normalized_email" in df.columns:
            email_col = col("normalized_email")

            agg_result = df.select(
                count(when(email_col.rlike(self.email_pattern), 1)).alias("email_valid_count"),
                count(when(email_col.isNotNull(), 1)).alias("email_total_count")
            ).collect()[0].asDict()

            total_emails = agg_result.get("email_total_count", 0)
            valid_emails = agg_result.get("email_valid_count", 0)

            if total_emails > 0:
                email_validity_percentage = (valid_emails / total_emails * 100)
            else:
                email_validity_percentage = 0

            metrics["email"] = {
                "valid_count": valid_emails,
                "total_records": total_emails,
                "invalid_count": total_emails - valid_emails,
                "validity_percentage": round(email_validity_percentage, 2)
            }

        if "normalized_phone" in df.columns:
            logger.info(f"Checking format validity for phone column")

            phone_col = col("normalized_phone")

            agg_result = df.select(
                count(when(phone_col.rlike(self.phone_pattern), 1)).alias("phone_valid_count"),
                count(when(phone_col.isNotNull(), 1)).alias("phone_total_count")
            ).collect()[0].asDict()

            total_phones = agg_result.get("phone_total_count", 0)
            valid_phones = agg_result.get("phone_valid_count", 0)

            if total_phones > 0:
                phone_validity_percentage = (valid_phones / total_phones * 100)
            else:
                phone_validity_percentage = 0

            metrics["phone"] = {
                "valid_count": valid_phones,
                "total_records": total_phones,
                "invalid_count": total_phones - valid_phones,
                "validity_percentage": round(phone_validity_percentage, 2)
            }

        return metrics

    
    def check_uniqueness(self, df: DataFrame, column_names: list[str]) -> dict[str, any]:
        """
        Checking for uniqueness by checking for duplicates  
        """
        logger.info(f"Checking uniqueness for columns: {', '.join(column_names)}")

        metrics = {}
        for column in column_names:
            if column in df.columns:
                total_records = df.count()
                distinct_records = df.select(column).distinct().count()
                duplicate_count = total_records - distinct_records

                duplicate_groups_count = df.groupBy(column).count().filter(col("count") > 1).count()

                metrics[column] = {
                    "total_records": total_records,
                    "distinct_records": distinct_records,
                    "duplicate_count": duplicate_count,
                    "duplicate_groups_count": duplicate_groups_count,
                    "uniqueness_percentage": round((distinct_records / total_records * 100), 2) if total_records > 0 else 0
                }
    
        return metrics
    
    def check_freshness(self, df: DataFrame, date_column: str) -> dict[str, any]:
        """
        Check for freshness by calculating the time difference between the current timestamp and the most recent timestamp in the specified column.
        """
        logger.info(f"Checking freshness based on timestamp column '{date_column}'")

        if date_column not in df.columns:
            logger.warning(f"Timestamp column '{date_column}' not found in DataFrame. Cannot check freshness.")
            return {
                "most_recent_timestamp": None,
                "freshness_in_seconds": None            
            }

        most_recent_timestamp = df.select(
            spark_min(col(date_column)).alias("min_date"),
            spark_max(col(date_column)).alias("max_date")
        ).collect()[0]

      

        return {
            "column": date_column,
            "min_date": str(most_recent_timestamp["min_date"]),
            "max_date": str(most_recent_timestamp["max_date"])
        }
    

    def generate_report(self, df: DataFrame, source_name: str) -> dict[str, any]: 
        
        """
        Generate a comprehensive data quality report.
        """
        logger.info(f"Generating data quality report for {source_name}...")
        required_columns = self.columns_to_check
        total_records = df.count()
        current_timestamp = datetime.now().isoformat()

        report = {
            "source": source_name,
            "total_records": total_records,
            "generated_at": current_timestamp,
            "column_count": len(df.columns),
            "completeness": self.check_completeness(df, [c for c in required_columns if c in df.columns]),
            "format_validity": self.check_format_validity(df),
            "uniqueness": self.check_uniqueness(df, ["customer_id"] if "customer_id" in df.columns else ["transaction_id"]),
            "freshness": self.check_freshness(df, "last_updated" if "last_updated" in df.columns else "purchase_date")
        }

        return report
    
    def save_report(self, report: dict, path:str) -> None:
        """
        Save the data quality report as a JSON file in the data quality_reports directory.
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        today_date = datetime.now().strftime("%d-%m-%y")
        output_dir = path
        date_dir = os.path.join(output_dir, today_date)
        output_path = output_path = os.path.join(date_dir, f"{report['source']}_data_quality_report_{timestamp}.json")
        logger.info(f"Saving data quality report to {output_path}...")
        
        # Ensure the output directory exists
        if not os.path.exists(date_dir):
            os.makedirs(date_dir)


        with open(output_path, 'w') as f:
            json.dump(report, f, indent=4, default=str)

        logger.info(f"Data quality report saved successfully to {output_path}")