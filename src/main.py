from src.data_loader import DataLoader
from src.data_quality import DataQualityChecker
from src.reconciliation import CustomerReconciliation
from src.golden_record import GoldenRecordBuilder
from utils.spark_utils import create_spark_session
from utils.general_utils import read_yaml_config as ryc
from utils.logging_utils import get_logger
from pyspark.sql.functions import col

config_path = "config/config.yaml"
config = ryc(config_path)
logger = get_logger(config)



# class RunPipeline:
#     def __init__(self) -> None:
#         pass


def run_pipeline():
    logger.info("Starting Golden Customer Record Data Quality Check")
    try:
        logger.info("Starting a Spark Session")
        spark = create_spark_session(
            app_name=config['spark']['app_name'],
            master=config['spark']['master'],
            partition=config["spark"]["shuffle_partitions"]
        )
        

        # Initialize DataLoader and load data
        logger.info("Loading data using DataLoader...")
        data_loader = DataLoader(spark, config)
        crm_df, trx_df = data_loader.load_data()


        # Initialize DataQualityChecker and perform quality checks
        logger.info("Performing data quality checks...")
        data_quality_checker = DataQualityChecker(spark, config)
        
        # Generating the report
        crm_report = data_quality_checker.generate_report(crm_df, "CRM")
        trx_report = data_quality_checker.generate_report(trx_df, "TRX")

        # Save the reports to JSON files
        data_quality_checker.save_report(crm_report, path=config['output']['quality_report_path'])
        data_quality_checker.save_report(trx_report, path=config['output']['quality_report_path'])

        # Checking records saved. 

        logger.info(f"CRM records: {crm_report['total_records']}")
        logger.info(f"Transaction records: {trx_report['total_records']}")

        # Reconcile the data
        logger.info("Reconciling data using CustomerReconciliation...")
        reconciliation = CustomerReconciliation(spark, config)
        final_reconciliation, stats = reconciliation.reconcile(crm_df, trx_df)


        logger.info(f"Reconciliation stats...")
        for row in stats.collect():
            logger.info(f"  {row['match_type']}: {row['count']} ({row['percentage']}%)")

        #  Getting the golden results
        logger.info("Building Golden Customer Record...")
        golden = GoldenRecordBuilder(spark, config)
        golden_record = golden.build_golden_record(crm_df, trx_df, final_reconciliation)
        golden_report = data_quality_checker.generate_report(golden_record, source_name="GOLDEN")
        data_quality_checker.save_report(golden_report, path=config['output']['quality_report_path'])

        logger.info("Creating the business layer gold subset")
        ba_gold_df = golden_record.select(
                        "gold_customer_id",
                        "first_name",
                        "last_name",
                        "email",
                        "phone",
                        "address",
                        "city",
                        "country",
                        "registration_date",
                        "first_purchase_date",
                        "last_purchase_date",
                        "total_transactions",
                        "customer_tenure_days",
                        "customer_tenure_months",
                        "is_active",
                        "data_quality_score"
                    )
        # Saving the golden records as a parquet file
        try:
            logger.info("Writing the golden_record ")
            golden_record.write.mode("overwrite").parquet(path=f"{config['output']['golden_record_path']}/gold_raw")
            ba_gold_df.write.mode("overwrite").parquet(path=f"{config['output']['golden_record_path']}/ba_gold")
        except KeyError as e:
            logger.info("The path doesn't exist: {e}")
            raise

        # Saving the golden record as a parquet file
        try:
            logger.info("Writing the golden_record ")
            golden_record.write.mode("overwrite").parquet(path=f"{config['output']['golden_record_path']}/gold_raw")
            ba_gold_df.write.mode("overwrite").parquet(path=f"{config['output']['golden_record_path']}/ba_gold")
        except KeyError as e:
            logger.info("The path doesn't exist: {e}")
            raise

        logger.info("=="*30)
        logger.info(f"Total CRM Records: {crm_df.count()}")
        logger.info(f"Total Transaction Records: {trx_df.count()}")
        logger.info(f"Golden Records Created: {golden_record.count()}")
        logger.info(f"Match Rate: {final_reconciliation.filter(col('match_type') != 'unmatched').count() / final_reconciliation.count() * 100:.2f}%")
        logger.info("=="*30)
        logger.info("Showing sample data: ")
        golden_record.select(
            "*"
        ).show(10, truncate=20)
    except Exception as e:
        logger.info(f"Pipeline failed with the exception message: {str(e)}")
        raise

    spark.stop()

if __name__ == "__main__":
    run_pipeline()