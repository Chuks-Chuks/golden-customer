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


def run_pipeline():
    """Execute the complete Golden Customer Record pipeline."""
    spark = None
    logger.info("="*60)
    logger.info("STARTING GOLDEN CUSTOMER RECORD PIPELINE")
    logger.info("="*60)
    
    try:
        # Step 1: Initialize Spark
        logger.info("Initializing Spark session...")
        spark = create_spark_session(
            app_name=config['spark']['app_name'],
            master=config['spark']['master'],
            partition=config["spark"]["shuffle_partitions"]
        )
        
        # Step 2: Load data
        logger.info("STEP 1: Loading source data...")
        data_loader = DataLoader(spark, config)
        crm_df, trx_df = data_loader.load_data()
        
        # Step 3: Data quality assessment
        logger.info("STEP 2: Assessing data quality...")
        data_quality_checker = DataQualityChecker(spark, config)
        
        crm_report = data_quality_checker.generate_report(crm_df, "CRM")
        trx_report = data_quality_checker.generate_report(trx_df, "TRX")
        
        # Save quality reports
        data_quality_checker.save_report(
            crm_report, path=config['output']['quality_report_path']
        )
        data_quality_checker.save_report(
            trx_report, path=config['output']['quality_report_path']
        )
        
        logger.info(f"CRM Records: {crm_report['total_records']}")
        logger.info(f"Transaction Records: {trx_report['total_records']}")
        
        # Step 4: Customer reconciliation
        logger.info("STEP 3: Reconciling customers...")
        reconciliation = CustomerReconciliation(spark, config)
        final_reconciliation, stats = reconciliation.reconcile(crm_df, trx_df)
        
        logger.info("Reconciliation statistics:")
        for row in stats.collect():
            logger.info(
                f"  {row['match_type'] if row['match_type'] is not None else 'unmatched'}: "
                f"{row['count']} ({row['percentage']}%)"
            )
        
        # Step 5: Build golden record
        logger.info("STEP 4: Building Golden Customer Record...")
        golden = GoldenRecordBuilder(spark, config)
        golden_record = golden.build_golden_record(
            crm_df, trx_df, final_reconciliation
        )
        
        # Step 6: Validate golden record quality
        logger.info("STEP 5: Validating golden record quality...")
        golden_report = data_quality_checker.generate_report(
            golden_record, source_name="GOLDEN"
        )
        data_quality_checker.save_report(
            golden_report, path=config['output']['quality_report_path']
        )
        
        # Step 7: Create business analytics subset
        logger.info("STEP 6: Creating business analytics layer...")
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
        
        # Step 8: Save outputs
        logger.info("STEP 7: Saving golden records...")
        try:
            # Full golden record (all columns for audit/debugging)
            golden_record.write.mode("overwrite").parquet(
                f"{config['output']['golden_record_path']}/gold_raw"
            )
            
            # Business analytics subset (clean columns for analysts)
            ba_gold_df.write.mode("overwrite").parquet(
                f"{config['output']['golden_record_path']}/ba_gold"
            )
            
            logger.info("Golden records saved successfully!")
        except KeyError as e:
            logger.error(f"Output path configuration error: {e}")
            raise
        
        # Pipeline summary
        logger.info("="*60)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*60)
        
        total_matched = final_reconciliation.filter(
            col("match_type").isNotNull()
        ).count()
        total_records = final_reconciliation.count()
        match_rate = (total_matched / total_records * 100) if total_records > 0 else 0
        
        logger.info(f"Total CRM Records: {crm_df.count()}")
        logger.info(f"Total Transaction Records: {trx_df.count()}")
        logger.info(f"Golden Records Created: {golden_record.count()}")
        logger.info(f"Match Rate: {match_rate:.2f}%")
        logger.info("="*60)
        
        logger.info("Sample golden records:")
        golden_record.select(
            "gold_customer_id", "first_name", "last_name", 
            "email", "match_type", "confidence", "total_transactions"
        ).show(10, truncate=20)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    
    finally:
        if spark is not None and 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    run_pipeline()