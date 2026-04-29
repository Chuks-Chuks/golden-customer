
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType
)
from pyspark.sql.functions import (lower, regexp_replace, trim, when, col, lit, coalesce)
import shutil
from pathlib import Path
import csv
from src.data_quality import DataQualityChecker
from src.reconciliation import CustomerReconciliation
from src.golden_record import GoldenRecordBuilder


# PYSPARK SESSION FIXTURES

@pytest.fixture(scope="session")
def spark():
    """
    Session-scoped Spark session for all tests.
    
    Using session scope dramatically improves test performance by 
    reusing the same SparkSession across all test modules.
    """
    spark_session = (SparkSession.builder
                    .master("local[2]")
                    .appName("GoldenRecordTests")
                    .config("spark.sql.shuffle.partitions", "2")
                    .config("spark.sql.adaptive.enabled", "false")
                    .config("spark.driver.memory", "2g")
                    .config("spark.ui.enabled", "false")
                    .config("spark.sql.warehouse.dir", "/tmp/spark-test-warehouse")
                    .getOrCreate())
    
    # Set log level to reduce noise during tests
    spark_session.sparkContext.setLogLevel("WARN")
    
    yield spark_session
    
    spark_session.stop()


@pytest.fixture(scope="function")
def spark_test(spark):
    """
    Function-scoped Spark session for tests needing isolation.
    
    Creates a new session derived from the main session, providing
    temporary view/table isolation between tests.
    """
    return spark.newSession()


# CONFIGURATION FIXTURES

@pytest.fixture
def test_config():
    """Production-like test configuration with all required sections."""
    return {
        "input": {
            "crm_path": "tests/data/test_crm.csv",
            "transaction_path": "tests/data/test_transaction.csv"
        },
        "output": {
            "golden_record_path": "tests/data/output/golden",
            "quality_report_path": "tests/data/output/reports"
        },
        "spark": {
            "app_name": "TestPipeline",
            "master": "local[2]",
            "shuffle_partitions": "2"
        },
        "reconciliation": {
            "match_keys": ["email", "phone"],
            "confidence_scoring": {
                "email_match": 1.0,
                "phone_match": 0.8,
                "name_address_match": 0.6
            },
            "confidence_threshold": 0.5
        },
        "data_quality": {
            "email_pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
            "phone_pattern": r"^\+[1-9]\d{10,14}$",
            "columns_to_check": [
                "first_name", "last_name", "normalized_email",
                "normalized_phone", "city", "country"
            ]
        },
        "logging": {
            "log_dir": "test_logs/",
            "log_file": "test_pipeline.log",
            "level": "WARN",
            "format": "%(asctime)s - %(levelname)s - %(message)s"
        }
    }


# SCHEMA FIXTURES

@pytest.fixture
def crm_schema():
    """CRM input schema with all expected fields."""
    return StructType([
        StructField("customer_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("registration_date", StringType(), True),
        StructField("last_updated", StringType(), True),
    ])


@pytest.fixture
def transaction_schema():
    """Transaction input schema with all expected fields."""
    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_email", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("shipping_address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("purchase_date", StringType(), True),
    ])


@pytest.fixture
def mapping_schema():
    """Mapping schema with all expected fields."""
    return StructType([
        StructField("crm_customer_id", StringType(), True),
        StructField("transaction_id", StringType(), True),
        StructField("match_type", StringType(), True),
        StructField("confidence_score", DoubleType(), True),
        StructField("gold_customer_id", StringType(), True),
    ])


# CRM SAMPLE DATA

@pytest.fixture
def sample_crm_data(spark, crm_schema):
    """
    Create sample CRM data covering key scenarios:
    - Complete records with all fields
    - Records missing phone numbers
    - Records with different date formats (YYYYMMDD)
    - Records with null emails
    - Duplicate records for dedup testing
    """
    
    data = [
        # Complete record - UK
        ("CRM001", "John", "Doe", "john.doe@email.com", "+447911123456",
         "123 Main St", "Manchester", "UK", "2021-01-15", "2024-01-15"),
        
        # Complete record - France
        ("CRM002", "Marie", "Curie", "marie.curie@email.fr", "+33612345678",
         "456 Rue Paris", "Paris", "France", "2020-06-01", "2023-12-01"),
        
        # Missing phone
        ("CRM003", "Bob", "Wilson", "bob.wilson@email.com", None,
         "789 Oak Ave", "London", "UK", "2019-03-10", "2024-02-01"),
        
        # Missing email
        ("CRM004", "Alice", "Smith", None, "+44876543210",
         "321 Pine Rd", "Glasgow", "UK", "2022-11-20", "2023-11-20"),
        
        # YYYYMMDD date format
        ("CRM005", "Tom", "Jones", "tom.jones@email.com", "+44123456789",
         "555 Elm Blvd", "Manchester", "UK", "20211015", "20231015"),
        
        # Duplicate - older version
        ("CRM001", "John", "Doe", "john.doe@email.com", "+447911123456",
         "100 Old Address", "London", "UK", "2021-01-15", "2023-06-01"),
    ]
    
    return spark.createDataFrame(data, schema=crm_schema)


# TRANSACTION SAMPLE DATA

@pytest.fixture
def sample_txn_data(spark, transaction_schema):
    """
    Create sample transaction data covering:
    - Transactions matching CRM customers by email
    - Transactions matching CRM customers by phone
    - Transactions from new customers (no CRM match)
    - Multiple transactions per customer
    - Transactions with missing contact info
    """
    
    data = [
        # Matches CRM001 by email and phone
        ("TXN001", "john.doe@email.com", "John", "Doe", "+447911123456",
         "123 Main St", "Manchester", "UK", "2023-06-15"),
        
        # Matches CRM001 - second transaction
        ("TXN002", "john.doe@email.com", "John", "Doe", "+447911123456",
         "123 Main St", "Manchester", "UK", "2024-02-20"),
        
        # Matches CRM002 by email only (different phone)
        ("TXN003", "marie.curie@email.fr", "Marie", "Curie", "+33699999999",
         "789 New Ave", "Lyon", "France", "2023-08-10"),
        
        # New customer - no CRM match
        ("TXN004", "new.user@email.com", "New", "User", "+1234567890",
         "999 New St", "London", "UK", "2024-01-05"),
        
        # Missing contact info
        ("TXN005", None, "Guest", "User", None,
         "N/A", "Unknown", "UK", "2023-01-01"),
        
        # Matches CRM003 by phone only
        ("TXN006", "different.email@email.com", "Bob", "Wilson", "+44876543210",
         "900 New St", "Edinburgh", "UK", "2023-12-01"),
    ]
    
    return spark.createDataFrame(data, schema=transaction_schema)

@pytest.fixture
def create_crm_df(spark):
    """
    Factory fixture to create CRM DataFrames with required columns.
    
    Usage:
        crm = create_crm_df(
            customer_id="CRM001",
            normalized_email="john@email.com"
        )
    """
    from pyspark.sql.types import StructType, StructField, StringType
    
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("normalized_email", StringType(), True),
        StructField("normalized_phone", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
    ])
    
    def _create(**kwargs):
        # Build a tuple in the correct order (must match schema order)
        row = (
            kwargs.get("customer_id"),
            kwargs.get("normalized_email"),
            kwargs.get("normalized_phone"),
            kwargs.get("first_name"),
            kwargs.get("last_name"),
            kwargs.get("city"),
            kwargs.get("country"),
        )
        return spark.createDataFrame([row], schema)
    
    return _create


@pytest.fixture
def create_txn_df(spark):
    """
    Factory fixture to create Transaction DataFrames with required columns.
    """
    from pyspark.sql.types import StructType, StructField, StringType
    
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("normalized_email", StringType(), True),
        StructField("normalized_phone", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
    ])
    
    def _create(**kwargs):
        # Build a tuple in the correct order (must match schema order)
        row = (
            kwargs.get("transaction_id"),
            kwargs.get("normalized_email"),
            kwargs.get("normalized_phone"),
            kwargs.get("first_name"),
            kwargs.get("last_name"),
            kwargs.get("city"),
            kwargs.get("country"),
        )
        return spark.createDataFrame([row], schema)
    
    return _create


# RECONCILIATION MAPPING DATA

@pytest.fixture
def sample_mapping_data(spark):
    """
    Sample reconciliation mapping with all match types.
    """
    
    data = [
        # High confidence email match
        ("CRM001", "TXN001", "email", 1.0, "GOLD_CRM001"),
        # Medium confidence phone match
        ("CRM003", "TXN006", "phone", 0.8, "GOLD_CRM003"),
        # Lower confidence name/address match
        ("CRM005", "TXN002", "name_address", 0.6, "GOLD_CRM005"),
        # Unmatched
        ("CRM004", None, None, 0.0, "GOLD_CRM004"),
    ]
    
    return spark.createDataFrame(data, schema=mapping_schema)


# SHARED TRANSFORM HELPERS

# For email formatting
@pytest.fixture
def normalize_email_expr():
    """
    Reusable email normalization expression for tests.

    Mirrors production logic:
    - trim
    - remove whitespace
    - lowercase
    """

    def _expr(column_name: str):
        return when(
            col(column_name).isNotNull(),
            lower(regexp_replace(trim(col(column_name)), "\\s+", ""))
        ).otherwise(lit(None))

    return _expr

# For date parsing
@pytest.fixture
def parse_date_expr():
    """
    Reusable date parsing expression supporting multiple formats.
    """
    from pyspark.sql.functions import coalesce, to_date, col

    def _expr(column_name: str):
        return coalesce(
            to_date(col(column_name), "yyyy-MM-dd"),
            to_date(col(column_name), "yyyyMMdd")
        )

    return _expr

@pytest.fixture
def match_quality_expr():
    """
    Reusable match quality categorization expression.
    
    Returns a Column expression that categorizes confidence scores:
    - high: confidence >= 0.8
    - medium: confidence >= 0.5
    - low: confidence < 0.5
    """
    from pyspark.sql.functions import when, col
    
    def _expr(confidence_col: str = "confidence"):
        return when(col(confidence_col) >= 0.8, "high") \
                .when(col(confidence_col) >= 0.5, "medium") \
                .otherwise("low")
    
    return _expr

@pytest.fixture
def coalesce_crm_preferred():
    """
    Returns a Column expression that prefers CRM values over transaction values.
    
    Usage:
        df.withColumn("final_name", coalesce_crm_preferred("crm_name", "txn_name"))
    """
    
    def _expr(crm_col: str, txn_col: str):
        return coalesce(col(crm_col), col(txn_col))
    
    return _expr


@pytest.fixture
def coalesce_transaction_preferred():
    """
    Returns a Column expression that prefers transaction values over CRM values.
    
    Use this for fields where transaction data is considered more reliable/current
    (e.g., address, phone).
    
    Usage:
        df.withColumn("final_address", coalesce_transaction_preferred("crm_addr", "txn_addr"))
    """
    
    def _expr(crm_col: str, txn_col: str):
        return coalesce(col(txn_col), col(crm_col))
    
    return _expr




# INSTANCE HELPERS

@pytest.fixture
def dq_checker(spark, test_config):
    """Reusable DataQualityChecker instance."""
    return DataQualityChecker(spark, test_config)


@pytest.fixture
def cr_checker(spark, test_config):
    return CustomerReconciliation(spark, test_config)

@pytest.fixture
def gb_checker(spark, test_config):
    return GoldenRecordBuilder(spark, test_config)


# UTILITY HELPERS

@pytest.fixture(autouse=True)
def clean_test_output():
    """
    Automatically clean test output directories.
    
    autouse=True ensures this runs before/after every test 
    without needing to be explicitly referenced.
    """
    
    dirs_to_clean = [
        Path("tests/data/output"),
        Path("test_logs"),
    ]
    
    for dir_path in dirs_to_clean:
        if dir_path.exists():
            shutil.rmtree(dir_path)
    
    yield
    
    for dir_path in dirs_to_clean:
        if dir_path.exists():
            shutil.rmtree(dir_path)


def create_temp_csv(tmp_path, headers, rows, filename):
    """Helper to create temporary CSV files for load tests."""
    
    filepath = tmp_path / filename
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    with open(filepath, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    
    return str(filepath)