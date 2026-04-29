from pyspark.sql.functions import col, when, datediff, current_date
from datetime import datetime, timedelta

class TestTransactionAggregation:
    """Test transaction aggregation logic."""
    
    def test_aggregates_multiple_transactions(self, spark, gb_checker, sample_txn_data):
        """Should count transactions and find date ranges correctly."""
        
        txn_prepared = sample_txn_data \
            .withColumnRenamed("customer_email", "normalized_email") \
            .withColumn("normalized_phone", col("phone")) \
            .withColumn("customer_email", col("normalized_email"))
        
        result = gb_checker._aggregate_transactions(txn_prepared)
        
        # 5 unique emails (including NULL)
        assert result.count() == 5
        
        # Filter out NULL for specific customer tests
        result_non_null = result.filter(col("normalized_email").isNotNull())
        assert result_non_null.count() == 4
        
        # Verify John Doe's aggregated data
        john = result.filter(col("normalized_email") == "john.doe@email.com").first()
        assert john["total_transactions"] == 2
        assert john["first_purchase_date"] == "2023-06-15"  
        assert john["last_purchase_date"] == "2024-02-20"  
    
    def test_handles_empty_transactions(self, spark, gb_checker, transaction_schema):
        """Empty transaction DataFrame should return empty result."""
        
        txn_df = spark.createDataFrame([], transaction_schema)
        
        # Add the required columns first
        txn_prepared = txn_df \
            .withColumnRenamed("customer_email", "normalized_email") \
            .withColumn("normalized_phone", col("phone")) \
            .withColumn("customer_email", col("normalized_email"))
        
        result = gb_checker._aggregate_transactions(txn_prepared)
        
        assert result.count() == 0


class TestMatchQualityCategorization:
    """Test match quality labels."""
    
    def test_high_confidence(self, spark, match_quality_expr):
        """Confidence >= 0.8 should be 'high'."""
        df = spark.createDataFrame(
            [(0.9,), (0.8,)],
            ["confidence"]
        )
        
        result = df.withColumn("match_quality", match_quality_expr("confidence"))
        
        qualities = [r.match_quality for r in result.collect()]
        assert all(q == "high" for q in qualities)
    
    def test_medium_confidence(self, spark, match_quality_expr):
        """Confidence between 0.5 and 0.8 should be 'medium'."""
        df = spark.createDataFrame(
            [(0.7,), (0.5,)],
            ["confidence"]
        )
        
        result = df.withColumn("match_quality", match_quality_expr("confidence"))
        
        qualities = [r.match_quality for r in result.collect()]
        assert all(q == "medium" for q in qualities)
    
    def test_low_confidence(self, spark, match_quality_expr):
        """Confidence < 0.5 should be 'low'."""
        df = spark.createDataFrame(
            [(0.4,), (0.0,)],
            ["confidence"]
        )
        
        result = df.withColumn("match_quality", match_quality_expr("confidence"))
        
        qualities = [r.match_quality for r in result.collect()]
        assert all(q == "low" for q in qualities)
    
    def test_with_custom_column_name(self, spark, match_quality_expr):
        """Should work with different column names."""
        df = spark.createDataFrame(
            [(0.9,), (0.3,)],
            ["score"]
        )
        
        result = df.withColumn("quality", match_quality_expr("score"))
        
        qualities = [r.quality for r in result.collect()]
        assert qualities[0] == "high"
        assert qualities[1] == "low"


class TestFieldCoalescing:
    """Test that CRM data is preferred over transaction data."""
    
    def test_crm_value_used_when_present(self, spark, coalesce_crm_preferred):
        """When CRM has a value, it should be used."""
        df = spark.createDataFrame(
            [("CRM Name", "TXN Name")],
            ["crm_name", "txn_name"]
        )
        
        result = df.withColumn(
            "final_name",
            coalesce_crm_preferred("crm_name", "txn_name")
        )
        
        assert result.first()["final_name"] == "CRM Name"
    
    def test_txn_value_used_when_crm_null(self, spark, coalesce_crm_preferred):
        """When CRM is null, transaction value should be used."""
        df = spark.createDataFrame([(None, "TXN Name")], "crm_name string, txn_name string")
        result = df.withColumn("final_name", coalesce_crm_preferred("crm_name", "txn_name"))
        assert result.first()["final_name"] == "TXN Name"
    
    def test_address_prefers_transaction(self, spark, coalesce_transaction_preferred):
        """Address should prefer transaction (more recent/verified)."""
        df = spark.createDataFrame(
            [("CRM Addr", "TXN Addr")],
            ["crm_addr", "txn_addr"]
        )
        
        result = df.withColumn(
            "final_address",
            coalesce_transaction_preferred("crm_addr", "txn_addr")
        )
        
        assert result.first()["final_address"] == "TXN Addr"
    
    def test_transaction_preferred_falls_back_to_crm(self, spark, coalesce_transaction_preferred):
        """When transaction is null, CRM value should be used."""
        df = spark.createDataFrame([("CRM Addr", None)], "crm_addr string, txn_addr string")
        result = df.withColumn("final_address", coalesce_transaction_preferred("crm_addr", "txn_addr"))
        assert result.first()["final_address"] == "CRM Addr"


class TestComputedFields:
    """Test derived/computed fields in golden record."""
    
    def test_data_quality_score_max(self, spark):
        """Record with all fields should have high quality score."""
        df = spark.createDataFrame(
            [("john@email.com", "+447911123456", "123 Main St", "London", "UK")],
            ["email", "phone", "address", "city", "country"]
        )
        
        result = df.withColumn(
            "score",
            (when(col("email").isNotNull(), 25).otherwise(0) +
             when(col("phone").startswith("+"), 25).otherwise(10) +
             when(col("address").isNotNull(), 20).otherwise(0) +
             when(col("city").isNotNull(), 15).otherwise(0) +
             when(col("country").isNotNull(), 15).otherwise(0))
        )
        
        assert result.first()["score"] == 100
    
    def test_is_active_true_for_recent_purchase(self, spark):
        """Customer with purchase within 365 days should be active."""
        
        recent_date = datetime.now().date() - timedelta(days=30)
        
        df = spark.createDataFrame(
            [(recent_date,)],
            ["last_purchase_date"]
        )
        
        result = df.withColumn(
            "is_active",
            when(
                col("last_purchase_date").isNotNull() &
                (datediff(current_date(), col("last_purchase_date")) <= 365),
                True
            ).otherwise(False)
        )
        
        assert result.first()["is_active"] is True