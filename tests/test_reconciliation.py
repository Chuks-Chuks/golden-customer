from pyspark.sql.functions import col

from src.reconciliation import CustomerReconciliation

# MATCHING STRATEGY TESTS


class TestMatchingLogic:
    """Test individual matching strategies."""

    def test_email_match(self, spark, cr_checker):
        crm = spark.createDataFrame(
            [("CRM001", "john@email.com")],
            ["customer_id", "normalized_email"]
        )

        txn = spark.createDataFrame(
            [("TXN001", "john@email.com")],
            ["transaction_id", "normalized_email"]
        )

        result = cr_checker.find_email_matches(crm, txn)

        assert result.count() == 1
        row = result.collect()[0]

        assert row["crm_customer_id"] == "CRM001"
        assert row["transaction_id"] == "TXN001"
        assert row["match_type"] == "email"
        assert row["confidence_score"] == 1.0

    def test_phone_match(self, spark, cr_checker):
        crm = spark.createDataFrame(
            [("CRM001", "+447911123456")],
            ["customer_id", "normalized_phone"]
        )

        txn = spark.createDataFrame(
            [("TXN001", "+447911123456")],
            ["transaction_id", "normalized_phone"]
        )

        result = cr_checker.find_phone_matches(crm, txn)

        assert result.count() == 1
        assert result.collect()[0]["match_type"] == "phone"

    def test_name_address_match(self, spark, cr_checker):
        crm = spark.createDataFrame(
            [("CRM001", "John", "Doe", "London", "UK")],
            ["customer_id", "first_name", "last_name", "city", "country"]
        )

        txn = spark.createDataFrame(
            [("TXN001", "John", "Doe", "London", "UK")],
            ["transaction_id", "first_name", "last_name", "city", "country"]
        )

        result = cr_checker.find_name_address_matches(crm, txn)

        assert result.count() == 1
        assert result.collect()[0]["match_type"] == "name_address"

    def test_null_email_does_not_match(self, spark, cr_checker):
        """Null emails should never match."""
        crm = spark.createDataFrame([("CRM001", None)], "customer_id string, normalized_email string")
        txn = spark.createDataFrame([("TXN001", None)], "transaction_id string, normalized_email string")
        result = cr_checker.find_email_matches(crm, txn)
        assert result.count() == 0


# RECONCILIATION PIPELINE TESTS

class TestReconciliationPipeline:
    """Test full reconciliation behavior."""

    def test_threshold_filtering(self, spark, test_config):
        """Matches below threshold should be removed."""
        
        test_config["reconciliation"]["confidence_threshold"] = 0.9
        cr_checker = CustomerReconciliation(spark, test_config)
        
        # Add all required columns with dummy values
        crm = spark.createDataFrame(
            [("CRM001", None, "+447911123456", "Dummy", "User", "City", "Country")],
            "customer_id string, normalized_email string, normalized_phone string, first_name string, last_name string, city string, country string"
        )
        
        txn = spark.createDataFrame(
            [("TXN001", None, "+447911123456", "Dummy", "User", "City", "Country")],
            "transaction_id string, normalized_email string, normalized_phone string, first_name string, last_name string, city string, country string"
        )
        
        final_df, _ = cr_checker.reconcile(crm, txn)
        assert final_df.filter(col("match_type").isNotNull()).count() == 0

    def test_match_prioritization(self, spark, cr_checker):
        """Email should win over phone when both match."""
        
        crm = spark.createDataFrame(
            [("CRM001", "john@email.com", "+447911123456", "John", "Doe", "London", "UK")],
            ["customer_id", "normalized_email", "normalized_phone", "first_name", "last_name", "city", "country"]
        )
        
        txn = spark.createDataFrame(
            [("TXN001", "john@email.com", "+447911123456", "John", "Doe", "London", "UK")],
            ["transaction_id", "normalized_email", "normalized_phone", "first_name", "last_name", "city", "country"]
        )
        
        final_df, _ = cr_checker.reconcile(crm, txn)
        
        row = final_df.filter(col("match_type").isNotNull()).collect()[0]
        assert row["match_type"] == "email"

    def test_multiple_match_types_same_pair(self, spark, cr_checker):
        """Same record matching via multiple strategies should resolve correctly."""

        crm = spark.createDataFrame(
            [("CRM001", "john@email.com", "+447911123456", "John", "Doe", "London", "UK")],
            ["customer_id", "normalized_email", "normalized_phone", "first_name", "last_name", "city", "country"]
        )

        txn = spark.createDataFrame(
            [("TXN001", "john@email.com", "+447911123456", "John", "Doe", "London", "UK")],
            ["transaction_id", "normalized_email", "normalized_phone", "first_name", "last_name", "city", "country"]
        )

        final_df, _ = cr_checker.reconcile(crm, txn)

        row = final_df.filter(col("match_type").isNotNull()).collect()[0]

        assert row["match_type"] == "email"

    def test_one_record_per_customer(self, spark, cr_checker, create_crm_df, create_txn_df):
        """Should keep only one match per CRM customer."""
        
        crm = create_crm_df(customer_id="CRM001", normalized_email="john@email.com")
        
        txn1 = create_txn_df(transaction_id="TXN001", normalized_email="john@email.com")
        txn2 = create_txn_df(transaction_id="TXN002", normalized_email="john@email.com")
        txn = txn1.union(txn2)
        
        final_df, _ = cr_checker.reconcile(crm, txn)
        assert final_df.filter(col("match_type").isNotNull()).count() == 1

    def test_highest_confidence_selected(self, spark, cr_checker, create_crm_df, create_txn_df):
        """Highest confidence match should be selected."""
        
        crm = create_crm_df(
            customer_id="CRM001",
            normalized_email="john@email.com",
            normalized_phone="+447911123456",
            first_name="John",
            last_name="Doe",
            city="London",
            country="UK"
        )
        
        txn1 = create_txn_df(
            transaction_id="TXN001",
            normalized_email="john@email.com",
            first_name="John",
            last_name="Doe"
        )
        
        txn2 = create_txn_df(
            transaction_id="TXN002",
            normalized_phone="+447911123456"
        )
        
        txn = txn1.union(txn2)
        
        final_df, _ = cr_checker.reconcile(crm, txn)
        row = final_df.filter(col("match_type").isNotNull()).collect()[0]
        assert row["match_type"] == "email"

    def test_unmatched_records(self, spark, cr_checker):
        """Unmatched CRM records should still exist."""
        
        crm = spark.createDataFrame(
            [("CRM001", "a@email.com", None, "John", "Doe", "London", "UK")],
            "customer_id string, normalized_email string, normalized_phone string, first_name string, last_name string, city string, country string"
        )
        
        txn = spark.createDataFrame(
            [("TXN001", "b@email.com", None, "Jane", "Smith", "Paris", "France")],
            "transaction_id string, normalized_email string, normalized_phone string, first_name string, last_name string, city string, country string"
        )
        
        final_df, _ = cr_checker.reconcile(crm, txn)
        row = final_df.collect()[0]
        assert row["match_type"] is None
        assert row["confidence_score"] == 0.0


    def test_gold_customer_id_format(self, spark, cr_checker):
        """Gold ID should follow GOLD_<crm_id> format."""
        
        crm = spark.createDataFrame(
            [("CRM001", "john@email.com", None, "John", "Doe", "London", "UK")],
            "customer_id string, normalized_email string, normalized_phone string, first_name string, last_name string, city string, country string"
        )
        
        txn = spark.createDataFrame(
            [("TXN001", "john@email.com", None, "John", "Doe", "London", "UK")],
            "transaction_id string, normalized_email string, normalized_phone string, first_name string, last_name string, city string, country string"
        )
        
        final_df, _ = cr_checker.reconcile(crm, txn)
        assert final_df.collect()[0]["gold_customer_id"] == "GOLD_CRM001"

    def test_empty_inputs(self, spark, cr_checker):
        """Pipeline should handle empty inputs gracefully."""
        
        # Add all required columns to empty schemas
        empty_crm = spark.createDataFrame(
            [], 
            "customer_id string, normalized_email string, normalized_phone string, first_name string, last_name string, city string, country string"
        )
        empty_txn = spark.createDataFrame(
            [], 
            "transaction_id string, normalized_email string, normalized_phone string, first_name string, last_name string, city string, country string"
        )
        
        result, stats = cr_checker.reconcile(empty_crm, empty_txn)
        
        assert result.count() == 0
        assert stats.count() == 0
    

# STATS TESTS


class TestStatsOutput:
    """Test reconciliation stats output."""

    def test_stats_structure(self, spark, cr_checker):
        crm = spark.createDataFrame(
            [("CRM001", "john@email.com", None, "John", "Doe", "London", "UK")],
            "customer_id string, normalized_email string, normalized_phone string, first_name string, last_name string, city string, country string"
        )
        
        txn = spark.createDataFrame(
            [("TXN001", "john@email.com", None, "John", "Doe", "London", "UK")],
            "transaction_id string, normalized_email string, normalized_phone string, first_name string, last_name string, city string, country string"
        )
        
        _, stats = cr_checker.reconcile(crm, txn)
        
        assert "match_type" in stats.columns
        assert "count" in stats.columns
        assert "percentage" in stats.columns
        assert stats.count() > 0

    def test_stats_values(self, spark, cr_checker):
        """Percentages should sum ~100%."""
        
        crm = spark.createDataFrame(
            [
                ("CRM001", "a@email.com", None, "John", "Doe", "London", "UK"),
                ("CRM002", "b@email.com", None, "Jane", "Smith", "Paris", "France")
            ],
            "customer_id string, normalized_email string, normalized_phone string, first_name string, last_name string, city string, country string"
        )
        
        txn = spark.createDataFrame(
            [("TXN001", "a@email.com", None, "John", "Doe", "London", "UK")],
            "transaction_id string, normalized_email string, normalized_phone string, first_name string, last_name string, city string, country string"
        )
        
        _, stats = cr_checker.reconcile(crm, txn)
        
        total_percentage = sum(r["percentage"] for r in stats.collect())
        assert 99.0 <= total_percentage <= 101.0