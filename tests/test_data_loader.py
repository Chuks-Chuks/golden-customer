import pytest
from datetime import date
from pyspark.sql.functions import col
from tests.conftest import create_temp_csv
from src.data_loader import DataLoader, _normalize_phone


class TestEmailNormalization:
    """Test email normalization logic."""
    
    def test_basic_normalization(self, spark, normalize_email_expr):
        """Email should be lowercased and trimmed."""
        
        df = spark.createDataFrame(
            [("  John.Doe@Email.COM  ",)],
            ["email"]
        )
        
        result = df.withColumn(
            "normalized", normalize_email_expr("email")
        )
        
        assert result.first()["normalized"] == "john.doe@email.com"
    
    def test_null_email(self, spark, normalize_email_expr):
        """Null emails should remain null."""
        
        df = spark.createDataFrame([(None,)], ["email"])
        
        result = df.withColumn(
            "normalized", normalize_email_expr("email")
        )
        
        assert result.first()["normalized"] is None
    
    def test_email_with_internal_spaces(self, spark, normalize_email_expr):
        """Internal spaces should be removed."""
        
        df = spark.createDataFrame(
            [("john . doe @ email . com",)],
            ["email"]
        )
        
        result = df.withColumn(
            "normalized", normalize_email_expr("email")
        )
        
        normalized = result.first()["normalized"]
        assert " " not in normalized


class TestPhoneNormalization:
    """Test phone number normalization with country context."""
    
    def test_null_phone(self):
        """Null input should return None."""
        assert _normalize_phone(None, "UK") is None
    
    def test_empty_string(self):
        """Empty string should return None."""
        result = _normalize_phone("", "UK")
        assert result is None
    
    def test_already_international_uk(self):
        """Already formatted UK number should be preserved."""
        result = _normalize_phone("+447911123456", "UK")
        assert result is not None
        assert "+447911123456" in str(result)
    
    def test_already_international_france(self):
        """Already formatted French number should be preserved."""
        result = _normalize_phone("+33612345678", "France")
        assert result is not None
        assert "+33612345678" in str(result)
    
    def test_local_uk_number(self):
        """Local UK number should get country code."""
        result = _normalize_phone("07911123456", "UK")
        assert result is not None
        # Should start with +44
        assert "+44" in str(result) or result.startswith("+44")
    
    def test_local_france_number(self):
        """Local French number should get country code."""
        result = _normalize_phone("0612345678", "France")
        assert result is not None
        # Should start with +33
        assert "+33" in str(result) or result.startswith("+33")
    
    def test_phone_with_formatting(self):
        """Phone with parentheses, dashes, spaces should be cleaned."""
        result = _normalize_phone("+44 (791) 112-3456", "UK")
        assert result is not None
        # Should not contain formatting chars
        assert "(" not in str(result)
        assert ")" not in str(result)
        assert " " not in str(result)
        assert "-" not in str(result)
    
    def test_invalid_phone_returns_safe_output(self):
        """
        Invalid input should not raise and should return None or cleaned value.
        """
        result = _normalize_phone("abc123", "UK")

        # Must not throw and must be string or None
        assert result is None or isinstance(result, str)


class TestDateNormalization:
    """Test date parsing with multiple formats."""
    
    def test_standard_date_format(self, spark, parse_date_expr):
        """YYYY-MM-DD should parse correctly."""
        
        df = spark.createDataFrame([("2021-10-15",)], ["d"])
        result = df.withColumn(
            "parsed",
            parse_date_expr("d")
        )
        assert result.first()["parsed"] == date(2021, 10, 15)
    
    def test_compact_date_format(self, spark, parse_date_expr):
        """YYYYMMDD should parse correctly."""
        
        df = spark.createDataFrame([("20211015",)], ["d"])
        result = df.withColumn(
            "parsed",
            parse_date_expr("d")
        )
        assert result.first()["parsed"] == date(2021, 10, 15)


class TestDuplicateRemoval:
    """Test deduplication logic."""
    
    def test_keeps_most_recent(self, spark, sample_crm_data):
        """Should keep the record with the most recent last_updated."""
        
        result = DataLoader._remove_duplicates(
            sample_crm_data, "customer_id", "last_updated"
        )
        
        # CRM001 should appear only once
        
        crm001 = result.filter(col("customer_id") == "CRM001")
        rows = crm001.collect()
        
        # Should have the newer last_updated (2024-01-15)
        
        assert len(rows) == 1
        assert rows[0]["last_updated"] == "2024-01-15"
    
    def test_distinct_count_matches_result(self, spark, sample_crm_data):
        """Result count should equal distinct IDs in input."""
        
        distinct_count = sample_crm_data.select("customer_id").distinct().count()
        result = DataLoader._remove_duplicates(
            sample_crm_data, "customer_id", "last_updated"
        )
        
        assert result.count() == distinct_count


class TestLineageTracking:
    """Test that lineage columns are added."""
    
    def test_source_column_present(self, spark, test_config, tmp_path):
        """Loaded data should have 'source' column."""
        
        csv_path = create_temp_csv(
                    tmp_path,
                    headers=[
                        "customer_id", "first_name", "last_name", "email",
                        "phone", "address", "city", "country",
                        "registration_date", "last_updated"
                    ],
                    rows=[
                        ["C001", "John", "Doe", "j@e.com", "+123",
                        "Addr", "City", "UK", "2021-01-01", "2023-01-01"]
                    ],
                    filename="test.csv"
                )
        
        test_config["input"]["crm_path"] = str(csv_path)
        loader = DataLoader(spark, test_config)
        crm_df = loader.load_crm_data()
        
        assert "source" in crm_df.columns