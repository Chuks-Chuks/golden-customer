import pytest
import json
from pathlib import Path
from datetime import date



class TestCompleteness:
    """Test data completeness checks."""
    
    def test_all_complete(self, spark, dq_checker):
        """Fully populated columns should have 100% completeness."""
        df = spark.createDataFrame(
            [("John", "Doe", "john@email.com", "+1234567890")],
            ["first_name", "last_name", "normalized_email", "normalized_phone"]
        )
        
        
        result = dq_checker.check_completeness(
            df, ["first_name", "last_name", "normalized_email", "normalized_phone"]
        )
        
        for column_name in result:
            assert result[column_name]["completeness_percentage"] == 100.0
    
    def test_partial_completeness(self, spark, dq_checker):
        """Columns with nulls should show correct completeness."""
        df = spark.createDataFrame(
            [
                ("John", "Doe", "john@email.com", None),
                ("Jane", None, None, None),
            ],
            ["first_name", "last_name", "normalized_email", "normalized_phone"]
        )
        
        result = dq_checker.check_completeness(
            df, ["first_name", "last_name", "normalized_email", "normalized_phone"]
        )
        
        # first_name: both records have it = 100%
        assert result["first_name"]["completeness_percentage"] == 100.0
        # last_name: only 1 of 2 = 50%
        assert result["last_name"]["completeness_percentage"] == 50.0
    
    def test_missing_column_handled(self, spark, dq_checker):
        """Requesting a column not in DataFrame should warn but not crash."""
        df = spark.createDataFrame([("John",)], ["first_name"])
        
        result = dq_checker.check_completeness(df, ["first_name", "nonexistent_col"])
        
        assert result["nonexistent_col"]["completeness_percentage"] is None

    
    def test_empty_dataframe_completeness(self, spark, dq_checker):
        """Empty DataFrame should return 0% completeness."""
        df = spark.createDataFrame([], "first_name STRING, last_name STRING")

        result = dq_checker.check_completeness(df, ["first_name", "last_name"])

        assert result["first_name"]["completeness_percentage"] == 0.0


class TestFormatValidity:
    """Test email and phone format validation."""
    
    def test_valid_emails(self, spark, dq_checker):
        """Valid emails should be counted correctly."""
        df = spark.createDataFrame(
            [
                ("valid@email.com",),
                ("also.valid@sub.domain.co.uk",),
                ("invalid-email",),
                (None,),
            ],
            ["normalized_email"]
        )
        
        result = dq_checker.check_format_validity(df)
        
        assert result["email"]["valid_count"] == 2
        assert result["email"]["invalid_count"] == 1  # invalid-email has no @domain
        assert result["email"]["null_count"] == 1
    
    def test_valid_phones(self, spark, dq_checker):
        """Valid E.164 phones should be counted correctly."""
        df = spark.createDataFrame(
            [
                ("+447911123456",),
                ("+33612345678",),
                ("invalid-phone",),
                (None,),
            ],
            ["normalized_phone"]
        )
        
        result = dq_checker.check_format_validity(df)
        
        assert result["phone"]["valid_count"] == 2
        assert result["phone"]["invalid_count"] == 1


class TestUniqueness:
    """Test uniqueness checks."""
    
    def test_all_unique(self, spark, dq_checker):
        """All unique values should show 100% uniqueness."""
        df = spark.createDataFrame(
            [("A",), ("B",), ("C",)],
            ["id"]
        )
        
        result = dq_checker.check_uniqueness(df, ["id"])
        
        assert result["id"]["uniqueness_percentage"] == 100.0
        assert result["id"]["duplicate_count"] == 0
    
    def test_with_duplicates(self, spark, dq_checker):
        """Duplicate detection should work correctly."""
        df = spark.createDataFrame(
            [("A",), ("B",), ("A",)],
            ["id"]
        )
        
        result = dq_checker.check_uniqueness(df, ["id"])
        expected = (2 / 3) * 100
        assert result["id"]["uniqueness_percentage"] == pytest.approx(expected, 0.01)
        assert result["id"]["duplicate_count"] == 1


class TestFreshness:
    """Test date freshness checks."""
    
    def test_date_range_correct(self, spark, dq_checker):
        """Should return correct min and max dates."""
        df = spark.createDataFrame(
            [
                (date(2023, 1, 1),),
                (date(2023, 6, 15),),
                (date(2024, 1, 1),),
            ],
            ["last_updated"]
        )
        
        result = dq_checker.check_freshness(df, "last_updated")
        
        assert result["min_date"].startswith("2023-01-01")
        assert result["max_date"].startswith("2024-01-01")


class TestReportGeneration:
    """Test full report generation and saving."""
    
    def test_report_has_all_sections(self, spark, dq_checker):
        """Generated report should contain all expected sections."""
        df = spark.createDataFrame(
            [("C001", "John", "Doe", "john@email.com", "+447911123456")],
            ["customer_id", "first_name", "last_name", 
             "normalized_email", "normalized_phone"]
        )
        
        report = dq_checker.generate_report(df, "TEST")
        
        assert isinstance(report["source"], dict)
        assert isinstance(report["total_records"], dict)
        assert isinstance(report["completeness"], dict)
        assert isinstance(report["format_validity"], dict)
        assert isinstance(report["uniqueness"], dict)
        assert isinstance(report["freshness"], dict)
        assert report["source"] == "TEST"
        assert report["total_records"] == 1
    
    def test_report_saved_to_file(self, spark, dq_checker, tmp_path):
        """Report should be saved as JSON file."""
        report_path = str(tmp_path / "reports")
        
        df = spark.createDataFrame(
            [("C001", "John", "Doe")],
            ["customer_id", "first_name", "last_name"]
        )
        
        report = dq_checker.generate_report(df, "TEST")
        dq_checker.save_report(report, report_path)
        
        # Check at least one JSON file exists anywhere under the report path
        report_dir = Path(report_path)
        all_json_files = list(report_dir.rglob("*.json"))
        
        assert len(all_json_files) > 0, "No JSON report files were created"
        
        # Verify content of the first found report
        with open(all_json_files[0]) as f:
            saved_report = json.load(f)
        
        assert saved_report["source"] == "TEST"

    
    class TestDataQualityIntegration:
        """Integration-style tests covering end-to-end quality checks."""

        def test_full_quality_pipeline(self, spark, dq_checker):
            """End-to-end data quality check should run without failure."""
            df = spark.createDataFrame(
                [
                    ("C001", "John", "Doe", "john@email.com", "+447911123456"),
                    ("C002", None, "Smith", "invalid-email", "123"),
                ],
                ["customer_id", "first_name", "last_name",
                "normalized_email", "normalized_phone"]
            )

            report = dq_checker.generate_report(df, "TEST")

            assert report["total_records"] == 2
            assert report["completeness"]["first_name"]["completeness_percentage"] < 100
            assert report["format_validity"]["email"]["invalid_count"] > 0