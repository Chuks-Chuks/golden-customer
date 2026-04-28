## Assumptions

- The `country` column is authoritative for phone number formatting
- City/country mismatches in the sample data do not affect the matching logic
- Focus is on demonstrating the reconciliation approach, not fixing synthetic data issues

## Observations

- **Email matching** was the most effective strategy (46% match rate)
- **Name + address matching** captured 8% additional matches
- **Phone matching** yielded 0 matches due to:
  - Poor phone data quality in source systems
  - Many invalid phone numbers in the sample data
  - Missing country codes for international numbers