# ETL Task

Source: s3://radhakanta-etl-agent-bucket/raw/

Rules file: dq_rules.json
Transform mapping: business_mapping.xlsx
Output (access/data_mart): s3://radhakanta-etl-agent-bucket/access/  and s3://radhakanta-etl-agent-bucket/mart/

Requirements:
1. Read all CSV files from Source dynamically (wildcard).
2. Apply DQ rules from dq_rules.json and write clean and rejects.
3. Partition outputs by load_date (YYYY-MM-DD).
4. Apply mappings from business_mapping.xlsx to create a mart table.
5. Create/update AWS Glue external tables for Access and Mart layers.
6. If errors occur, use Claude to review logs, generate fixes, patch code, re-run (retry up to 3 times).