# Lab Guide (draft scaffold)

## Flow
1. Run `sql/00_setup.sql` (ACCOUNTADMIN) to create role/db/warehouses.
2. Configure S3 IAM + storage integration (see `PLAN.md` AWS section) and run `sql/01_stages_formats.sql`.
3. Upload sample data to internal `@raw_stage` or S3 `@raw_ext_stage`.
4. Run COPY commands (to add to `sql/01_stages_formats.sql` or via worksheet) to load into `RAW` tables.
5. Deploy Snowpark proc from `python/sp_transform_orders.py` (CREATE PROCEDURE).
6. Run `sql/02_streams_tasks.sql` to create streams and after-stream task chain; resume tasks.
7. Observe transformations flowing into STAGE/CURATED; test scale-up/down on `MLP_TRANSFORM_WH`.
8. Use `sql/03_cost_monitoring.sql` to show credit consumption.

## Next steps to flesh out
- Add COPY statements for CSV/JSON/Parquet.
- Add worksheet/notebook steps for Snowpark deployment and verification queries.
- Add visuals for credit usage (worksheet charts or Streamlit).

