from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import datetime
import json
import uuid

def setup_test_data():
    print("🚀 Starting Observability Test Setup...")
    
    # 1. Initialize Spark
    spark = SparkSession.builder.getOrCreate()
    print("✓ Spark Session Created")
    
    # 2. Configuration
    TEST_JOB_ID = 999999
    TEST_RUN_ID = 1001
    MANIFEST_TABLE = "main.rca_history.manifest_log"  # Update if different in your config
    SOURCE_TABLE = "default.rca_test_source"
    TARGET_TABLE = "default.rca_test_target"
    
    # 3. Create & Populate Source Table (100 Rows)
    print(f"\nCreating Source Table: {SOURCE_TABLE}")
    source_data = [(i, f"user_{i}", datetime.datetime.now()) for i in range(100)]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("user_id", StringType(), True),
        StructField("created_at", TimestampType(), True)
    ])
    df_source = spark.createDataFrame(source_data, schema)
    df_source.write.format("delta").mode("overwrite").saveAsTable(SOURCE_TABLE)
    print(f"✓ Populated {SOURCE_TABLE} with 100 rows.")

    # 4. Create & Populate Target Table (90 Rows - Simulating 10 dropped rows)
    print(f"\nCreating Target Table: {TARGET_TABLE}")
    target_data = [(i, f"user_{i}", datetime.datetime.now()) for i in range(90)] # First 90 only
    df_target = spark.createDataFrame(target_data, schema)
    df_target.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
    print(f"✓ Populated {TARGET_TABLE} with 90 rows (simulating drop).")

    # 5. Register Dummy Run (Optional, usually Databricks handles this, but for local test we fake it?)
    # Actually ObservabilityCollector queries `client.jobs.list_runs`. 
    # If we are running this locally, we might trick the collector by passing the run object mocked?
    # Or we just rely on the collector finding "no runs" and doing backfill?
    # Wait, collector calls `client.jobs.list_runs`. If job 999999 doesn't exist, it fails.
    # We might need to mock the `ObserverabilityCollector.client` or use an existing real Job ID.
    # For now, let's just setup the DATA and MANIFEST. 
    # The user might need to actually create a dummy Job in Databricks UI with ID 999999 or use a real one.
    
    # 6. Push Manifest to Table (Critical for Collector)
    print(f"\nPushing Manifest for Job {TEST_JOB_ID} to {MANIFEST_TABLE}...")
    
    manifest_payload = {
        "test_step_1": {
            "sources": [{"name": SOURCE_TABLE, "type": "TABLE"}],
            "targets": [{"name": TARGET_TABLE, "type": "TABLE"}],
            "code_content": {
                "notebook.py": "# Simple Pass through\ndf = spark.read.table('source')\ndf.write.save('target')"
            }
        }
    }
    
    manifest_row = [{
        "id": str(uuid.uuid4()),
        "job_id": str(TEST_JOB_ID),
        "manifest": json.dumps(manifest_payload),
        "version": "1.0",
        "date": datetime.datetime.now(),
        "created_by": "test_script"
    }]
    
    manifest_schema = StructType([
        StructField("id", StringType(), False),
        StructField("job_id", StringType(), True),
        StructField("manifest", StringType(), True),
        StructField("version", StringType(), True),
        StructField("date", TimestampType(), True),
        StructField("created_by", StringType(), True)
    ])
    
    # Check if manifest table exists, create if not
    if not spark.catalog.tableExists(MANIFEST_TABLE):
        print(f"Creating Manifest Table {MANIFEST_TABLE}...")
        spark.createDataFrame([], manifest_schema).write.format("delta").saveAsTable(MANIFEST_TABLE)

    df_manifest = spark.createDataFrame(manifest_row, manifest_schema)
    df_manifest.write.format("delta").mode("append").saveAsTable(MANIFEST_TABLE)
    print("✓ Manifest inserted.")

    print("\n✅ Setup Complete!")
    print(f"1. A dummy Manifest for Job {TEST_JOB_ID} is in {MANIFEST_TABLE}")
    print(f"2. Data is in {SOURCE_TABLE} and {TARGET_TABLE}")
    print("\nTo Test Observability:")
    print(f"python observability/collect_metrics.py --job-id {TEST_JOB_ID} --run-id {TEST_RUN_ID}")
    print("(Note: Since Job {TEST_JOB_ID} likely doesn't exist in Databricks, the collector might fail listing runs.")
    print("You may need to temporarily edit `collect_metrics.py` to bypass `sync_metrics` logic and call `_process_run` directly with a dummy run object if you don't have a real Job ID.)")

if __name__ == "__main__":
    setup_test_data()
