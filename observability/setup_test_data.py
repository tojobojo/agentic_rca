from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
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
    SOURCE_TABLE = "default.merchant_source"
    TARGET_TABLE = "default.merchant_retention_output"
    
    # 3. Create & Populate Source Table (1000 Rows)
    print(f"\nCreating Merchant Source Table: {SOURCE_TABLE}")
    
    # Columns: biz_header_id, merchant_id, mcc_code, zip_code, state_code, state_code, city, latitude, longitude, cbsa_name, OPT_OUT_FLAG, sb_flag_by_sba, CLOVER_FLAG, total_txn_amt, total_txn_cnt, loaddt, naics3
    source_schema = StructType([
        StructField("biz_header_id", StringType(), False),
        StructField("merchant_id", StringType(), False),
        StructField("mcc_code", StringType(), True),
        StructField("total_txn_amt", DoubleType(), True),
        StructField("loaddt", StringType(), True)
    ])
    
    # Generate dummy data
    source_data = []
    for i in range(100):
        source_data.append((f"BH_{i}", f"M_{i}", "5411", 100.0 * i, "2024-01-01"))
        
    df_source = spark.createDataFrame(source_data, source_schema)
    df_source.write.format("delta").mode("overwrite").saveAsTable(SOURCE_TABLE)
    print(f"✓ Populated {SOURCE_TABLE} with 100 rows.")

    # 4. Create & Populate Target Table (Simulate drop in 'merchant_retention')
    print(f"\nCreating Target Table: {TARGET_TABLE}")
    target_data = []
    # Drop 20% rows to simulate anomaly
    for i in range(80):
        target_data.append((f"BH_{i}", f"M_{i}", "5411", 100.0 * i, "2024-01-01"))
        
    df_target = spark.createDataFrame(target_data, source_schema)
    df_target.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
    print(f"✓ Populated {TARGET_TABLE} with 80 rows (20% Drop).")
    
    # 4b. Create Intermediate Tables (Pass-through for now)
    # identify_smb -> intermediate.smb_list
    # dynamic_sample_size -> intermediate.sample_size
    # proximity_calculation -> intermediate.proximity
    # ats_calculation -> intermediate.ats_scores
    
    intermediates = [
        "intermediate.smb_list",
        "intermediate.sample_size",
        "intermediate.proximity",
        "intermediate.ats_scores"
    ]
    
    print("\nCreating Intermediate Tables (100 rows each)...")
    for tbl in intermediates:
        df_source.write.format("delta").mode("overwrite").saveAsTable(tbl)
        print(f"  ✓ {tbl}")

    # 6. Push Manifest
    print(f"\nPushing Manifest for Job {TEST_JOB_ID}...")
    
    manifest_payload = {
        "identify_smb": {
            "sources": [{"name": SOURCE_TABLE, "type": "TABLE"}], 
            "targets": [{"name": "intermediate.smb_list", "type": "TABLE"}],
            "code_content": {"nb1.py": "# Code"}
        },
        "dynamic_sample_size": {
            "sources": [{"name": "intermediate.smb_list", "type": "TABLE"}],
            "targets": [{"name": "intermediate.sample_size", "type": "TABLE"}],
            "code_content": {"nb2.py": "# Code"}
        },
        "proximity_calculation": {
            "sources": [{"name": "intermediate.sample_size", "type": "TABLE"}],
            "targets": [{"name": "intermediate.proximity", "type": "TABLE"}],
            "code_content": {"nb3.py": "# Code"}  
        },
        "ats_calculation": {
            "sources": [{"name": "intermediate.proximity", "type": "TABLE"}],
            "targets": [{"name": "intermediate.ats_scores", "type": "TABLE"}],
            "code_content": {"nb4.py": "# Code"}
        },
        "merchant_retention": {
            "sources": [{"name": "intermediate.ats_scores", "type": "TABLE"}], 
            "targets": [{"name": TARGET_TABLE, "type": "TABLE"}],
            "code_content": {
                "retention_logic.py": "# Retention Logic\ndf = spark.read.table('intermediate.ats_scores')\n# Filter logic that might be wrong\nfinal = df.filter(df.total_txn_amt > 500) \nfinal.write.save('default.merchant_retention_output')"
            }
        },
        "all_mcc_segmentation": {
             "sources": [{"name": TARGET_TABLE, "type": "TABLE"}],
             "targets": [{"name": "final.mcc_segments", "type": "TABLE"}],
             "code_content": {"nb6.py": "# Code"}
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
