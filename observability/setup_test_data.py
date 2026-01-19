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
    MANIFEST_TABLE = "dev_dcs_catalog.dev_peergroup_benchmark.rca_manifest_log"  # Update if different in your config
    SOURCE_TABLE = "dev_dcs_catalog.dev_peergroup_benchmark.rca_merchant_source"
    TARGET_TABLE = "dev_dcs_catalog.dev_peergroup_benchmark.rca_merchant_retention_output"
    
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
        "dev_dcs_catalog.dev_peergroup_benchmark.rca_smb_list",
        "dev_dcs_catalog.dev_peergroup_benchmark.rca_sample_size",
        "dev_dcs_catalog.dev_peergroup_benchmark.rca_proximity",
        "dev_dcs_catalog.dev_peergroup_benchmark.rca_ats_scores",
        "dev_dcs_catalog.dev_peergroup_benchmark.rca_mcc_segments"
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
            "targets": [{"name": "dev_dcs_catalog.dev_peergroup_benchmark.rca_smb_list", "type": "TABLE"}],
            "code_content": {"nb1.py": "# Code"}
        },
        "dynamic_sample_size": {
            "sources": [{"name": "dev_dcs_catalog.dev_peergroup_benchmark.rca_smb_list", "type": "TABLE"}],
            "targets": [{"name": "dev_dcs_catalog.dev_peergroup_benchmark.rca_sample_size", "type": "TABLE"}],
            "code_content": {"nb2.py": "# Code"}
        },
        "proximity_calculation": {
            "sources": [{"name": "dev_dcs_catalog.dev_peergroup_benchmark.rca_sample_size", "type": "TABLE"}],
            "targets": [{"name": "dev_dcs_catalog.dev_peergroup_benchmark.rca_proximity", "type": "TABLE"}],
            "code_content": {"nb3.py": "# Code"}  
        },
        "ats_calculation": {
            "sources": [{"name": "dev_dcs_catalog.dev_peergroup_benchmark.rca_proximity", "type": "TABLE"}],
            "targets": [{"name": "dev_dcs_catalog.dev_peergroup_benchmark.rca_ats_scores", "type": "TABLE"}],
            "code_content": {"nb4.py": "# Code"}
        },
        "merchant_retention": {
            "sources": [{"name": "dev_dcs_catalog.dev_peergroup_benchmark.rca_ats_scores", "type": "TABLE"}], 
            "targets": [{"name": TARGET_TABLE, "type": "TABLE"}],
            "code_content": {
                "retention_logic.py": "# Retention Logic\ndf = spark.read.table('dev_dcs_catalog.dev_peergroup_benchmark.rca_ats_scores')\n# Filter logic that might be wrong\nfinal = df.filter(df.total_txn_amt > 500) \nfinal.write.save('dev_dcs_catalog.dev_peergroup_benchmark.rca_merchant_retention_output')"
            }
        },
        "all_mcc_segmentation": {
             "sources": [{"name": TARGET_TABLE, "type": "TABLE"}],
             "targets": [{"name": "dev_dcs_catalog.dev_peergroup_benchmark.rca_mcc_segments", "type": "TABLE"}],
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
    print("(Note: Since Job {TEST_JOB_ID} likely doesn't exist in Databricks, the collector might fail listing runs.")
    
    # 7. Populate Metrics History (Bypass Collector for Mocking)
    METRICS_TABLE = "dev_dcs_catalog.dev_peergroup_benchmark.rca_metrics_history"
    print(f"\nPopulating Metrics History Table: {METRICS_TABLE}...")
    
    # Schema for Metrics (Simplified)
    # run_id, job_id, task_key, target_table, metric_type, rows_total, timestamp, etc.
    metrics_data = []
    
    # Run 1000: Baseline (Healthy)
    # All steps have 100 rows
    tasks = [
        "identify_smb", "dynamic_sample_size", "proximity_calculation", 
        "ats_calculation", "all_mcc_segmentation"
    ]
    # Merchant Retention is the one with logic
    
    timestamp_base = datetime.datetime.now() - datetime.timedelta(hours=2)
    
    for t_key in tasks + ["merchant_retention"]:
        # Source Metric
        metrics_data.append({
            "run_id": "1000", "job_id": str(TEST_JOB_ID), "task_key": t_key,
            "target_table": "table_x", "metric_type": "SOURCE", 
            "rows_total": 100, "rows_null_vital": {}, "timestamp": str(timestamp_base)
        })
        # Target Metric
        metrics_data.append({
            "run_id": "1000", "job_id": str(TEST_JOB_ID), "task_key": t_key,
            "target_table": "table_x", "metric_type": "TARGET", 
            "rows_total": 100, "rows_null_vital": {}, "timestamp": str(timestamp_base)
        })

    # Run 1001: Anomaly
    # identify_smb -> ats_calculation: Healthy (100 rows)
    # merchant_retention: DROP (80 rows target, 100 source)
    timestamp_curr = datetime.datetime.now()
    
    for t_key in tasks:
        metrics_data.append({
            "run_id": str(TEST_RUN_ID), "job_id": str(TEST_JOB_ID), "task_key": t_key,
            "target_table": "table_x", "metric_type": "SOURCE", 
            "rows_total": 100, "rows_null_vital": {}, "timestamp": str(timestamp_curr)
        })
        metrics_data.append({
            "run_id": str(TEST_RUN_ID), "job_id": str(TEST_JOB_ID), "task_key": t_key,
            "target_table": "table_x", "metric_type": "TARGET", 
            "rows_total": 100, "rows_null_vital": {}, "timestamp": str(timestamp_curr)
        })
        
    # Anomaly Step
    metrics_data.append({
        "run_id": str(TEST_RUN_ID), "job_id": str(TEST_JOB_ID), "task_key": "merchant_retention",
        "target_table": "dev_dcs_catalog.dev_peergroup_benchmark.rca_merchant_retention_output", "metric_type": "SOURCE", 
        "rows_total": 100, "rows_null_vital": {}, "timestamp": str(timestamp_curr)
    })
    metrics_data.append({
        "run_id": str(TEST_RUN_ID), "job_id": str(TEST_JOB_ID), "task_key": "merchant_retention",
        "target_table": "dev_dcs_catalog.dev_peergroup_benchmark.rca_merchant_retention_output", "metric_type": "TARGET", 
        "rows_total": 80, "rows_null_vital": {}, "timestamp": str(timestamp_curr) # <--- DROP
    })

    # Convert to DF
    # We use a MapType for rows_null_vital to match Schema
    from pyspark.sql.types import MapType
    
    m_schema = StructType([
        StructField("run_id", StringType(), True),
        StructField("job_id", StringType(), True),
        StructField("task_key", StringType(), True),
        StructField("target_table", StringType(), True),
        StructField("metric_type", StringType(), True),
        StructField("rows_total", IntegerType(), True),
        StructField("rows_null_vital", MapType(StringType(), IntegerType()), True),
        StructField("timestamp", StringType(), True)
        # Missing columns will be null, that's fine for Delta mergeSchema
    ])
    
    df_metrics = spark.createDataFrame(metrics_data, m_schema)
    
    # Save
    if not spark.catalog.tableExists(METRICS_TABLE):
         df_metrics.write.format("delta").saveAsTable(METRICS_TABLE)
    else:
         df_metrics.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(METRICS_TABLE)
         
    print(f"✓ Populated Metrics History with Baseline (Run 1000) and Anomaly (Run {TEST_RUN_ID})")
    print(f"  -> Merchant Retention: Run 1000 (100 -> 100), Run {TEST_RUN_ID} (100 -> 80)")

if __name__ == "__main__":
    setup_test_data()
