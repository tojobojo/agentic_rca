"""Manifest management service for Delta table operations."""
from typing import Dict, Any
import logging
import json
import datetime
import uuid

logger = logging.getLogger(__name__)


class ManifestService:
    """Handles manifest CRUD operations in Delta tables."""
    
    def __init__(self, config, get_spark_fn):
        self.config = config
        self._get_spark = get_spark_fn  # Function to get Spark session
    
    def load_latest_manifest(self, table_name: str, job_id: str) -> Dict[str, Any]:
        """
        Loads the latest manifest for a given job_id.
        Priority: SUBMITTED (latest) > DRAFT (single)
        Returns: {
            'found': bool,
            'manifest_data': dict,
            'version': str,
            'status': str,
            'created_by': str,
            'date': datetime
        }
        """
        spark = self._get_spark()
        if not spark:
            return {'found': False, 'error': 'Spark connection unavailable'}
        
        try:
            if not spark.catalog.tableExists(table_name):
                return {'found': False}
            
            query = f"""
                SELECT manifest, version, manifest_status, created_by, date
                FROM {table_name}
                WHERE job_id = '{job_id}'
                ORDER BY 
                    CASE WHEN manifest_status = 'SUBMITTED' THEN 1 ELSE 2 END,
                    date DESC
                LIMIT 1
            """
            
            result = spark.sql(query).collect()
            
            if not result:
                return {'found': False}
            
            row = result[0]
            manifest_json = json.loads(row['manifest'])
            
            return {
                'found': True,
                'manifest_data': manifest_json,
                'version': row['version'],
                'status': row['manifest_status'],
                'created_by': row['created_by'],
                'date': row['date']
            }
            
        except Exception as e:
            logger.error(f"Failed to load manifest for job {job_id}: {e}")
            return {'found': False, 'error': str(e)}
    
    def _increment_version(self, current_version: str) -> str:
        """Increment patch version: 1.0 -> 1.1, 1.9 -> 1.10"""
        try:
            parts = current_version.split('.')
            major, minor = int(parts[0]), int(parts[1])
            return f"{major}.{minor + 1}"
        except:
            return "1.0"
    
    def _get_latest_submitted_version(self, table_name: str, job_id: str) -> str:
        """Get the latest SUBMITTED version for a job, or return '1.0' if none."""
        spark = self._get_spark()
        if not spark:
            return "1.0"
        
        try:
            if not spark.catalog.tableExists(table_name):
                return "1.0"
            
            query = f"""
                SELECT version
                FROM {table_name}
                WHERE job_id = '{job_id}' AND manifest_status = 'SUBMITTED'
                ORDER BY date DESC
                LIMIT 1
            """
            
            result = spark.sql(query).collect()
            if result:
                return result[0]['version']
            return "1.0"
            
        except Exception as e:
            logger.warning(f"Could not get latest version: {e}")
            return "1.0"

    def save_manifest_to_table(self, table_name: str, manifest: Dict[str, Any], job_id: str, 
                               version: str = "1.0", status: str = "DRAFT", current_user: str = "unknown") -> str:
        """
        Saves the generated manifest to a Delta table.
        
        DRAFT mode: UPSERT (delete old draft + insert new)
        SUBMIT mode: APPEND (insert new row with auto-incremented version)
        """
        spark = self._get_spark()
        if not spark:
            return "❌ Spark connection unavailable (Databricks Connect required). Cannot save to table."

        from pyspark.sql.types import StructType, StructField, StringType, TimestampType

        try:
            # 1. Handle DRAFT UPSERT
            if status == "DRAFT":
                if spark.catalog.tableExists(table_name):
                    spark.sql(f"DELETE FROM {table_name} WHERE job_id = '{job_id}' AND manifest_status = 'DRAFT'")
                    logger.info(f"Deleted existing DRAFT for job {job_id}")
            
            # 2. Handle SUBMIT version increment
            elif status == "SUBMITTED":
                latest_version = self._get_latest_submitted_version(table_name, job_id)
                version = self._increment_version(latest_version)
                logger.info(f"Auto-incremented version from {latest_version} to {version}")
            
            # 3. Define Schema
            schema = StructType([
                StructField("id", StringType(), False),
                StructField("job_id", StringType(), True),
                StructField("manifest", StringType(), True),
                StructField("version", StringType(), True),
                StructField("manifest_status", StringType(), True),
                StructField("date", TimestampType(), True),
                StructField("created_by", StringType(), True)
            ])

            # 4. Prepare Data
            data = [{
                "id": str(uuid.uuid4()),
                "job_id": str(job_id),
                "manifest": json.dumps(manifest),
                "version": version,
                "manifest_status": status,
                "date": datetime.datetime.now(),
                "created_by": current_user
            }]

            # 5. Create DataFrame
            df = spark.createDataFrame(data, schema=schema)
            
            # 6. Write to Delta Table (Append)
            logger.info(f"Saving manifest to {table_name} with status={status}, version={version}...")
            df.write.format("delta").mode("append").saveAsTable(table_name)
            
            return f"✅ Successfully saved manifest to table `{table_name}` (v{version}, {status})."

        except Exception as e:
            logger.error(f"Failed to save to table {table_name}: {e}")
            return f"❌ Error saving to table: {str(e)[:100]}..."
