"""Asset validation service for Unity Catalog and file assets."""
from databricks.sdk import WorkspaceClient
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)


class AssetValidator:
    """Handles asset validation for Unity Catalog tables and files."""
    
    def __init__(self, client: WorkspaceClient, config, get_spark_fn):
        self.client = client
        self.config = config
        self._get_spark = get_spark_fn  # Function to get Spark session
    
    def validate_assets(self, assets: List[Dict[str, Any]]) -> Dict[str, str]:
        """
        Validates the existence of the provided assets.
        Returns a dict mapping Identifier -> Status Message (e.g. "✅ Exists", "❌ Not Found", "⚠️ External").
        """
        results = {}
        
        for asset in assets:
            ident = asset.get("identifier")
            subtype = asset.get("subtype", "UNKNOWN")
            
            if not ident:
                continue

            try:
                # 1. Unity Catalog / Hive Tables
                if subtype in ["UNITY_CATALOG_TABLE", "HIVE_METASTORE_TABLE", "GENERIC_TABLE"] or "TABLE" in subtype:
                    try:
                        table_info = self.client.tables.get(ident)
                        t_type = str(table_info.table_type).split('.')[-1] if table_info.table_type else "UNKNOWN"
                        t_fmt = str(table_info.data_source_format).split('.')[-1] if table_info.data_source_format else "UNKNOWN"
                        results[ident] = f"✅ Exists ({t_type}, {t_fmt})"
                    except Exception as e:
                        err_str = str(e)
                        if "NOT_FOUND" in err_str or "does not exist" in err_str.lower():
                            results[ident] = "❌ Not Found"
                        else:
                            results[ident] = f"⚠️ Check Failed: {str(e)}"

                # 2. Files / Paths
                elif subtype in ["ADLS", "S3", "GCS", "DBFS", "LOCAL_FILE", "PARQUET_FILE", "CSV_FILE", "DELTA_PATH"] or "FILE" in subtype:
                    if ident.startswith("dbfs:") or ident.startswith("/dbfs"):
                         try:
                             check_path = ident if ident.startswith("dbfs:") else f"dbfs:{ident}"
                             self.client.dbfs.get_status(check_path)
                             results[ident] = "✅ Exists"
                         except Exception as e:
                             if "RESOURCE_DOES_NOT_EXIST" in str(e):
                                 results[ident] = "❌ Not Found"
                             else:
                                 results[ident] = f"⚠️ Error: {str(e)[:30]}"

                    elif ident.startswith("/Volumes") or ident.startswith("/Workspace"):
                        try:
                            self.client.files.get_metadata(ident)
                            results[ident] = "✅ Exists"
                        except Exception as e:
                             if "NOT_FOUND" in str(e):
                                 results[ident] = "❌ Not Found"
                             else:
                                 results[ident] = f"⚠️ Error: {str(e)[:30]}"
                    
                    elif "abfss" in ident or "s3" in ident:
                        spark = self._get_spark()
                        if spark:
                            try:
                                res_df = spark.sql(f"DESCRIBE DETAIL '{ident}'").collect()
                                if res_df:
                                    row = res_df[0].asDict()
                                    fmt = row.get("format", "UNKNOWN").upper()
                                    results[ident] = f"✅ Exists ({fmt})"
                                else:
                                    results[ident] = "❌ Not Found (Empty Detail)"
                            except Exception as e:
                                err = str(e)
                                if "Path does not exist" in err or "FileNotFoundException" in err:
                                    results[ident] = "❌ Not Found"
                                else:
                                    results[ident] = f"⚠️ Spark Error: {err[:40]}..."
                        else:
                             results[ident] = "⚠️ Skipped (No Spark)"
                    
                    else:
                        results[ident] = "❔ Unchecked"
                
                else:
                    results[ident] = "❔ Unchecked Type"

            except Exception as e:
                results[ident] = f"⚠️ Error: {str(e)}"
        
        return results

    def get_asset_columns(self, identifier: str) -> List[str]:
        """
        Fetches column names for a given table identifier.
        Returns empty list if not found or not a table.
        """
        try:
            table_info = self.client.tables.get(identifier)
            if table_info.columns:
                return [c.name for c in table_info.columns]
            return []
        except Exception:
            spark = self._get_spark()
            if spark:
                try:
                    df = spark.sql(f"DESCRIBE '{identifier}'").collect()
                    return [row['col_name'] for row in df if row['col_name'] and not row['col_name'].startswith("#")]
                except:
                    pass
            return []
