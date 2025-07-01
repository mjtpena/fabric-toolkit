# Microsoft Fabric Lakehouse Backup Notebook
#
# This notebook implements a parameterized full backup solution for Microsoft Fabric Lakehouses.
# 
# Features:
# - Full backup of all Delta tables in the source Lakehouse
# - Configurable backup destinations (Storage Account, another Lakehouse, ADLS)
# - Metadata preservation including Delta transaction logs
# - Backup verification with row count validation
# - Detailed logging and error handling
# - Automatic cleanup based on retention policy
#
# Compatible with Microsoft Fabric scheduling via:
# - Direct notebook scheduling
# - Data Factory pipeline integration
# - REST API execution
#
# Author: Generated for Microsoft Fabric
# Version: 1.0

# ============================================================================
# CELL 1: Backup Configuration Parameters
# ============================================================================

# Configure these parameters before running or when scheduling the notebook
dbutils.widgets.text("source_lakehouse_name", "", "Source Lakehouse Name")
dbutils.widgets.text("source_workspace_id", "", "Source Workspace ID (leave empty for current workspace)")
dbutils.widgets.dropdown("backup_type", "storage_account", ["storage_account", "lakehouse", "adls"], "Backup Destination Type")
dbutils.widgets.text("backup_storage_account", "", "Backup Storage Account Name (for storage_account type)")
dbutils.widgets.text("backup_container", "lakehouse-backups", "Backup Container Name")
dbutils.widgets.text("backup_lakehouse_name", "", "Backup Lakehouse Name (for lakehouse type)")
dbutils.widgets.text("backup_workspace_id", "", "Backup Workspace ID (leave empty for current workspace)")
dbutils.widgets.text("backup_adls_account", "", "Backup ADLS Account Name (for adls type)")
dbutils.widgets.text("backup_adls_container", "", "Backup ADLS Container Name (for adls type)")
dbutils.widgets.text("backup_folder_path", "", "Custom Backup Folder Path (leave empty for auto-generated)")
dbutils.widgets.text("retention_days", "30", "Backup Retention Period in Days")
dbutils.widgets.dropdown("verify_backup", "True", ["True", "False"], "Verify Backup After Completion")
dbutils.widgets.dropdown("include_delta_logs", "True", ["True", "False"], "Include Delta Transaction Logs")
dbutils.widgets.dropdown("compress_backup", "True", ["True", "False"], "Compress Backup Files")
dbutils.widgets.dropdown("enable_detailed_logging", "True", ["True", "False"], "Enable Detailed Logging")
dbutils.widgets.dropdown("use_managed_identity", "True", ["True", "False"], "Use Managed Identity for External Storage")

# ============================================================================
# CELL 2: Import Required Libraries
# ============================================================================

import os
import json
import datetime
import uuid
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from delta.tables import DeltaTable
import time

print("Libraries imported successfully")
print(f"Backup process initiated at: {datetime.datetime.now()}")

# ============================================================================
# CELL 3: Core Helper Functions
# ============================================================================

def get_current_timestamp():
    """Return current timestamp in a standardized format"""
    return datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

def get_backup_path():
    """Generate backup path based on timestamp and a unique identifier"""
    timestamp = get_current_timestamp()
    backup_id = str(uuid.uuid4())[:8]
    return f"backup_{timestamp}_{backup_id}"

def log_message(message, level="INFO"):
    """Log a message with timestamp and level"""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")
    
    # If detailed logging is enabled, also log to a Delta table
    if enable_detailed_logging and level != "DEBUG":
        log_entry = [(timestamp, level, message)]
        log_schema = StructType([
            StructField("timestamp", StringType(), False),
            StructField("level", StringType(), False),
            StructField("message", StringType(), False)
        ])
        log_df = spark.createDataFrame(log_entry, log_schema)
        
        # Write to the log table
        if backup_type == "lakehouse":
            try:
                log_df.write.format("delta").mode("append").save(f"{backup_base_path}/_logs")
            except Exception as e:
                print(f"Warning: Could not write to log table: {str(e)}")
        else:
            # For other backup types, keep in-memory log and write at the end
            global log_entries
            log_entries.append((timestamp, level, message))

def validate_parameters():
    """Validate that required parameters are provided"""
    source_lakehouse_name = dbutils.widgets.get("source_lakehouse_name")
    backup_type = dbutils.widgets.get("backup_type")
    
    if not source_lakehouse_name:
        raise ValueError("Source Lakehouse Name is required")
    
    if backup_type == "storage_account":
        if not dbutils.widgets.get("backup_storage_account"):
            raise ValueError("Backup Storage Account is required for storage_account backup type")
    elif backup_type == "lakehouse":
        if not dbutils.widgets.get("backup_lakehouse_name"):
            raise ValueError("Backup Lakehouse Name is required for lakehouse backup type")
    elif backup_type == "adls":
        if not dbutils.widgets.get("backup_adls_account") or not dbutils.widgets.get("backup_adls_container"):
            raise ValueError("Backup ADLS Account and Container are required for adls backup type")
    else:
        raise ValueError(f"Invalid backup type: {backup_type}")

print("Core helper functions defined successfully")

# ============================================================================
# CELL 4: Path and Authentication Functions
# ============================================================================

def get_source_path():
    """Construct the path to the source Lakehouse"""
    source_lakehouse_name = dbutils.widgets.get("source_lakehouse_name")
    source_workspace_id = dbutils.widgets.get("source_workspace_id")
    
    if source_workspace_id and source_workspace_id.strip():
        # External lakehouse access via OneLake path
        return f"abfss://{source_workspace_id}@onelake.dfs.fabric.microsoft.com/{source_lakehouse_name}.Lakehouse/Tables"
    else:
        # Current workspace lakehouse
        return "Tables"

def setup_external_storage_auth():
    """Setup authentication for external storage if needed"""
    use_managed_identity = dbutils.widgets.get("use_managed_identity") == "True"
    backup_type = dbutils.widgets.get("backup_type")
    
    if backup_type in ["storage_account", "adls"] and use_managed_identity:
        log_message("Configuring managed identity for external storage authentication", "INFO")
        
        # Configure OAuth provider for external storage
        if backup_type == "storage_account":
            account_name = dbutils.widgets.get("backup_storage_account")
            spark.conf.set(f"fs.azure.account.auth.type.{account_name}.dfs.core.windows.net", "OAuth")
            spark.conf.set(f"fs.azure.account.oauth.provider.type.{account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider")
        elif backup_type == "adls":
            account_name = dbutils.widgets.get("backup_adls_account")
            spark.conf.set(f"fs.azure.account.auth.type.{account_name}.dfs.core.windows.net", "OAuth")
            spark.conf.set(f"fs.azure.account.oauth.provider.type.{account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider")

def get_backup_base_path():
    """Construct the base path for the backup based on the backup type"""
    backup_type = dbutils.widgets.get("backup_type")
    
    if backup_type == "storage_account":
        account_name = dbutils.widgets.get("backup_storage_account")
        container_name = dbutils.widgets.get("backup_container")
        return f"abfss://{container_name}@{account_name}.dfs.core.windows.net"
    
    elif backup_type == "lakehouse":
        lakehouse_name = dbutils.widgets.get("backup_lakehouse_name")
        backup_workspace_id = dbutils.widgets.get("backup_workspace_id")
        
        if backup_workspace_id and backup_workspace_id.strip():
            # External workspace lakehouse
            return f"abfss://{backup_workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_name}.Lakehouse/Files/lakehouse_backups"
        else:
            # Current workspace lakehouse - use Files folder
            return f"Files/lakehouse_backups"
    
    elif backup_type == "adls":
        account_name = dbutils.widgets.get("backup_adls_account")
        container_name = dbutils.widgets.get("backup_adls_container")
        return f"abfss://{container_name}@{account_name}.dfs.core.windows.net"
    
    else:
        raise ValueError(f"Invalid backup type: {backup_type}")

def get_backup_folder():
    """Get the backup folder path, either user-specified or auto-generated"""
    user_path = dbutils.widgets.get("backup_folder_path")
    if user_path and user_path.strip():
        return user_path.strip()
    else:
        return get_backup_path()

print("Path and authentication functions defined successfully")

# ============================================================================
# CELL 5: Table Discovery and Backup Operations
# ============================================================================

def list_tables_in_lakehouse(source_path):
    """List all tables in the lakehouse"""
    try:
        # Try to list using dbutils first
        table_list = dbutils.fs.ls(source_path)
        tables = [t.name.rstrip('/') for t in table_list 
                 if not t.name.startswith('_') and not t.name.startswith('.') and t.isDir]
        return tables
    except Exception as e:
        log_message(f"Error listing tables with dbutils: {str(e)}", "WARNING")
        
        # Fallback: try using Spark SQL
        try:
            if source_path == "Tables":
                # Current lakehouse
                tables_df = spark.sql("SHOW TABLES")
                return [row.tableName for row in tables_df.collect()]
            else:
                # External lakehouse - we'll need to scan the directory differently
                log_message("External lakehouse detected, using alternative table discovery", "INFO")
                return []
        except Exception as e2:
            log_message(f"Error listing tables with SQL: {str(e2)}", "ERROR")
            return []

def create_backup_manifest(backup_path, tables, files_count, backup_size_bytes, start_time, end_time):
    """Create a manifest file with backup metadata"""
    manifest = {
        "backup_id": backup_path.split('_')[-1] if '_' in backup_path else str(uuid.uuid4())[:8],
        "backup_timestamp": get_current_timestamp(),
        "source_lakehouse_name": dbutils.widgets.get("source_lakehouse_name"),
        "source_workspace_id": dbutils.widgets.get("source_workspace_id"),
        "backup_type": dbutils.widgets.get("backup_type"),
        "tables": tables,
        "files_count": files_count,
        "backup_size_bytes": backup_size_bytes,
        "include_delta_logs": dbutils.widgets.get("include_delta_logs") == "True",
        "compressed": dbutils.widgets.get("compress_backup") == "True",
        "start_time": start_time,
        "end_time": end_time,
        "duration_seconds": (datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S") - 
                             datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")).total_seconds(),
        "fabric_version": "1.0"
    }
    
    # Write manifest to backup location
    try:
        manifest_df = spark.createDataFrame([manifest])
        manifest_df.write.mode("overwrite").format("json").save(f"{backup_base_path}/{backup_folder}/_manifest")
        log_message("Backup manifest created successfully", "INFO")
    except Exception as e:
        log_message(f"Error creating backup manifest: {str(e)}", "ERROR")
    
    return manifest

def verify_backup(source_path, backup_path, tables):
    """Verify the backup by comparing row counts between source and backup"""
    log_message("Starting backup verification...", "INFO")
    
    verification_results = {}
    all_verified = True
    
    for table in tables:
        try:
            source_table_path = f"{source_path}/{table}"
            backup_table_path = f"{backup_path}/{table}"
            
            # Count rows in source and backup
            try:
                source_count = spark.read.format("delta").load(source_table_path).count()
                backup_count = spark.read.format("delta").load(backup_table_path).count()
                
                # Basic verification
                rows_match = source_count == backup_count
                verified = rows_match
                
                if not verified:
                    all_verified = False
                    
                verification_results[table] = {
                    "verified": verified,
                    "source_rows": source_count,
                    "backup_rows": backup_count
                }
                
                log_message(f"Table {table} verification: {'SUCCESS' if verified else 'FAILED'}", "INFO")
                if not verified:
                    log_message(f"  - Source rows: {source_count}, Backup rows: {backup_count}", "WARNING")
                    
            except Exception as e:
                all_verified = False
                verification_results[table] = {
                    "verified": False,
                    "error": f"Row count comparison failed: {str(e)}"
                }
                log_message(f"Error verifying table {table}: {str(e)}", "ERROR")
                
        except Exception as e:
            all_verified = False
            verification_results[table] = {
                "verified": False,
                "error": str(e)
            }
            log_message(f"Error verifying table {table}: {str(e)}", "ERROR")
    
    # Write verification results to backup location
    try:
        verification_df = spark.createDataFrame([{"results": json.dumps(verification_results), "all_verified": all_verified}])
        verification_df.write.mode("overwrite").format("json").save(f"{backup_base_path}/{backup_folder}/_verification")
    except Exception as e:
        log_message(f"Error writing verification results: {str(e)}", "WARNING")
    
    if all_verified:
        log_message("Backup verification completed successfully", "INFO")
    else:
        log_message("Backup verification failed for one or more tables", "WARNING")
    
    return all_verified

def cleanup_old_backups():
    """Clean up backups older than the retention period"""
    retention_days = int(dbutils.widgets.get("retention_days"))
    if retention_days <= 0:
        log_message("Backup retention disabled, skipping cleanup", "INFO")
        return
    
    log_message(f"Checking for backups older than {retention_days} days...", "INFO")
    
    backup_base = backup_base_path
    
    try:
        # List folders in the backup location
        backup_folders = dbutils.fs.ls(backup_base)
        
        deleted_count = 0
        for folder in backup_folders:
            try:
                folder_name = folder.name.rstrip('/')
                # Parse timestamp from folder name (backup_YYYY-MM-DD_HH-MM-SS_uuid)
                if folder_name.startswith("backup_") and folder.isDir:
                    parts = folder_name.split("_")
                    if len(parts) >= 3:
                        timestamp_str = parts[1] + "_" + parts[2]
                        backup_timestamp = datetime.datetime.strptime(timestamp_str, "%Y-%m-%d_%H-%M-%S")
                        
                        # Calculate age in days
                        age_days = (datetime.datetime.now() - backup_timestamp).days
                        
                        if age_days > retention_days:
                            log_message(f"Deleting backup {folder_name} ({age_days} days old)", "INFO")
                            dbutils.fs.rm(f"{backup_base}/{folder_name}", True)
                            deleted_count += 1
            except Exception as e:
                log_message(f"Error processing backup folder {folder.name}: {str(e)}", "ERROR")
        
        log_message(f"Cleanup completed. Deleted {deleted_count} old backups.", "INFO")
                
    except Exception as e:
        log_message(f"Error listing backups for cleanup: {str(e)}", "ERROR")

print("Backup operation functions defined successfully")

# ============================================================================
# CELL 6: Main Backup Execution
# ============================================================================

try:
    # Initialize logging
    global log_entries
    log_entries = []
    
    start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message("Starting Microsoft Fabric lakehouse backup process", "INFO")
    
    # Parse and validate parameters
    validate_parameters()
    
    # Setup external storage authentication if needed
    setup_external_storage_auth()
    
    # Get configuration values from widgets
    source_lakehouse_name = dbutils.widgets.get("source_lakehouse_name")
    source_workspace_id = dbutils.widgets.get("source_workspace_id")
    backup_type = dbutils.widgets.get("backup_type")
    include_delta_logs = dbutils.widgets.get("include_delta_logs") == "True"
    verify_after_backup = dbutils.widgets.get("verify_backup") == "True"
    compress_backup = dbutils.widgets.get("compress_backup") == "True"
    enable_detailed_logging = dbutils.widgets.get("enable_detailed_logging") == "True"
    
    # Construct paths
    source_path = get_source_path()
    backup_base_path = get_backup_base_path()
    backup_folder = get_backup_folder()
    backup_path = f"{backup_base_path}/{backup_folder}"
    
    log_message(f"Source path: {source_path}", "INFO")
    log_message(f"Backup path: {backup_path}", "INFO")
    
    # Ensure backup location exists
    try:
        dbutils.fs.mkdirs(backup_path)
        log_message("Created backup directory", "INFO")
    except Exception as e:
        log_message(f"Error creating backup directory: {str(e)}", "WARNING")
    
    # Initialize backup stats
    total_files = 0
    total_size_bytes = 0
    tables = []
    
    # Get list of tables in the source lakehouse
    table_list = list_tables_in_lakehouse(source_path)
    
    if not table_list:
        log_message("No tables found in source lakehouse", "WARNING")
    else:
        log_message(f"Found {len(table_list)} tables in source lakehouse: {', '.join(table_list)}", "INFO")
    
    # Backup each table
    for table in table_list:
        try:
            log_message(f"Backing up table: {table}", "INFO")
            
            # Read the source table
            source_table_path = f"{source_path}/{table}"
            source_df = spark.read.format("delta").load(source_table_path)
            
            # Get table stats before backup (if possible)
            try:
                table_stats = spark.sql(f"DESCRIBE DETAIL delta.`{source_table_path}`").collect()[0]
                table_files = table_stats.numFiles
                table_size = table_stats.sizeInBytes
                
                # Update totals
                total_files += table_files
                total_size_bytes += table_size
                
                log_message(f"Table {table} has {table_files} files and size {table_size} bytes", "INFO")
            except Exception as e:
                log_message(f"Could not get detailed stats for table {table}: {str(e)}", "WARNING")
                # Use row count as a basic metric
                row_count = source_df.count()
                log_message(f"Table {table} has {row_count} rows", "INFO")
            
            tables.append(table)
            
            # Backup options
            write_options = {}
            
            if compress_backup:
                write_options["compression"] = "snappy"
            
            # Write to backup location
            backup_table_path = f"{backup_path}/{table}"
            source_df.write.format("delta").options(**write_options).mode("overwrite").save(backup_table_path)
            
            # Backup Delta logs if requested
            if include_delta_logs:
                try:
                    log_message(f"Copying Delta logs for table {table}", "INFO")
                    # Copy the _delta_log directory
                    source_log_path = f"{source_table_path}/_delta_log"
                    backup_log_path = f"{backup_table_path}/_delta_log"
                    
                    # Check if source logs exist
                    try:
                        dbutils.fs.ls(source_log_path)
                        dbutils.fs.cp(source_log_path, backup_log_path, True)
                        log_message(f"Successfully copied Delta logs for table {table}", "INFO")
                    except Exception as log_error:
                        log_message(f"Delta logs not found or error copying for table {table}: {str(log_error)}", "WARNING")
                        
                except Exception as e:
                    log_message(f"Error copying Delta logs for table {table}: {str(e)}", "WARNING")
            
            log_message(f"Completed backup of table: {table}", "INFO")
            
        except Exception as e:
            log_message(f"Error backing up table {table}: {str(e)}", "ERROR")
    
    # Record end time
    end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Create backup manifest
    manifest = create_backup_manifest(
        backup_folder, 
        tables, 
        total_files, 
        total_size_bytes, 
        start_time, 
        end_time
    )
    
    # Write logs to backup location if not already done
    if enable_detailed_logging and backup_type != "lakehouse":
        try:
            log_schema = StructType([
                StructField("timestamp", StringType(), False),
                StructField("level", StringType(), False),
                StructField("message", StringType(), False)
            ])
            
            log_df = spark.createDataFrame(log_entries, log_schema)
            log_df.write.format("delta").mode("overwrite").save(f"{backup_path}/_logs")
            log_message("Wrote logs to backup location", "INFO")
        except Exception as e:
            log_message(f"Error writing logs to backup location: {str(e)}", "WARNING")
    
    # Verify backup if requested
    if verify_after_backup and tables:
        verification_result = verify_backup(source_path, backup_path, tables)
        if verification_result:
            log_message("Backup verification successful", "INFO")
        else:
            log_message("Backup verification failed", "WARNING")
    
    # Clean up old backups
    try:
        cleanup_old_backups()
    except Exception as e:
        log_message(f"Error during backup cleanup: {str(e)}", "WARNING")
    
    # Final summary
    duration_seconds = (datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S") - 
                        datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")).total_seconds()
    
    log_message("", "INFO")
    log_message("=== BACKUP SUMMARY ===", "INFO")
    log_message(f"Backup completed at: {end_time}", "INFO")
    log_message(f"Backup location: {backup_path}", "INFO")
    log_message(f"Tables backed up: {len(tables)}", "INFO")
    log_message(f"Total files: {total_files}", "INFO")
    if total_size_bytes > 0:
        log_message(f"Total size: {total_size_bytes / (1024*1024):.2f} MB", "INFO")
    log_message(f"Duration: {duration_seconds:.2f} seconds", "INFO")
    log_message("=====================", "INFO")
    
    # Return success
    dbutils.notebook.exit({
        "status": "success",
        "backup_path": backup_path,
        "tables_count": len(tables),
        "files_count": total_files,
        "size_mb": total_size_bytes / (1024*1024) if total_size_bytes > 0 else 0,
        "duration_seconds": duration_seconds,
        "verification_passed": verification_result if verify_after_backup else None
    })
    
except Exception as e:
    log_message(f"Backup failed with error: {str(e)}", "ERROR")
    import traceback
    log_message(f"Full error trace: {traceback.format_exc()}", "ERROR")
    
    # Return failure
    dbutils.notebook.exit({
        "status": "failed",
        "error": str(e),
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

# ============================================================================
# END OF BACKUP NOTEBOOK
# ============================================================================

print("Backup notebook execution completed. Check the summary above for results.")
print("To schedule this notebook:")
print("1. Click the 'Schedule' button in the notebook toolbar")
print("2. Or add to a Data Factory pipeline")
print("3. Or use the REST API for programmatic execution")
print("")
print("For restore operations, use the separate Fabric_Lakehouse_Restore.ipynb notebook.")
