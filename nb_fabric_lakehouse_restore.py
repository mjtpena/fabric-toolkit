# Microsoft Fabric Lakehouse Restore Notebook
#
# This notebook provides restore capabilities for Microsoft Fabric Lakehouse backups
# created with the Fabric_Lakehouse_Backup.ipynb notebook.
#
# Features:
# - Restore all tables or specific tables from backup
# - Cross-workspace restore capabilities
# - Delta transaction log restoration
# - Backup manifest validation
# - Progress tracking and detailed logging
# - Support for all backup types (Storage Account, Lakehouse, ADLS)
#
# Use Cases:
# - Disaster recovery
# - Point-in-time restore
# - Environment synchronization (dev/test/prod)
# - Data migration between workspaces
#
# Author: Generated for Microsoft Fabric
# Version: 1.0

# ============================================================================
# CELL 1: Restore Configuration Parameters
# ============================================================================

# Configure these parameters before running the restore operation
dbutils.widgets.text("restore_source_path", "", "Full Path to Backup Location")
dbutils.widgets.text("restore_target_lakehouse", "", "Target Lakehouse Name")
dbutils.widgets.text("restore_target_workspace_id", "", "Target Workspace ID (leave empty for current workspace)")
dbutils.widgets.dropdown("restore_specific_tables", "False", ["True", "False"], "Restore Specific Tables Only")
dbutils.widgets.text("tables_to_restore", "", "Tables to Restore (comma-separated, if specific)")
dbutils.widgets.dropdown("overwrite_existing", "True", ["True", "False"], "Overwrite Existing Tables")
dbutils.widgets.dropdown("restore_delta_logs", "True", ["True", "False"], "Restore Delta Transaction Logs")
dbutils.widgets.dropdown("validate_before_restore", "True", ["True", "False"], "Validate Backup Before Restore")
dbutils.widgets.dropdown("verify_after_restore", "True", ["True", "False"], "Verify Restore After Completion")
dbutils.widgets.dropdown("enable_detailed_logging", "True", ["True", "False"], "Enable Detailed Logging")

print("Restore parameters configured. Ready to begin restore operation.")
print("Available backup types: Storage Account, Lakehouse, ADLS")
print("Support for cross-workspace operations enabled.")

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
print(f"Restore process initiated at: {datetime.datetime.now()}")

# ============================================================================
# CELL 3: Restore Helper Functions
# ============================================================================

def log_message(message, level="INFO"):
    """Log a message with timestamp and level"""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")

def validate_restore_parameters():
    """Validate that required restore parameters are provided"""
    restore_source_path = dbutils.widgets.get("restore_source_path")
    restore_target_lakehouse = dbutils.widgets.get("restore_target_lakehouse")
    
    if not restore_source_path:
        raise ValueError("Restore source path is required")
    
    if not restore_target_lakehouse:
        raise ValueError("Target lakehouse name is required")
    
    # Check if source path exists
    try:
        dbutils.fs.ls(restore_source_path)
        log_message(f"Backup source validated: {restore_source_path}", "INFO")
    except Exception as e:
        raise ValueError(f"Backup source path does not exist or is not accessible: {restore_source_path}")

def get_target_path():
    """Construct the target path for restore operation"""
    target_lakehouse = dbutils.widgets.get("restore_target_lakehouse")
    target_workspace_id = dbutils.widgets.get("restore_target_workspace_id")
    
    if target_workspace_id and target_workspace_id.strip():
        # External workspace lakehouse
        return f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_lakehouse}.Lakehouse/Tables"
    else:
        # Current workspace lakehouse
        return "Tables"

def read_backup_manifest(restore_source_path):
    """Read and validate the backup manifest"""
    try:
        manifest_path = f"{restore_source_path}/_manifest"
        manifest_df = spark.read.format("json").load(manifest_path)
        manifest_data = manifest_df.collect()[0].asDict()
        
        log_message("Backup manifest loaded successfully", "INFO")
        log_message(f"Backup ID: {manifest_data.get('backup_id', 'Unknown')}", "INFO")
        log_message(f"Backup timestamp: {manifest_data.get('backup_timestamp', 'Unknown')}", "INFO")
        log_message(f"Source lakehouse: {manifest_data.get('source_lakehouse_name', 'Unknown')}", "INFO")
        log_message(f"Tables in backup: {len(manifest_data.get('tables', []))}", "INFO")
        
        return manifest_data
    except Exception as e:
        log_message(f"Warning: Could not read backup manifest: {str(e)}", "WARNING")
        return None

def get_tables_to_restore(restore_source_path, manifest_data=None):
    """Get list of tables to restore based on parameters"""
    restore_specific = dbutils.widgets.get("restore_specific_tables") == "True"
    tables_str = dbutils.widgets.get("tables_to_restore")
    
    if restore_specific and tables_str.strip():
        # User specified specific tables
        tables_to_restore = [t.strip() for t in tables_str.split(",")]
        log_message(f"Restoring specific tables: {', '.join(tables_to_restore)}", "INFO")
        return tables_to_restore
    else:
        # Get all tables from backup
        if manifest_data and 'tables' in manifest_data:
            # Use manifest if available
            tables_to_restore = manifest_data['tables']
            log_message(f"Using manifest: Found {len(tables_to_restore)} tables", "INFO")
            return tables_to_restore
        else:
            # Scan backup directory
            try:
                backup_contents = dbutils.fs.ls(restore_source_path)
                tables_to_restore = [item.name.rstrip('/') for item in backup_contents 
                                     if not item.name.startswith('_') and not item.name.startswith('.') and item.isDir]
                log_message(f"Directory scan: Found {len(tables_to_restore)} tables", "INFO")
                return tables_to_restore
            except Exception as e:
                log_message(f"Error listing backup contents: {str(e)}", "ERROR")
                return []

def validate_backup_integrity(restore_source_path, tables_to_restore):
    """Validate backup integrity before restore"""
    log_message("Validating backup integrity...", "INFO")
    
    validation_results = {}
    all_valid = True
    
    for table in tables_to_restore:
        try:
            table_path = f"{restore_source_path}/{table}"
            
            # Check if table directory exists
            dbutils.fs.ls(table_path)
            
            # Try to read the table
            df = spark.read.format("delta").load(table_path)
            row_count = df.count()
            
            validation_results[table] = {
                "valid": True,
                "row_count": row_count
            }
            
            log_message(f"Table {table}: Valid ({row_count} rows)", "INFO")
            
        except Exception as e:
            all_valid = False
            validation_results[table] = {
                "valid": False,
                "error": str(e)
            }
            log_message(f"Table {table}: Invalid - {str(e)}", "ERROR")
    
    if all_valid:
        log_message("Backup integrity validation passed", "INFO")
    else:
        log_message("Backup integrity validation failed for some tables", "WARNING")
    
    return all_valid, validation_results

def verify_restore_result(source_path, target_path, tables):
    """Verify restore results by comparing row counts"""
    log_message("Verifying restore results...", "INFO")
    
    verification_results = {}
    all_verified = True
    
    for table in tables:
        try:
            source_table_path = f"{source_path}/{table}"
            target_table_path = f"{target_path}/{table}"
            
            # Count rows in source backup and restored table
            source_count = spark.read.format("delta").load(source_table_path).count()
            target_count = spark.read.format("delta").load(target_table_path).count()
            
            rows_match = source_count == target_count
            
            if not rows_match:
                all_verified = False
            
            verification_results[table] = {
                "verified": rows_match,
                "backup_rows": source_count,
                "restored_rows": target_count
            }
            
            log_message(f"Table {table} verification: {'SUCCESS' if rows_match else 'FAILED'}", "INFO")
            if not rows_match:
                log_message(f"  - Backup rows: {source_count}, Restored rows: {target_count}", "WARNING")
            
        except Exception as e:
            all_verified = False
            verification_results[table] = {
                "verified": False,
                "error": str(e)
            }
            log_message(f"Error verifying table {table}: {str(e)}", "ERROR")
    
    if all_verified:
        log_message("Restore verification completed successfully", "INFO")
    else:
        log_message("Restore verification failed for one or more tables", "WARNING")
    
    return all_verified, verification_results

print("Restore helper functions defined successfully")

# ============================================================================
# CELL 4: Main Restore Function
# ============================================================================

def execute_restore_operation():
    """Execute the main restore operation"""
    
    start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message("Starting Microsoft Fabric lakehouse restore process", "INFO")
    
    try:
        # Validate parameters
        validate_restore_parameters()
        
        # Get configuration values
        restore_source_path = dbutils.widgets.get("restore_source_path")
        target_lakehouse = dbutils.widgets.get("restore_target_lakehouse")
        overwrite_existing = dbutils.widgets.get("overwrite_existing") == "True"
        restore_delta_logs = dbutils.widgets.get("restore_delta_logs") == "True"
        validate_before_restore = dbutils.widgets.get("validate_before_restore") == "True"
        verify_after_restore = dbutils.widgets.get("verify_after_restore") == "True"
        
        # Construct target path
        target_path = get_target_path()
        
        log_message(f"Restore source: {restore_source_path}", "INFO")
        log_message(f"Restore target: {target_path}", "INFO")
        log_message(f"Target lakehouse: {target_lakehouse}", "INFO")
        
        # Read backup manifest if available
        manifest_data = read_backup_manifest(restore_source_path)
        
        # Get list of tables to restore
        tables_to_restore = get_tables_to_restore(restore_source_path, manifest_data)
        
        if not tables_to_restore:
            log_message("No tables found to restore", "WARNING")
            return {
                "status": "warning",
                "message": "No tables found to restore",
                "tables_restored": 0
            }
        
        log_message(f"Tables to restore: {', '.join(tables_to_restore)}", "INFO")
        
        # Validate backup integrity if requested
        if validate_before_restore:
            integrity_valid, validation_results = validate_backup_integrity(restore_source_path, tables_to_restore)
            if not integrity_valid:
                log_message("Backup integrity validation failed. Restore aborted.", "ERROR")
                return {
                    "status": "failed",
                    "error": "Backup integrity validation failed",
                    "validation_results": validation_results
                }
        
        # Restore each table
        restored_count = 0
        failed_tables = []
        
        for table in tables_to_restore:
            try:
                log_message(f"Restoring table: {table}", "INFO")
                
                # Read from backup
                backup_table_path = f"{restore_source_path}/{table}"
                backup_df = spark.read.format("delta").load(backup_table_path)
                
                # Write to target
                target_table_path = f"{target_path}/{table}"
                
                if overwrite_existing:
                    backup_df.write.format("delta").mode("overwrite").save(target_table_path)
                else:
                    # Check if table exists first
                    try:
                        existing_df = spark.read.format("delta").load(target_table_path)
                        log_message(f"Table {table} already exists. Skipping (overwrite disabled).", "WARNING")
                        continue
                    except:
                        # Table doesn't exist, proceed with create
                        backup_df.write.format("delta").mode("overwrite").save(target_table_path)
                
                log_message(f"Data restored for table: {table}", "INFO")
                
                # Restore Delta logs if requested
                if restore_delta_logs:
                    try:
                        backup_log_path = f"{backup_table_path}/_delta_log"
                        target_log_path = f"{target_table_path}/_delta_log"
                        
                        # Check if backup logs exist
                        dbutils.fs.ls(backup_log_path)
                        log_message(f"Restoring Delta logs for table {table}", "INFO")
                        dbutils.fs.cp(backup_log_path, target_log_path, True)
                        log_message(f"Delta logs restored for table: {table}", "INFO")
                        
                    except Exception as log_error:
                        log_message(f"Delta logs not found or error restoring for table {table}: {str(log_error)}", "WARNING")
                
                log_message(f"Completed restore of table: {table}", "INFO")
                restored_count += 1
                
            except Exception as e:
                log_message(f"Error restoring table {table}: {str(e)}", "ERROR")
                failed_tables.append(table)
        
        # Record end time
        end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration_seconds = (datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S") - 
                           datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")).total_seconds()
        
        # Verify restore if requested
        verification_results = None
        if verify_after_restore and restored_count > 0:
            successfully_restored = [t for t in tables_to_restore if t not in failed_tables]
            verification_passed, verification_results = verify_restore_result(
                restore_source_path, target_path, successfully_restored
            )
        
        # Final summary
        log_message("", "INFO")
        log_message("=== RESTORE SUMMARY ===", "INFO")
        log_message(f"Restore completed at: {end_time}", "INFO")
        log_message(f"Source: {restore_source_path}", "INFO")
        log_message(f"Target: {target_path}", "INFO")
        log_message(f"Tables requested: {len(tables_to_restore)}", "INFO")
        log_message(f"Tables restored: {restored_count}", "INFO")
        log_message(f"Tables failed: {len(failed_tables)}", "INFO")
        if failed_tables:
            log_message(f"Failed tables: {', '.join(failed_tables)}", "WARNING")
        log_message(f"Duration: {duration_seconds:.2f} seconds", "INFO")
        log_message("======================", "INFO")
        
        # Return results
        result = {
            "status": "success" if len(failed_tables) == 0 else "partial_success",
            "restore_source": restore_source_path,
            "restore_target": target_path,
            "tables_requested": len(tables_to_restore),
            "tables_restored": restored_count,
            "tables_failed": len(failed_tables),
            "failed_tables": failed_tables,
            "duration_seconds": duration_seconds,
            "verification_results": verification_results
        }
        
        if len(failed_tables) == 0:
            log_message("Restore operation completed successfully", "INFO")
        else:
            log_message(f"Restore operation completed with {len(failed_tables)} failures", "WARNING")
        
        return result
        
    except Exception as e:
        log_message(f"Restore operation failed with error: {str(e)}", "ERROR")
        import traceback
        log_message(f"Full error trace: {traceback.format_exc()}", "ERROR")
        
        return {
            "status": "failed",
            "error": str(e),
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

print("Main restore function defined successfully")

# ============================================================================
# CELL 5: Execute Restore Operation
# ============================================================================

# Execute the restore operation
print("Starting restore operation...")
print("=" * 50)

restore_result = execute_restore_operation()

print("=" * 50)
print("Restore operation completed.")
print(f"Final status: {restore_result['status']}")

# Display results in notebook output
if restore_result['status'] == 'success':
    print(f"✅ SUCCESS: All {restore_result['tables_restored']} tables restored successfully")
elif restore_result['status'] == 'partial_success':
    print(f"⚠️  PARTIAL SUCCESS: {restore_result['tables_restored']} tables restored, {restore_result['tables_failed']} failed")
    if restore_result['failed_tables']:
        print(f"Failed tables: {', '.join(restore_result['failed_tables'])}")
else:
    print(f"❌ FAILED: {restore_result.get('error', 'Unknown error')}")

# Exit with results for pipeline integration
dbutils.notebook.exit(restore_result)

# ============================================================================
# CELL 6: Additional Utility Functions (Optional)
# ============================================================================

# These functions can be used for advanced restore scenarios
# Uncomment and run separately as needed

def list_available_backups(backup_base_path):
    """
    List all available backups in a backup location
    Usage: Run this cell separately to explore available backups
    """
    try:
        backup_folders = dbutils.fs.ls(backup_base_path)
        backups = []
        
        for folder in backup_folders:
            folder_name = folder.name.rstrip('/')
            if folder_name.startswith("backup_") and folder.isDir:
                try:
                    # Try to read manifest
                    manifest_path = f"{backup_base_path}/{folder_name}/_manifest"
                    manifest_df = spark.read.format("json").load(manifest_path)
                    manifest_data = manifest_df.collect()[0].asDict()
                    
                    backups.append({
                        "folder": folder_name,
                        "full_path": f"{backup_base_path}/{folder_name}",
                        "backup_id": manifest_data.get('backup_id', 'Unknown'),
                        "timestamp": manifest_data.get('backup_timestamp', 'Unknown'),
                        "source_lakehouse": manifest_data.get('source_lakehouse_name', 'Unknown'),
                        "tables_count": len(manifest_data.get('tables', [])),
                        "size_mb": manifest_data.get('backup_size_bytes', 0) / (1024*1024)
                    })
                except Exception as e:
                    # Backup without manifest or invalid manifest
                    backups.append({
                        "folder": folder_name,
                        "full_path": f"{backup_base_path}/{folder_name}",
                        "backup_id": "Unknown",
                        "timestamp": "Unknown",
                        "source_lakehouse": "Unknown",
                        "tables_count": "Unknown",
                        "size_mb": "Unknown",
                        "error": str(e)
                    })
        
        return backups
    except Exception as e:
        print(f"Error listing backups: {str(e)}")
        return []

def compare_backup_tables(backup_path1, backup_path2):
    """
    Compare tables between two backups
    Usage: Run this cell separately to compare backup contents
    """
    try:
        # Get tables from first backup
        tables1 = set()
        try:
            contents1 = dbutils.fs.ls(backup_path1)
            tables1 = {item.name.rstrip('/') for item in contents1 
                      if not item.name.startswith('_') and not item.name.startswith('.') and item.isDir}
        except Exception as e:
            print(f"Error reading backup 1: {str(e)}")
        
        # Get tables from second backup
        tables2 = set()
        try:
            contents2 = dbutils.fs.ls(backup_path2)
            tables2 = {item.name.rstrip('/') for item in contents2 
                      if not item.name.startswith('_') and not item.name.startswith('.') and item.isDir}
        except Exception as e:
            print(f"Error reading backup 2: {str(e)}")
        
        # Compare
        common_tables = tables1.intersection(tables2)
        only_in_1 = tables1 - tables2
        only_in_2 = tables2 - tables1
        
        print(f"Backup 1: {len(tables1)} tables")
        print(f"Backup 2: {len(tables2)} tables")
        print(f"Common tables: {len(common_tables)}")
        print(f"Only in backup 1: {len(only_in_1)}")
        print(f"Only in backup 2: {len(only_in_2)}")
        
        if only_in_1:
            print(f"Tables only in backup 1: {', '.join(sorted(only_in_1))}")
        if only_in_2:
            print(f"Tables only in backup 2: {', '.join(sorted(only_in_2))}")
        
        return {
            "backup1_tables": tables1,
            "backup2_tables": tables2,
            "common_tables": common_tables,
            "only_in_backup1": only_in_1,
            "only_in_backup2": only_in_2
        }
    except Exception as e:
        print(f"Error comparing backups: {str(e)}")
        return None

# Uncomment these lines to use the utility functions:
# backup_base = "abfss://backups@mystorageaccount.dfs.core.windows.net"
# available_backups = list_available_backups(backup_base)
# print(f"Found {len(available_backups)} backups")

print("Utility functions defined. Uncomment and modify the example lines above to use them.")

# ============================================================================
# END OF RESTORE NOTEBOOK
# ============================================================================

print("")
print("Restore notebook execution completed.")
print("Use this notebook for:")
print("1. Disaster recovery operations")
print("2. Point-in-time restore from backups")
print("3. Environment synchronization")
print("4. Data migration between workspaces")
print("")
print("For creating new backups, use the Fabric_Lakehouse_Backup.ipynb notebook.")
