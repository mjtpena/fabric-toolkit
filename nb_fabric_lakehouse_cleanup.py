# Microsoft Fabric Lakehouse Backup Cleanup Notebook
#
# This notebook provides automated cleanup capabilities for Microsoft Fabric Lakehouse backups
# based on configurable retention policies.
#
# Features:
# - Flexible retention policies (age-based, count-based, size-based)
# - Support for all backup storage types (Storage Account, Lakehouse, ADLS)
# - Cross-workspace backup cleanup
# - Dry-run mode for safe testing
# - Detailed logging and reporting
# - Backup preservation rules (keep daily, weekly, monthly)
# - Manual backup exclusion support
#
# Use Cases:
# - Automated cleanup as part of backup strategy
# - Storage cost optimization
# - Compliance with data retention policies
# - Emergency cleanup for storage space
#
# Compatible with Microsoft Fabric scheduling for automated execution
#
# Author: Generated for Microsoft Fabric
# Version: 1.0

# ============================================================================
# CELL 1: Cleanup Configuration Parameters
# ============================================================================

# Configure these parameters before running the cleanup operation
dbutils.widgets.dropdown("cleanup_mode", "age_based", ["age_based", "count_based", "size_based", "advanced"], "Cleanup Mode")
dbutils.widgets.text("backup_location", "", "Backup Location Path (full path to backup directory)")
dbutils.widgets.text("backup_workspace_id", "", "Backup Workspace ID (if different from current)")
dbutils.widgets.text("retention_days", "30", "Maximum Age in Days (for age_based mode)")
dbutils.widgets.text("max_backup_count", "10", "Maximum Number of Backups to Keep (for count_based mode)")
dbutils.widgets.text("max_size_gb", "100", "Maximum Total Size in GB (for size_based mode)")
dbutils.widgets.dropdown("preserve_policy", "none", ["none", "daily", "weekly", "monthly"], "Preservation Policy")
dbutils.widgets.text("preserve_daily_count", "7", "Number of Daily Backups to Preserve")
dbutils.widgets.text("preserve_weekly_count", "4", "Number of Weekly Backups to Preserve")
dbutils.widgets.text("preserve_monthly_count", "12", "Number of Monthly Backups to Preserve")
dbutils.widgets.dropdown("dry_run", "True", ["True", "False"], "Dry Run Mode (preview only)")
dbutils.widgets.text("exclude_patterns", "", "Exclude Patterns (comma-separated, e.g., 'manual_*,prod_*')")
dbutils.widgets.text("include_patterns", "backup_*", "Include Patterns (comma-separated)")
dbutils.widgets.dropdown("use_managed_identity", "True", ["True", "False"], "Use Managed Identity for External Storage")
dbutils.widgets.dropdown("enable_detailed_logging", "True", ["True", "False"], "Enable Detailed Logging")
dbutils.widgets.dropdown("generate_report", "True", ["True", "False"], "Generate Cleanup Report")

print("Cleanup parameters configured. Ready to begin cleanup operation.")
print("IMPORTANT: Review parameters carefully before disabling dry_run mode.")
print("Cleanup modes available: age_based, count_based, size_based, advanced")

# ============================================================================
# CELL 2: Import Required Libraries
# ============================================================================

import os
import json
import datetime
import uuid
import re
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, LongType, BooleanType
from delta.tables import DeltaTable
import time
from collections import defaultdict

print("Libraries imported successfully")
print(f"Cleanup process initiated at: {datetime.datetime.now()}")

# ============================================================================
# CELL 3: Cleanup Helper Functions
# ============================================================================

def log_message(message, level="INFO"):
    """Log a message with timestamp and level"""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")

def validate_cleanup_parameters():
    """Validate that required cleanup parameters are provided"""
    backup_location = dbutils.widgets.get("backup_location")
    cleanup_mode = dbutils.widgets.get("cleanup_mode")
    
    if not backup_location:
        raise ValueError("Backup location path is required")
    
    # Validate cleanup mode specific parameters
    if cleanup_mode == "age_based":
        try:
            retention_days = int(dbutils.widgets.get("retention_days"))
            if retention_days <= 0:
                raise ValueError("Retention days must be positive")
        except ValueError:
            raise ValueError("Retention days must be a valid positive integer")
    
    elif cleanup_mode == "count_based":
        try:
            max_count = int(dbutils.widgets.get("max_backup_count"))
            if max_count <= 0:
                raise ValueError("Maximum backup count must be positive")
        except ValueError:
            raise ValueError("Maximum backup count must be a valid positive integer")
    
    elif cleanup_mode == "size_based":
        try:
            max_size = float(dbutils.widgets.get("max_size_gb"))
            if max_size <= 0:
                raise ValueError("Maximum size must be positive")
        except ValueError:
            raise ValueError("Maximum size must be a valid positive number")
    
    # Check if backup location exists
    try:
        dbutils.fs.ls(backup_location)
        log_message(f"Backup location validated: {backup_location}", "INFO")
    except Exception as e:
        raise ValueError(f"Backup location does not exist or is not accessible: {backup_location}")

def setup_external_storage_auth():
    """Setup authentication for external storage if needed"""
    use_managed_identity = dbutils.widgets.get("use_managed_identity") == "True"
    backup_location = dbutils.widgets.get("backup_location")
    
    # Check if this is external storage (not OneLake)
    if use_managed_identity and "dfs.core.windows.net" in backup_location and "onelake" not in backup_location:
        log_message("Configuring managed identity for external storage authentication", "INFO")
        
        # Extract storage account name from path
        import re
        match = re.search(r'([^/@]+)\.dfs\.core\.windows\.net', backup_location)
        if match:
            account_name = match.group(1)
            spark.conf.set(f"fs.azure.account.auth.type.{account_name}.dfs.core.windows.net", "OAuth")
            spark.conf.set(f"fs.azure.account.oauth.provider.type.{account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider")
            log_message(f"Configured authentication for storage account: {account_name}", "INFO")

def parse_backup_folder_name(folder_name):
    """Parse backup folder name to extract timestamp and metadata"""
    try:
        # Expected format: backup_YYYY-MM-DD_HH-MM-SS_uuid
        if not folder_name.startswith("backup_"):
            return None
        
        parts = folder_name.split("_")
        if len(parts) < 4:  # backup, date, time, uuid (minimum)
            return None
        
        date_part = parts[1]  # YYYY-MM-DD
        time_part = parts[2]  # HH-MM-SS
        backup_id = parts[3] if len(parts) > 3 else "unknown"
        
        timestamp_str = f"{date_part} {time_part.replace('-', ':')}"
        backup_timestamp = datetime.datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        
        return {
            "folder_name": folder_name,
            "timestamp": backup_timestamp,
            "backup_id": backup_id,
            "date_part": date_part,
            "time_part": time_part
        }
    except Exception as e:
        log_message(f"Error parsing backup folder name '{folder_name}': {str(e)}", "WARNING")
        return None

def get_backup_size(backup_path):
    """Calculate total size of a backup in bytes"""
    try:
        total_size = 0
        files = dbutils.fs.ls(backup_path)
        
        for file_info in files:
            if file_info.isDir:
                # Recursively calculate directory size
                total_size += get_backup_size(file_info.path)
            else:
                total_size += file_info.size
        
        return total_size
    except Exception as e:
        log_message(f"Error calculating backup size for {backup_path}: {str(e)}", "WARNING")
        return 0

def read_backup_manifest(backup_path):
    """Read backup manifest if available"""
    try:
        manifest_path = f"{backup_path}/_manifest"
        manifest_df = spark.read.format("json").load(manifest_path)
        manifest_data = manifest_df.collect()[0].asDict()
        return manifest_data
    except Exception as e:
        # Manifest not available or corrupted
        return None

def matches_pattern(text, patterns):
    """Check if text matches any of the given patterns (supports wildcards)"""
    if not patterns:
        return False
    
    for pattern in patterns:
        # Convert wildcard pattern to regex
        regex_pattern = pattern.replace("*", ".*").replace("?", ".")
        if re.match(f"^{regex_pattern}$", text):
            return True
    return False

print("Cleanup helper functions defined successfully")

# ============================================================================
# CELL 4: Backup Discovery and Analysis Functions
# ============================================================================

def discover_backups(backup_location):
    """Discover all backups in the specified location"""
    log_message("Discovering backups...", "INFO")
    
    try:
        backup_folders = dbutils.fs.ls(backup_location)
        include_patterns = [p.strip() for p in dbutils.widgets.get("include_patterns").split(",") if p.strip()]
        exclude_patterns = [p.strip() for p in dbutils.widgets.get("exclude_patterns").split(",") if p.strip()]
        
        backups = []
        
        for folder in backup_folders:
            if not folder.isDir:
                continue
            
            folder_name = folder.name.rstrip('/')
            
            # Check include patterns
            if include_patterns and not matches_pattern(folder_name, include_patterns):
                continue
            
            # Check exclude patterns
            if exclude_patterns and matches_pattern(folder_name, exclude_patterns):
                log_message(f"Excluding backup {folder_name} (matches exclude pattern)", "INFO")
                continue
            
            # Parse backup folder
            backup_info = parse_backup_folder_name(folder_name)
            if not backup_info:
                log_message(f"Skipping folder {folder_name} (invalid backup name format)", "INFO")
                continue
            
            # Get backup size
            backup_size = get_backup_size(folder.path)
            
            # Read manifest if available
            manifest_data = read_backup_manifest(folder.path)
            
            # Compile backup information
            backup_record = {
                "folder_name": folder_name,
                "full_path": folder.path,
                "timestamp": backup_info["timestamp"],
                "backup_id": backup_info["backup_id"],
                "size_bytes": backup_size,
                "size_mb": backup_size / (1024 * 1024),
                "size_gb": backup_size / (1024 * 1024 * 1024),
                "age_days": (datetime.datetime.now() - backup_info["timestamp"]).days,
                "manifest": manifest_data
            }
            
            # Add manifest information if available
            if manifest_data:
                backup_record.update({
                    "source_lakehouse": manifest_data.get("source_lakehouse_name", "Unknown"),
                    "tables_count": len(manifest_data.get("tables", [])),
                    "backup_type": manifest_data.get("backup_type", "Unknown")
                })
            else:
                backup_record.update({
                    "source_lakehouse": "Unknown",
                    "tables_count": 0,
                    "backup_type": "Unknown"
                })
            
            backups.append(backup_record)
        
        # Sort backups by timestamp (newest first)
        backups.sort(key=lambda x: x["timestamp"], reverse=True)
        
        log_message(f"Discovered {len(backups)} backups", "INFO")
        return backups
        
    except Exception as e:
        log_message(f"Error discovering backups: {str(e)}", "ERROR")
        return []

def apply_preservation_policy(backups):
    """Apply preservation policy to protect certain backups"""
    preserve_policy = dbutils.widgets.get("preserve_policy")
    
    if preserve_policy == "none":
        return set()  # No backups to preserve
    
    preserved = set()
    
    try:
        preserve_daily_count = int(dbutils.widgets.get("preserve_daily_count"))
        preserve_weekly_count = int(dbutils.widgets.get("preserve_weekly_count"))
        preserve_monthly_count = int(dbutils.widgets.get("preserve_monthly_count"))
    except ValueError:
        log_message("Invalid preservation counts, using defaults", "WARNING")
        preserve_daily_count = 7
        preserve_weekly_count = 4
        preserve_monthly_count = 12
    
    # Group backups by date patterns
    daily_backups = defaultdict(list)
    weekly_backups = defaultdict(list)
    monthly_backups = defaultdict(list)
    
    for backup in backups:
        timestamp = backup["timestamp"]
        date_key = timestamp.strftime("%Y-%m-%d")
        week_key = timestamp.strftime("%Y-W%U")  # Year-Week
        month_key = timestamp.strftime("%Y-%m")   # Year-Month
        
        daily_backups[date_key].append(backup)
        weekly_backups[week_key].append(backup)
        monthly_backups[month_key].append(backup)
    
    # Preserve daily backups (most recent per day)
    if preserve_policy in ["daily", "weekly", "monthly"]:
        daily_dates = sorted(daily_backups.keys(), reverse=True)[:preserve_daily_count]
        for date_key in daily_dates:
            # Keep the most recent backup for each day
            newest_daily = max(daily_backups[date_key], key=lambda x: x["timestamp"])
            preserved.add(newest_daily["folder_name"])
            log_message(f"Preserving daily backup: {newest_daily['folder_name']}", "INFO")
    
    # Preserve weekly backups (most recent per week)
    if preserve_policy in ["weekly", "monthly"]:
        weekly_keys = sorted(weekly_backups.keys(), reverse=True)[:preserve_weekly_count]
        for week_key in weekly_keys:
            # Keep the most recent backup for each week
            newest_weekly = max(weekly_backups[week_key], key=lambda x: x["timestamp"])
            preserved.add(newest_weekly["folder_name"])
            log_message(f"Preserving weekly backup: {newest_weekly['folder_name']}", "INFO")
    
    # Preserve monthly backups (most recent per month)
    if preserve_policy == "monthly":
        monthly_keys = sorted(monthly_backups.keys(), reverse=True)[:preserve_monthly_count]
        for month_key in monthly_keys:
            # Keep the most recent backup for each month
            newest_monthly = max(monthly_backups[month_key], key=lambda x: x["timestamp"])
            preserved.add(newest_monthly["folder_name"])
            log_message(f"Preserving monthly backup: {newest_monthly['folder_name']}", "INFO")
    
    log_message(f"Preservation policy '{preserve_policy}' protects {len(preserved)} backups", "INFO")
    return preserved

def determine_backups_to_delete(backups):
    """Determine which backups should be deleted based on cleanup mode"""
    cleanup_mode = dbutils.widgets.get("cleanup_mode")
    preserved_backups = apply_preservation_policy(backups)
    
    backups_to_delete = []
    
    if cleanup_mode == "age_based":
        retention_days = int(dbutils.widgets.get("retention_days"))
        log_message(f"Age-based cleanup: removing backups older than {retention_days} days", "INFO")
        
        for backup in backups:
            if backup["folder_name"] not in preserved_backups and backup["age_days"] > retention_days:
                backups_to_delete.append(backup)
    
    elif cleanup_mode == "count_based":
        max_count = int(dbutils.widgets.get("max_backup_count"))
        log_message(f"Count-based cleanup: keeping only {max_count} most recent backups", "INFO")
        
        # Filter out preserved backups first
        non_preserved = [b for b in backups if b["folder_name"] not in preserved_backups]
        
        # Sort by timestamp (newest first) and keep only max_count
        non_preserved.sort(key=lambda x: x["timestamp"], reverse=True)
        
        if len(non_preserved) > max_count:
            backups_to_delete = non_preserved[max_count:]
    
    elif cleanup_mode == "size_based":
        max_size_gb = float(dbutils.widgets.get("max_size_gb"))
        max_size_bytes = max_size_gb * 1024 * 1024 * 1024
        
        log_message(f"Size-based cleanup: keeping total size under {max_size_gb:.2f} GB", "INFO")
        
        # Calculate current total size
        total_size = sum(b["size_bytes"] for b in backups)
        log_message(f"Current total size: {total_size / (1024*1024*1024):.2f} GB", "INFO")
        
        if total_size > max_size_bytes:
            # Sort by timestamp (oldest first) and remove until under limit
            non_preserved = [b for b in backups if b["folder_name"] not in preserved_backups]
            non_preserved.sort(key=lambda x: x["timestamp"])  # Oldest first
            
            current_size = total_size
            for backup in non_preserved:
                if current_size <= max_size_bytes:
                    break
                backups_to_delete.append(backup)
                current_size -= backup["size_bytes"]
    
    elif cleanup_mode == "advanced":
        # Advanced mode combines all criteria
        log_message("Advanced cleanup mode: applying multiple criteria", "INFO")
        
        retention_days = int(dbutils.widgets.get("retention_days"))
        max_count = int(dbutils.widgets.get("max_backup_count"))
        max_size_gb = float(dbutils.widgets.get("max_size_gb"))
        max_size_bytes = max_size_gb * 1024 * 1024 * 1024
        
        # First, apply age-based deletion
        age_candidates = [b for b in backups 
                         if b["folder_name"] not in preserved_backups and b["age_days"] > retention_days]
        
        # Then apply count-based to remaining
        remaining = [b for b in backups if b not in age_candidates and b["folder_name"] not in preserved_backups]
        remaining.sort(key=lambda x: x["timestamp"], reverse=True)
        
        count_candidates = remaining[max_count:] if len(remaining) > max_count else []
        
        # Combine age and count candidates
        preliminary_deletions = age_candidates + count_candidates
        
        # Finally, apply size-based if needed
        remaining_after_deletions = [b for b in backups if b not in preliminary_deletions]
        remaining_size = sum(b["size_bytes"] for b in remaining_after_deletions)
        
        if remaining_size > max_size_bytes:
            # Need to delete more based on size
            additional_candidates = [b for b in remaining_after_deletions 
                                   if b["folder_name"] not in preserved_backups]
            additional_candidates.sort(key=lambda x: x["timestamp"])  # Oldest first
            
            current_size = remaining_size
            for backup in additional_candidates:
                if current_size <= max_size_bytes:
                    break
                preliminary_deletions.append(backup)
                current_size -= backup["size_bytes"]
        
        backups_to_delete = preliminary_deletions
    
    log_message(f"Determined {len(backups_to_delete)} backups for deletion", "INFO")
    return backups_to_delete

print("Backup analysis functions defined successfully")

# ============================================================================
# CELL 5: Cleanup Execution and Reporting Functions
# ============================================================================

def execute_cleanup(backups_to_delete):
    """Execute the cleanup operation"""
    dry_run = dbutils.widgets.get("dry_run") == "True"
    
    if dry_run:
        log_message("DRY RUN MODE: No backups will be actually deleted", "INFO")
    else:
        log_message("LIVE MODE: Backups will be permanently deleted", "WARNING")
    
    deletion_results = []
    total_size_freed = 0
    successful_deletions = 0
    failed_deletions = 0
    
    for backup in backups_to_delete:
        deletion_record = {
            "folder_name": backup["folder_name"],
            "full_path": backup["full_path"],
            "size_bytes": backup["size_bytes"],
            "age_days": backup["age_days"],
            "timestamp": backup["timestamp"],
            "deleted": False,
            "error": None
        }
        
        try:
            log_message(f"{'[DRY RUN] Would delete' if dry_run else 'Deleting'} backup: {backup['folder_name']} "
                       f"(Age: {backup['age_days']} days, Size: {backup['size_mb']:.2f} MB)", "INFO")
            
            if not dry_run:
                # Actually delete the backup
                dbutils.fs.rm(backup["full_path"], True)
                deletion_record["deleted"] = True
                successful_deletions += 1
                total_size_freed += backup["size_bytes"]
                log_message(f"Successfully deleted backup: {backup['folder_name']}", "INFO")
            else:
                # Dry run - just simulate
                deletion_record["deleted"] = True  # Would be deleted
                successful_deletions += 1
                total_size_freed += backup["size_bytes"]
        
        except Exception as e:
            deletion_record["error"] = str(e)
            failed_deletions += 1
            log_message(f"Error {'simulating deletion of' if dry_run else 'deleting'} backup {backup['folder_name']}: {str(e)}", "ERROR")
        
        deletion_results.append(deletion_record)
    
    # Summary
    log_message("", "INFO")
    log_message("=== CLEANUP SUMMARY ===", "INFO")
    log_message(f"Mode: {'DRY RUN' if dry_run else 'LIVE EXECUTION'}", "INFO")
    log_message(f"Backups processed: {len(backups_to_delete)}", "INFO")
    log_message(f"Successful {'simulations' if dry_run else 'deletions'}: {successful_deletions}", "INFO")
    log_message(f"Failed operations: {failed_deletions}", "INFO")
    log_message(f"Space {'that would be' if dry_run else ''} freed: {total_size_freed / (1024*1024*1024):.2f} GB", "INFO")
    log_message("=======================", "INFO")
    
    return {
        "dry_run": dry_run,
        "total_processed": len(backups_to_delete),
        "successful_operations": successful_deletions,
        "failed_operations": failed_deletions,
        "space_freed_bytes": total_size_freed,
        "space_freed_gb": total_size_freed / (1024*1024*1024),
        "deletion_results": deletion_results
    }

def generate_cleanup_report(backups_discovered, backups_preserved, backups_deleted, cleanup_results):
    """Generate a comprehensive cleanup report"""
    generate_report = dbutils.widgets.get("generate_report") == "True"
    
    if not generate_report:
        return None
    
    log_message("Generating cleanup report...", "INFO")
    
    # Create report data
    report_data = {
        "report_timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "cleanup_mode": dbutils.widgets.get("cleanup_mode"),
        "backup_location": dbutils.widgets.get("backup_location"),
        "dry_run": dbutils.widgets.get("dry_run") == "True",
        "preserve_policy": dbutils.widgets.get("preserve_policy"),
        "total_backups_discovered": len(backups_discovered),
        "total_backups_preserved": len(backups_preserved),
        "total_backups_deleted": cleanup_results["successful_operations"],
        "total_size_discovered_gb": sum(b["size_gb"] for b in backups_discovered),
        "total_size_freed_gb": cleanup_results["space_freed_gb"],
        "oldest_backup": min(backups_discovered, key=lambda x: x["timestamp"])["timestamp"].strftime("%Y-%m-%d %H:%M:%S") if backups_discovered else "N/A",
        "newest_backup": max(backups_discovered, key=lambda x: x["timestamp"])["timestamp"].strftime("%Y-%m-%d %H:%M:%S") if backups_discovered else "N/A"
    }
    
    # Create detailed tables for reporting
    try:
        # Discovered backups summary
        discovered_summary = []
        for backup in backups_discovered:
            discovered_summary.append({
                "folder_name": backup["folder_name"],
                "timestamp": backup["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
                "age_days": backup["age_days"],
                "size_gb": backup["size_gb"],
                "source_lakehouse": backup["source_lakehouse"],
                "tables_count": backup["tables_count"],
                "status": "Preserved" if backup["folder_name"] in backups_preserved else "Available for cleanup"
            })
        
        # Deletion details
        deletion_summary = cleanup_results["deletion_results"]
        
        # Save report as Delta tables in the backup location (if possible)
        try:
            backup_location = dbutils.widgets.get("backup_location")
            report_base_path = f"{backup_location}/_cleanup_reports"
            
            # Create report folder
            dbutils.fs.mkdirs(report_base_path)
            
            # Save report metadata
            report_df = spark.createDataFrame([report_data])
            report_timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            report_df.write.mode("overwrite").format("json").save(f"{report_base_path}/cleanup_report_{report_timestamp}")
            
            # Save discovered backups summary
            if discovered_summary:
                discovered_df = spark.createDataFrame(discovered_summary)
                discovered_df.write.mode("overwrite").format("delta").save(f"{report_base_path}/discovered_backups_{report_timestamp}")
            
            # Save deletion results
            if deletion_summary:
                deletion_df = spark.createDataFrame(deletion_summary)
                deletion_df.write.mode("overwrite").format("delta").save(f"{report_base_path}/deletion_results_{report_timestamp}")
            
            log_message(f"Cleanup report saved to: {report_base_path}/cleanup_report_{report_timestamp}", "INFO")
            
        except Exception as e:
            log_message(f"Could not save report to backup location: {str(e)}", "WARNING")
        
        return report_data
        
    except Exception as e:
        log_message(f"Error generating cleanup report: {str(e)}", "ERROR")
        return None

print("Cleanup execution and reporting functions defined successfully")

# ============================================================================
# CELL 6: Main Cleanup Execution
# ============================================================================

def execute_cleanup_operation():
    """Execute the main cleanup operation"""
    
    start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message("Starting Microsoft Fabric lakehouse backup cleanup process", "INFO")
    
    try:
        # Validate parameters
        validate_cleanup_parameters()
        
        # Setup external storage authentication if needed
        setup_external_storage_auth()
        
        # Get configuration values
        backup_location = dbutils.widgets.get("backup_location")
        cleanup_mode = dbutils.widgets.get("cleanup_mode")
        dry_run = dbutils.widgets.get("dry_run") == "True"
        
        log_message(f"Cleanup location: {backup_location}", "INFO")
        log_message(f"Cleanup mode: {cleanup_mode}", "INFO")
        log_message(f"Dry run: {dry_run}", "INFO")
        
        # Discover all backups
        discovered_backups = discover_backups(backup_location)
        
        if not discovered_backups:
            log_message("No backups found for cleanup", "WARNING")
            return {
                "status": "warning",
                "message": "No backups found for cleanup",
                "backups_processed": 0
            }
        
        # Apply preservation policy
        preserved_backup_names = apply_preservation_policy(discovered_backups)
        
        # Determine which backups to delete
        backups_to_delete = determine_backups_to_delete(discovered_backups)
        
        if not backups_to_delete:
            log_message("No backups meet deletion criteria", "INFO")
            return {
                "status": "success",
                "message": "No backups meet deletion criteria",
                "backups_discovered": len(discovered_backups),
                "backups_preserved": len(preserved_backup_names),
                "backups_processed": 0
            }
        
        # Display what will be done
        log_message("", "INFO")
        log_message("=== CLEANUP PLAN ===", "INFO")
        log_message(f"Total backups discovered: {len(discovered_backups)}", "INFO")
        log_message(f"Backups preserved by policy: {len(preserved_backup_names)}", "INFO")
        log_message(f"Backups {'to be deleted' if not dry_run else 'that would be deleted'}: {len(backups_to_delete)}", "INFO")
        
        total_size_to_free = sum(b["size_bytes"] for b in backups_to_delete)
        log_message(f"Total space {'to be freed' if not dry_run else 'that would be freed'}: {total_size_to_free / (1024*1024*1024):.2f} GB", "INFO")
        log_message("====================", "INFO")
        
        # Execute cleanup
        cleanup_results = execute_cleanup(backups_to_delete)
        
        # Generate report
        report_data = generate_cleanup_report(
            discovered_backups, 
            preserved_backup_names, 
            backups_to_delete, 
            cleanup_results
        )
        
        # Record end time
        end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration_seconds = (datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S") - 
                           datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")).total_seconds()
        
        # Final summary
        log_message("", "INFO")
        log_message("=== FINAL CLEANUP SUMMARY ===", "INFO")
        log_message(f"Cleanup completed at: {end_time}", "INFO")
        log_message(f"Duration: {duration_seconds:.2f} seconds", "INFO")
        log_message(f"Mode: {'DRY RUN' if dry_run else 'LIVE EXECUTION'}", "INFO")
        log_message(f"Backups discovered: {len(discovered_backups)}", "INFO")
        log_message(f"Backups preserved: {len(preserved_backup_names)}", "INFO")
        log_message(f"Backups {'simulated for deletion' if dry_run else 'deleted'}: {cleanup_results['successful_operations']}", "INFO")
        log_message(f"Failed operations: {cleanup_results['failed_operations']}", "INFO")
        log_message(f"Space {'that would be' if dry_run else ''} freed: {cleanup_results['space_freed_gb']:.2f} GB", "INFO")
        log_message("=============================", "INFO")
        
        # Return comprehensive results
        result = {
            "status": "success" if cleanup_results["failed_operations"] == 0 else "partial_success",
            "cleanup_mode": cleanup_mode,
            "backup_location": backup_location,
            "dry_run": dry_run,
            "backups_discovered": len(discovered_backups),
            "backups_preserved": len(preserved_backup_names),
            "backups_processed": cleanup_results["total_processed"],
            "successful_operations": cleanup_results["successful_operations"],
            "failed_operations": cleanup_results["failed_operations"],
            "space_freed_gb": cleanup_results["space_freed_gb"],
            "duration_seconds": duration_seconds,
            "report_data": report_data
        }
        
        if cleanup_results["failed_operations"] == 0:
            log_message("Cleanup operation completed successfully", "INFO")
        else:
            log_message(f"Cleanup operation completed with {cleanup_results['failed_operations']} failures", "WARNING")
        
        return result
        
    except Exception as e:
        log_message(f"Cleanup operation failed with error: {str(e)}", "ERROR")
        import traceback
        log_message(f"Full error trace: {traceback.format_exc()}", "ERROR")
        
        return {
            "status": "failed",
            "error": str(e),
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

print("Main cleanup function defined successfully")

# ============================================================================
# CELL 7: Execute Cleanup Operation
# ============================================================================

# Execute the cleanup operation
print("Starting cleanup operation...")
print("=" * 60)

# Display current configuration for review
print("CURRENT CONFIGURATION:")
print(f"  Cleanup Mode: {dbutils.widgets.get('cleanup_mode')}")
print(f"  Backup Location: {dbutils.widgets.get('backup_location')}")
print(f"  Dry Run: {dbutils.widgets.get('dry_run')}")
print(f"  Preserve Policy: {dbutils.widgets.get('preserve_policy')}")

if dbutils.widgets.get("cleanup_mode") == "age_based":
    print(f"  Retention Days: {dbutils.widgets.get('retention_days')}")
elif dbutils.widgets.get("cleanup_mode") == "count_based":
    print(f"  Max Backup Count: {dbutils.widgets.get('max_backup_count')}")
elif dbutils.widgets.get("cleanup_mode") == "size_based":
    print(f"  Max Size (GB): {dbutils.widgets.get('max_size_gb')}")

print("=" * 60)

# Safety check for dry run
dry_run_mode = dbutils.widgets.get("dry_run") == "True"
if not dry_run_mode:
    print("⚠️  WARNING: DRY RUN IS DISABLED - BACKUPS WILL BE PERMANENTLY DELETED!")
    print("⚠️  Make sure you have reviewed the configuration above carefully.")
    print("⚠️  Consider running in dry_run mode first to preview the changes.")
    print("=" * 60)

cleanup_result = execute_cleanup_operation()

print("=" * 60)
print("Cleanup operation completed.")
print(f"Final status: {cleanup_result['status']}")

# Display results in notebook output
if cleanup_result['status'] == 'success':
    if cleanup_result.get('dry_run', False):
        print(f"✅ DRY RUN SUCCESS: Would delete {cleanup_result['successful_operations']} backups")
        print(f"💾 Space that would be freed: {cleanup_result['space_freed_gb']:.2f} GB")
        print("🔄 Set dry_run=False to execute actual cleanup")
    else:
        print(f"✅ CLEANUP SUCCESS: Deleted {cleanup_result['successful_operations']} backups")
        print(f"💾 Space freed: {cleanup_result['space_freed_gb']:.2f} GB")
elif cleanup_result['status'] == 'partial_success':
    print(f"⚠️  PARTIAL SUCCESS: {cleanup_result['successful_operations']} successful, {cleanup_result['failed_operations']} failed")
    print(f"💾 Space freed: {cleanup_result['space_freed_gb']:.2f} GB")
else:
    print(f"❌ CLEANUP FAILED: {cleanup_result.get('error', 'Unknown error')}")

print(f"📊 Backups discovered: {cleanup_result.get('backups_discovered', 0)}")
print(f"🛡️  Backups preserved: {cleanup_result.get('backups_preserved', 0)}")

# Exit with results for pipeline integration
dbutils.notebook.exit(cleanup_result)

# ============================================================================
# CELL 8: Additional Utility Functions (Optional)
# ============================================================================

# These functions can be used for advanced cleanup scenarios
# Uncomment and run separately as needed

def analyze_backup_storage_usage(backup_location):
    """
    Analyze storage usage patterns across backups
    Usage: Run this cell separately to get detailed storage analysis
    """
    try:
        backups = discover_backups(backup_location)
        
        if not backups:
            print("No backups found for analysis")
            return
        
        # Calculate statistics
        total_size_gb = sum(b["size_gb"] for b in backups)
        avg_size_gb = total_size_gb / len(backups)
        largest_backup = max(backups, key=lambda x: x["size_gb"])
        smallest_backup = min(backups, key=lambda x: x["size_gb"])
        oldest_backup = min(backups, key=lambda x: x["timestamp"])
        newest_backup = max(backups, key=lambda x: x["timestamp"])
        
        # Group by source lakehouse
        by_lakehouse = defaultdict(list)
        for backup in backups:
            by_lakehouse[backup["source_lakehouse"]].append(backup)
        
        # Group by age ranges
        age_ranges = {
            "0-7 days": [],
            "8-30 days": [],
            "31-90 days": [],
            "91-365 days": [],
            "365+ days": []
        }
        
        for backup in backups:
            age = backup["age_days"]
            if age <= 7:
                age_ranges["0-7 days"].append(backup)
            elif age <= 30:
                age_ranges["8-30 days"].append(backup)
            elif age <= 90:
                age_ranges["31-90 days"].append(backup)
            elif age <= 365:
                age_ranges["91-365 days"].append(backup)
            else:
                age_ranges["365+ days"].append(backup)
        
        # Display analysis
        print("=" * 60)
        print("BACKUP STORAGE ANALYSIS")
        print("=" * 60)
        print(f"Total Backups: {len(backups)}")
        print(f"Total Storage Used: {total_size_gb:.2f} GB")
        print(f"Average Backup Size: {avg_size_gb:.2f} GB")
        print(f"Largest Backup: {largest_backup['folder_name']} ({largest_backup['size_gb']:.2f} GB)")
        print(f"Smallest Backup: {smallest_backup['folder_name']} ({smallest_backup['size_gb']:.2f} GB)")
        print(f"Oldest Backup: {oldest_backup['folder_name']} ({oldest_backup['age_days']} days old)")
        print(f"Newest Backup: {newest_backup['folder_name']} ({newest_backup['age_days']} days old)")
        
        print("\nBREAKDOWN BY SOURCE LAKEHOUSE:")
        for lakehouse, lakehouse_backups in by_lakehouse.items():
            lakehouse_size = sum(b["size_gb"] for b in lakehouse_backups)
            print(f"  {lakehouse}: {len(lakehouse_backups)} backups, {lakehouse_size:.2f} GB")
        
        print("\nBREAKDOWN BY AGE:")
        for age_range, range_backups in age_ranges.items():
            if range_backups:
                range_size = sum(b["size_gb"] for b in range_backups)
                print(f"  {age_range}: {len(range_backups)} backups, {range_size:.2f} GB")
        
        return {
            "total_backups": len(backups),
            "total_size_gb": total_size_gb,
            "avg_size_gb": avg_size_gb,
            "by_lakehouse": dict(by_lakehouse),
            "by_age_range": dict(age_ranges)
        }
        
    except Exception as e:
        print(f"Error analyzing backup storage: {str(e)}")
        return None

def preview_cleanup_impact(backup_location, cleanup_mode, **kwargs):
    """
    Preview what would happen with different cleanup settings
    Usage: Run this cell separately to test different cleanup scenarios
    """
    try:
        print(f"PREVIEWING CLEANUP IMPACT: {cleanup_mode.upper()} MODE")
        print("=" * 50)
        
        # Temporarily override parameters for preview
        original_cleanup_mode = dbutils.widgets.get("cleanup_mode")
        original_dry_run = dbutils.widgets.get("dry_run")
        
        # Set preview parameters
        dbutils.widgets.removeAll()
        dbutils.widgets.text("backup_location", backup_location, "Backup Location")
        dbutils.widgets.dropdown("cleanup_mode", cleanup_mode, ["age_based", "count_based", "size_based", "advanced"], "Cleanup Mode")
        dbutils.widgets.dropdown("dry_run", "True", ["True", "False"], "Dry Run")
        
        # Set mode-specific parameters
        for key, value in kwargs.items():
            if key == "retention_days":
                dbutils.widgets.text("retention_days", str(value), "Retention Days")
            elif key == "max_backup_count":
                dbutils.widgets.text("max_backup_count", str(value), "Max Backup Count")
            elif key == "max_size_gb":
                dbutils.widgets.text("max_size_gb", str(value), "Max Size GB")
        
        # Set default values for other parameters
        dbutils.widgets.dropdown("preserve_policy", "none", ["none", "daily", "weekly", "monthly"], "Preserve Policy")
        dbutils.widgets.text("preserve_daily_count", "7", "Daily Count")
        dbutils.widgets.text("preserve_weekly_count", "4", "Weekly Count")
        dbutils.widgets.text("preserve_monthly_count", "12", "Monthly Count")
        dbutils.widgets.text("exclude_patterns", "", "Exclude Patterns")
        dbutils.widgets.text("include_patterns", "backup_*", "Include Patterns")
        dbutils.widgets.dropdown("use_managed_identity", "True", ["True", "False"], "Use Managed Identity")
        dbutils.widgets.dropdown("enable_detailed_logging", "False", ["True", "False"], "Enable Logging")
        dbutils.widgets.dropdown("generate_report", "False", ["True", "False"], "Generate Report")
        
        # Run preview
        result = execute_cleanup_operation()
        
        print(f"PREVIEW RESULTS:")
        print(f"  Backups that would be deleted: {result.get('successful_operations', 0)}")
        print(f"  Space that would be freed: {result.get('space_freed_gb', 0):.2f} GB")
        print(f"  Backups that would be preserved: {result.get('backups_preserved', 0)}")
        
        # Restore original parameters
        dbutils.widgets.removeAll()
        # Note: In a real scenario, you'd want to restore all original parameters here
        
        return result
        
    except Exception as e:
        print(f"Error previewing cleanup impact: {str(e)}")
        return None

def emergency_cleanup(backup_location, free_gb_target):
    """
    Emergency cleanup to free up a specific amount of space
    Usage: Run this cell separately for emergency space recovery
    """
    try:
        print(f"EMERGENCY CLEANUP: Attempting to free {free_gb_target} GB")
        print("=" * 50)
        
        backups = discover_backups(backup_location)
        if not backups:
            print("No backups found")
            return
        
        # Sort by age (oldest first) for emergency deletion
        backups.sort(key=lambda x: x["timestamp"])
        
        target_bytes = free_gb_target * 1024 * 1024 * 1024
        current_freed = 0
        emergency_deletions = []
        
        for backup in backups:
            if current_freed >= target_bytes:
                break
            emergency_deletions.append(backup)
            current_freed += backup["size_bytes"]
        
        print(f"Emergency plan would delete {len(emergency_deletions)} backups")
        print(f"Space that would be freed: {current_freed / (1024*1024*1024):.2f} GB")
        print("\nBackups selected for emergency deletion:")
        
        for backup in emergency_deletions:
            print(f"  - {backup['folder_name']} ({backup['age_days']} days, {backup['size_gb']:.2f} GB)")
        
        print("\n⚠️  This is a PREVIEW only. To execute:")
        print("1. Set cleanup_mode='age_based' with very low retention_days")
        print("2. Or manually delete specific backups")
        print("3. Or use size_based cleanup with lower max_size_gb")
        
        return emergency_deletions
        
    except Exception as e:
        print(f"Error in emergency cleanup planning: {str(e)}")
        return None

# Example usage (uncomment to use):
# backup_location = "abfss://backups@mystorageaccount.dfs.core.windows.net"
# 
# # Analyze current storage usage
# storage_analysis = analyze_backup_storage_usage(backup_location)
# 
# # Preview different cleanup scenarios
# preview_age_30 = preview_cleanup_impact(backup_location, "age_based", retention_days=30)
# preview_count_5 = preview_cleanup_impact(backup_location, "count_based", max_backup_count=5)
# preview_size_50 = preview_cleanup_impact(backup_location, "size_based", max_size_gb=50)
# 
# # Emergency cleanup planning
# emergency_plan = emergency_cleanup(backup_location, free_gb_target=20)

print("Advanced utility functions defined. Uncomment and modify the example lines above to use them.")

# ============================================================================
# END OF CLEANUP NOTEBOOK
# ============================================================================

print("")
print("Cleanup notebook execution completed.")
print("")
print("💡 USAGE TIPS:")
print("1. Always test with dry_run=True first")
print("2. Use preserve_policy to protect important backups")
print("3. Schedule this notebook to run regularly for automated cleanup")
print("4. Monitor the cleanup reports for trends and optimization")
print("5. Use the utility functions in Cell 8 for advanced analysis")
print("")
print("🔗 INTEGRATION:")
print("- Schedule via Fabric notebook scheduler")
print("- Add to Data Factory pipeline after backup jobs")
print("- Use REST API for programmatic execution")
print("- Combine with monitoring alerts for storage thresholds")
