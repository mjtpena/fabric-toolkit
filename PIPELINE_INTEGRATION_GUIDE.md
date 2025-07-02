# Microsoft Fabric Data Pipeline Integration Guide

This guide explains how to use the Microsoft Fabric Data Pipelines to orchestrate lakehouse backup and cleanup operations.

## Pipeline Overview

### 1. Main Orchestration Pipeline (`fabric-pipeline-orchestration.json`)
**Purpose**: Complete backup and cleanup workflow with parameter management, error handling, and notifications.

**Key Features**:
- Parameter validation and configuration management
- Sequential backup and cleanup execution
- Conditional cleanup execution based on backup success
- Comprehensive error handling and notifications
- Final report generation

**Use Cases**:
- Scheduled daily/weekly backup operations
- Production environments requiring full workflow
- Scenarios requiring both backup and cleanup

### 2. Backup Only Pipeline (`fabric-pipeline-backup-only.json`)
**Purpose**: Simplified pipeline for backup operations only.

**Key Features**:
- Lightweight parameter set
- Fast execution for backup-only scenarios
- Minimal dependencies

**Use Cases**:
- Development environments
- Manual backup requests
- Testing backup functionality

### 3. Cleanup Only Pipeline (`fabric-pipeline-cleanup-only.json`)
**Purpose**: Standalone cleanup operations for existing backups.

**Key Features**:
- Independent cleanup execution
- Full cleanup parameter support
- Can be scheduled separately from backups

**Use Cases**:
- Scheduled cleanup maintenance
- Emergency space recovery
- Cleanup policy changes

### 4. Multi-Environment Pipeline (`fabric-pipeline-multi-environment.json`)
**Purpose**: Enterprise-scale pipeline for managing multiple lakehouses across environments.

**Key Features**:
- Environment-specific configurations
- Batch processing of multiple lakehouses
- Consolidated reporting
- Parallel or sequential execution modes

**Use Cases**:
- Enterprise environments with multiple lakehouses
- Cross-environment backup strategies
- Automated deployment scenarios

## Setup Instructions

### Step 1: Import Pipeline Definitions

1. **Access Microsoft Fabric Portal**
   - Navigate to your Fabric workspace
   - Go to Data Factory experience

2. **Import Pipeline Files**
   ```
   1. Click "New" → "Data Pipeline"
   2. Use "Import from pipeline template" or "Copy pipeline definition"
   3. Paste the JSON content from each pipeline file
   4. Save with appropriate names
   ```

3. **Required Pipeline Names**
   - `Fabric Lakehouse Backup Orchestration Pipeline`
   - `Fabric Lakehouse Backup Only Pipeline`
   - `Fabric Lakehouse Cleanup Only Pipeline`
   - `Fabric Lakehouse Multi-Environment Pipeline`

### Step 2: Configure Notebook References

Ensure the pipeline notebook references match your actual notebook names:

```json
"notebookPath": "Fabric_Lakehouse_Backup.ipynb"
"notebookPath": "Fabric_Lakehouse_Cleanup.ipynb"
```

### Step 3: Set Up Authentication

1. **Managed Identity Configuration**
   ```
   - Enable managed identity for your Fabric workspace
   - Grant permissions to target storage accounts
   - Configure RBAC roles (Storage Blob Data Contributor)
   ```

2. **External Storage Access**
   ```
   - For Azure Storage: Configure managed identity
   - For OneLake: Use workspace identity
   - For ADLS Gen2: Set up service principal or managed identity
   ```

## Parameter Configuration

### Main Orchestration Pipeline Parameters

#### Required Parameters
```json
{
  "SourceLakehouseName": "your-lakehouse-name",
  "BackupLocation": "abfss://backups@storageaccount.dfs.core.windows.net/lakehouse-backups"
}
```

#### Optional Parameters (with defaults)
```json
{
  "Environment": "production",
  "BackupMode": "full",
  "TableFilter": "*",
  "CompressionEnabled": "True",
  "ParallelOperations": "4",
  "EnableCleanup": true,
  "CleanupMode": "age_based",
  "RetentionDays": "30",
  "PreservePolicy": "weekly",
  "CleanupDryRun": "True",
  "NotificationEnabled": false
}
```

### Environment-Specific Configurations

#### Development Environment
```json
{
  "Environment": "development",
  "BackupMode": "incremental",
  "RetentionDays": "7",
  "PreservePolicy": "daily",
  "CleanupDryRun": "False"
}
```

#### Staging Environment
```json
{
  "Environment": "staging",
  "BackupMode": "full",
  "RetentionDays": "14",
  "PreservePolicy": "weekly",
  "CleanupDryRun": "True"
}
```

#### Production Environment
```json
{
  "Environment": "production",
  "BackupMode": "full",
  "RetentionDays": "90",
  "PreservePolicy": "monthly",
  "CleanupDryRun": "True",
  "NotificationEnabled": true
}
```

## Scheduling and Automation

### 1. Schedule Configuration

#### Daily Backup Schedule
```json
{
  "name": "Daily Lakehouse Backup",
  "pipeline": "Fabric Lakehouse Backup Orchestration Pipeline",
  "trigger": {
    "type": "ScheduleTrigger",
    "typeProperties": {
      "recurrence": {
        "frequency": "Day",
        "interval": 1,
        "startTime": "02:00:00",
        "timeZone": "UTC"
      }
    }
  }
}
```

#### Weekly Cleanup Schedule
```json
{
  "name": "Weekly Cleanup",
  "pipeline": "Fabric Lakehouse Cleanup Only Pipeline",
  "trigger": {
    "type": "ScheduleTrigger",
    "typeProperties": {
      "recurrence": {
        "frequency": "Week",
        "interval": 1,
        "startTime": "03:00:00",
        "daysOfWeek": ["Sunday"],
        "timeZone": "UTC"
      }
    }
  }
}
```

### 2. Event-Based Triggers

#### Backup After Data Load
```json
{
  "name": "Post-ETL Backup",
  "pipeline": "Fabric Lakehouse Backup Only Pipeline",
  "trigger": {
    "type": "CustomEventTrigger",
    "typeProperties": {
      "events": ["DataLoadCompleted"],
      "scope": "/subscriptions/{subscription}/resourceGroups/{rg}/providers/Microsoft.Fabric/workspaces/{workspace}"
    }
  }
}
```

## Monitoring and Alerts

### 1. Pipeline Monitoring

```json
{
  "monitoringQueries": [
    {
      "name": "Failed Backup Operations",
      "query": "FabricPipelineRuns | where PipelineName contains 'Backup' and Status == 'Failed'",
      "alertThreshold": 1
    },
    {
      "name": "Long Running Backups",
      "query": "FabricPipelineRuns | where PipelineName contains 'Backup' and DurationInMinutes > 60",
      "alertThreshold": 1
    }
  ]
}
```

### 2. Notification Setup

#### Logic App Integration
```json
{
  "NotificationWebhookUrl": "https://prod-XX.eastus.logic.azure.com:443/workflows/xxx/triggers/manual/paths/invoke",
  "NotificationEnabled": true
}
```

#### Power Automate Integration
```json
{
  "NotificationWebhookUrl": "https://prod-XX.eastus.logic.azure.com/workflows/xxx/triggers/manual/paths/invoke",
  "NotificationEnabled": true
}
```

## Usage Examples

### Example 1: Basic Daily Backup

```json
{
  "pipelineName": "Fabric Lakehouse Backup Orchestration Pipeline",
  "parameters": {
    "SourceLakehouseName": "sales_data_lakehouse",
    "BackupLocation": "abfss://backups@company.dfs.core.windows.net/sales",
    "BackupMode": "incremental",
    "EnableCleanup": true,
    "CleanupMode": "age_based",
    "RetentionDays": "30",
    "CleanupDryRun": "False"
  }
}
```

### Example 2: Multi-Lakehouse Enterprise Backup

```json
{
  "pipelineName": "Fabric Lakehouse Multi-Environment Pipeline",
  "parameters": {
    "Environment": "production",
    "LakehouseList": [
      {
        "name": "sales_lakehouse",
        "workspaceId": "12345678-1234-1234-1234-123456789012",
        "tableFilter": "*"
      },
      {
        "name": "finance_lakehouse",
        "workspaceId": "12345678-1234-1234-1234-123456789013",
        "tableFilter": "transactions_*,reports_*"
      },
      {
        "name": "hr_lakehouse",
        "workspaceId": "12345678-1234-1234-1234-123456789014",
        "tableFilter": "employees,payroll"
      }
    ],
    "BaseBackupLocation": "abfss://enterprise-backups@company.dfs.core.windows.net",
    "SequentialProcessing": false,
    "BatchSize": 2,
    "NotificationEnabled": true,
    "NotificationWebhookUrl": "https://company.webhook.url/fabric-notifications"
  }
}
```

### Example 3: Emergency Cleanup

```json
{
  "pipelineName": "Fabric Lakehouse Cleanup Only Pipeline",
  "parameters": {
    "BackupLocation": "abfss://backups@company.dfs.core.windows.net/sales",
    "CleanupMode": "size_based",
    "MaxSizeGB": "50",
    "PreservePolicy": "weekly",
    "DryRun": "False",
    "ExcludePatterns": "manual_*,important_*",
    "GenerateReport": "True"
  }
}
```

## Troubleshooting

### Common Issues

1. **Authentication Failures**
   ```
   Error: Unauthorized access to storage
   Solution: Verify managed identity permissions and RBAC roles
   ```

2. **Notebook Not Found**
   ```
   Error: Cannot find notebook 'Fabric_Lakehouse_Backup.ipynb'
   Solution: Ensure notebooks are in the same workspace and names match exactly
   ```

3. **Parameter Validation Errors**
   ```
   Error: Required parameter 'SourceLakehouseName' is missing
   Solution: Check parameter mapping and ensure all required parameters are provided
   ```

4. **Storage Access Issues**
   ```
   Error: Failed to access backup location
   Solution: Verify storage account exists, path is correct, and permissions are set
   ```

### Debugging Steps

1. **Check Pipeline Execution Logs**
   - Review activity execution details
   - Check variable values and expressions
   - Verify conditional logic outcomes

2. **Validate Notebook Outputs**
   - Run notebooks individually to test functionality
   - Check notebook parameter acceptance
   - Verify notebook return values

3. **Test Storage Connectivity**
   - Use storage explorer to verify paths
   - Test managed identity access
   - Check firewall and network rules

## Best Practices

### 1. Environment Management
- Use separate pipelines for different environments
- Implement environment-specific parameter sets
- Use naming conventions for easy identification

### 2. Parameter Management
- Use pipeline parameters for all configurable values
- Implement parameter validation in pipeline logic
- Document all parameter meanings and valid values

### 3. Error Handling
- Implement comprehensive error handling
- Use conditional activities for graceful degradation
- Set up appropriate notifications for failures

### 4. Performance Optimization
- Use parallel processing where appropriate
- Configure optimal batch sizes for multi-lakehouse scenarios
- Monitor execution times and optimize bottlenecks

### 5. Security
- Use managed identities for authentication
- Implement least-privilege access principles
- Secure webhook URLs and notification endpoints
- Regularly review and audit permissions

## Integration with Other Services

### 1. Azure Monitor Integration
```json
{
  "diagnosticSettings": {
    "logs": ["PipelineRuns", "ActivityRuns", "TriggerRuns"],
    "destination": "Log Analytics Workspace"
  }
}
```

### 2. Power BI Reporting
```json
{
  "powerBIDataset": {
    "name": "Fabric Backup Analytics",
    "tables": ["BackupExecutions", "CleanupOperations", "StorageMetrics"]
  }
}
```

### 3. Azure DevOps Integration
```json
{
  "cicdPipeline": {
    "deploymentTrigger": "Fabric Pipeline Deployment",
    "testingPhase": "Backup Pipeline Validation"
  }
}
```

This comprehensive guide provides everything needed to implement and operate the Fabric Data Pipelines for lakehouse backup and cleanup operations.
