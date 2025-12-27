------------------------------------------------------------
-- Database Tuner Legacy v0.01
------------------------------------------------------------
-- Description:
--   Collects a read-only analytical snapshot of a database's performance signals and schema.
--   Stores results in temp tables and then exports to a single markdown file (*.md).
--   Markdown can then be uploaded to an LLM of your choice for review and deep analysis.
-- Requirements:
--   SQL Server 2008 / 2008 R2 (compat 100) minimum (not tested with Azure SQL Database).
--   LLM such as ChatGPT, Claude, Gemini etc. The deeper thinking model, the better.
-- Usage:
--   1) SSMS -> Query -> SQLCMD Mode (enable)
--   2) Update "User Config" section below (ensure OutputDir exists)
--   3) Run and monitor progress from "Messages" tab
--   4) Collect generated file from OutputDir (filename: dt_legacy_report ({TargetDB} - {Version}).md)
--   5) Upload to LLM (ChatGPT etc) for analysis (zip md if need)
-- Notes:
--   - Ensure OutputDir exists otherwise data will be printed to the SSMS console instead of to file.
--   - Toggle ExportSchema or SafeMode (which redacts sensitive information) when needed (1 or 0).
--   - Ignore "The join order has been enforced" and "Null value is eliminated by an aggregate" warnings.
--   - No database changes are made by this script aside from temp tables which are discarded when query closes.
--   - Author: Dean Lafferty (laffo16@hotmail.com)

------------------------------------------------------------
-- User Config
------------------------------------------------------------
:SETVAR TargetDB "DatabaseName"
:SETVAR OutputDir "C:\Temp\DatabaseTuner\"
:SETVAR ExportSchema "1"
:SETVAR SafeMode "1"

------------------------------------------------------------
-- Version
------------------------------------------------------------
:SETVAR Version "0.01"

------------------------------------------------------------
-- Prerequisites
------------------------------------------------------------
:ON ERROR EXIT

DECLARE @ProductMajorVersion int = CONVERT(int, PARSENAME(CONVERT(varchar(30), SERVERPROPERTY('ProductVersion')), 4));
DECLARE @CompatLevel int = (SELECT compatibility_level FROM sys.databases WHERE name = '$(TargetDB)');

IF @ProductMajorVersion < 10
	RAISERROR('Database Tuner Legacy requires SQL Server 2008 / 2008 R2 (10.x) minimum.', 16, 1);
ELSE IF @CompatLevel < 100
	RAISERROR('Database Tuner Legacy requires Database Compatibility Level 100 or higher.', 16, 1);
GO

------------------------------------------------------------
-- Initialisation
------------------------------------------------------------
USE $(TargetDB)

SET NOCOUNT ON
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
SET LOCK_TIMEOUT 15000

IF OBJECT_ID('tempdb..#DT_Config') IS NOT NULL DROP TABLE #DT_Config

SELECT
CONVERT(int, PARSENAME(CONVERT(varchar(30), SERVERPROPERTY('ProductVersion')), 4)) AS ProductMajorVersion,
d.compatibility_level AS CompatLevel,
CONVERT(bit, ISNULL(IS_SRVROLEMEMBER('sysadmin'), 0)) AS IsSysAdmin,
CASE WHEN '$(ExportSchema)' = '1' THEN 1 ELSE 0 END AS ExportSchema,
CASE WHEN '$(SafeMode)' = '1' THEN 1 ELSE 0 END AS SafeMode
INTO #DT_Config
FROM sys.databases AS d
WHERE d.name = DB_NAME()

PRINT 'Database Tuner Legacy Report $(Version)'
GO

------------------------------------------------------------
-- 00a. Metadata
------------------------------------------------------------
PRINT N'▶ 00a. Metadata - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_Metadata') IS NOT NULL DROP TABLE #DTR_Metadata
GO

SELECT
IDENTITY(int) AS RowNumber,
SYSUTCDATETIME() AS CollectionTimeUtc,
CASE WHEN cfg.SafeMode = 0 THEN SERVERPROPERTY('ServerName') ELSE '[SafeMode]' END AS ServerName,
CASE WHEN cfg.SafeMode = 0 THEN SERVERPROPERTY('InstanceName') ELSE '[SafeMode]' END AS InstanceName,
CASE WHEN cfg.SafeMode = 0 THEN SERVERPROPERTY('MachineName') ELSE '[SafeMode]' END AS MachineName,
CASE WHEN cfg.SafeMode = 0 THEN SERVERPROPERTY('ComputerNamePhysicalNetBIOS') ELSE '[SafeMode]' END AS ComputerNamePhysicalNetBIOS,
SERVERPROPERTY('Collation') AS ServerCollation,
SERVERPROPERTY('ProductVersion') AS ProductVersion,
SERVERPROPERTY('ProductBuild') AS ProductBuild,
SERVERPROPERTY('ProductBuildType') AS ProductBuildType,
SERVERPROPERTY('ProductLevel') AS ProductLevel,
SERVERPROPERTY('ProductMajorVersion') AS ProductMajorVersion,
SERVERPROPERTY('ProductMinorVersion') AS ProductMinorVersion,
SERVERPROPERTY('ProductUpdateLevel') AS ProductUpdateLevel,
SERVERPROPERTY('ProductUpdateReference') AS ProductUpdateReference,
SERVERPROPERTY('Edition') AS Edition,
SERVERPROPERTY('EngineEdition') AS EngineEdition,
CASE WHEN cfg.SafeMode = 0 THEN SERVERPROPERTY('InstanceDefaultBackupPath') ELSE '[SafeMode]' END AS InstanceDefaultBackupPath,
CASE WHEN cfg.SafeMode = 0 THEN SERVERPROPERTY('InstanceDefaultDataPath') ELSE '[SafeMode]' END AS InstanceDefaultDataPath,
CASE WHEN cfg.SafeMode = 0 THEN SERVERPROPERTY('InstanceDefaultLogPath') ELSE '[SafeMode]' END AS InstanceDefaultLogPath,
SERVERPROPERTY('IsClustered') AS IsClustered,
SERVERPROPERTY('IsHadrEnabled') AS IsHadrEnabled,
SERVERPROPERTY('HadrManagerStatus') AS HadrManagerStatus,
SERVERPROPERTY('FilestreamConfiguredLevel') AS FilestreamConfiguredLevel,
SERVERPROPERTY('FilestreamEffectiveLevel') AS FilestreamEffectiveLevel,
CASE WHEN cfg.SafeMode = 0 THEN SERVERPROPERTY('FilestreamShareName') ELSE '[SafeMode]' END AS FilestreamShareName,
SERVERPROPERTY('BuildClrVersion') AS BuildClrVersion,
DB_NAME() AS DatabaseName,
DATABASEPROPERTYEX(DB_NAME(), 'Recovery') AS DatabaseRecoveryModel,
DATABASEPROPERTYEX(DB_NAME(), 'Collation') AS DatabaseCollation,
DATABASEPROPERTYEX(DB_NAME(), 'Status') AS DatabaseStatus,
DATABASEPROPERTYEX(DB_NAME(), 'Updateability') AS DatabaseUpdateability,
DATABASEPROPERTYEX(DB_NAME(), 'UserAccess') AS DatabaseUserAccess,
DATABASEPROPERTYEX(DB_NAME(), 'IsReadCommittedSnapshotOn') AS IsReadCommittedSnapshotOn,
DATABASEPROPERTYEX(DB_NAME(), 'IsSnapshotIsolationOn') AS IsSnapshotIsolationOn,
DATABASEPROPERTYEX(DB_NAME(), 'IsAutoCreateStatistics') AS IsAutoCreateStatistics,
DATABASEPROPERTYEX(DB_NAME(), 'IsAutoUpdateStatistics') AS IsAutoUpdateStatistics,
DATABASEPROPERTYEX(DB_NAME(), 'IsAutoUpdateStatisticsAsync') AS IsAutoUpdateStatisticsAsync,
DATABASEPROPERTYEX(DB_NAME(), 'IsAutoClose') AS IsAutoClose,
DATABASEPROPERTYEX(DB_NAME(), 'IsAutoShrink') AS IsAutoShrink,
DATABASEPROPERTYEX(DB_NAME(), 'IsParameterizationForced') AS IsParameterizationForced,
DATABASEPROPERTYEX(DB_NAME(), 'IsAnsiNullDefault') AS IsAnsiNullDefaultOn,
DATABASEPROPERTYEX(DB_NAME(), 'IsAnsiWarningsOn') AS IsAnsiWarningsOn,
DATABASEPROPERTYEX(DB_NAME(), 'IsArithAbortOn') AS IsArithAbortOn,
DATABASEPROPERTYEX(DB_NAME(), 'IsBrokerEnabled') AS IsBrokerEnabled,
DATABASEPROPERTYEX(DB_NAME(), 'IsSyncWithBackup') AS IsSyncWithBackup,
DATABASEPROPERTYEX(DB_NAME(), 'LastBackupTime') AS LastBackupTime,
DATABASEPROPERTYEX(DB_NAME(), 'LastLogBackupTime') AS LastLogBackupTime,
DATABASEPROPERTYEX(DB_NAME(), 'LastGoodCheckDbTime') AS LastGoodCheckDbTime
INTO #DTR_Metadata
FROM #DT_Config AS cfg
GO

------------------------------------------------------------
-- 00b. Database Configurations
------------------------------------------------------------
PRINT N'▶ 00b. Database Configurations - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_DatabaseConfigurations') IS NOT NULL DROP TABLE #DTR_DatabaseConfigurations
GO

SELECT
IDENTITY(int) AS RowNumber,
compatibility_level,
recovery_model_desc,
page_verify_option_desc,
user_access_desc,
state_desc,
is_read_only,
is_encrypted,
snapshot_isolation_state_desc,
is_read_committed_snapshot_on,
is_auto_close_on,
is_auto_shrink_on,
is_auto_create_stats_on,
is_auto_update_stats_on,
is_auto_update_stats_async_on,
is_parameterization_forced,
is_cdc_enabled,
is_broker_enabled,
is_trustworthy_on,
is_db_chaining_on,
log_reuse_wait_desc,
log_reuse_wait
INTO #DTR_DatabaseConfigurations
FROM sys.databases AS d
WHERE d.database_id = DB_ID()
GO

------------------------------------------------------------
-- 00c. Instance-Level Configurations
------------------------------------------------------------
PRINT N'▶ 00c. Instance-Level Configurations - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_InstanceConfigs') IS NOT NULL DROP TABLE #DTR_InstanceConfigs
GO

SELECT
IDENTITY(int) AS RowNumber,
configuration_id,
name,
minimum,
maximum,
value,
value_in_use,
is_dynamic,
is_advanced,
description
INTO #DTR_InstanceConfigs
FROM sys.configurations
ORDER BY name
GO

------------------------------------------------------------
-- 00d. Server Environment
------------------------------------------------------------
PRINT N'▶ 00d. Server Environment - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_ServerInfo') IS NOT NULL DROP TABLE #DTR_ServerInfo
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	DECLARE @sql nvarchar(max);

	IF COL_LENGTH('sys.dm_os_sys_info', 'physical_memory_kb') IS NOT NULL
	BEGIN
		SET @sql = N'
			SELECT
			IDENTITY(int) AS RowNumber,
			cpu_count,
			scheduler_count,
			hyperthread_ratio,
			max_workers_count,
			sqlserver_start_time,
			CONVERT(decimal(19, 1), physical_memory_kb / 1024.0) AS physical_memory_mb,
			CONVERT(decimal(19, 1), virtual_memory_kb / 1024.0) AS virtual_memory_mb,
			CONVERT(decimal(19, 1), committed_kb / 1024.0) AS bpool_committed_mb,
			CONVERT(decimal(19, 1), committed_target_kb / 1024.0) AS bpool_commit_target_mb,
			CONVERT(decimal(19, 1), visible_target_kb / 1024.0) AS bpool_visible_mb,
			affinity_type_desc,
			time_source_desc
			INTO #DTR_ServerInfo
			FROM sys.dm_os_sys_info;'
	END

	IF COL_LENGTH('sys.dm_os_sys_info', 'physical_memory_kb') IS NULL
	BEGIN
		SET @sql = N'
			SELECT
			IDENTITY(int) AS RowNumber,
			cpu_count,
			scheduler_count,
			hyperthread_ratio,
			max_workers_count,
			sqlserver_start_time,
			CONVERT(decimal(19, 1), physical_memory_in_bytes / 1024.0 / 1024.0) AS physical_memory_mb,
			CONVERT(decimal(19, 1), virtual_memory_in_bytes / 1024.0 / 1024.0) AS virtual_memory_mb,
			CONVERT(decimal(19, 1), bpool_committed * 8.0 / 1024.0) AS bpool_committed_mb,
			CONVERT(decimal(19, 1), bpool_commit_target * 8.0 / 1024.0) AS bpool_commit_target_mb,
			CONVERT(decimal(19, 1), bpool_visible * 8.0 / 1024.0) AS bpool_visible_mb,
			affinity_type_desc,
			time_source_desc
			INTO #DTR_ServerInfo
			FROM sys.dm_os_sys_info;'
	END

	EXEC sys.sp_executesql
	@sql
END
GO

------------------------------------------------------------
-- 00e. Extended Events Sessions (Defined vs Running)
------------------------------------------------------------
PRINT N'▶ 00e. Extended Events Sessions (Defined vs Running) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_XESessions') IS NOT NULL DROP TABLE #DTR_XESessions
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH defn AS (
		SELECT
		s.event_session_id,
		s.name,
		s.startup_state,
		s.event_retention_mode_desc,
		s.memory_partition_mode_desc,
		s.max_memory,
		s.max_dispatch_latency,
		s.track_causality
		FROM sys.server_event_sessions AS s
	),
	run AS (
		SELECT
		xs.name,
		1 AS is_running,
		xs.create_time,
		xs.buffer_policy_desc,
		xs.pending_buffers,
		xs.total_buffer_size,
		xs.total_regular_buffers,
		xs.regular_buffer_size,
		xs.total_large_buffers,
		xs.large_buffer_size,
		xs.dropped_event_count,
		xs.dropped_buffer_count
		FROM sys.dm_xe_sessions AS xs
	),
	tgt AS (
		SELECT
		st.event_session_id,
		st.name AS target_name
		FROM sys.server_event_session_targets AS st
	),
	fdef AS (
		SELECT
		t.event_session_id,
		f1.def_file_name,
		f2.def_max_file_size_mb,
		f3.def_max_rollover_files
		FROM sys.server_event_session_targets AS t
		LEFT JOIN (
			SELECT
			event_session_id,
			object_id,
			CONVERT(nvarchar(4000), value) AS def_file_name
			FROM sys.server_event_session_fields
			WHERE name = 'filename'
		) AS f1 ON f1.event_session_id = t.event_session_id AND f1.object_id = t.target_id
		LEFT JOIN (
			SELECT
			event_session_id,
			object_id,
			CONVERT(int, value) AS def_max_file_size_mb
			FROM sys.server_event_session_fields
			WHERE name IN ('max_file_size', 'maxFileSize')
		) AS f2 ON f2.event_session_id = t.event_session_id AND f2.object_id = t.target_id
		LEFT JOIN (
			SELECT
			event_session_id,
			object_id,
			CONVERT(int, value) AS def_max_rollover_files
			FROM sys.server_event_session_fields
			WHERE name IN ('max_rollover_files', 'maxRolloverFiles')
		) AS f3 ON f3.event_session_id = t.event_session_id AND f3.object_id = t.target_id
		WHERE t.name = 'event_file'
	),
	rfile AS (
		SELECT
		xs.name AS session_name,
		CAST(xst.target_data AS xml) AS xdata,
		xst.execution_count,
		xst.execution_duration_ms
		FROM sys.dm_xe_sessions AS xs
		JOIN sys.dm_xe_session_targets AS xst ON xst.event_session_address = xs.address
		WHERE xst.target_name = 'event_file'
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	d.name AS session_name,
	d.startup_state,
	d.event_retention_mode_desc,
	d.memory_partition_mode_desc,
	d.max_memory,
	d.max_dispatch_latency,
	d.track_causality,
	CASE WHEN run.is_running = 1 THEN 1 ELSE 0 END AS is_running,
	run.create_time AS session_start_time,
	run.buffer_policy_desc,
	run.pending_buffers,
	run.total_buffer_size,
	run.total_regular_buffers,
	run.regular_buffer_size,
	run.total_large_buffers,
	run.large_buffer_size,
	run.dropped_event_count,
	run.dropped_buffer_count,
	CASE
		WHEN d.event_retention_mode_desc IN ('ALLOW_SINGLE_EVENT_LOSS', 'ALLOW_MULTIPLE_EVENT_LOSS') THEN CAST(1 AS bit)
		ELSE CAST(0 AS bit)
	END AS is_lossy_mode,
	CASE
		WHEN ISNULL(run.dropped_event_count, 0) > 0
			OR ISNULL(run.dropped_buffer_count, 0) > 0 THEN CAST(1 AS bit)
		ELSE CAST(0 AS bit)
	END AS is_dropping_events,
	t.target_name,
	CASE WHEN cfg.SafeMode = 0 THEN ISNULL(rf_runtime.file_name, fdef.def_file_name) ELSE '[SafeMode]' END AS file_name,
	ISNULL(rf_runtime.max_file_size_mb, fdef.def_max_file_size_mb) AS max_file_size_mb,
	ISNULL(rf_runtime.max_rollover_files, fdef.def_max_rollover_files) AS max_rollover_files,
	rfile.execution_count AS target_execution_count,
	rfile.execution_duration_ms AS target_execution_duration_ms
	INTO #DTR_XESessions
	FROM defn AS d
	LEFT JOIN run ON run.name = d.name
	LEFT JOIN tgt AS t ON t.event_session_id = d.event_session_id
	LEFT JOIN fdef ON fdef.event_session_id = d.event_session_id
	LEFT JOIN rfile ON rfile.session_name = d.name
	OUTER APPLY (
		SELECT
		CASE WHEN run.is_running = 1 THEN rfile.xdata.value('(//EventFileTarget/@name)[1]', 'nvarchar(4000)') END AS file_name,
		CASE WHEN run.is_running = 1 THEN COALESCE(
		rfile.xdata.value('(//EventFileTarget/@max_file_size)[1]', 'int'),
		rfile.xdata.value('(//EventFileTarget/@maxFileSize)[1]', 'int')
		) END AS max_file_size_mb,
		CASE WHEN run.is_running = 1 THEN COALESCE(
		rfile.xdata.value('(//EventFileTarget/@max_rollover_files)[1]', 'int'),
		rfile.xdata.value('(//EventFileTarget/@maxRolloverFiles)[1]', 'int')
		) END AS max_rollover_files
	) AS rf_runtime
	CROSS JOIN #DT_Config AS cfg
END
GO

------------------------------------------------------------
-- 00f. Resource Governor (Configuration & State)
------------------------------------------------------------
PRINT N'▶ 00f. Resource Governor (Configuration & State) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_RG_Config') IS NOT NULL DROP TABLE #DTR_RG_Config
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	rgc.is_enabled,
	d.is_reconfiguration_pending,
	rgc.classifier_function_id AS stored_classifier_function_id,
	CASE
		WHEN rgc.classifier_function_id IS NULL THEN NULL
		ELSE QUOTENAME(OBJECT_SCHEMA_NAME(rgc.classifier_function_id, DB_ID('master'))) + '.' + QUOTENAME(OBJECT_NAME(rgc.classifier_function_id, DB_ID('master')))
	END AS stored_classifier_function_name,
	d.classifier_function_id AS effective_classifier_function_id,
	CASE
		WHEN d.classifier_function_id IS NULL OR d.classifier_function_id = 0 THEN NULL
		ELSE QUOTENAME(OBJECT_SCHEMA_NAME(d.classifier_function_id, DB_ID('master'))) + '.' + QUOTENAME(OBJECT_NAME(d.classifier_function_id, DB_ID('master')))
	END AS effective_classifier_function_name
	INTO #DTR_RG_Config
	FROM sys.resource_governor_configuration AS rgc
	CROSS JOIN sys.dm_resource_governor_configuration AS d
END
GO

------------------------------------------------------------
-- 00g. Linked Servers (Inventory)
------------------------------------------------------------
PRINT N'▶ 00g. Linked Servers (Inventory) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_LinkedServers') IS NOT NULL DROP TABLE #DTR_LinkedServers
GO

WITH logins AS (
	SELECT
	ls.server_id,
	COUNT(*) AS login_mappings,
	SUM(CASE WHEN ls.uses_self_credential = 1 THEN 1 ELSE 0 END) AS self_mapping_count,
	SUM(CASE WHEN ls.uses_self_credential = 0 THEN 1 ELSE 0 END) AS explicit_mapping_count
	FROM sys.linked_logins AS ls
	GROUP BY ls.server_id
)
SELECT
IDENTITY(int) AS RowNumber,
s.server_id,
s.name,
s.product,
s.provider,
CASE WHEN cfg.SafeMode = 0 THEN s.data_source ELSE '[SafeMode]' END AS data_source,
CASE WHEN cfg.SafeMode = 0 THEN s.catalog ELSE '[SafeMode]' END AS catalog,
s.is_linked,
s.is_system,
s.is_publisher,
s.is_subscriber,
s.is_distributor,
s.is_rpc_out_enabled,
s.is_data_access_enabled,
s.is_remote_login_enabled,
ISNULL(l.login_mappings, 0) AS login_mappings,
ISNULL(l.self_mapping_count, 0) AS self_credential_mappings,
ISNULL(l.explicit_mapping_count, 0) AS explicit_mappings,
s.modify_date,
s.is_collation_compatible,
s.uses_remote_collation,
s.collation_name,
s.lazy_schema_validation,
s.is_remote_proc_transaction_promotion_enabled,
s.connect_timeout,
s.query_timeout
INTO #DTR_LinkedServers
FROM sys.servers AS s
LEFT JOIN logins AS l ON l.server_id = s.server_id
CROSS JOIN #DT_Config AS cfg
WHERE s.server_id <> 0 -- exclude only the local server
GO

------------------------------------------------------------
-- 00h. Active Trace Flags
------------------------------------------------------------
PRINT N'▶ 00h. Active Trace Flags - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_TraceFlags') IS NOT NULL DROP TABLE #DTR_TraceFlags
GO

CREATE TABLE #DTR_TraceFlags_src (TraceFlag int, Status int, [Global] int, [Session] int)
INSERT INTO #DTR_TraceFlags_src
EXEC('DBCC TRACESTATUS(-1) WITH NO_INFOMSGS')

SELECT
IDENTITY(int) AS RowNumber,
TraceFlag,
Status,
[Global],
[Session]
INTO #DTR_TraceFlags
FROM #DTR_TraceFlags_src
ORDER BY TraceFlag

DROP TABLE #DTR_TraceFlags_src
GO

------------------------------------------------------------
-- 00i. Connection Encryption & Protocol Mix (Server & Target DB)
------------------------------------------------------------
PRINT N'▶ 00i. Connection Encryption & Protocol Mix (Server & Target DB) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_ConnEncryptionMix') IS NOT NULL DROP TABLE #DTR_ConnEncryptionMix
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH c AS (
		SELECT
		ec.session_id,
		ec.net_transport,
		ec.encrypt_option,
		ec.protocol_version,
		ec.auth_scheme
		FROM sys.dm_exec_connections AS ec
	),
	s AS (
		SELECT
		spid AS session_id,
		dbid AS database_id
		FROM sys.sysprocesses
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	c.net_transport,
	c.auth_scheme,
	c.encrypt_option,
	c.protocol_version,
	SUM(CASE WHEN s.database_id = DB_ID() THEN 1 ELSE 0 END) AS sessions_in_targetdb,
	COUNT(*) AS sessions_total
	INTO #DTR_ConnEncryptionMix
	FROM c
	LEFT JOIN s ON s.session_id = c.session_id
	GROUP BY c.net_transport, c.auth_scheme, c.encrypt_option, c.protocol_version
END
GO

------------------------------------------------------------
-- 00j. HADR Endpoint (Database Mirroring) - Encryption & Auth
------------------------------------------------------------
PRINT N'▶ 00j. HADR Endpoint (Database Mirroring) - Encryption & Auth - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_HadrEndpoint') IS NOT NULL DROP TABLE #DTR_HadrEndpoint
GO

SELECT
IDENTITY(int) AS RowNumber,
e.name AS endpoint_name,
e.state_desc,
e.protocol_desc,
e.is_admin_endpoint,
dme.role_desc,
dme.is_encryption_enabled,
dme.encryption_algorithm_desc,
dme.connection_auth_desc,
dme.certificate_id,
CASE WHEN cfg.SafeMode = 0 THEN c.name ELSE '[SafeMode]' END AS certificate_name,
c.expiry_date,
te.port,
te.is_dynamic_port,
CASE WHEN cfg.SafeMode = 0 THEN sp.name ELSE '[SafeMode]' END AS owner,
sp.type_desc AS owner_type_desc
INTO #DTR_HadrEndpoint
FROM sys.endpoints AS e
JOIN sys.database_mirroring_endpoints AS dme ON dme.endpoint_id = e.endpoint_id
LEFT JOIN sys.tcp_endpoints AS te ON te.endpoint_id = e.endpoint_id
LEFT JOIN sys.certificates AS c ON c.certificate_id = dme.certificate_id
LEFT JOIN sys.server_principals AS sp ON sp.principal_id = e.principal_id
CROSS JOIN #DT_Config AS cfg
WHERE e.type_desc = 'DATABASE_MIRRORING'
GO

------------------------------------------------------------
-- 00k. Server Configuration - Focused Risk Summary
------------------------------------------------------------
PRINT N'▶ 00k. Server Configuration - Focused Risk Summary - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_ServerConfigRisk') IS NOT NULL DROP TABLE #DTR_ServerConfigRisk
GO

SELECT
IDENTITY(int) AS RowNumber,
configuration_id,
name,
minimum,
maximum,
value AS configured_value,
value_in_use,
is_dynamic,
is_advanced,
description
INTO #DTR_ServerConfigRisk
FROM sys.configurations
ORDER BY name;
GO

------------------------------------------------------------
-- 00l. Collation Posture (Server vs. Target DB)
------------------------------------------------------------
PRINT N'▶ 00l. Collation Posture (Server vs. Target DB) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_CollationPosture') IS NOT NULL DROP TABLE #DTR_CollationPosture;
GO

SELECT
IDENTITY(int) AS RowNumber,
CONVERT(sysname, SERVERPROPERTY('Collation')) AS ServerCollation,
DB_NAME() AS DatabaseName,
CONVERT(sysname, d.collation_name) AS DatabaseCollation,
CONVERT(sysname, DATABASEPROPERTYEX('tempdb', 'Collation')) AS TempdbCollation,
CASE WHEN CONVERT(sysname, SERVERPROPERTY('Collation')) <> CONVERT(sysname, d.collation_name) THEN 1 ELSE 0 END AS is_mismatch_server_db,
CASE WHEN CONVERT(sysname, DATABASEPROPERTYEX('tempdb', 'Collation')) <> CONVERT(sysname, d.collation_name) THEN 1 ELSE 0 END AS is_mismatch_tempdb_db,
CASE WHEN d.collation_name LIKE '%_CS_%' THEN 'CS' ELSE 'CI' END AS CaseSensitivity,
CASE WHEN d.collation_name LIKE '%_AI%' THEN 'AI' ELSE 'AS' END AS AccentSensitivity
INTO #DTR_CollationPosture
FROM sys.databases AS d
WHERE d.database_id = DB_ID();
GO

------------------------------------------------------------
-- 00m. Endpoints Inventory (TLS/Port/Type)
------------------------------------------------------------
PRINT N'▶ 00m. Endpoints Inventory (TLS/Port/Type) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_Endpoints') IS NOT NULL DROP TABLE #DTR_Endpoints;
GO

SELECT
IDENTITY(int) AS RowNumber,
e.endpoint_id,
e.name,
e.type_desc,
e.state_desc,
e.protocol_desc,
te.port,
te.is_dynamic_port,
te.is_admin_endpoint,
CASE WHEN cfg.SafeMode = 0 THEN sp.name ELSE '[SafeMode]' END AS owner,
COALESCE(dme.connection_auth_desc, sbe.connection_auth_desc) AS connection_auth_desc,
dme.is_encryption_enabled,
COALESCE(dme.encryption_algorithm_desc, sbe.encryption_algorithm_desc) AS encryption_algorithm_desc,
COALESCE(dme.certificate_id, sbe.certificate_id) AS certificate_id,
e.principal_id
INTO #DTR_Endpoints
FROM sys.endpoints AS e
LEFT JOIN sys.tcp_endpoints AS te ON te.endpoint_id = e.endpoint_id
LEFT JOIN sys.database_mirroring_endpoints AS dme ON dme.endpoint_id = e.endpoint_id
LEFT JOIN sys.service_broker_endpoints AS sbe ON sbe.endpoint_id = e.endpoint_id
LEFT JOIN sys.server_principals AS sp ON sp.principal_id = e.principal_id
CROSS JOIN #DT_Config AS cfg
ORDER BY e.type_desc, e.name;
GO

------------------------------------------------------------
-- 00n. Effective Parallelism Posture (Server/DB/RG)
------------------------------------------------------------
PRINT N'▶ 00n. Effective Parallelism Posture (Server/DB/RG) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_EffectiveParallelism') IS NOT NULL DROP TABLE #DTR_EffectiveParallelism;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH cfg AS (
		SELECT
		SUM(CASE WHEN name = 'max degree of parallelism' THEN CAST(value_in_use AS int) ELSE 0 END) AS server_maxdop,
		SUM(CASE WHEN name = 'cost threshold for parallelism' THEN CAST(value_in_use AS int) ELSE 0 END) AS server_cost_threshold
		FROM sys.configurations
		WHERE name IN ('max degree of parallelism', 'cost threshold for parallelism')
	),
	dbp AS (
		SELECT
		compatibility_level AS db_compat_level
		FROM sys.databases
		WHERE database_id = DB_ID()
	),
	rgo AS (
		SELECT
		is_enabled AS rg_is_enabled
		FROM sys.resource_governor_configuration
	),
	rg AS (
		SELECT TOP (1)
		wg.max_dop
		FROM sys.dm_resource_governor_workload_groups AS wg
		WHERE wg.name = 'default'
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	cfg.server_maxdop AS Server_MAXDOP,
	cfg.server_cost_threshold AS Server_CostThreshold,
	ISNULL(rg.max_dop, 0) AS RG_DefaultGroup_MAXDOP,
	dbp.db_compat_level AS DB_CompatLevel,
	rgo.rg_is_enabled AS RG_IsEnabled
	INTO #DTR_EffectiveParallelism
	FROM cfg
	CROSS JOIN dbp
	CROSS JOIN rgo
	CROSS JOIN rg;
END
GO

------------------------------------------------------------
-- 00o. TempDB File Space Usage (DB)
------------------------------------------------------------
PRINT N'▶ 00o. TempDB File Space Usage (DB) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO
IF OBJECT_ID('tempdb..#DTR_TempdbFileSpace') IS NOT NULL DROP TABLE #DTR_TempdbFileSpace;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	SUM(unallocated_extent_page_count) * 8.0 / 1024 AS unallocated_mb,
	SUM(version_store_reserved_page_count) * 8.0 / 1024 AS version_store_mb,
	SUM(user_object_reserved_page_count) * 8.0 / 1024 AS user_objects_mb,
	SUM(internal_object_reserved_page_count) * 8.0 / 1024 AS internal_objects_mb,
	SUM(mixed_extent_page_count) * 8.0 / 1024 AS mixed_extents_mb
	INTO #DTR_TempdbFileSpace
	FROM tempdb.sys.dm_db_file_space_usage;
END
GO

------------------------------------------------------------
-- 00p. TempDB Session Space Usage (Top 50)
------------------------------------------------------------
PRINT N'▶ 00p. TempDB Session Space Usage (Top 50) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO
IF OBJECT_ID('tempdb..#DTR_TempdbSessionSpace') IS NOT NULL DROP TABLE #DTR_TempdbSessionSpace;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT TOP (50)
	IDENTITY(int) AS RowNumber,
	ssu.session_id,
	CASE WHEN cfg.SafeMode = 0 THEN es.host_name ELSE '[SafeMode]' END AS host_name,
	CASE WHEN cfg.SafeMode = 0 THEN es.login_name ELSE '[SafeMode]' END AS login_name,
	CASE WHEN cfg.SafeMode = 0 THEN es.program_name ELSE '[SafeMode]' END AS program_name,
	(ssu.user_objects_alloc_page_count - ssu.user_objects_dealloc_page_count) * 8.0 / 1024 AS user_objects_mb,
	(ssu.internal_objects_alloc_page_count - ssu.internal_objects_dealloc_page_count) * 8.0 / 1024 AS internal_objects_mb
	INTO #DTR_TempdbSessionSpace
	FROM tempdb.sys.dm_db_session_space_usage AS ssu
	LEFT JOIN sys.dm_exec_sessions AS es ON es.session_id = ssu.session_id
	CROSS JOIN #DT_Config AS cfg
	ORDER BY (ssu.user_objects_alloc_page_count - ssu.user_objects_dealloc_page_count + ssu.internal_objects_alloc_page_count - ssu.internal_objects_dealloc_page_count) DESC;
END
GO

------------------------------------------------------------
-- 00q. TempDB Task Space Usage (Top 50)
------------------------------------------------------------
PRINT N'▶ 00q. TempDB Task Space Usage (Top 50) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO
IF OBJECT_ID('tempdb..#DTR_TempdbTaskSpace') IS NOT NULL DROP TABLE #DTR_TempdbTaskSpace;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT TOP (50)
	IDENTITY(int) AS RowNumber,
	tsu.session_id,
	tsu.request_id,
	(tsu.user_objects_alloc_page_count - tsu.user_objects_dealloc_page_count) * 8.0 / 1024 AS user_objects_mb,
	(tsu.internal_objects_alloc_page_count - tsu.internal_objects_dealloc_page_count) * 8.0 / 1024 AS internal_objects_mb
	INTO #DTR_TempdbTaskSpace
	FROM tempdb.sys.dm_db_task_space_usage AS tsu
	ORDER BY (tsu.user_objects_alloc_page_count - tsu.user_objects_dealloc_page_count + tsu.internal_objects_alloc_page_count - tsu.internal_objects_dealloc_page_count) DESC;
END
GO

------------------------------------------------------------
-- 01a. Waiting Tasks (Target DB Snapshot)
------------------------------------------------------------
PRINT N'▶ 01a. Waiting Tasks (Target DB Snapshot) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO
IF OBJECT_ID('tempdb..#DTR_WaitingTasks') IS NOT NULL DROP TABLE #DTR_WaitingTasks;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT TOP (200)
	IDENTITY(int) AS RowNumber,
	wt.session_id,
	r.blocking_session_id,
	wt.wait_type,
	CASE
		WHEN wt.wait_type LIKE 'LCK_%' THEN 'Lock'
		WHEN wt.wait_type LIKE 'PAGEIOLATCH_%' THEN 'IO Latch'
		WHEN wt.wait_type LIKE 'PAGELATCH_%' THEN 'Buffer Latch'
		WHEN wt.wait_type IN ('CXPACKET', 'CXCONSUMER', 'EXCHANGE') THEN 'Parallelism'
		WHEN wt.wait_type IN ('WRITELOG', 'LOGBUFFER') THEN 'Log IO'
		WHEN wt.wait_type = 'ASYNC_NETWORK_IO' THEN 'Network'
		WHEN wt.wait_type IN ('RESOURCE_SEMAPHORE', 'RESOURCE_SEMAPHORE_QUERY_COMPILE') THEN 'Memory'
		WHEN wt.wait_type = 'SOS_SCHEDULER_YIELD' THEN 'CPU'
		ELSE 'Other'
	END AS wait_category,
	wt.wait_duration_ms,
	wt.resource_description,
	wt.exec_context_id,
	r.status,
	r.command,
	r.scheduler_id,
	r.cpu_time AS request_cpu_time_ms,
	r.total_elapsed_time AS request_elapsed_time_ms,
	r.reads AS request_reads,
	r.writes AS request_writes,
	DB_NAME(r.database_id) AS database_name,
	OBJECT_SCHEMA_NAME(st.objectid, st.dbid) AS schema_name,
	OBJECT_NAME(st.objectid, st.dbid) AS object_name,
	CASE WHEN cfg.SafeMode = 0 THEN SUBSTRING(st.text,
		CASE WHEN r.statement_start_offset < 0 THEN 0 ELSE r.statement_start_offset END/2 + 1,
		CASE WHEN r.statement_end_offset < 0
			THEN (LEN(CONVERT(nvarchar(MAX), st.text)) * 2 - r.statement_start_offset)/2 + 1
			ELSE (r.statement_end_offset - r.statement_start_offset)/2 + 1
		END) ELSE '[SafeMode]' END AS statement_text,
	CASE WHEN cfg.SafeMode = 0 THEN es.host_name ELSE '[SafeMode]' END AS host_name,
	CASE WHEN cfg.SafeMode = 0 THEN es.program_name ELSE '[SafeMode]' END AS program_name,
	CASE WHEN cfg.SafeMode = 0 THEN es.login_name ELSE '[SafeMode]' END AS login_name
	INTO #DTR_WaitingTasks
	FROM sys.dm_os_waiting_tasks AS wt
	LEFT JOIN sys.dm_exec_requests AS r ON r.session_id = wt.session_id
	LEFT JOIN sys.dm_exec_sessions AS es ON es.session_id = wt.session_id
	OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) AS st
	CROSS JOIN #DT_Config AS cfg
	WHERE r.database_id = DB_ID()
	ORDER BY wt.wait_duration_ms DESC;
END
GO

------------------------------------------------------------
-- 01b. OS Tasks by Scheduler (Target DB)
------------------------------------------------------------
PRINT N'▶ 01b. OS Tasks by Scheduler (Target DB) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO
IF OBJECT_ID('tempdb..#DTR_OSTasksByScheduler') IS NOT NULL DROP TABLE #DTR_OSTasksByScheduler;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	WITH t AS (
		SELECT
		sc.scheduler_id,
		ot.task_state
		FROM sys.dm_os_tasks AS ot
		JOIN sys.dm_os_schedulers AS sc ON sc.scheduler_id = ot.scheduler_id
		JOIN sys.dm_exec_requests AS r ON r.session_id = ot.session_id
		WHERE sc.status = 'VISIBLE ONLINE' AND r.database_id = DB_ID()
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	t.scheduler_id,
	SUM(CASE WHEN task_state = 'RUNNING' THEN 1 ELSE 0 END) AS running_tasks,
	SUM(CASE WHEN task_state = 'RUNNABLE' THEN 1 ELSE 0 END) AS runnable_tasks,
	SUM(CASE WHEN task_state = 'SUSPENDED' THEN 1 ELSE 0 END) AS suspended_tasks,
	COUNT(*) AS total_tasks,
	MAX(s.runnable_tasks_count) AS runnable_queue_len,
	MAX(s.current_tasks_count) AS current_tasks,
	MAX(s.active_workers_count) AS active_workers,
	MAX(s.yield_count) AS yield_count,
	MAX(s.pending_disk_io_count) AS pending_disk_io_count
	INTO #DTR_OSTasksByScheduler
	FROM t
	JOIN sys.dm_os_schedulers AS s ON s.scheduler_id = t.scheduler_id
	GROUP BY t.scheduler_id
	ORDER BY runnable_queue_len DESC, t.scheduler_id;
END
GO

------------------------------------------------------------
-- 01c. OS Workers - Top Pressure (Target DB)
------------------------------------------------------------
PRINT N'▶ 01c. OS Workers - Top Pressure (Target DB) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO
IF OBJECT_ID('tempdb..#DTR_OSWorkersTop') IS NOT NULL DROP TABLE #DTR_OSWorkersTop;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT TOP (50)
	IDENTITY(int) AS RowNumber,
	sc.scheduler_id,
	r.session_id,
	t.request_id,
	w.state AS worker_state,
	w.is_preemptive,
	w.is_fiber,
	w.pending_io_count,
	w.context_switch_count AS context_switches_count,
	CASE WHEN cfg.SafeMode = 0 THEN es.host_name ELSE '[SafeMode]' END AS host_name,
	CASE WHEN cfg.SafeMode = 0 THEN es.login_name ELSE '[SafeMode]' END AS login_name,
	CASE WHEN cfg.SafeMode = 0 THEN SUBSTRING(st.text,
		CASE WHEN r.statement_start_offset < 0 THEN 0 ELSE r.statement_start_offset END/2 + 1,
		CASE WHEN r.statement_end_offset < 0
			THEN (LEN(CONVERT(nvarchar(MAX), st.text)) * 2 - r.statement_start_offset)/2 + 1
			ELSE (r.statement_end_offset - r.statement_start_offset)/2 + 1
		END) ELSE '[SafeMode]' END AS statement_text,
	r.cpu_time,
	r.reads,
	r.writes
	INTO #DTR_OSWorkersTop
	FROM sys.dm_os_workers AS w
	LEFT JOIN sys.dm_os_tasks AS t ON t.worker_address = w.worker_address
	LEFT JOIN sys.dm_os_schedulers AS sc ON sc.scheduler_address = w.scheduler_address
	LEFT JOIN sys.dm_exec_requests AS r ON r.session_id = t.session_id
	LEFT JOIN sys.dm_exec_sessions AS es ON es.session_id = r.session_id
	OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) AS st
	CROSS JOIN #DT_Config AS cfg
	WHERE sc.status = 'VISIBLE ONLINE' AND r.database_id = DB_ID()
	ORDER BY w.pending_io_count DESC, w.context_switch_count DESC;
END
GO

------------------------------------------------------------
-- 01d. Memory Grant Semaphores (Grant Pressure)
------------------------------------------------------------
PRINT N'▶ 01d. Memory Grant Semaphores (Grant Pressure) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO
IF OBJECT_ID('tempdb..#DTR_ResourceSemaphores') IS NOT NULL DROP TABLE #DTR_ResourceSemaphores;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	ISNULL(rp.name, 'default') AS pool_name,
	rs.resource_semaphore_id,
	rs.grantee_count AS active_grants,
	rs.waiter_count,
	rs.timeout_error_count,
	rs.forced_grant_count,
	rs.used_memory_kb,
	rs.granted_memory_kb,
	rs.available_memory_kb,
	rs.target_memory_kb,
	rs.max_target_memory_kb,
	CAST(CASE WHEN rs.target_memory_kb > 0 THEN (100.0 * rs.granted_memory_kb) / rs.target_memory_kb END AS decimal(6,2)) AS granted_pct_of_target,
	CAST(CASE WHEN (rs.grantee_count + rs.waiter_count) > 0 THEN (100.0 * rs.waiter_count) / (rs.grantee_count + rs.waiter_count) END AS decimal(6,2)) AS pct_waiters
	INTO #DTR_ResourceSemaphores
	FROM sys.dm_exec_query_resource_semaphores AS rs
	LEFT JOIN sys.dm_resource_governor_resource_pools AS rp ON rp.pool_id = rs.pool_id
	ORDER BY rs.waiter_count DESC, rs.grantee_count DESC;
END
GO

------------------------------------------------------------
-- 01e. SQL Server Process Memory (Snapshot)
------------------------------------------------------------
PRINT N'▶ 01e. SQL Server Process Memory (Snapshot) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO
IF OBJECT_ID('tempdb..#DTR_ProcessMemory') IS NOT NULL DROP TABLE #DTR_ProcessMemory;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	DECLARE @max_server_mem_mb int = (
		SELECT CONVERT(int, value_in_use)
		FROM sys.configurations
		WHERE name = 'max server memory (MB)'
	);

	SELECT
	IDENTITY(int) AS RowNumber,
	pm.physical_memory_in_use_kb / 1024.0 AS physical_memory_in_use_mb,
	pm.large_page_allocations_kb / 1024.0 AS large_page_allocations_mb,
	pm.locked_page_allocations_kb / 1024.0 AS locked_page_allocations_mb,
	pm.total_virtual_address_space_kb / 1024.0 AS total_va_space_mb,
	pm.virtual_address_space_reserved_kb / 1024.0 AS va_reserved_mb,
	pm.virtual_address_space_committed_kb / 1024.0 AS va_committed_mb,
	pm.virtual_address_space_available_kb / 1024.0 AS va_available_mb,
	pm.memory_utilization_percentage AS memory_utilization_pct,
	pm.available_commit_limit_kb / 1024.0 AS available_commit_limit_mb,
	pm.process_physical_memory_low AS process_physical_memory_low,
	pm.process_virtual_memory_low AS process_virtual_memory_low,
	@max_server_mem_mb AS max_server_memory_config_mb
	INTO #DTR_ProcessMemory
	FROM sys.dm_os_process_memory AS pm; -- columns & perms documented by MS
END
GO

------------------------------------------------------------
-- 01f. OS Memory Snapshot & Headroom
------------------------------------------------------------
PRINT N'▶ 01f. OS Memory Snapshot & Headroom - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO
IF OBJECT_ID('tempdb..#DTR_OsSysMemory') IS NOT NULL DROP TABLE #DTR_OsSysMemory;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	DECLARE @sql nvarchar(max);

	IF COL_LENGTH('sys.dm_os_sys_info', 'committed_kb') IS NOT NULL
	BEGIN
		SET @sql = N'
			SELECT
			IDENTITY(int) AS RowNumber,
			os.system_memory_state_desc,
			CAST(os.total_physical_memory_kb / 1048576.0 AS decimal(18,3)) AS total_physical_memory_gb,
			CAST(os.available_physical_memory_kb / 1048576.0 AS decimal(18,3)) AS available_physical_memory_gb,
			CAST((os.total_physical_memory_kb - os.available_physical_memory_kb) / 1048576.0 AS decimal(18,3)) AS used_physical_memory_gb,
			CAST(os.total_page_file_kb / 1048576.0 AS decimal(18,3)) AS total_page_file_gb,
			CAST(os.available_page_file_kb / 1048576.0 AS decimal(18,3)) AS available_page_file_gb,
			CAST(os.system_cache_kb / 1048576.0 AS decimal(18,3)) AS system_cache_gb,
			CAST(os.kernel_paged_pool_kb / 1024.0 AS decimal(18,1)) AS kernel_paged_pool_mb,
			CAST(os.kernel_nonpaged_pool_kb / 1024.0 AS decimal(18,1)) AS kernel_nonpaged_pool_mb,
			os.system_high_memory_signal_state,
			os.system_low_memory_signal_state,
			CAST(pm.physical_memory_in_use_kb / 1048576.0 AS decimal(18,3)) AS sql_physical_in_use_gb,
			CAST(si.committed_kb / 1048576.0 AS decimal(18,3)) AS sql_bpool_committed_gb,
			CAST(si.committed_target_kb / 1048576.0 AS decimal(18,3)) AS sql_bpool_commit_target_gb,
			CAST(si.visible_target_kb / 1048576.0 AS decimal(18,3)) AS sql_bpool_visible_gb,
			pm.memory_utilization_percentage AS sql_memory_utilization_pct,
			CAST((si.committed_kb * 100.0) / NULLIF(os.total_physical_memory_kb,0) AS decimal(5,1)) AS sql_bpool_committed_pct_of_physical,
			CAST((os.available_physical_memory_kb * 100.0) / NULLIF(os.total_physical_memory_kb,0) AS decimal(5,1)) AS os_available_pct
			INTO #DTR_OsSysMemory
			FROM sys.dm_os_sys_memory AS os
			CROSS JOIN sys.dm_os_process_memory AS pm
			CROSS JOIN sys.dm_os_sys_info AS si;'
	END

	IF COL_LENGTH('sys.dm_os_sys_info', 'committed_kb') IS NULL
	BEGIN
		SET @sql = N'
			SELECT
			IDENTITY(int) AS RowNumber,
			os.system_memory_state_desc,
			CAST(os.total_physical_memory_kb / 1048576.0 AS decimal(18,3)) AS total_physical_memory_gb,
			CAST(os.available_physical_memory_kb / 1048576.0 AS decimal(18,3)) AS available_physical_memory_gb,
			CAST((os.total_physical_memory_kb - os.available_physical_memory_kb) / 1048576.0 AS decimal(18,3)) AS used_physical_memory_gb,
			CAST(os.total_page_file_kb / 1048576.0 AS decimal(18,3)) AS total_page_file_gb,
			CAST(os.available_page_file_kb / 1048576.0 AS decimal(18,3)) AS available_page_file_gb,
			CAST(os.system_cache_kb / 1048576.0 AS decimal(18,3)) AS system_cache_gb,
			CAST(os.kernel_paged_pool_kb / 1024.0 AS decimal(18,1)) AS kernel_paged_pool_mb,
			CAST(os.kernel_nonpaged_pool_kb / 1024.0 AS decimal(18,1)) AS kernel_nonpaged_pool_mb,
			os.system_high_memory_signal_state,
			os.system_low_memory_signal_state,
			CAST(pm.physical_memory_in_use_kb / 1048576.0 AS decimal(18,3)) AS sql_physical_in_use_gb,
			CAST(si.bpool_committed * 8.0 / 1048576.0 AS decimal(18,3)) AS sql_bpool_committed_gb,
			CAST(si.bpool_commit_target * 8.0 / 1048576.0 AS decimal(18,3)) AS sql_bpool_commit_target_gb,
			CAST(si.bpool_visible * 8.0 / 1048576.0 AS decimal(18,3)) AS sql_bpool_visible_gb,
			pm.memory_utilization_percentage AS sql_memory_utilization_pct,
			CAST((si.bpool_committed * 8.0 * 100.0) / NULLIF(os.total_physical_memory_kb,0) AS decimal(5,1)) AS sql_bpool_committed_pct_of_physical,
			CAST((os.available_physical_memory_kb * 100.0) / NULLIF(os.total_physical_memory_kb,0) AS decimal(5,1)) AS os_available_pct
			INTO #DTR_OsSysMemory
			FROM sys.dm_os_sys_memory AS os
			CROSS JOIN sys.dm_os_process_memory AS pm
			CROSS JOIN sys.dm_os_sys_info AS si;'
	END

	EXEC sys.sp_executesql
	@sql
END
GO

------------------------------------------------------------
-- 01g. NUMA Nodes & Scheduler Distribution
------------------------------------------------------------
PRINT N'▶ 01g. NUMA Nodes & Scheduler Distribution - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO
IF OBJECT_ID('tempdb..#DTR_NumaNodes') IS NOT NULL DROP TABLE #DTR_NumaNodes;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	n.node_id,
	n.memory_node_id,
	n.node_state_desc,
	SUM(CASE WHEN s.status = 'VISIBLE ONLINE' THEN 1 ELSE 0 END) AS online_schedulers,
	COUNT(*) AS schedulers_on_node,
	SUM(s.runnable_tasks_count) AS runnable_tasks_count,
	SUM(s.pending_disk_io_count) AS pending_disk_io_count,
	SUM(s.active_workers_count) AS active_workers_count,
	SUM(s.work_queue_count) AS work_queue_count
	INTO #DTR_NumaNodes
	FROM sys.dm_os_nodes AS n
	JOIN sys.dm_os_schedulers AS s ON s.parent_node_id = n.node_id AND s.scheduler_id < 255 -- exclude hidden schedulers
	GROUP BY n.node_id, n.memory_node_id, n.node_state_desc
	ORDER BY n.node_id;
END
GO

------------------------------------------------------------
-- 01h. Resource Governor Pools (Definitions)
------------------------------------------------------------
PRINT N'▶ 01h. Resource Governor Pools (Definitions) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_RG_PoolDefs') IS NOT NULL DROP TABLE #DTR_RG_PoolDefs;
GO

SELECT
IDENTITY(int) AS RowNumber,
rp.pool_id,
rp.name,
rp.min_cpu_percent,
rp.max_cpu_percent,
rp.min_memory_percent,
rp.max_memory_percent
INTO #DTR_RG_PoolDefs
FROM sys.resource_governor_resource_pools AS rp;
GO

------------------------------------------------------------
-- 01i. Resource Governor Workload Groups (Definitions)
------------------------------------------------------------
PRINT N'▶ 01i. Resource Governor Workload Groups (Definitions) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_RG_WGDefs') IS NOT NULL DROP TABLE #DTR_RG_WGDefs;
GO

SELECT
IDENTITY(int) AS RowNumber,
wg.group_id,
wg.name,
wg.pool_id,
wg.importance,
wg.max_dop,
wg.request_max_memory_grant_percent,
wg.request_memory_grant_timeout_sec,
wg.group_max_requests
INTO #DTR_RG_WGDefs
FROM sys.resource_governor_workload_groups AS wg;
GO

------------------------------------------------------------
-- 02a. Files, Size & Growth
------------------------------------------------------------
PRINT N'▶ 02a. Files, Size & Growth - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_FilesSizeGrowth') IS NOT NULL DROP TABLE #DTR_FilesSizeGrowth
GO

SELECT
IDENTITY(int) AS RowNumber,
mf.type_desc AS file_type,
mf.file_id AS file_id,
mf.name AS logical_name,
CASE WHEN cfg.SafeMode = 0 THEN mf.physical_name ELSE '[SafeMode]' END AS physical_name,
CONVERT(decimal(18,1), mf.size/128.0) AS size_mb,
CASE
	WHEN mf.is_percent_growth=1 THEN CAST(mf.growth AS varchar(20)) + '%'
	ELSE CAST((mf.growth*8)/1024 AS varchar(20)) + ' MB'
END as growth_desc,
CASE WHEN mf.max_size = -1 THEN NULL ELSE mf.max_size/128.0 END AS max_size_mb,
FILEGROUP_NAME(mf.data_space_id) AS filegroup_name
INTO #DTR_FilesSizeGrowth
FROM sys.database_files AS mf
CROSS JOIN #DT_Config AS cfg
ORDER BY mf.type_desc, mf.file_id
GO

------------------------------------------------------------
-- 02b. File IO Stalls
------------------------------------------------------------
PRINT N'▶ 02b. File IO Stalls - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_FileIOStalls') IS NOT NULL DROP TABLE #DTR_FileIOStalls
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	mf.type_desc AS file_type,
	mf.file_id AS file_id,
	mf.name AS logical_name,
	CASE WHEN cfg.SafeMode = 0 THEN mf.physical_name ELSE '[SafeMode]' END AS physical_name,
	CONVERT(decimal(18, 1), mf.size / 128.0) AS size_mb,
	fs.num_of_reads AS num_of_reads,
	fs.num_of_writes AS num_of_writes,
	fs.io_stall_read_ms AS io_stall_read_ms,
	fs.io_stall_write_ms AS io_stall_write_ms,
	fs.io_stall AS io_stall,
	CASE WHEN fs.num_of_reads > 0 THEN (fs.io_stall_read_ms * 1.0 / fs.num_of_reads) ELSE NULL END AS avg_read_ms,
	CASE WHEN fs.num_of_writes > 0 THEN (fs.io_stall_write_ms * 1.0 / fs.num_of_writes) ELSE NULL END AS avg_write_ms,
	FILEGROUP_NAME(mf.data_space_id) AS filegroup_name
	INTO #DTR_FileIOStalls
	FROM sys.dm_io_virtual_file_stats(DB_ID(), NULL) AS fs
	JOIN sys.database_files AS mf ON mf.file_id = fs.file_id
	CROSS JOIN #DT_Config AS cfg
	ORDER BY mf.type_desc, mf.file_id
END
GO

------------------------------------------------------------
-- 02c. Recent Autogrowth Events
------------------------------------------------------------
PRINT N'▶ 02c. Recent Autogrowth Events - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_AutogrowthEvents') IS NOT NULL DROP TABLE #DTR_AutogrowthEvents
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	DECLARE @trace_path nvarchar(260)

	SELECT
	@trace_path = path
	FROM sys.traces
	WHERE is_default = 1

	SELECT TOP (0)
	CAST(NULL AS int) AS RowNumber,
	CAST(NULL AS datetime) AS event_time,
	CAST(NULL AS nvarchar(20)) AS file_type,
	CAST(NULL AS sysname) AS logical_name,
	CAST(NULL AS decimal(18,2)) AS growth_mb,
	CAST(NULL AS int) AS duration_ms,
	CAST(NULL AS nvarchar(200)) AS message
	INTO #DTR_AutogrowthEvents
	FROM sys.objects AS o;

	BEGIN TRY
		;WITH x AS (
			SELECT TOP (5)
			te.StartTime AS event_time,
			CASE te.EventClass WHEN 92 THEN 'DATA_FILE' WHEN 93 THEN 'LOG_FILE' END AS file_type,
			te.FileName AS logical_name,
			CONVERT(decimal(18,2), te.IntegerData * 8.0 / 1024.0) AS growth_mb,
			te.Duration / 1000 AS duration_ms,
			CAST(NULL AS nvarchar(200)) AS message
			FROM sys.fn_trace_gettable(@trace_path, DEFAULT) AS te
			WHERE te.EventClass IN (92,93) AND te.DatabaseName = DB_NAME()
			ORDER BY te.StartTime DESC
		)
		INSERT INTO #DTR_AutogrowthEvents (
			RowNumber,
			event_time,
			file_type,
			logical_name,
			growth_mb,
			duration_ms,
			message
		)
		SELECT
		ROW_NUMBER() OVER (ORDER BY x.event_time DESC) AS RowNumber,
		x.event_time,
		x.file_type,
		x.logical_name,
		x.growth_mb,
		x.duration_ms,
		x.message
		FROM x;
	END TRY
	BEGIN CATCH
	END CATCH
END
GO

------------------------------------------------------------
-- 02d. Log Space Usage Snapshot
------------------------------------------------------------
PRINT N'▶ 02d. Log Space Usage Snapshot - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108)
GO
IF OBJECT_ID('tempdb..#DTR_LogSpaceUsage') IS NOT NULL DROP TABLE #DTR_LogSpaceUsage
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	CREATE TABLE #DTR_LogSpaceUsage_src (
		[Database Name] sysname NOT NULL,
		[Log Size (MB)] float NOT NULL,
		[Log Space Used (%)] float NOT NULL,
		[Status] int NOT NULL
	);

	INSERT INTO #DTR_LogSpaceUsage_src
	EXEC('DBCC SQLPERF(LOGSPACE) WITH NO_INFOMSGS');

	SELECT
	IDENTITY(int) AS RowNumber,
	[Database Name] AS database_name,
	CAST([Log Size (MB)] AS decimal(19,3)) AS total_log_size_mb,
	CAST(([Log Size (MB)] * ([Log Space Used (%)] / 100.0)) AS decimal(19,3)) AS used_log_space_mb,
	CAST([Log Space Used (%)] AS decimal(9,3)) AS used_log_space_percent,
	[Status] AS status
	INTO #DTR_LogSpaceUsage
	FROM #DTR_LogSpaceUsage_src
	WHERE [Database Name] = DB_NAME();

	DROP TABLE #DTR_LogSpaceUsage_src;
END
GO

------------------------------------------------------------
-- 02e. Pending I/O Requests (Current DB)
------------------------------------------------------------
PRINT N'▶ 02e. Pending I/O Requests (Current DB) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108)
GO
IF OBJECT_ID('tempdb..#DTR_PendingIO') IS NOT NULL DROP TABLE #DTR_PendingIO
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	DB_NAME(vfs.database_id) AS database_name,
	df.type_desc,
	df.name AS file_name,
	CASE WHEN cfg.SafeMode = 0 THEN df.physical_name ELSE '[SafeMode]' END AS physical_name,
	pir.io_type,
	pir.io_pending,
	pir.io_pending_ms_ticks,
	(osi.ms_ticks - pir.io_pending_ms_ticks) AS wait_ms,
	pir.io_offset,
	vfs.database_id,
	vfs.file_id
	INTO #DTR_PendingIO
	FROM sys.dm_io_pending_io_requests AS pir
	CROSS JOIN sys.dm_os_sys_info AS osi
	LEFT JOIN sys.dm_io_virtual_file_stats(DB_ID(), NULL) AS vfs ON vfs.file_handle = pir.io_handle
	LEFT JOIN sys.database_files AS df ON df.file_id = vfs.file_id
	CROSS JOIN #DT_Config AS cfg
	WHERE vfs.database_id = DB_ID()
END
GO

------------------------------------------------------------
-- 02f. Volume Stats for Database Files
------------------------------------------------------------
PRINT N'▶ 02f. Volume Stats for Database Files - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108)
GO
IF OBJECT_ID('tempdb..#DTR_VolumeStats') IS NOT NULL DROP TABLE #DTR_VolumeStats
GO

-- Volume/mount information for current DB files
IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	CREATE TABLE #DTR_VolumeStats_src (
		drive char(1) NOT NULL,
		free_mb int NOT NULL
	);

	INSERT INTO #DTR_VolumeStats_src
	EXEC master..xp_fixeddrives;

	SELECT
	IDENTITY(int) AS RowNumber,
	mf.file_id,
	mf.type_desc,
	mf.name AS file_name,
	CASE WHEN cfg.SafeMode = 0 THEN mf.physical_name ELSE '[SafeMode]' END AS physical_name,
	UPPER(LEFT(mf.physical_name, 1)) AS drive_letter,
	v.free_mb AS drive_free_mb
	INTO #DTR_VolumeStats
	FROM sys.master_files AS mf
	CROSS JOIN #DT_Config AS cfg
	LEFT JOIN #DTR_VolumeStats_src AS v ON v.drive = UPPER(LEFT(mf.physical_name, 1))
	WHERE mf.database_id = DB_ID()
	ORDER BY mf.file_id;

	DROP TABLE #DTR_VolumeStats_src;
END
GO

------------------------------------------------------------
-- 03a. Object Sizes & Rowcounts
------------------------------------------------------------
PRINT N'▶ 03a. Object Sizes & Rowcounts - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_ObjectSizes') IS NOT NULL DROP TABLE #DTR_ObjectSizes
GO

SELECT
IDENTITY(int) AS RowNumber,
QUOTENAME(OBJECT_SCHEMA_NAME(ps.object_id)) + '.' + QUOTENAME(OBJECT_NAME(ps.object_id)) AS object_name,
o.type_desc,
MAX(CASE WHEN ps.index_id IN (0,1) THEN ps.row_count ELSE 0 END) AS total_rows,
CAST(SUM(ps.reserved_page_count) * 8.0 / 1024 AS decimal(18,2)) AS total_size_mb,
CAST(SUM(ps.used_page_count) * 8.0 / 1024 AS decimal(18,2)) AS used_size_mb,
CAST(SUM(CASE WHEN ps.index_id IN (0,1) THEN ps.used_page_count ELSE 0 END) * 8.0 / 1024 AS decimal(18,2)) AS data_size_mb,
CAST(SUM(ps.in_row_used_page_count) * 8.0 / 1024 AS decimal(18,2)) AS in_row_used_mb,
CAST(SUM(ps.lob_used_page_count) * 8.0 / 1024 AS decimal(18,2)) AS lob_used_mb,
CAST(SUM(ps.row_overflow_used_page_count) * 8.0 / 1024 AS decimal(18,2)) AS row_overflow_used_mb
INTO #DTR_ObjectSizes
FROM sys.dm_db_partition_stats AS ps
JOIN sys.objects AS o ON ps.object_id = o.object_id
WHERE OBJECTPROPERTY(o.object_id, 'IsMsShipped') = 0
	AND o.type IN ('U','V')
GROUP BY ps.object_id, o.type_desc
ORDER BY total_size_mb DESC
OPTION (RECOMPILE)
GO

------------------------------------------------------------
-- 04a. Index Usage
------------------------------------------------------------
PRINT N'▶ 04a. Index Usage - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_IndexUsage') IS NOT NULL DROP TABLE #DTR_IndexUsage
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	QUOTENAME(OBJECT_SCHEMA_NAME(i.object_id)) + '.' + QUOTENAME(OBJECT_NAME(i.object_id)) AS object_name,
	i.name AS index_name,
	i.index_id,
	i.type_desc,
	i.is_primary_key,
	i.is_unique,
	ISNULL(us.user_seeks, 0) AS user_seeks,
	ISNULL(us.user_scans, 0) AS user_scans,
	ISNULL(us.user_lookups, 0) AS user_lookups,
	ISNULL(us.user_updates, 0) AS user_updates,
	ISNULL(us.system_seeks, 0) AS system_seeks,
	ISNULL(us.system_scans, 0) AS system_scans,
	ISNULL(us.system_lookups, 0) AS system_lookups,
	ISNULL(us.system_updates, 0) AS system_updates,
	us.last_user_seek AS last_seek,
	us.last_user_scan AS last_scan,
	us.last_user_lookup AS last_lookup,
	us.last_user_update AS last_update,
	us.last_system_seek AS last_system_seek,
	us.last_system_scan AS last_system_scan,
	us.last_system_lookup AS last_system_lookup,
	us.last_system_update AS last_system_update,
	(ISNULL(us.user_seeks, 0) + ISNULL(us.user_scans, 0) + ISNULL(us.user_lookups, 0)) AS total_reads,
	(ISNULL(us.user_seeks, 0) + ISNULL(us.user_scans, 0) + ISNULL(us.user_lookups, 0) + ISNULL(us.user_updates, 0)) AS total_activity,
	CASE
		WHEN (ISNULL(us.user_seeks, 0) + ISNULL(us.user_scans, 0) + ISNULL(us.user_lookups, 0) + ISNULL(us.user_updates, 0)) > 0
			THEN CAST((ISNULL(us.user_seeks, 0) + ISNULL(us.user_scans, 0) + ISNULL(us.user_lookups, 0)) AS decimal(18,2)) * 100
				/ CAST((ISNULL(us.user_seeks, 0) + ISNULL(us.user_scans, 0) + ISNULL(us.user_lookups, 0) + ISNULL(us.user_updates, 0)) AS decimal(18,2))
	END AS read_percent,
	CASE
		WHEN (ISNULL(us.user_seeks, 0) + ISNULL(us.user_scans, 0) + ISNULL(us.user_lookups, 0) + ISNULL(us.user_updates, 0)) > 0
			THEN CAST(ISNULL(us.user_updates, 0) AS decimal(18,2)) * 100
				/ CAST((ISNULL(us.user_seeks, 0) + ISNULL(us.user_scans, 0) + ISNULL(us.user_lookups, 0) + ISNULL(us.user_updates, 0)) AS decimal(18,2))
	END AS write_percent,
	ISNULL(ps.row_count, 0) AS row_count
	INTO #DTR_IndexUsage
	FROM sys.indexes AS i
	LEFT JOIN sys.dm_db_index_usage_stats AS us ON us.object_id = i.object_id AND us.index_id = i.index_id AND us.database_id = DB_ID()
	OUTER APPLY
	(
		SELECT
		SUM(row_count) AS row_count
		FROM sys.dm_db_partition_stats AS ps
		WHERE ps.object_id = i.object_id
			AND ps.index_id = i.index_id
	) AS ps
	WHERE OBJECTPROPERTY(i.object_id, 'IsUserTable') = 1
	ORDER BY total_activity DESC
	OPTION (RECOMPILE)
END
GO

------------------------------------------------------------
-- 04b. Index Fragmentation
------------------------------------------------------------
PRINT N'▶ 04b. Index Fragmentation - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_IndexFragmentation') IS NOT NULL DROP TABLE #DTR_IndexFragmentation
GO

SELECT
IDENTITY(int) AS RowNumber,
QUOTENAME(OBJECT_SCHEMA_NAME(i.object_id)) + '.' + QUOTENAME(OBJECT_NAME(i.object_id)) AS table_name,
i.name AS index_name,
i.index_id,
ips.avg_fragmentation_in_percent AS avg_fragmentation_percent,
ips.fragment_count AS fragment_count,
ips.page_count AS page_count,
i.type_desc AS index_type_desc
INTO #DTR_IndexFragmentation
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') AS ips
JOIN sys.indexes AS i ON i.object_id = ips.object_id AND i.index_id = ips.index_id
WHERE OBJECTPROPERTY(i.object_id, 'IsUserTable') = 1
ORDER BY avg_fragmentation_percent DESC
OPTION (MAXDOP 1, RECOMPILE);
GO

------------------------------------------------------------
-- 04c. Unused Indexes
------------------------------------------------------------
PRINT N'▶ 04c. Unused Indexes - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_UnusedIndexes') IS NOT NULL DROP TABLE #DTR_UnusedIndexes
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH ips AS (
		SELECT
		object_id,
		index_id,
		SUM(page_count) AS index_page_count
		FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED')
		GROUP BY object_id, index_id
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	QUOTENAME(OBJECT_SCHEMA_NAME(i.object_id)) + '.' + QUOTENAME(OBJECT_NAME(i.object_id)) AS table_name,
	i.name AS index_name,
	i.index_id,
	i.is_primary_key,
	i.is_unique,
	i.is_disabled,
	ISNULL(us.user_updates, 0) AS user_updates,
	(ISNULL(us.user_seeks,0) + ISNULL(us.user_scans,0) + ISNULL(us.user_lookups,0)) AS total_reads,
	ISNULL(ips.index_page_count, 0) AS index_page_count
	INTO #DTR_UnusedIndexes
	FROM sys.indexes AS i
	LEFT JOIN sys.dm_db_index_usage_stats AS us ON us.object_id = i.object_id AND us.index_id = i.index_id AND us.database_id = DB_ID()
	LEFT JOIN ips ON ips.object_id = i.object_id AND ips.index_id = i.index_id
	WHERE OBJECTPROPERTY(i.object_id, 'IsUserTable') = 1
		AND i.index_id > 0
		AND (ISNULL(us.user_seeks,0) + ISNULL(us.user_scans,0) + ISNULL(us.user_lookups,0)) = 0
	ORDER BY user_updates DESC;
END
GO

------------------------------------------------------------
-- 04d. Disabled Indexes
------------------------------------------------------------
PRINT N'▶ 04d. Disabled Indexes - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_DisabledIndexes') IS NOT NULL DROP TABLE #DTR_DisabledIndexes
GO

SELECT
IDENTITY(int) AS RowNumber,
QUOTENAME(OBJECT_SCHEMA_NAME(i.object_id)) + '.' + QUOTENAME(OBJECT_NAME(i.object_id)) AS table_name,
i.name AS index_name,
i.index_id,
i.is_primary_key,
i.is_unique,
o.modify_date AS object_modify_date,
CASE WHEN cfg.SafeMode = 0 THEN i.filter_definition ELSE '[SafeMode]' END AS filter_definition,
i.type_desc AS index_type_desc
INTO #DTR_DisabledIndexes
FROM sys.indexes AS i
JOIN sys.objects AS o ON o.object_id = i.object_id
CROSS JOIN #DT_Config AS cfg
WHERE OBJECTPROPERTY(i.object_id, 'IsUserTable') = 1
	AND i.index_id > 0
	AND i.is_disabled = 1
ORDER BY table_name, index_name;
GO

------------------------------------------------------------
-- 05a. Statistics Staleness
------------------------------------------------------------
PRINT N'▶ 05a. Statistics Staleness - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_StatisticsStaleness') IS NOT NULL DROP TABLE #DTR_StatisticsStaleness
GO

SELECT
IDENTITY(int) AS RowNumber,
QUOTENAME(OBJECT_SCHEMA_NAME(s.object_id)) + '.' + QUOTENAME(OBJECT_NAME(s.object_id)) AS table_name,
s.name AS stat_name,
STATS_DATE(s.object_id, s.stats_id) AS last_updated,
si.rowcnt AS rows,
CAST(NULL AS bigint) AS rows_sampled,
si.rowmodctr AS modification_counter,
CASE
	WHEN si.rowcnt IS NULL THEN NULL
	WHEN si.rowcnt <= 500 THEN 500
	ELSE 500 + (si.rowcnt * 0.20)
END AS approx_autoupdate_threshold,
CASE
	WHEN si.rowmodctr >=
		CASE
			WHEN si.rowcnt IS NULL THEN 500
			WHEN si.rowcnt <= 500 THEN 500
			ELSE 500 + (si.rowcnt * 0.20)
		END THEN 1 ELSE 0
END AS needs_update_heuristic,
s.auto_created AS is_auto_created
INTO #DTR_StatisticsStaleness
FROM sys.stats AS s
JOIN sys.sysindexes AS si ON si.id = s.object_id AND si.name = s.name
WHERE OBJECTPROPERTY(s.object_id, 'IsUserTable') = 1
ORDER BY needs_update_heuristic DESC, si.rowmodctr DESC;
GO

------------------------------------------------------------
-- 05b. Statistics with NORECOMPUTE (Auto Update OFF)
------------------------------------------------------------
PRINT N'▶ 05b. Statistics with NORECOMPUTE (Auto Update OFF) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_StatsNoRecompute') IS NOT NULL DROP TABLE #DTR_StatsNoRecompute
GO

	SELECT
	IDENTITY(int) AS RowNumber,
	SCHEMA_NAME(o.schema_id) AS [Schema],
	o.name AS [Table],
	s.name AS [Statistic],
	s.auto_created,
	s.user_created,
	s.no_recompute,
	STATS_DATE(s.object_id, s.stats_id) AS last_updated,
	si.rowmodctr AS modification_counter
	INTO #DTR_StatsNoRecompute
	FROM sys.stats AS s
	JOIN sys.objects AS o ON o.object_id = s.object_id
	JOIN sys.sysindexes AS si ON si.id = s.object_id AND si.name = s.name
	WHERE o.is_ms_shipped = 0
	AND s.no_recompute = 1;
GO

------------------------------------------------------------
-- 05c. Filtered Statistics (Definitions + Recency)
------------------------------------------------------------
PRINT N'▶ 05c. Filtered Statistics (Definitions + Recency) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_FilteredStats') IS NOT NULL DROP TABLE #DTR_FilteredStats
GO

	SELECT
	IDENTITY(int) AS RowNumber,
	SCHEMA_NAME(o.schema_id) AS [Schema],
	o.name AS [Table],
	s.name AS [Statistic],
	s.auto_created,
	s.user_created,
	s.no_recompute,
	s.has_filter,
	CASE WHEN cfg.SafeMode = 0 THEN s.filter_definition ELSE '[SafeMode]' END AS filter_definition,
	STATS_DATE(s.object_id, s.stats_id) AS last_updated,
	si.rowmodctr AS modification_counter
	INTO #DTR_FilteredStats
	FROM sys.stats AS s
	JOIN sys.objects AS o ON o.object_id = s.object_id
	JOIN sys.sysindexes AS si ON si.id = s.object_id AND si.name = s.name
	CROSS JOIN #DT_Config AS cfg
	WHERE o.is_ms_shipped = 0
	AND s.has_filter = 1;
GO

------------------------------------------------------------
-- 05d. Predicate Columns Without Standalone Stats (Index Non-Leading/Included)
------------------------------------------------------------
PRINT N'▶ 05d. Predicate Columns Without Standalone Stats (Index Non-Leading/Included) - ' + CONVERT(char(8), GETDATE(), 108);
GO
IF OBJECT_ID('tempdb..#DTR_NoStandaloneStats') IS NOT NULL DROP TABLE #DTR_NoStandaloneStats;
GO

	;WITH idxcols AS (
	SELECT
	ic.object_id,
	ic.index_id,
	ic.column_id,
	ic.key_ordinal,
	ic.is_included_column,
	QUOTENAME(OBJECT_SCHEMA_NAME(ic.object_id)) + '.' + QUOTENAME(OBJECT_NAME(ic.object_id)) AS table_name,
	i.name AS index_name
	FROM sys.index_columns AS ic
	JOIN sys.indexes AS i ON i.object_id = ic.object_id AND i.index_id = ic.index_id
	JOIN sys.objects AS o ON o.object_id = ic.object_id
	WHERE o.type = 'U' AND i.is_hypothetical = 0
		AND (ic.key_ordinal > 1 OR ic.is_included_column = 1) -- non-leading or included
),
cols AS (
	SELECT
	c.object_id,
	c.column_id,
	c.name AS column_name,
	t.name AS data_type
	FROM sys.columns AS c
	JOIN sys.types AS t ON t.user_type_id = c.user_type_id
),
standalone AS (
	-- stats whose leading column is this column (counts as standalone stats)
	SELECT
	sc.object_id,
	sc.column_id,
	COUNT(*) AS leading_stats_count,
	MAX(CASE WHEN s.auto_created = 1 THEN 1 ELSE 0 END) AS has_auto_stats,
	MAX(CASE WHEN s.user_created = 1 THEN 1 ELSE 0 END) AS has_user_stats
	FROM sys.stats_columns AS sc
	JOIN sys.stats AS s ON s.object_id = sc.object_id AND s.stats_id = sc.stats_id
	WHERE sc.stats_column_id = 1
	GROUP BY sc.object_id, sc.column_id
),
refs AS (
	SELECT
	i.table_name,
	i.index_name,
	i.object_id,
	i.column_id,
	MAX(CASE WHEN i.key_ordinal > 1 THEN 1 ELSE 0 END) AS is_nonleading_key,
	MAX(CASE WHEN i.is_included_column = 1 THEN 1 ELSE 0 END) AS is_included
	FROM idxcols AS i
	GROUP BY i.table_name, i.index_name, i.object_id, i.column_id
)
	SELECT
	IDENTITY(int) AS RowNumber,
	r.table_name,
	c.column_name,
	c.data_type,
	MAX(r.is_nonleading_key) AS is_nonleading_key,
	MAX(r.is_included) AS is_included,
	ISNULL(sa.leading_stats_count, 0) AS standalone_stats_count,
	ISNULL(sa.has_auto_stats, 0) AS has_auto_stats,
	ISNULL(sa.has_user_stats, 0) AS has_user_stats,
	CASE
		WHEN ISNULL(sa.leading_stats_count, 0) = 0 THEN
			'CREATE STATISTICS ' + QUOTENAME('dtr_' + PARSENAME(r.table_name, 1) + '_' + c.column_name)
			+ ' ON ' + r.table_name + '(' + QUOTENAME(c.column_name) + ');'
		ELSE NULL
	END AS suggested_create_stats
	INTO #DTR_NoStandaloneStats
	FROM refs AS r
	JOIN cols AS c ON c.object_id = r.object_id AND c.column_id = r.column_id
	LEFT JOIN standalone AS sa ON sa.object_id = r.object_id AND sa.column_id = r.column_id
	GROUP BY r.table_name, c.column_name, c.data_type, sa.leading_stats_count, sa.has_auto_stats, sa.has_user_stats
	ORDER BY r.table_name, c.column_name;
GO

------------------------------------------------------------
-- 06a. Missing Indexes
------------------------------------------------------------
PRINT N'▶ 06a. Missing Indexes - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_MissingIndexes') IS NOT NULL DROP TABLE #DTR_MissingIndexes
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	QUOTENAME(OBJECT_SCHEMA_NAME(mid.object_id, mid.database_id)) + '.' + QUOTENAME(OBJECT_NAME(mid.object_id, mid.database_id)) AS table_name,
	migs.user_seeks,
	migs.user_scans,
	migs.avg_total_user_cost,
	migs.avg_user_impact,
	migs.last_user_seek,
	migs.last_user_scan,
	mid.equality_columns,
	mid.inequality_columns,
	mid.included_columns,
	'CREATE INDEX [IX_' + OBJECT_NAME(mid.object_id, mid.database_id) + '_' +
	REPLACE(REPLACE(REPLACE(ISNULL(mid.equality_columns, ''), ', ', '_'), '[', ''), ']', '') +
	CASE WHEN mid.inequality_columns IS NOT NULL THEN '_' +
	REPLACE(REPLACE(REPLACE(mid.inequality_columns, ', ', '_'), '[', ''), ']', '') ELSE '' END + ']' +
	' ON ' + mid.statement + ' (' + ISNULL(mid.equality_columns, '') +
	CASE WHEN mid.inequality_columns IS NOT NULL THEN ',' + mid.inequality_columns ELSE '' END + ')' +
	ISNULL(' INCLUDE (' + mid.included_columns + ')', '') AS create_index_statement
	INTO #DTR_MissingIndexes
	FROM sys.dm_db_missing_index_group_stats AS migs
	JOIN sys.dm_db_missing_index_groups AS mig ON migs.group_handle = mig.index_group_handle
	JOIN sys.dm_db_missing_index_details AS mid ON mig.index_handle = mid.index_handle
	WHERE mid.database_id = DB_ID()
	ORDER BY migs.user_seeks DESC, migs.avg_user_impact DESC
	OPTION (RECOMPILE)
END
GO

------------------------------------------------------------
-- 06b. Missing Index Suggestions (Dedup & Combined Impact)
------------------------------------------------------------
PRINT N'▶ 06b. Missing Index Suggestions (Dedup & Combined Impact) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_MissingIndexDedup') IS NOT NULL DROP TABLE #DTR_MissingIndexDedup
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH mi AS (
		SELECT
		mid.object_id,
		mid.equality_columns,
		mid.inequality_columns,
		mid.included_columns,
		gs.avg_total_user_cost,
		gs.avg_user_impact,
		gs.user_seeks,
		gs.user_scans
		FROM sys.dm_db_missing_index_groups AS g
		JOIN sys.dm_db_missing_index_group_stats AS gs ON gs.group_handle = g.index_group_handle
		JOIN sys.dm_db_missing_index_details AS mid ON mid.index_handle = g.index_handle
		WHERE mid.database_id = DB_ID()
	),
	agg AS (
		SELECT
		SCHEMA_NAME(o.schema_id) + '.' + o.name AS TableName,
		mi.equality_columns,
		mi.inequality_columns,
		mi.included_columns,
		SUM(mi.user_seeks + mi.user_scans) AS total_seeks_scans,
		SUM(CONVERT(decimal(19,4), mi.avg_total_user_cost) * mi.avg_user_impact * (mi.user_seeks + mi.user_scans)) AS cumulative_improvement
		FROM mi
		JOIN sys.objects AS o ON o.object_id = mi.object_id
		WHERE o.is_ms_shipped = 0
		GROUP BY SCHEMA_NAME(o.schema_id) + '.' + o.name, mi.equality_columns, mi.inequality_columns, mi.included_columns
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	TableName,
	equality_columns,
	inequality_columns,
	included_columns,
	total_seeks_scans,
	CONVERT(decimal(19,2), cumulative_improvement) AS imputed_benefit_score
	INTO #DTR_MissingIndexDedup
	FROM agg;
END
GO

------------------------------------------------------------
-- 07a. Plan Cache Hotspots & Bloat
------------------------------------------------------------
PRINT N'▶ 07a. Plan Cache Hotspots & Bloat - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_PlanCacheBloat') IS NOT NULL DROP TABLE #DTR_PlanCacheBloat
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	cp.objtype,
	cp.cacheobjtype,
	COUNT(*) AS total_plans,
	SUM(CASE WHEN cp.usecounts = 1 THEN 1 ELSE 0 END) AS single_use_plans,
	CAST(100.0 * SUM(CASE WHEN cp.usecounts = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS decimal(5,2)) AS pct_single_use,
	SUM(cp.size_in_bytes) / 1024.0 AS total_size_kb,
	SUM(CASE WHEN cp.usecounts = 1 THEN cp.size_in_bytes ELSE 0 END) / 1024.0 AS single_use_size_kb
	INTO #DTR_PlanCacheBloat
	FROM sys.dm_exec_cached_plans AS cp
	OUTER APPLY sys.dm_exec_sql_text(cp.plan_handle) AS st
	WHERE st.dbid = DB_ID()
	GROUP BY cp.objtype, cp.cacheobjtype
	ORDER BY pct_single_use DESC;
END
GO

------------------------------------------------------------
-- 07b. Multi-Plan by Query Hash
------------------------------------------------------------
PRINT N'▶ 07b. Multi-Plan by Query Hash - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_MultiPlanByQueryHash') IS NOT NULL DROP TABLE #DTR_MultiPlanByQueryHash
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT TOP (200)
	IDENTITY(int) AS RowNumber,
	st.dbid,
	qs.query_hash,
	COUNT(DISTINCT qs.query_plan_hash) AS distinct_plans,
	SUM(qs.execution_count) AS total_execs,
	SUM(qs.total_worker_time) AS total_worker_time,
	SUM(qs.total_logical_reads) AS total_logical_reads
	INTO #DTR_MultiPlanByQueryHash
	FROM sys.dm_exec_query_stats AS qs
	OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
	WHERE st.dbid = DB_ID()
	GROUP BY st.dbid, qs.query_hash
	HAVING COUNT(DISTINCT qs.query_plan_hash) > 1
	ORDER BY distinct_plans DESC, total_execs DESC;
END
GO

------------------------------------------------------------
-- 07c. Plan Cache - Trivial & Early-Abort Summary
------------------------------------------------------------
PRINT N'▶ 07c. Plan Cache - Trivial & Early-Abort Summary - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_CacheTrivialEarlyAbort') IS NOT NULL DROP TABLE #DTR_CacheTrivialEarlyAbort
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	DECLARE @TopPlans int = 300;

	;WITH TopQs AS (
		SELECT TOP (@TopPlans)
		qs.plan_handle,
		qs.sql_handle,
		qs.total_worker_time
		FROM sys.dm_exec_query_stats AS qs
		OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
		WHERE st.dbid = DB_ID()
		ORDER BY qs.total_worker_time DESC
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	CONVERT(varbinary(64), t.plan_handle) AS plan_handle,
	t.total_worker_time / 1000 AS total_cpu_ms,
	X.trivial_stmt_count,
	X.early_abort_count,
	X.total_stmt_count
	INTO #DTR_CacheTrivialEarlyAbort
	FROM TopQs AS t
	CROSS APPLY sys.dm_exec_query_plan(t.plan_handle) AS qp
	CROSS APPLY (
		SELECT
		qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan";
								count(//StmtSimple[@StatementOptmLevel="TRIVIAL"])','int') AS trivial_stmt_count,
		qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan";
								count(//StmtSimple[@StatementOptmEarlyAbortReason="GoodEnoughPlanFound"])','int') AS early_abort_count,
		qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan";
								count(//StmtSimple)','int') AS total_stmt_count
	) AS X;
END
GO

------------------------------------------------------------
------------------------------------------------------------
-- 07d. Active Cursor Usage (by Session)
------------------------------------------------------------
PRINT N'▶ 07d. Active Cursor Usage (by Session) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_ActiveCursors') IS NOT NULL DROP TABLE #DTR_ActiveCursors;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH c AS
	(
		SELECT
		session_id,
		cursor_id,
		name,
		properties,
		creation_time,
		is_open,
		fetch_status,
		worker_time,
		reads,
		writes,
		plan_generation_num,
		statement_start_offset,
		statement_end_offset,
		sql_handle
		FROM sys.dm_exec_cursors(0)
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	c.session_id,
	c.cursor_id,
	c.name AS cursor_name,
	c.properties,
	c.creation_time,
	c.is_open,
	c.fetch_status,
	c.worker_time,
	c.reads,
	c.writes,
	c.plan_generation_num,
	c.statement_start_offset,
	c.statement_end_offset,
	CASE WHEN cfg.SafeMode = 0 THEN t.text ELSE '[SafeMode]' END AS sql_text,
	CASE WHEN cfg.SafeMode = 0 THEN s.login_name ELSE '[SafeMode]' END AS login_name,
	CASE WHEN cfg.SafeMode = 0 THEN s.host_name ELSE '[SafeMode]' END AS host_name,
	s.program_name
	INTO #DTR_ActiveCursors
	FROM c
	OUTER APPLY sys.dm_exec_sql_text(c.sql_handle) AS t
	LEFT JOIN sys.dm_exec_sessions AS s ON s.session_id = c.session_id
	CROSS JOIN #DT_Config AS cfg
	WHERE t.dbid = DB_ID() AND s.is_user_process = 1
	ORDER BY c.worker_time DESC, c.reads DESC;
END
GO

------------------------------------------------------------
-- 08a. Top Queries - CPU
------------------------------------------------------------
PRINT N'▶ 08a. Top Queries - CPU - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_TopQueries_CPU') IS NOT NULL DROP TABLE #DTR_TopQueries_CPU
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT TOP (50)
	IDENTITY(int) AS RowNumber,
	qs.execution_count,
	qs.total_worker_time / 1000 AS total_cpu_ms,
	(qs.total_worker_time / 1000) / NULLIF(qs.execution_count, 0) AS avg_cpu_ms,
	qs.total_elapsed_time / 1000 AS total_duration_ms,
	(qs.total_elapsed_time / 1000) / NULLIF(qs.execution_count, 0) AS avg_duration_ms,
	qs.total_logical_reads AS total_reads,
	qs.total_logical_writes AS total_writes,
	qs.last_execution_time,
	CONVERT(varchar(34), qs.query_hash, 1) AS query_hash_hex,
	OBJECT_SCHEMA_NAME(st.objectid, st.dbid) + '.' + OBJECT_NAME(st.objectid, st.dbid) AS object_name,
	CASE WHEN cfg.SafeMode = 0 THEN LEFT(st.text, 4000) ELSE '[SafeMode]' END AS sql_text
	INTO #DTR_TopQueries_CPU
	FROM sys.dm_exec_query_stats AS qs
	OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
	CROSS JOIN #DT_Config AS cfg
	WHERE st.dbid = DB_ID()
	ORDER BY qs.total_worker_time DESC;
END
GO

------------------------------------------------------------
-- 08b. Top Queries - Reads
------------------------------------------------------------
PRINT N'▶ 08b. Top Queries - Reads - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_TopQueries_Reads') IS NOT NULL DROP TABLE #DTR_TopQueries_Reads
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT TOP (50)
	IDENTITY(int) AS RowNumber,
	qs.execution_count,
	qs.total_logical_reads AS total_reads,
	(qs.total_logical_reads) / NULLIF(qs.execution_count, 0) AS avg_reads,
	qs.total_logical_writes AS total_writes,
	qs.total_worker_time / 1000 AS total_cpu_ms,
	qs.total_elapsed_time / 1000 AS total_duration_ms,
	qs.last_execution_time,
	CONVERT(varchar(34), qs.query_hash, 1) AS query_hash_hex,
	OBJECT_SCHEMA_NAME(st.objectid, st.dbid) + '.' + OBJECT_NAME(st.objectid, st.dbid) AS object_name,
	CASE WHEN cfg.SafeMode = 0 THEN LEFT(st.text, 4000) ELSE '[SafeMode]' END AS sql_text
	INTO #DTR_TopQueries_Reads
	FROM sys.dm_exec_query_stats AS qs
	OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
	CROSS JOIN #DT_Config AS cfg
	WHERE st.dbid = DB_ID()
	ORDER BY qs.total_logical_reads DESC;
END
GO

------------------------------------------------------------
-- 08c. Top Queries - Duration
------------------------------------------------------------
PRINT N'▶ 08c. Top Queries - Duration - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_TopQueries_Duration') IS NOT NULL DROP TABLE #DTR_TopQueries_Duration
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT TOP (50)
	IDENTITY(int) AS RowNumber,
	qs.execution_count,
	qs.total_elapsed_time / 1000 AS total_duration_ms,
	(qs.total_elapsed_time / 1000) / NULLIF(qs.execution_count, 0) AS avg_duration_ms,
	qs.total_worker_time / 1000 AS total_cpu_ms,
	qs.total_logical_reads AS total_reads,
	qs.total_logical_writes AS total_writes,
	qs.last_execution_time,
	CONVERT(varchar(34), qs.query_hash, 1) AS query_hash_hex,
	OBJECT_SCHEMA_NAME(st.objectid, st.dbid) + '.' + OBJECT_NAME(st.objectid, st.dbid) AS object_name,
	CASE WHEN cfg.SafeMode = 0 THEN LEFT(st.text, 4000) ELSE '[SafeMode]' END AS sql_text
	INTO #DTR_TopQueries_Duration
	FROM sys.dm_exec_query_stats AS qs
	OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
	CROSS JOIN #DT_Config AS cfg
	WHERE st.dbid = DB_ID()
	ORDER BY qs.total_elapsed_time DESC;
END
GO

------------------------------------------------------------
-- 08d. Top Queries - Writes
------------------------------------------------------------
PRINT N'▶ 08d. Top Queries - Writes - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_TopQueries_Writes') IS NOT NULL DROP TABLE #DTR_TopQueries_Writes
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT TOP (50)
	IDENTITY(int) AS RowNumber,
	qs.execution_count,
	qs.total_logical_writes AS total_writes,
	(qs.total_logical_writes) / NULLIF(qs.execution_count, 0) AS avg_writes,
	qs.total_logical_reads AS total_reads,
	qs.total_worker_time / 1000 AS total_cpu_ms,
	qs.total_elapsed_time / 1000 AS total_duration_ms,
	qs.last_execution_time,
	CONVERT(varchar(34), qs.query_hash, 1) AS query_hash_hex,
	OBJECT_SCHEMA_NAME(st.objectid, st.dbid) + '.' + OBJECT_NAME(st.objectid, st.dbid) AS object_name,
	CASE WHEN cfg.SafeMode = 0 THEN LEFT(st.text, 4000) ELSE '[SafeMode]' END AS sql_text
	INTO #DTR_TopQueries_Writes
	FROM sys.dm_exec_query_stats AS qs
	OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
	CROSS JOIN #DT_Config AS cfg
	WHERE st.dbid = DB_ID()
	ORDER BY qs.total_logical_writes DESC;
END
GO

------------------------------------------------------------
-- 08e. Parameter-Sensitivity Candidates (Multi-Plan by query_hash + Spread)
------------------------------------------------------------
PRINT N'▶ 08e. Parameter-Sensitivity Candidates (Multi-Plan by query_hash + Spread) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_ParamSensitivity') IS NOT NULL DROP TABLE #DTR_ParamSensitivity
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH q AS (
		SELECT
		qs.query_hash,
		COUNT(DISTINCT qs.query_plan_hash) AS plan_variants,
		MIN(qs.min_elapsed_time) AS min_elapsed_ns,
		MAX(qs.max_elapsed_time) AS max_elapsed_ns,
		SUM(qs.execution_count) AS total_execs
		FROM sys.dm_exec_query_stats AS qs
		OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
		WHERE st.dbid = DB_ID()
		GROUP BY qs.query_hash
		HAVING COUNT(DISTINCT qs.query_plan_hash) > 1
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	q.query_hash,
	q.plan_variants,
	q.total_execs,
	CONVERT(decimal(18,2), q.min_elapsed_ns / 1000.0) AS min_duration_ms,
	CONVERT(decimal(18,2), q.max_elapsed_ns / 1000.0) AS max_duration_ms,
	CONVERT(decimal(18,2), CASE WHEN q.min_elapsed_ns > 0 THEN (1.0*q.max_elapsed_ns/q.min_elapsed_ns) ELSE NULL END) AS elapsed_spread_ratio
	INTO #DTR_ParamSensitivity
	FROM q
	WHERE q.total_execs > 10;
END
GO

------------------------------------------------------------
-- 08f. Top Queries - Memory Grants (Cached Plans)
------------------------------------------------------------
PRINT N'▶ 08f. Top Queries - Memory Grants (Cached Plans) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_TopQueries_MemoryGrants') IS NOT NULL DROP TABLE #DTR_TopQueries_MemoryGrants
GO


IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH TopQs AS (
		SELECT TOP (300)
		qs.plan_handle,
		qs.sql_handle,
		qs.query_hash,
		qs.total_worker_time,
		qs.total_elapsed_time,
		qs.execution_count,
		qs.last_execution_time
		FROM sys.dm_exec_query_stats AS qs
		CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
		WHERE st.dbid = DB_ID()
		ORDER BY qs.total_worker_time DESC
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	CONVERT(varbinary(64), t.plan_handle) AS plan_handle,
	CONVERT(varchar(34), t.query_hash, 1) AS query_hash_hex,
	t.execution_count,
	t.total_worker_time / 1000 AS total_cpu_ms,
	(t.total_worker_time / 1000) / NULLIF(t.execution_count, 0) AS avg_cpu_ms,
	t.total_elapsed_time / 1000 AS total_duration_ms,
	(t.total_elapsed_time / 1000) / NULLIF(t.execution_count, 0) AS avg_duration_ms,
	mg.GrantedMemoryKb,
	mg.MaxUsedMemoryKb,
	mg.RequestedMemoryKb,
	mg.RequiredMemoryKb,
	mg.MemoryGrantWarningCount,
	t.last_execution_time,
	CASE WHEN cfg.SafeMode = 0 THEN LEFT(st.text, 4000) ELSE '[SafeMode]' END AS sql_text
	INTO #DTR_TopQueries_MemoryGrants
	FROM TopQs AS t
	CROSS APPLY sys.dm_exec_query_plan(t.plan_handle) AS p
	OUTER APPLY (
		SELECT
		p.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; (//QueryPlan/MemoryGrantInfo/@GrantedMemoryKb, //QueryPlan/MemoryGrantInfo/@GrantedMemory)[1]','int') AS GrantedMemoryKb,
		p.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; (//QueryPlan/MemoryGrantInfo/@MaxUsedMemoryKb, //QueryPlan/MemoryGrantInfo/@MaxUsedMemory)[1]','int') AS MaxUsedMemoryKb,
		p.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; (//QueryPlan/MemoryGrantInfo/@RequestedMemoryKb, //QueryPlan/MemoryGrantInfo/@RequestedMemory)[1]','int') AS RequestedMemoryKb,
		p.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; (//QueryPlan/MemoryGrantInfo/@RequiredMemoryKb, //QueryPlan/MemoryGrantInfo/@RequiredMemory, //QueryPlan/MemoryGrantInfo/@SerialRequiredMemory)[1]','int') AS RequiredMemoryKb,
		p.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//Warnings/MemoryGrantWarning)','int') AS MemoryGrantWarningCount
	) AS mg
	CROSS APPLY sys.dm_exec_sql_text(t.sql_handle) AS st
	CROSS JOIN #DT_Config AS cfg;
END
GO

------------------------------------------------------------
-- 08g. Operator Prevalence (Text Plans, Top CPU)
------------------------------------------------------------
PRINT N'▶ 08g. Operator Prevalence (Text Plans, Top CPU) - ' + CONVERT(char(8), GETDATE(), 108);
GO
IF OBJECT_ID('tempdb..#DTR_TextPlanOperators') IS NOT NULL DROP TABLE #DTR_TextPlanOperators;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH topq AS (
		SELECT TOP (50)
		qs.plan_handle,
		qs.sql_handle,
		qs.statement_start_offset,
		qs.statement_end_offset,
		qs.total_worker_time,
		qs.total_logical_reads,
		qs.execution_count,
		qs.query_hash
		FROM sys.dm_exec_query_stats AS qs
		CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
		WHERE st.dbid = DB_ID()
		ORDER BY qs.total_worker_time DESC
	),
	t AS (
		SELECT
		tp.query_plan AS plan_text,
		tq.plan_handle,
		tq.sql_handle,
		tq.statement_start_offset,
		tq.statement_end_offset,
		tq.total_worker_time,
		tq.total_logical_reads,
		tq.execution_count,
		tq.query_hash
		FROM topq AS tq
		CROSS APPLY sys.dm_exec_text_query_plan(tq.plan_handle, tq.statement_start_offset, tq.statement_end_offset) AS tp
	),
	per_plan AS (
		SELECT
		t.query_hash,
		t.total_worker_time,
		t.execution_count,
		t.total_logical_reads,
		(LEN(t.plan_text) - LEN(REPLACE(t.plan_text, 'Table Scan', ''))) / NULLIF(LEN('Table Scan'), 0) AS cnt_table_scan,
		(LEN(t.plan_text) - LEN(REPLACE(t.plan_text, 'Index Scan', ''))) / NULLIF(LEN('Index Scan'), 0) AS cnt_index_scan,
		(LEN(t.plan_text) - LEN(REPLACE(t.plan_text, 'Index Seek', ''))) / NULLIF(LEN('Index Seek'), 0) AS cnt_index_seek,
		(LEN(t.plan_text) - LEN(REPLACE(t.plan_text, 'Key Lookup', ''))) / NULLIF(LEN('Key Lookup'), 0) AS cnt_key_lookup,
		(LEN(t.plan_text) - LEN(REPLACE(t.plan_text, 'RID Lookup', ''))) / NULLIF(LEN('RID Lookup'), 0) AS cnt_rid_lookup,
		(LEN(t.plan_text) - LEN(REPLACE(t.plan_text, 'Hash Match', ''))) / NULLIF(LEN('Hash Match'), 0) AS cnt_hash_match,
		(LEN(t.plan_text) - LEN(REPLACE(t.plan_text, 'Sort', ''))) / NULLIF(LEN('Sort'), 0) AS cnt_sort,
		(LEN(t.plan_text) - LEN(REPLACE(t.plan_text, 'Spool', ''))) / NULLIF(LEN('Spool'), 0) AS cnt_spool,
		(LEN(t.plan_text) - LEN(REPLACE(t.plan_text, 'Nested Loops', ''))) / NULLIF(LEN('Nested Loops'), 0) AS cnt_nested_loops,
		(LEN(t.plan_text) - LEN(REPLACE(t.plan_text, 'Merge Join', ''))) / NULLIF(LEN('Merge Join'), 0) AS cnt_merge_join,
		(LEN(t.plan_text) - LEN(REPLACE(t.plan_text, 'Parallelism', ''))) / NULLIF(LEN('Parallelism'), 0) AS cnt_parallelism
		FROM t
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	op.operator,
	SUM(op.occurrences) AS occurrences,
	SUM(CASE WHEN op.occurrences > 0 THEN 1 ELSE 0 END) AS plans_with_operator,
	SUM(CASE WHEN op.occurrences > 0 THEN pp.total_worker_time ELSE 0 END) / 1000 AS total_cpu_ms_in_plans
	INTO #DTR_TextPlanOperators
	FROM per_plan AS pp
	CROSS APPLY (VALUES
	('Table Scan', pp.cnt_table_scan),
	('Index Scan', pp.cnt_index_scan),
	('Index Seek', pp.cnt_index_seek),
	('Key Lookup', pp.cnt_key_lookup + pp.cnt_rid_lookup),
	('Hash Match', pp.cnt_hash_match),
	('Sort', pp.cnt_sort),
	('Spool', pp.cnt_spool),
	('Nested Loops', pp.cnt_nested_loops),
	('Merge Join', pp.cnt_merge_join),
	('Parallelism', pp.cnt_parallelism)
	) AS op(operator, occurrences)
	GROUP BY op.operator
	ORDER BY occurrences DESC, total_cpu_ms_in_plans DESC;
END
GO

------------------------------------------------------------
-- 09a. Plan Warnings
------------------------------------------------------------
PRINT N'▶ 09a. Plan Warnings - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_PlanWarnings') IS NOT NULL DROP TABLE #DTR_PlanWarnings
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH TopPlans AS (
		SELECT TOP (500)
		qs.plan_handle,
		qs.sql_handle,
		qs.query_hash,
		qs.query_plan_hash,
		qs.total_worker_time,
		qs.total_logical_reads,
		qs.total_elapsed_time,
		qs.execution_count,
		qs.last_execution_time
		FROM sys.dm_exec_query_stats AS qs
		OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
		WHERE st.dbid = DB_ID()
		ORDER BY qs.total_worker_time DESC
	)
	SELECT TOP (200)
	IDENTITY(int) AS RowNumber,
	CONVERT(varchar(34), tp.query_hash, 1) AS query_hash_hex,
	CONVERT(varchar(34), tp.query_plan_hash, 1) AS query_plan_hash_hex,
	tp.last_execution_time,
	CASE
		WHEN qp.query_plan.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //SpillToTempDb') = 1 THEN 1
		ELSE 0
	END AS has_spill,
	CASE
		WHEN qp.query_plan.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //MemoryGrantWarning') = 1 THEN 1
		ELSE 0
	END AS has_memgrant_warning,
	CASE
		WHEN qp.query_plan.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //PlanAffectingConvert') = 1 THEN 1
		ELSE 0
	END AS has_plan_affecting_convert,
	CASE
		WHEN qp.query_plan.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //NoJoinPredicate') = 1 THEN 1
		ELSE 0
	END AS has_no_join_predicate,
	CASE
		WHEN st.text LIKE '%CONVERT_IMPLICIT%' THEN 1
		ELSE 0
	END AS has_convert_implicit_text,
	tp.execution_count,
	tp.total_worker_time,
	tp.total_logical_reads,
	tp.total_elapsed_time,
	OBJECT_SCHEMA_NAME(st.objectid, st.dbid) AS schema_name,
	OBJECT_NAME(st.objectid, st.dbid) AS object_name,
	CASE WHEN cfg.SafeMode = 0 THEN LEFT(st.text, 4000) ELSE '[SafeMode]' END AS sql_text
	INTO #DTR_PlanWarnings
	FROM TopPlans AS tp
	CROSS APPLY sys.dm_exec_query_plan(tp.plan_handle) AS qp
	OUTER APPLY sys.dm_exec_sql_text(tp.sql_handle) AS st
	CROSS JOIN #DT_Config AS cfg
	WHERE qp.query_plan.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //Warnings') = 1
	ORDER BY tp.total_worker_time DESC, tp.total_logical_reads DESC, tp.last_execution_time DESC
	OPTION (MAXDOP 1, RECOMPILE);
END
GO

------------------------------------------------------------
-- 09b. Plan Warning Details
------------------------------------------------------------
PRINT N'▶ 09b. Plan Warning Details - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_PlanWarningDetails') IS NOT NULL DROP TABLE #DTR_PlanWarningDetails
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH TopPlans AS (
		SELECT TOP (1000)
		qs.plan_handle,
		qs.sql_handle,
		qs.query_hash,
		qs.query_plan_hash,
		qs.last_execution_time,
		qs.total_worker_time
		FROM sys.dm_exec_query_stats AS qs
		OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
		WHERE st.dbid = DB_ID()
		ORDER BY qs.total_worker_time DESC
	)
	SELECT TOP (1000)
	IDENTITY(int) AS RowNumber,
	CONVERT(varchar(34), tp.query_hash, 1) AS query_hash_hex,
	CONVERT(varchar(34), tp.query_plan_hash, 1) AS query_plan_hash_hex,
	tp.last_execution_time,
	CASE
		WHEN qp.query_plan.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //SpillToTempDb') = 1 THEN 1
		ELSE 0
	END AS has_spill,
	CASE
		WHEN qp.query_plan.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //MemoryGrantWarning') = 1 THEN 1
		ELSE 0
	END AS has_memgrant_warning,
	CASE
		WHEN qp.query_plan.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //PlanAffectingConvert') = 1 THEN 1
		ELSE 0
	END AS has_plan_affecting_convert,
	CASE
		WHEN qp.query_plan.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //NoJoinPredicate') = 1 THEN 1
		ELSE 0
	END AS has_no_join_predicate,
	CASE
		WHEN st.text LIKE '%CONVERT_IMPLICIT%' THEN 1
		ELSE 0
	END AS has_convert_implicit_text,
	CASE WHEN cfg.SafeMode = 0 THEN LEFT(st.text, 4000) ELSE '[SafeMode]' END AS sql_text,
	qp.query_plan.value(
		'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan";
			count(//ScalarOperator[contains(@ScalarString,"CONVERT_IMPLICIT")])',
		'int'
	) AS implicit_convert_count,
	qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//@EstimateRowsWithoutRowGoal) + count(//@EstimatedRowsWithoutRowGoal)','int') AS rowgoal_attr_count,
	qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//RelOp[@PhysicalOp="Top" or @LogicalOp="Top"])','int') AS top_operator_count,
	CASE
		WHEN st.text LIKE '%OPTION%FAST%' THEN 1
		ELSE 0
	END AS has_fast_hint,
	qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//RelOp[@PhysicalOp="Table Spool"])','int') AS table_spool_count,
	qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//RelOp[@PhysicalOp="Index Spool"])','int') AS index_spool_count,
	qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//RelOp[@PhysicalOp="Row Count Spool"])','int') AS rowcount_spool_count,
	qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//RelOp[@EstimatedExecutionMode="Batch"])','int') AS batch_ops,
	qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//RelOp[@EstimatedExecutionMode="Row"])','int') AS row_ops,
	qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//StmtSimple[@StatementOptmLevel="TRIVIAL"])','int') AS trivial_stmt_count,
	qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//StmtSimple[@StatementOptmEarlyAbortReason="GoodEnoughPlanFound"])','int') AS early_abort_count
	INTO #DTR_PlanWarningDetails
	FROM TopPlans AS tp
	CROSS APPLY sys.dm_exec_query_plan(tp.plan_handle) AS qp
	OUTER APPLY sys.dm_exec_sql_text(tp.sql_handle) AS st
	CROSS JOIN #DT_Config AS cfg
	WHERE qp.query_plan.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //Warnings') = 1
	ORDER BY tp.last_execution_time DESC
	OPTION (MAXDOP 1, RECOMPILE);
END
GO

------------------------------------------------------------
-- 09c. Row-Goal Proxies (Top Operators & FAST Hint) in Cached Plans
------------------------------------------------------------
PRINT N'▶ 09c. Row-Goal Proxies (Top Operators & FAST Hint) in Cached Plans - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_RowGoalProxies') IS NOT NULL DROP TABLE #DTR_RowGoalProxies
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	DECLARE @TopPlans int = 300;

	;WITH TopQs AS (
		SELECT TOP (@TopPlans)
		qs.plan_handle,
		qs.sql_handle
		FROM sys.dm_exec_query_stats AS qs
		CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
		WHERE st.dbid = DB_ID()
		ORDER BY qs.total_worker_time DESC
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	CONVERT(varbinary(64), t.plan_handle) AS plan_handle,
	x.top_ops,
	CASE
		WHEN st.text LIKE '%OPTION%FAST%' THEN 1
		ELSE 0
	END AS has_fast_hint
	INTO #DTR_RowGoalProxies
	FROM TopQs AS t
	CROSS APPLY sys.dm_exec_query_plan(t.plan_handle) AS qp
	CROSS APPLY (
		SELECT
		qp.query_plan.value(
			'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan";
				count(//RelOp[@PhysicalOp="Top" or @LogicalOp="Top"])',
			'int'
		) AS top_ops
	) AS x
	CROSS APPLY sys.dm_exec_sql_text(t.sql_handle) AS st
	WHERE x.top_ops > 0 OR st.text LIKE '%OPTION%FAST%';
END
GO

------------------------------------------------------------
-- 09d. Spool Builder/Consumer Summary (Table/Index/RowCount Spools)
------------------------------------------------------------
PRINT N'▶ 09d. Spool Builder/Consumer Summary (Table/Index/RowCount Spools) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_SpoolSummary') IS NOT NULL DROP TABLE #DTR_SpoolSummary
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH TopQs AS (
		SELECT TOP (300)
		qs.plan_handle
		FROM sys.dm_exec_query_stats AS qs
		OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
		WHERE st.dbid = DB_ID()
		ORDER BY qs.total_worker_time DESC
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	CONVERT(varbinary(64), t.plan_handle) AS plan_handle,
	x.table_spool_count,
	x.index_spool_count,
	x.rowcount_spool_count,
	x.spools_under_nested_loops
	INTO #DTR_SpoolSummary
	FROM TopQs AS t
	CROSS APPLY sys.dm_exec_query_plan(t.plan_handle) AS qp
	CROSS APPLY (
		SELECT
		qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//RelOp[@PhysicalOp="Table Spool"])','int') AS table_spool_count,
		qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//RelOp[@PhysicalOp="Index Spool"])','int') AS index_spool_count,
		qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//RelOp[@PhysicalOp="Row Count Spool"])','int') AS rowcount_spool_count,
		qp.query_plan.value(
			'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan";
				count(//RelOp[@PhysicalOp="Nested Loops"]//RelOp[@PhysicalOp="Table Spool" or @PhysicalOp="Index Spool" or @PhysicalOp="Row Count Spool"])',
			'int'
		) AS spools_under_nested_loops
	) AS x
	WHERE (x.table_spool_count + x.index_spool_count + x.rowcount_spool_count) > 0;
END
GO

------------------------------------------------------------
-- 09e. Spool Builder/Consumer Pairs (Cached Plans)
------------------------------------------------------------
PRINT N'▶ 09e. Spool Builder/Consumer Pairs (Cached Plans) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_SpoolPairs') IS NOT NULL DROP TABLE #DTR_SpoolPairs;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH TopQs AS (
		SELECT TOP (300)
		qs.plan_handle,
		qs.sql_handle,
		qs.statement_start_offset,
		qs.statement_end_offset
		FROM sys.dm_exec_query_stats AS qs
		OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
		WHERE st.dbid = DB_ID()
		ORDER BY qs.total_worker_time DESC
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	CONVERT(varbinary(64), t.plan_handle) AS plan_handle,
	c.pNode.value('@NodeId','int') AS consumer_node_id,
	c.pNode.value('@LogicalOp','nvarchar(60)') AS consumer_logical_op,
	c.pNode.value('@PhysicalOp','nvarchar(60)') AS consumer_physical_op,
	CASE WHEN ca.WithStack IN ('true', '1') THEN 1 ELSE 0 END AS is_stack_spool,
	ca.PrimaryNodeId AS builder_node_id,
	b.bNode.value('@LogicalOp','nvarchar(60)') AS builder_logical_op,
	b.bNode.value('@PhysicalOp','nvarchar(60)') AS builder_physical_op,
	CASE
		WHEN qp.query_plan.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //RelOp[@PhysicalOp="Nested Loops"]//RelOp[@NodeId = sql:column("CA.ConsumerNodeId")]') = 1 THEN 1
		ELSE 0
	END AS under_nested_loops
	INTO #DTR_SpoolPairs
	FROM TopQs AS t
	CROSS APPLY sys.dm_exec_query_plan(t.plan_handle) AS qp
	CROSS APPLY qp.query_plan.nodes('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //RelOp[Spool[@PrimaryNodeId or @PrimaryNodeID]]') AS c(pNode)
	CROSS APPLY c.pNode.nodes('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; ./Spool') AS s(sNode)
	CROSS APPLY (
		SELECT
		CASE
			WHEN sNode.exist('@PrimaryNodeId') = 1 THEN sNode.value('@PrimaryNodeId[1]','int')
			WHEN sNode.exist('@PrimaryNodeID') = 1 THEN sNode.value('@PrimaryNodeID[1]','int')
			ELSE NULL
		END AS PrimaryNodeId,
		c.pNode.value('@NodeId','int') AS ConsumerNodeId,
		NULLIF(sNode.value('@WithStack','nvarchar(5)'), '') AS WithStack
	) AS ca
	OUTER APPLY qp.query_plan.nodes('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //RelOp[@NodeId = sql:column("CA.PrimaryNodeId")]') AS b(bNode)
	WHERE ca.PrimaryNodeId IS NOT NULL;
END
GO

------------------------------------------------------------
-- 09f. Plan Spill Warnings Summary (Cached Plans)
------------------------------------------------------------
PRINT N'▶ 09f. Plan Spill Warnings Summary (Cached Plans) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_PlanSpillSummary') IS NOT NULL DROP TABLE #DTR_PlanSpillSummary;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	DECLARE @TopPlans int = 300;

	;WITH TopQs AS (
		SELECT TOP (@TopPlans)
		qs.plan_handle,
		qs.sql_handle,
		qs.query_hash,
		qs.query_plan_hash
		FROM sys.dm_exec_query_stats AS qs
		CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
		WHERE st.dbid = DB_ID()
		ORDER BY qs.total_worker_time DESC
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	CONVERT(varbinary(64), t.plan_handle) AS plan_handle,
	t.query_hash,
	t.query_plan_hash,
	x.total_spills,
	x.sort_spills,
	x.hash_spills,
	x.parallelism_spills,
	x.sum_spilled_threads,
	x.total_spill_writes_kb
	INTO #DTR_PlanSpillSummary
	FROM TopQs AS t
	CROSS APPLY sys.dm_exec_query_plan(t.plan_handle) AS p
	CROSS APPLY (
		SELECT
		CONVERT(
			int,
			p.query_plan.value(
				'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan";
					count(//Warnings/SpillToTempDb)',
				'int'
			)
		) AS total_spills,
		CONVERT(
			int,
			p.query_plan.value(
				'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan";
					count(//RelOp[@PhysicalOp="Sort"]/Warnings/SpillToTempDb)',
				'int'
			)
		) AS sort_spills,
		CONVERT(
			int,
			p.query_plan.value(
				'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan";
					count(//RelOp[@PhysicalOp="Hash Match"]/Warnings/SpillToTempDb)',
				'int'
			)
		) AS hash_spills,
		CONVERT(
			int,
			p.query_plan.value(
				'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan";
					count(//RelOp[@PhysicalOp="Parallelism"]/Warnings/SpillToTempDb)',
				'int'
			)
		) AS parallelism_spills,
		CONVERT(
			int,
			ISNULL(
				(
					SELECT
					SUM(T.value('@SpilledThreadCount','int'))
					FROM p.query_plan.nodes('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //Warnings/SpillToTempDb') AS S(T)
				),
				0
			)
		) AS sum_spilled_threads,
		CONVERT(
			bigint,
			ISNULL(
				(
					SELECT
					SUM(ISNULL(T.value('@WritesToTempDb','bigint'), 0))
					FROM p.query_plan.nodes('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //Warnings/*[self::SortSpillDetails or self::HashSpillDetails]') AS S(T)
				),
				0
			)
		) AS total_spill_writes_kb
	) AS x;
END
GO

------------------------------------------------------------
------------------------------------------------------------
-- 09g. Memory Grant Utilization Summary (Cached Plans)
------------------------------------------------------------
PRINT N'▶ 09g. Memory Grant Utilization Summary (Cached Plans) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_MemoryGrantUtil') IS NOT NULL DROP TABLE #DTR_MemoryGrantUtil;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	DECLARE @TopPlans int = 300;

	;WITH TopQs AS (
		SELECT TOP (@TopPlans)
		qs.plan_handle
		FROM sys.dm_exec_query_stats AS qs
		CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
		WHERE st.dbid = DB_ID()
		ORDER BY qs.total_worker_time DESC
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	CONVERT(varbinary(64), t.plan_handle) AS plan_handle,
	MAX(mg.node.value('@RequestedMemory','int')) AS requested_kb,
	MAX(mg.node.value('@GrantedMemory','int')) AS granted_kb,
	MAX(mg.node.value('@MaxUsedMemory','int')) AS max_used_kb,
	MAX(mg.node.value('@DesiredMemory','int')) AS desired_kb,
	MAX(mg.node.value('@GrantWaitTime','int')) AS grant_wait_s,
	MAX(mg.node.value('@MaxQueryMemory','int')) AS max_query_kb,
	CONVERT(
		decimal(6, 2),
		CASE
			WHEN MAX(mg.node.value('@GrantedMemory','int')) > 0
				THEN (1.0 * MAX(mg.node.value('@MaxUsedMemory','int')) / MAX(mg.node.value('@GrantedMemory','int'))) * 100
			ELSE NULL
		END
	) AS grant_utilization_pct,
	MAX(mg.node.value('@GrantedMemory','int')) - MAX(mg.node.value('@MaxUsedMemory','int')) AS overshoot_kb
	INTO #DTR_MemoryGrantUtil
	FROM TopQs AS t
	CROSS APPLY sys.dm_exec_query_plan(t.plan_handle) AS qp
	CROSS APPLY qp.query_plan.nodes('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //MemoryGrantInfo') AS mg(node)
	GROUP BY t.plan_handle;
END
GO

------------------------------------------------------------
-- 09h. Lookup Hotspots (Key/Rid) in Cached Plans
------------------------------------------------------------
PRINT N'▶ 09h. Lookup Hotspots (Key/Rid) in Cached Plans - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_LookupHotspots') IS NOT NULL DROP TABLE #DTR_LookupHotspots;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH TopQs AS (
		SELECT TOP (300)
		qs.plan_handle
		FROM sys.dm_exec_query_stats AS qs
		OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
		WHERE st.dbid = DB_ID()
		ORDER BY qs.total_worker_time DESC
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	CONVERT(varbinary(64), t.plan_handle) AS plan_handle,
	x.total_lookups,
	x.key_lookups,
	x.rid_lookups,
	x.lookups_under_nested_loops
	INTO #DTR_LookupHotspots
	FROM TopQs AS t
	CROSS APPLY sys.dm_exec_query_plan(t.plan_handle) AS qp
	CROSS APPLY (
		SELECT
		qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//RelOp[@PhysicalOp="Key Lookup" or @PhysicalOp="RID Lookup"])','int') AS total_lookups,
		qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//RelOp[@PhysicalOp="Key Lookup"])','int') AS key_lookups,
		qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//RelOp[@PhysicalOp="RID Lookup"])','int') AS rid_lookups,
		qp.query_plan.value(
			'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan";
				count(//RelOp[@PhysicalOp="Nested Loops"]//RelOp[@PhysicalOp="Key Lookup" or @PhysicalOp="RID Lookup"])',
			'int'
		) AS lookups_under_nested_loops
	) AS x
	WHERE x.total_lookups > 0;
END
GO

------------------------------------------------------------
-- 09i. Parallelism Summary (Dop & NonParallelPlanReason)
------------------------------------------------------------
PRINT N'▶ 09i. Parallelism Summary (Dop & NonParallelPlanReason) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_ParallelismSummary') IS NOT NULL DROP TABLE #DTR_ParallelismSummary
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	DECLARE @TopPlans int = 300;

	;WITH TopQs AS (
		SELECT TOP (@TopPlans)
		qs.plan_handle
		FROM sys.dm_exec_query_stats AS qs
		CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
		WHERE st.dbid = DB_ID()
		ORDER BY qs.total_worker_time DESC
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	CONVERT(varbinary(64), t.plan_handle) AS plan_handle,
	qp.query_plan.value(
		'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan";
			(/ShowPlanXML/BatchSequence/Batch/Statements/*/StmtSimple/QueryPlan/@DegreeOfParallelism)[1]',
		'int'
	) AS dop,
	qp.query_plan.value(
		'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan";
			(/ShowPlanXML/BatchSequence/Batch/Statements/*/StmtSimple/QueryPlan/@NonParallelPlanReason)[1]',
		'nvarchar(128)'
	) AS nonparallel_reason,
	qp.query_plan.value(
		'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan";
			count(//RelOp[@Parallel="1"])',
		'int'
	) AS parallel_ops_count
	INTO #DTR_ParallelismSummary
	FROM TopQs AS t
	CROSS APPLY sys.dm_exec_query_plan(t.plan_handle) AS qp;
END
GO

------------------------------------------------------------
-- 09j. Residual Predicate Summary (Seeks)
------------------------------------------------------------
PRINT N'▶ 09j. Residual Predicate Summary (Seeks) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_ResidualPredicates') IS NOT NULL DROP TABLE #DTR_ResidualPredicates
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH TopQs AS (
		SELECT TOP (300)
		qs.plan_handle
		FROM sys.dm_exec_query_stats AS qs
		OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
		WHERE st.dbid = DB_ID()
		ORDER BY qs.total_worker_time DESC
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	CONVERT(varbinary(64), t.plan_handle) AS plan_handle,
	X.seek_ops,
	X.seek_predicates,
	X.residual_predicates,
	CONVERT(
		decimal(6, 2),
		CASE
			WHEN x.seek_ops > 0 THEN (1.0 * x.residual_predicates / x.seek_ops) * 100
			ELSE NULL
		END
	) AS residual_per_seek_pct
	INTO #DTR_ResidualPredicates
	FROM TopQs AS t
	CROSS APPLY sys.dm_exec_query_plan(t.plan_handle) AS qp
	CROSS APPLY (
		SELECT
		qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//RelOp[@PhysicalOp="Index Seek" or @PhysicalOp="Clustered Index Seek"])','int') AS seek_ops,
		qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//RelOp[@PhysicalOp="Index Seek" or @PhysicalOp="Clustered Index Seek"]//SeekPredicates/*)','int') AS seek_predicates,
		(
			qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//RelOp[@PhysicalOp="Index Seek" or @PhysicalOp="Clustered Index Seek"]//Predicate)','int')
			-
			qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//RelOp[@PhysicalOp="Index Seek" or @PhysicalOp="Clustered Index Seek"]//SeekPredicates/*)','int')
		) AS residual_predicates
	) AS X
	WHERE X.seek_ops > 0;
END
GO

------------------------------------------------------------
-- 09k. Plan Warning Variants (NoJoinPredicate, ColumnsWithNoStatistics, UnmatchedIndexes, PlanAffectingConvert)
------------------------------------------------------------
PRINT N'▶ 09k. Plan Warning Variants (NoJoinPredicate, ColumnsWithNoStatistics, UnmatchedIndexes, PlanAffectingConvert) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_PlanWarningVariants') IS NOT NULL DROP TABLE #DTR_PlanWarningVariants
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	DECLARE @TopPlans int = 300;

	;WITH TopQs AS (
		SELECT TOP (@TopPlans)
		qs.plan_handle
		FROM sys.dm_exec_query_stats AS qs
		CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
		WHERE st.dbid = DB_ID()
		ORDER BY qs.total_worker_time DESC
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	CONVERT(varbinary(64), t.plan_handle) AS plan_handle,
	qp.query_plan.value(
		'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan";
			count(//Warnings/NoJoinPredicate)',
		'int'
	) AS warn_no_join_predicate,
	qp.query_plan.value(
		'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan";
			count(//Warnings/ColumnsWithNoStatistics)',
		'int'
	) AS warn_cols_no_stats,
	qp.query_plan.value(
		'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan";
			count(//Warnings/UnmatchedIndexes)',
		'int'
	) AS warn_unmatched_indexes,
	qp.query_plan.value(
		'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan";
			count(//Warnings/PlanAffectingConvert)',
		'int'
	) AS warn_plan_affecting_convert
	INTO #DTR_PlanWarningVariants
	FROM TopQs AS t
	CROSS APPLY sys.dm_exec_query_plan(t.plan_handle) AS qp
	WHERE qp.query_plan.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //Warnings/*') = 1;
END
GO

------------------------------------------------------------
------------------------------------------------------------
-- 09l. Join-Shape Mix (Loops/Hash/Merge) in Cached Plans
------------------------------------------------------------
PRINT N'▶ 09l. Join-Shape Mix (Loops/Hash/Merge) in Cached Plans - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_JoinShapeMix') IS NOT NULL DROP TABLE #DTR_JoinShapeMix
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	DECLARE @TopPlans int = 300;

	;WITH TopQs AS (
		SELECT TOP (@TopPlans)
		qs.plan_handle
		FROM sys.dm_exec_query_stats AS qs
		CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
		WHERE st.dbid = DB_ID()
		ORDER BY qs.total_worker_time DESC
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	CONVERT(varbinary(64), t.plan_handle) AS plan_handle,
	qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//RelOp[@PhysicalOp="Nested Loops"])','int') AS loop_joins,
	qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//RelOp[@PhysicalOp="Hash Match"])','int') AS hash_joins,
	qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//RelOp[@PhysicalOp="Merge Join"])','int') AS merge_joins
	INTO #DTR_JoinShapeMix
	FROM TopQs AS t
	CROSS APPLY sys.dm_exec_query_plan(t.plan_handle) AS qp;
END
GO

------------------------------------------------------------
-- 09m. Exchange Operators (Parallelism) Summary
------------------------------------------------------------
PRINT N'▶ 09m. Exchange Operators (Parallelism) Summary - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_ExchangesSummary') IS NOT NULL DROP TABLE #DTR_ExchangesSummary;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	DECLARE @TopPlans int = 300;

	;WITH TopQs AS (
		SELECT TOP (@TopPlans)
		qs.plan_handle
		FROM sys.dm_exec_query_stats AS qs
		CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
		WHERE st.dbid = DB_ID()
		ORDER BY qs.total_worker_time DESC
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	CONVERT(varbinary(64), t.plan_handle) AS plan_handle,
	x.repartition_streams,
	x.distribute_streams,
	x.gather_streams,
	x.repartition_streams + x.distribute_streams + x.gather_streams AS total_exchanges
	INTO #DTR_ExchangesSummary
	FROM TopQs AS t
	CROSS APPLY sys.dm_exec_query_plan(t.plan_handle) AS qp
	CROSS APPLY (
		SELECT
		qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//RelOp[@PhysicalOp="Parallelism" and @LogicalOp="Repartition Streams"])','int') AS repartition_streams,
		qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//RelOp[@PhysicalOp="Parallelism" and @LogicalOp="Distribute Streams"])','int') AS distribute_streams,
		qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//RelOp[@PhysicalOp="Parallelism" and @LogicalOp="Gather Streams"])','int') AS gather_streams
	) AS x
	WHERE x.repartition_streams + x.distribute_streams + x.gather_streams > 0;
END
GO

------------------------------------------------------------
-- 09n. Memory Grant Warnings Summary (Cached Plans)
------------------------------------------------------------
PRINT N'▶ 09n. Memory Grant Warnings Summary (Cached Plans) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_MemGrantWarnings') IS NOT NULL DROP TABLE #DTR_MemGrantWarnings
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	DECLARE @TopPlans int = 300;

	;WITH TopQs AS (
		SELECT TOP (@TopPlans)
		qs.plan_handle
		FROM sys.dm_exec_query_stats AS qs
		CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
		WHERE st.dbid = DB_ID()
		ORDER BY qs.total_worker_time DESC
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	CONVERT(varbinary(64), t.plan_handle) AS plan_handle,
	x.total_warnings,
	x.excessive_grant_count,
	x.insufficient_grant_count,
	x.wait_count
	INTO #DTR_MemGrantWarnings
	FROM TopQs AS t
	CROSS APPLY sys.dm_exec_query_plan(t.plan_handle) AS qp
	CROSS APPLY (
		SELECT
		qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//Warnings/MemoryGrantWarning)','int') AS total_warnings,
		qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//Warnings/MemoryGrantWarning[@GrantWarningKind="ExcessiveGrant"])','int') AS excessive_grant_count,
		qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//Warnings/MemoryGrantWarning[@GrantWarningKind="InsufficientGrant"])','int') AS insufficient_grant_count,
		qp.query_plan.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; count(//Warnings/MemoryGrantWarning[@GrantWarningKind="Wait"])','int') AS wait_count
	) AS x
	WHERE x.total_warnings > 0;
END
GO

------------------------------------------------------------
-- 09o. Memory Broker Pressure & Targets
------------------------------------------------------------
PRINT N'▶ 09o. Memory Broker Pressure & Targets - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO
IF OBJECT_ID('tempdb..#DTR_MemoryBrokers') IS NOT NULL DROP TABLE #DTR_MemoryBrokers;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	mb.memory_broker_type,
	mb.pool_id,
	mb.allocations_kb,
	mb.predicted_allocations_kb,
	mb.target_allocations_kb,
	mb.future_allocations_kb,
	mb.overall_limit_kb,
	mb.allocations_kb_per_sec,
	mb.last_notification,
	CAST(100.0 * mb.allocations_kb / NULLIF(mb.target_allocations_kb, 0) AS decimal(6,1)) AS pct_of_target,
	CAST(100.0 * mb.allocations_kb / NULLIF(mb.overall_limit_kb, 0) AS decimal(6,1)) AS pct_of_overall_limit
	INTO #DTR_MemoryBrokers
	FROM sys.dm_os_memory_brokers AS mb
	ORDER BY pct_of_target DESC, allocations_kb DESC;
END
GO

------------------------------------------------------------
-- 09p. Resource Governor Pools - Runtime Utilization
------------------------------------------------------------
PRINT N'▶ 09p. Resource Governor Pools - Runtime Utilization - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_RG_PoolRuntime') IS NOT NULL DROP TABLE #DTR_RG_PoolRuntime;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	rp.name,
	rp.pool_id,
	rp.statistics_start_time,
	rp.min_cpu_percent,
	rp.max_cpu_percent,
	rp.min_memory_percent,
	rp.max_memory_percent,
	rp.total_cpu_usage_ms,
	rp.max_memory_kb,
	rp.target_memory_kb,
	rp.used_memory_kb,
	rp.cache_memory_kb,
	rp.compile_memory_kb,
	rp.used_memgrant_kb,
	rp.active_memgrant_kb,
	rp.total_memgrant_count,
	rp.total_memgrant_timeout_count,
	rp.active_memgrant_count,
	rp.memgrant_waiter_count,
	rp.out_of_memory_count
	INTO #DTR_RG_PoolRuntime
	FROM sys.dm_resource_governor_resource_pools AS rp
	ORDER BY rp.total_cpu_usage_ms DESC, rp.used_memory_kb DESC;
END
GO

------------------------------------------------------------
-- 09q. Memory Objects Breakdown (dm_os_memory_objects)
------------------------------------------------------------
PRINT N'▶ 09q. Memory Objects Breakdown (dm_os_memory_objects) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_MemoryObjects') IS NOT NULL DROP TABLE #DTR_MemoryObjects;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	DECLARE @sql nvarchar(max);

	IF COL_LENGTH('sys.dm_os_memory_objects', 'pages_in_bytes') IS NOT NULL
	BEGIN
		SET @sql = N'
			SELECT
			IDENTITY(int) AS RowNumber,
			mo.type,
			mo.name,
			mo.memory_node_id,
			mo.creation_time,
			mo.creation_options,
			mo.page_size_in_bytes,
			CONVERT(bigint, mo.pages_in_bytes / NULLIF(mo.page_size_in_bytes, 0)) AS pages_allocated_count,
			CONVERT(decimal(19,2), mo.pages_in_bytes / 1024.0 / 1024.0) AS allocated_mb,
			CONVERT(bigint, mo.max_pages_in_bytes / NULLIF(mo.page_size_in_bytes, 0)) AS max_pages_allocated_count,
			CONVERT(decimal(19,2), mo.max_pages_in_bytes / 1024.0 / 1024.0) AS max_allocated_mb,
			mo.bytes_used,
			mo.sequence_num,
			mo.memory_object_address,
			mo.parent_address,
			mo.page_allocator_address,
			mo.creation_stack_address
			INTO #DTR_MemoryObjects
			FROM sys.dm_os_memory_objects AS mo
			WHERE mo.pages_in_bytes >= (256 * 1024)
			ORDER BY allocated_mb DESC, mo.pages_in_bytes DESC;'
	END

	IF COL_LENGTH('sys.dm_os_memory_objects', 'pages_in_bytes') IS NULL
	BEGIN
		SET @sql = N'
			SELECT
			IDENTITY(int) AS RowNumber,
			mo.type,
			mo.name,
			mo.memory_node_id,
			mo.creation_time,
			mo.creation_options,
			mo.page_size_in_bytes,
			mo.pages_allocated_count,
			CONVERT(decimal(19,2), (mo.pages_allocated_count * mo.page_size_in_bytes) / 1024.0 / 1024.0) AS allocated_mb,
			mo.max_pages_allocated_count,
			CONVERT(decimal(19,2), (mo.max_pages_allocated_count * mo.page_size_in_bytes) / 1024.0 / 1024.0) AS max_allocated_mb,
			mo.bytes_used,
			mo.sequence_num,
			mo.memory_object_address,
			mo.parent_address,
			mo.page_allocator_address,
			mo.creation_stack_address
			INTO #DTR_MemoryObjects
			FROM sys.dm_os_memory_objects AS mo
			WHERE (mo.pages_allocated_count * mo.page_size_in_bytes) >= (256 * 1024)
			ORDER BY allocated_mb DESC, mo.pages_allocated_count DESC;'
	END

	EXEC sys.sp_executesql
	@sql;
END
GO

------------------------------------------------------------
-- 10a. Memory Grants
------------------------------------------------------------
PRINT N'▶ 10a. Memory Grants - ' + CONVERT(char(8), GETDATE(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_MemoryGrants') IS NOT NULL DROP TABLE #DTR_MemoryGrants;;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT TOP (50)
	IDENTITY(int) AS RowNumber,
	mg.session_id,
	mg.request_time,
	mg.grant_time,
	mg.requested_memory_kb,
	mg.granted_memory_kb,
	mg.required_memory_kb,
	mg.used_memory_kb,
	mg.ideal_memory_kb,
	mg.dop AS dop,
	mg.queue_id,
	mg.pool_id,
	mg.is_next_candidate,
	mg.wait_order,
	mg.wait_time_ms,
	mg.timeout_sec,
	mg.query_cost,
	CASE WHEN cfg.SafeMode = 0 THEN LEFT(t.text, 4000) ELSE '[SafeMode]' END AS sql_text
	INTO #DTR_MemoryGrants
	FROM sys.dm_exec_query_memory_grants AS mg
	OUTER APPLY sys.dm_exec_sql_text(mg.sql_handle) AS t
	CROSS JOIN #DT_Config AS cfg
	WHERE t.dbid = DB_ID()
	ORDER BY mg.requested_memory_kb DESC;
END
GO

------------------------------------------------------------
-- 10b. TempDB Session Space
------------------------------------------------------------
PRINT N'▶ 10b. TempDB Session Space - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_TempdbSessionSpace_10b') IS NOT NULL DROP TABLE #DTR_TempdbSessionSpace_10b
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT TOP (50)
	IDENTITY(int) AS RowNumber,
	s.session_id,
	CASE WHEN cfg.SafeMode = 0 THEN s.login_name ELSE '[SafeMode]' END AS login_name,
	CASE WHEN cfg.SafeMode = 0 THEN s.host_name ELSE '[SafeMode]' END AS host_name,
	CASE WHEN cfg.SafeMode = 0 THEN s.program_name ELSE '[SafeMode]' END AS program_name,
	CASE
		WHEN (ssu.user_objects_alloc_page_count - ssu.user_objects_dealloc_page_count) < 0 THEN 0
		ELSE (ssu.user_objects_alloc_page_count - ssu.user_objects_dealloc_page_count)
	END * 8.0 / 1024 AS user_objects_mb,
	CASE
		WHEN (ssu.internal_objects_alloc_page_count - ssu.internal_objects_dealloc_page_count) < 0 THEN 0
		ELSE (ssu.internal_objects_alloc_page_count - ssu.internal_objects_dealloc_page_count)
	END * 8.0 / 1024 AS internal_objects_mb,
	(
		CASE
			WHEN (ssu.user_objects_alloc_page_count - ssu.user_objects_dealloc_page_count) < 0 THEN 0
			ELSE (ssu.user_objects_alloc_page_count - ssu.user_objects_dealloc_page_count)
		END
		+
		CASE
			WHEN (ssu.internal_objects_alloc_page_count - ssu.internal_objects_dealloc_page_count) < 0 THEN 0
			ELSE (ssu.internal_objects_alloc_page_count - ssu.internal_objects_dealloc_page_count)
		END
	) * 8.0 / 1024 AS total_mb
	INTO #DTR_TempdbSessionSpace_10b
	FROM sys.dm_db_session_space_usage AS ssu
	JOIN sys.dm_exec_sessions AS s ON s.session_id = ssu.session_id
	LEFT JOIN sys.dm_exec_requests AS r ON r.session_id = s.session_id
	CROSS JOIN #DT_Config AS cfg
	WHERE ISNULL(r.database_id, DB_ID()) = DB_ID()
	ORDER BY total_mb DESC;
END
GO

------------------------------------------------------------
-- 10c. Top Buffer Pool Consumers
------------------------------------------------------------
PRINT N'▶ 10c. Top Buffer Pool Consumers - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_TopBufferPool') IS NOT NULL DROP TABLE #DTR_TopBufferPool
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT TOP (10)
	IDENTITY(int) AS RowNumber,
	QUOTENAME(OBJECT_SCHEMA_NAME(p.object_id)) + '.' + QUOTENAME(OBJECT_NAME(p.object_id)) AS object_name,
	COUNT(*) * 8.0 / 1024 AS buffer_mb
	INTO #DTR_TopBufferPool
	FROM sys.dm_os_buffer_descriptors AS bd
	JOIN sys.allocation_units AS au ON bd.allocation_unit_id = au.allocation_unit_id
	JOIN sys.partitions AS p ON au.container_id = p.partition_id
	WHERE bd.database_id = DB_ID() AND p.object_id IS NOT NULL
	GROUP BY p.object_id
	ORDER BY buffer_mb DESC
	OPTION (RECOMPILE);
END
GO

------------------------------------------------------------
-- 10d. Top Objects in Buffer Pool (by Pages)
------------------------------------------------------------
PRINT N'▶ 10d. Top Objects in Buffer Pool (by Pages) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_BufferPoolTopObjects') IS NOT NULL DROP TABLE #DTR_BufferPoolTopObjects
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH bd AS (
		SELECT
		bd.page_id,
		bd.allocation_unit_id
		FROM sys.dm_os_buffer_descriptors AS bd
		WHERE bd.database_id = DB_ID()
	),
	au AS (
		SELECT
		au.allocation_unit_id,
		au.container_id,
		au.type_desc
		FROM sys.allocation_units AS au
	),
	p AS (
		SELECT
		p.hobt_id,
		p.object_id,
		p.index_id
		FROM sys.partitions AS p
	)
	SELECT TOP (50)
	IDENTITY(int) AS RowNumber,
	SCHEMA_NAME(o.schema_id) + '.' + o.name AS ObjectName,
	i.name AS IndexName,
	i.type_desc,
	COUNT_BIG(*) AS page_count,
	CONVERT(decimal(18, 2), (COUNT_BIG(*) * 8.0) / 1024.0) AS approx_size_mb
	INTO #DTR_BufferPoolTopObjects
	FROM bd
	JOIN au ON au.allocation_unit_id = bd.allocation_unit_id
	JOIN p ON p.hobt_id = au.container_id
	JOIN sys.objects AS o ON o.object_id = p.object_id
	LEFT JOIN sys.indexes AS i ON i.object_id = p.object_id AND i.index_id = p.index_id
	WHERE o.is_ms_shipped = 0
	GROUP BY o.schema_id, o.name, i.name, i.type_desc;
END
GO

------------------------------------------------------------
-- 10e. Recent User Commands (Active Sessions - Input Buffer)
------------------------------------------------------------
PRINT N'▶ 10e. Recent User Commands (Active Sessions - Input Buffer) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_RecentUserCommands') IS NOT NULL DROP TABLE #DTR_RecentUserCommands;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	IF OBJECT_ID('tempdb..#DT_Sessions_10e') IS NOT NULL DROP TABLE #DT_Sessions_10e;
	IF OBJECT_ID('tempdb..#DT_InputBuffer_10e') IS NOT NULL DROP TABLE #DT_InputBuffer_10e;

	CREATE TABLE #DT_Sessions_10e (
		RowNumber int NOT NULL IDENTITY(1, 1),
		session_id int NOT NULL,
		request_id int NULL,
		database_id int NULL,
		login_name nvarchar(256) NULL,
		host_name nvarchar(256) NULL,
		program_name nvarchar(256) NULL,
		last_request_end_time datetime NULL
	);

	INSERT INTO #DT_Sessions_10e (
		session_id,
		request_id,
		database_id,
		login_name,
		host_name,
		program_name,
		last_request_end_time
	)
	SELECT TOP (50)
	r.session_id,
	r.request_id,
	r.database_id,
	s.login_name,
	s.host_name,
	s.program_name,
	s.last_request_end_time
	FROM sys.dm_exec_requests AS r
	JOIN sys.dm_exec_sessions AS s ON s.session_id = r.session_id
	WHERE r.database_id = DB_ID()
		AND s.is_user_process = 1
	ORDER BY s.last_request_end_time DESC;

	SELECT TOP (0)
	IDENTITY(int) AS RowNumber,
	CONVERT(int, NULL) AS session_id,
	CONVERT(int, NULL) AS request_id,
	CONVERT(sysname, NULL) AS database_name,
	CONVERT(nvarchar(256), NULL) AS login_name,
	CONVERT(nvarchar(256), NULL) AS host_name,
	CONVERT(nvarchar(256), NULL) AS program_name,
	CONVERT(datetime, NULL) AS last_request_end_time,
	CONVERT(nvarchar(30), NULL) AS event_type,
	CONVERT(int, NULL) AS parameters,
	CONVERT(nvarchar(4000), NULL) AS last_command_text
	INTO #DTR_RecentUserCommands;

	CREATE TABLE #DT_InputBuffer_10e (
		EventType nvarchar(30) NULL,
		Parameters int NULL,
		EventInfo nvarchar(4000) NULL
	);

	DECLARE @session_id int;
	DECLARE @request_id int;
	DECLARE @database_id int;
	DECLARE @login_name nvarchar(256);
	DECLARE @host_name nvarchar(256);
	DECLARE @program_name nvarchar(256);
	DECLARE @last_request_end_time datetime;
	DECLARE @cmd nvarchar(4000);

	DECLARE ibcur CURSOR LOCAL FAST_FORWARD FOR
		SELECT
		s.session_id,
		s.request_id,
		s.database_id,
		s.login_name,
		s.host_name,
		s.program_name,
		s.last_request_end_time
		FROM #DT_Sessions_10e AS s
		ORDER BY s.RowNumber;

	OPEN ibcur;

	FETCH NEXT FROM ibcur INTO
		@session_id,
		@request_id,
		@database_id,
		@login_name,
		@host_name,
		@program_name,
		@last_request_end_time;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		TRUNCATE TABLE #DT_InputBuffer_10e;

		SET @cmd = N'DBCC INPUTBUFFER (' + CONVERT(nvarchar(12), @session_id) + N') WITH NO_INFOMSGS;';

		INSERT INTO #DT_InputBuffer_10e (
			EventType,
			Parameters,
			EventInfo
		)
		EXEC (@cmd);

		INSERT INTO #DTR_RecentUserCommands (
			session_id,
			request_id,
			database_name,
			login_name,
			host_name,
			program_name,
			last_request_end_time,
			event_type,
			parameters,
			last_command_text
		)
		SELECT
		@session_id AS session_id,
		@request_id AS request_id,
		DB_NAME(@database_id) AS database_name,
		CASE WHEN cfg.SafeMode = 0 THEN @login_name ELSE '[SafeMode]' END AS login_name,
		CASE WHEN cfg.SafeMode = 0 THEN @host_name ELSE '[SafeMode]' END AS host_name,
		CASE WHEN cfg.SafeMode = 0 THEN @program_name ELSE '[SafeMode]' END AS program_name,
		@last_request_end_time AS last_request_end_time,
		ib.EventType AS event_type,
		ib.Parameters AS parameters,
		CASE WHEN cfg.SafeMode = 0 THEN ib.EventInfo ELSE '[SafeMode]' END AS last_command_text
		FROM #DT_Config AS cfg
		OUTER APPLY (
			SELECT TOP (1)
			EventType,
			Parameters,
			EventInfo
			FROM #DT_InputBuffer_10e
		) AS ib;

		FETCH NEXT FROM ibcur INTO
			@session_id,
			@request_id,
			@database_id,
			@login_name,
			@host_name,
			@program_name,
			@last_request_end_time;
	END

	CLOSE ibcur;
	DEALLOCATE ibcur;
END
GO

------------------------------------------------------------
-- 11a. Blocking
------------------------------------------------------------
PRINT N'▶ 11a. Blocking - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_BlockingActive') IS NOT NULL DROP TABLE #DTR_BlockingActive
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	r.session_id,
	r.blocking_session_id,
	r.status,
	r.command,
	r.wait_type,
	r.wait_time AS wait_time_ms,
	r.wait_resource,
	r.cpu_time AS cpu_time_ms,
	r.reads,
	r.writes,
	r.logical_reads,
	r.total_elapsed_time AS total_elapsed_ms,
	DB_NAME(r.database_id) AS database_name,
	CASE WHEN cfg.SafeMode = 0 THEN
		SUBSTRING(st.text, (r.statement_start_offset/2)+1,
			CASE WHEN r.statement_end_offset = -1 THEN (LEN(CONVERT(nvarchar(max), st.text)) * 2 - r.statement_start_offset)/2
			ELSE (r.statement_end_offset - r.statement_start_offset)/2 END)
		ELSE '[SafeMode]' END AS statement_text
	INTO #DTR_BlockingActive
	FROM sys.dm_exec_requests AS r
	OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) AS st
	CROSS JOIN #DT_Config AS cfg
	WHERE r.blocking_session_id > 0
		AND r.database_id = DB_ID()
	ORDER BY r.total_elapsed_time DESC;
END
GO

------------------------------------------------------------
-- 11b. Long Transactions
------------------------------------------------------------
PRINT N'▶ 11b. Long Transactions - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_LongTransactions') IS NOT NULL DROP TABLE #DTR_LongTransactions
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	at.transaction_id,
	at.name AS transaction_name,
	at.transaction_begin_time,
	DATEDIFF(second, at.transaction_begin_time, SYSDATETIME()) AS duration_seconds,
	dt.database_id,
	DB_NAME(dt.database_id) AS database_name,
	st.session_id,
	r.status,
	r.wait_type,
	r.wait_time AS wait_time_ms,
	r.cpu_time AS cpu_time_ms,
	r.reads,
	r.writes,
	r.logical_reads,
	CASE WHEN cfg.SafeMode = 0 THEN LEFT(t.text, 4000) ELSE '[SafeMode]' END AS sql_text
	INTO #DTR_LongTransactions
	FROM sys.dm_tran_active_transactions AS at
	JOIN sys.dm_tran_session_transactions AS st ON st.transaction_id = at.transaction_id
	JOIN sys.dm_tran_database_transactions AS dt ON dt.transaction_id = at.transaction_id
	LEFT JOIN sys.dm_exec_requests AS r ON r.session_id = st.session_id
	OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) AS t
	CROSS JOIN #DT_Config AS cfg
	WHERE dt.database_id = DB_ID()
	ORDER BY DATEDIFF(second, at.transaction_begin_time, SYSDATETIME()) DESC;
END
GO

------------------------------------------------------------
-- 11c. Top Blocked Objects
------------------------------------------------------------
PRINT N'▶ 11c. Top Blocked Objects - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_TopBlockedObjects') IS NOT NULL DROP TABLE #DTR_TopBlockedObjects
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT TOP (200)
	IDENTITY(int) AS RowNumber,
	tl.resource_type AS resource,
	NULLIF(LTRIM(RTRIM(tl.resource_description)), '') AS resource_desc,
	COUNT(*) AS requests
	INTO #DTR_TopBlockedObjects
	FROM sys.dm_tran_locks AS tl
	JOIN sys.dm_exec_sessions AS s ON tl.request_session_id = s.session_id
	LEFT JOIN sys.dm_exec_requests AS r ON r.session_id = s.session_id
	WHERE ISNULL(r.database_id, DB_ID()) = DB_ID()
	GROUP BY tl.resource_type, tl.resource_description
	ORDER BY requests DESC;
END
GO

------------------------------------------------------------
-- 11d. Long Snapshot Transactions (Version Store Consumers)
------------------------------------------------------------
PRINT N'▶ 11d. Long Snapshot Transactions (Version Store Consumers) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_LongSnapshotXacts') IS NOT NULL DROP TABLE #DTR_LongSnapshotXacts
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	-- This DMV is scoped by current database context for versioned activity
	SELECT
	IDENTITY(int) AS RowNumber,
	x.session_id,
	x.is_snapshot,
	x.elapsed_time_seconds,
	x.max_version_chain_traversed,
	x.average_version_chain_traversed
	INTO #DTR_LongSnapshotXacts
	FROM sys.dm_tran_active_snapshot_database_transactions AS x;
END
GO

------------------------------------------------------------
-- 11e. Top Version-Store Generators (by Table)
------------------------------------------------------------
PRINT N'▶ 11e. Top Version-Store Generators (by Table) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_TopVersionGenerators') IS NOT NULL DROP TABLE #DTR_TopVersionGenerators
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	SCHEMA_NAME(o.schema_id) + '.' + o.name AS ObjectName,
	vs.aggregated_record_length_in_bytes AS AggregatedRecordLengthBytes
	INTO #DTR_TopVersionGenerators
	FROM sys.dm_tran_top_version_generators AS vs
	JOIN sys.partitions AS p ON p.hobt_id = vs.rowset_id
	JOIN sys.objects AS o ON o.object_id = p.object_id
	WHERE vs.database_id = DB_ID() AND p.index_id IN (0,1);
END
GO

------------------------------------------------------------
-- 12a. Heaps & Forwarded Records
------------------------------------------------------------
PRINT N'▶ 12a. Heaps & Forwarded Records - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_HeapsForwarded') IS NOT NULL DROP TABLE #DTR_HeapsForwarded
GO

;WITH HeapCandidates AS (
	SELECT
	t.object_id,
	QUOTENAME(SCHEMA_NAME(t.schema_id)) + '.' + QUOTENAME(t.name) AS table_name,
	SUM(ps.row_count) AS row_count
	FROM sys.tables AS t
	JOIN sys.dm_db_partition_stats AS ps ON ps.object_id = t.object_id AND ps.index_id = 0
	WHERE t.is_ms_shipped = 0
	GROUP BY
	t.object_id,
	t.schema_id,
	t.name
),
FilteredHeaps AS (
	SELECT TOP (200)
	object_id,
	table_name,
	row_count
	FROM HeapCandidates
	WHERE row_count >= 100000
	ORDER BY row_count DESC
),
ps AS (
	SELECT
	object_id,
	SUM(forwarded_record_count) AS forwarded_record_count,
	AVG(avg_record_size_in_bytes) AS avg_record_size_in_bytes,
	AVG(avg_page_space_used_in_percent) AS avg_page_space_used_in_percent
	FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, 0, NULL, 'SAMPLED')
	GROUP BY object_id
)
SELECT
IDENTITY(int) AS RowNumber,
fh.table_name,
ps.forwarded_record_count,
ps.avg_record_size_in_bytes,
ps.avg_page_space_used_in_percent
INTO #DTR_HeapsForwarded
FROM FilteredHeaps AS fh
JOIN ps ON ps.object_id = fh.object_id
WHERE ps.forwarded_record_count > 0
ORDER BY ps.forwarded_record_count DESC;
GO

------------------------------------------------------------
-- 12b. All Heaps
------------------------------------------------------------
PRINT N'▶ 12b. All Heaps - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_AllHeaps') IS NOT NULL DROP TABLE #DTR_AllHeaps
GO

SELECT
IDENTITY(int) AS RowNumber,
QUOTENAME(SCHEMA_NAME(t.schema_id)) + '.' + QUOTENAME(t.name) AS table_name
INTO #DTR_AllHeaps
FROM sys.tables AS t
WHERE NOT EXISTS (
	SELECT
	1
	FROM sys.indexes AS i
	WHERE i.object_id = t.object_id AND i.index_id = 1
)
ORDER BY table_name;
GO

------------------------------------------------------------
-- 13a. Foreign Keys Without Supporting Indexes
------------------------------------------------------------
PRINT N'▶ 13a. Foreign Keys Without Supporting Indexes - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_FK_NoIndex_Child') IS NOT NULL DROP TABLE #DTR_FK_NoIndex_Child
GO

SELECT
IDENTITY(int) AS RowNumber,
QUOTENAME(SCHEMA_NAME(tp.schema_id)) + '.' + QUOTENAME(tp.name) AS fk_table,
fk.name AS fk_name,
STUFF((
	SELECT
	',' + QUOTENAME(c.name)
	FROM sys.columns AS c
	JOIN sys.foreign_key_columns AS fkc ON c.object_id = fkc.parent_object_id AND c.column_id = fkc.parent_column_id
	WHERE fkc.constraint_object_id = fk.object_id
	ORDER BY c.column_id
	FOR XML PATH(''), TYPE
).value('.', 'nvarchar(max)'), 1, 1, '') AS fk_columns,
CASE
	WHEN EXISTS (
		SELECT 1
		FROM sys.indexes AS ix
		JOIN sys.index_columns AS ic ON ic.object_id = ix.object_id AND ic.index_id = ix.index_id
		WHERE ix.object_id = fk.parent_object_id AND ix.is_hypothetical = 0
		GROUP BY ix.object_id, ix.index_id
		HAVING COUNT(*) = (
			SELECT COUNT(*)
			FROM sys.foreign_key_columns AS fkc
			WHERE
			fkc.constraint_object_id = fk.object_id) AND
				MIN(CASE WHEN ic.is_included_column = 1 THEN 1 ELSE 0 END) = 0 AND
				COUNT(CASE WHEN ic.key_ordinal > 0 THEN 1 END) = (
					SELECT COUNT(*)
					FROM sys.foreign_key_columns AS fkc
					WHERE fkc.constraint_object_id = fk.object_id)) THEN 1 ELSE 0
END AS has_index
INTO #DTR_FK_NoIndex_Child
FROM sys.foreign_keys AS fk
JOIN sys.tables AS tp ON tp.object_id = fk.parent_object_id
WHERE fk.is_disabled = 0 AND fk.is_ms_shipped = 0
ORDER BY fk_table, fk_name;
GO

------------------------------------------------------------
-- 13b. Foreign Keys Without Supporting Indexes
------------------------------------------------------------
PRINT N'▶ 13b. Foreign Keys Without Supporting Indexes - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_FK_NoIndex_Parent') IS NOT NULL DROP TABLE #DTR_FK_NoIndex_Parent
GO

SELECT
IDENTITY(int) AS RowNumber,
QUOTENAME(SCHEMA_NAME(tr.schema_id)) + '.' + QUOTENAME(tr.name) AS referenced_table,
fk.name AS fk_name,
STUFF((
	SELECT
	',' + QUOTENAME(c.name)
	FROM sys.columns AS c
	JOIN sys.foreign_key_columns AS fkc ON c.object_id = fkc.referenced_object_id AND c.column_id = fkc.referenced_column_id
	WHERE fkc.constraint_object_id = fk.object_id
	ORDER BY c.column_id
	FOR XML PATH(''), TYPE
).value('.', 'nvarchar(max)'), 1, 1, '') AS referenced_columns,
CASE
	WHEN EXISTS (
		SELECT 1
		FROM sys.indexes AS ix JOIN sys.index_columns AS ic ON ic.object_id = ix.object_id AND ic.index_id = ix.index_id
		WHERE ix.object_id = fk.referenced_object_id AND ix.is_hypothetical = 0
		GROUP BY ix.object_id, ix.index_id
		HAVING COUNT(*) >= (
			SELECT COUNT(*)
			FROM sys.foreign_key_columns AS fkc
			WHERE fkc.constraint_object_id = fk.object_id
		)
	) THEN 1 ELSE 0
END AS has_index_on_parent
INTO #DTR_FK_NoIndex_Parent
FROM sys.foreign_keys AS fk
JOIN sys.tables AS tr ON tr.object_id = fk.referenced_object_id
WHERE fk.is_disabled = 0 AND fk.is_ms_shipped = 0
ORDER BY referenced_table, fk_name;
GO

------------------------------------------------------------
-- 13c. Fk Type / Collation Mismatch (Heuristic)
------------------------------------------------------------
PRINT N'▶ 13c. Fk Type / Collation Mismatch (Heuristic) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_FK_TypeCollationMismatch') IS NOT NULL DROP TABLE #DTR_FK_TypeCollationMismatch
GO

SELECT
IDENTITY(int) AS RowNumber,
SCHEMA_NAME(cp.schema_id) + '.' + OBJECT_NAME(fkc.parent_object_id) AS ChildTable,
cpc.name AS ChildColumn,
SCHEMA_NAME(cr.schema_id) + '.' + OBJECT_NAME(fkc.referenced_object_id) AS ParentTable,
cpr.name AS ParentColumn,
tpc.name AS ChildType,
tpr.name AS ParentType,
cpc.max_length AS ChildLen,
cpr.max_length AS ParentLen,
cpc.precision AS ChildPrec,
cpr.precision AS ParentPrec,
cpc.scale AS ChildScale,
cpr.scale AS ParentScale,
cpc.collation_name AS ChildCollation,
cpr.collation_name AS ParentCollation,
CASE WHEN tpc.user_type_id <> tpr.user_type_id
	OR cpc.max_length <> cpr.max_length
	OR cpc.precision <> cpr.precision
	OR cpc.scale <> cpr.scale
	THEN 1 ELSE 0 END AS TypeLengthMismatch,
CASE WHEN cpc.collation_name IS NOT NULL AND cpr.collation_name IS NOT NULL
	AND cpc.collation_name <> cpr.collation_name
	THEN 1 ELSE 0 END AS CollationMismatch
INTO #DTR_FK_TypeCollationMismatch
FROM sys.foreign_key_columns AS fkc
JOIN sys.columns AS cpc ON cpc.object_id = fkc.parent_object_id AND cpc.column_id = fkc.parent_column_id
JOIN sys.columns AS cpr ON cpr.object_id = fkc.referenced_object_id AND cpr.column_id = fkc.referenced_column_id
JOIN sys.types AS tpc ON tpc.user_type_id = cpc.user_type_id
JOIN sys.types AS tpr ON tpr.user_type_id = cpr.user_type_id
JOIN sys.objects AS cp ON cp.object_id = fkc.parent_object_id
JOIN sys.objects AS cr ON cr.object_id = fkc.referenced_object_id
WHERE cp.type = 'U' AND cr.type = 'U'
	AND cp.is_ms_shipped = 0 AND cr.is_ms_shipped = 0
	AND (
		tpc.user_type_id <> tpr.user_type_id
		OR cpc.max_length <> cpr.max_length
		OR cpc.precision <> cpr.precision
		OR cpc.scale <> cpr.scale
		OR (cpc.collation_name IS NOT NULL AND cpr.collation_name IS NOT NULL AND cpc.collation_name <> cpr.collation_name)
	);
GO

------------------------------------------------------------
-- 13d. Untrusted or Disabled Foreign Keys
------------------------------------------------------------
PRINT N'▶ 13d. Untrusted or Disabled Foreign Keys - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_UntrustedFKs') IS NOT NULL DROP TABLE #DTR_UntrustedFKs
GO

SELECT
IDENTITY(int) AS RowNumber,
SCHEMA_NAME(o.schema_id) + '.' + o.name AS TableName,
fk.name AS ForeignKeyName,
fk.is_not_trusted,
fk.is_disabled
INTO #DTR_UntrustedFKs
FROM sys.foreign_keys AS fk
JOIN sys.objects AS o ON o.object_id = fk.parent_object_id
WHERE o.is_ms_shipped = 0
	AND (fk.is_not_trusted = 1 OR fk.is_disabled = 1);
GO

------------------------------------------------------------
-- 13e. Foreign Keys with Cascades & Trust Status
------------------------------------------------------------
PRINT N'▶ 13e. Foreign Keys with Cascades & Trust Status - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_FKCascades') IS NOT NULL DROP TABLE #DTR_FKCascades;
GO

SELECT
IDENTITY(int) AS RowNumber,
s.name + '.' + o.name AS ParentTable,
rs.name + '.' + ro.name AS ReferencedTable,
fk.name AS ForeignKeyName,
fk.delete_referential_action_desc AS OnDelete,
fk.update_referential_action_desc AS OnUpdate,
fk.is_disabled AS IsDisabled,
fk.is_not_trusted AS IsNotTrusted
INTO #DTR_FKCascades
FROM sys.foreign_keys AS fk
JOIN sys.objects AS o ON o.object_id = fk.parent_object_id
JOIN sys.schemas AS s ON s.schema_id = o.schema_id
JOIN sys.objects AS ro ON ro.object_id = fk.referenced_object_id
JOIN sys.schemas AS rs ON rs.schema_id = ro.schema_id
WHERE o.type = 'U' AND ro.type = 'U';
GO

------------------------------------------------------------
-- 14a. Top Objects by Reads/Writes
------------------------------------------------------------
PRINT N'▶ 14a. Top Objects by Reads/Writes - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_TopObjectsReadWrite') IS NOT NULL DROP TABLE #DTR_TopObjectsReadWrite
GO


IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH usage AS (
		SELECT
		i.object_id,
		SUM(ISNULL(s.user_seeks, 0) + ISNULL(s.user_scans, 0) + ISNULL(s.user_lookups, 0)) AS reads,
		SUM(ISNULL(s.user_updates, 0)) AS writes
		FROM sys.indexes AS i
		LEFT JOIN sys.dm_db_index_usage_stats AS s ON s.object_id = i.object_id AND s.index_id = i.index_id AND s.database_id = DB_ID()
		WHERE OBJECTPROPERTY(i.object_id, 'IsUserTable') = 1
		GROUP BY i.object_id
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	QUOTENAME(OBJECT_SCHEMA_NAME(u.object_id)) + '.' + QUOTENAME(OBJECT_NAME(u.object_id)) AS object_name,
	u.reads,
	u.writes
	INTO #DTR_TopObjectsReadWrite
	FROM usage AS u
	ORDER BY u.reads + u.writes DESC;
END
GO


------------------------------------------------------------
-- 15a. Backup History
------------------------------------------------------------
PRINT N'▶ 15a. Backup History - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_BackupHistoryTop1') IS NOT NULL DROP TABLE #DTR_BackupHistoryTop1
GO

SELECT TOP (1)
IDENTITY(int) AS RowNumber,
b.backup_start_date,
b.backup_finish_date,
CASE b.type WHEN 'D' THEN 'FULL' WHEN 'I' THEN 'DIFF' WHEN 'L' THEN 'LOG' ELSE b.type END AS backup_type,
b.backup_size,
mf.physical_device_name
INTO #DTR_BackupHistoryTop1
FROM msdb.dbo.backupset AS b
LEFT JOIN msdb.dbo.backupmediafamily AS mf ON mf.media_set_id = b.media_set_id
WHERE b.database_name = DB_NAME()
ORDER BY b.backup_finish_date DESC;
GO

------------------------------------------------------------
-- 15b. Vlf Count
------------------------------------------------------------
PRINT N'▶ 15b. Vlf Count - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_VLF_Count') IS NOT NULL DROP TABLE #DTR_VLF_Count
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	DECLARE @sql nvarchar(max);

	IF OBJECT_ID('sys.dm_db_log_info') IS NOT NULL
	BEGIN
		SET @sql = N'
			SELECT
			IDENTITY(int) AS RowNumber,
			DB_NAME() AS database_name,
			COUNT(*) AS vlf_count
			INTO #DTR_VLF_Count
			FROM sys.dm_db_log_info(DB_ID());'
	END

	IF OBJECT_ID('sys.dm_db_log_info') IS NULL
	BEGIN
		SET @sql = N'
			IF OBJECT_ID(''tempdb..#DT_LogInfo_16b'') IS NOT NULL DROP TABLE #DT_LogInfo_16b;

			CREATE TABLE #DT_LogInfo_16b (
				FileId int NULL,
				FileSize bigint NULL,
				StartOffset bigint NULL,
				FSeqNo int NULL,
				[Status] int NULL,
				Parity int NULL,
				CreateLSN numeric(25, 0) NULL
			);

			INSERT INTO #DT_LogInfo_16b (
				FileId,
				FileSize,
				StartOffset,
				FSeqNo,
				[Status],
				Parity,
				CreateLSN
			)
			EXEC(''DBCC LOGINFO WITH NO_INFOMSGS'');

			SELECT
			IDENTITY(int) AS RowNumber,
			DB_NAME() AS database_name,
			COUNT(*) AS vlf_count
			INTO #DTR_VLF_Count
			FROM #DT_LogInfo_16b;'
	END

	EXEC sys.sp_executesql
	@sql;
END
GO

------------------------------------------------------------
-- 15c. Log Stats
------------------------------------------------------------
PRINT N'▶ 15c. Log Stats - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_LogStats') IS NOT NULL DROP TABLE #DTR_LogStats
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	IF OBJECT_ID('tempdb..#DT_LogSpace_16c') IS NOT NULL DROP TABLE #DT_LogSpace_16c;

	CREATE TABLE #DT_LogSpace_16c (
		DatabaseName sysname NULL,
		LogSizeMB decimal(18, 2) NULL,
		LogSpaceUsedPct decimal(18, 2) NULL,
		[Status] int NULL
	);

	INSERT INTO #DT_LogSpace_16c (
		DatabaseName,
		LogSizeMB,
		LogSpaceUsedPct,
		[Status]
	)
	EXEC('DBCC SQLPERF(LOGSPACE) WITH NO_INFOMSGS');

	SELECT
	IDENTITY(int) AS RowNumber,
	recovery_model = d.recovery_model_desc,
	log_reuse_wait_desc = d.log_reuse_wait_desc,
	total_log_size_mb = CAST(ls.LogSizeMB AS decimal(18, 1)),
	active_log_size_mb = CAST(ls.LogSizeMB * (ls.LogSpaceUsedPct / 100.0) AS decimal(18, 1))
	INTO #DTR_LogStats
	FROM master.sys.databases AS d
	JOIN #DT_LogSpace_16c AS ls ON ls.DatabaseName = d.name
	WHERE d.database_id = DB_ID();
END
GO

------------------------------------------------------------
-- 15d. Backup Compression Ratio (Last 14 Days)
------------------------------------------------------------
PRINT N'▶ 15d. Backup Compression Ratio (Last 14 Days) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_BackupCompression') IS NOT NULL DROP TABLE #DTR_BackupCompression
GO

IF DB_ID('msdb') IS NOT NULL AND OBJECT_ID('msdb.dbo.backupset') IS NOT NULL
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	b.database_name,
	b.backup_start_date,
	b.type AS backup_type,
	b.backup_size,
	b.compressed_backup_size,
	CASE WHEN b.compressed_backup_size > 0 AND b.backup_size > 0
			THEN CAST((1.0 - (CONVERT(decimal(19,4), b.compressed_backup_size) / b.backup_size)) * 100.0 AS decimal(5,2))
			ELSE NULL END AS compression_savings_pct
	INTO #DTR_BackupCompression
	FROM msdb.dbo.backupset AS b
	WHERE b.database_name = DB_NAME()
	AND b.backup_start_date >= DATEADD(day, -14, SYSDATETIME())
	AND b.type IN ('D','I','L');
END
GO

------------------------------------------------------------
-- 15e. Backup Checksums (Last 14 Days)
------------------------------------------------------------
PRINT N'▶ 15e. Backup Checksums (Last 14 Days) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_BackupChecksums') IS NOT NULL DROP TABLE #DTR_BackupChecksums
GO

IF DB_ID('msdb') IS NOT NULL AND OBJECT_ID('msdb.dbo.backupset') IS NOT NULL
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	b.database_name,
	b.backup_start_date,
	b.type AS backup_type,
	b.has_backup_checksums,
	b.is_damaged,
	b.is_copy_only
INTO #DTR_BackupChecksums
FROM msdb.dbo.backupset AS b
WHERE b.database_name = DB_NAME()
AND b.backup_start_date >= DATEADD(day, -14, SYSDATETIME())
AND b.type IN ('D','I','L');
END
GO

------------------------------------------------------------
-- 16a. Lob Usage
------------------------------------------------------------
PRINT N'▶ 16a. Lob Usage - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_LOB_Usage') IS NOT NULL DROP TABLE #DTR_LOB_Usage
GO

SELECT TOP (50)
IDENTITY(int) AS RowNumber,
object_name = QUOTENAME(OBJECT_SCHEMA_NAME(t.object_id)) + '.' + QUOTENAME(t.name),
lob_reserved_mb = CAST(SUM(CASE WHEN au.type = 3 THEN au.total_pages ELSE 0 END) * 8 / 1024.0 AS DECIMAL(18,1)),
row_overflow_reserved_mb = CAST(SUM(CASE WHEN au.type = 4 THEN au.total_pages ELSE 0 END) * 8 / 1024.0 AS DECIMAL(18,1)),
total_lob_reserved_mb = CAST(SUM(CASE WHEN au.type IN (3,4) THEN au.total_pages ELSE 0 END) * 8 / 1024.0 AS DECIMAL(18,1))
INTO #DTR_LOB_Usage
FROM sys.allocation_units AS au
JOIN sys.partitions AS p ON au.container_id = p.partition_id
JOIN sys.tables AS t ON p.object_id = t.object_id
WHERE au.type IN (3,4) -- LOB data or row-overflow data
GROUP BY t.object_id, t.name
ORDER BY total_lob_reserved_mb DESC;
GO

------------------------------------------------------------
-- 16b. Check Constraints
------------------------------------------------------------
PRINT N'▶ 16b. Check Constraints - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_Check_Constraints') IS NOT NULL DROP TABLE #DTR_Check_Constraints
GO

SELECT
IDENTITY(int) AS RowNumber,
QUOTENAME(OBJECT_SCHEMA_NAME(t.object_id)) + '.' + QUOTENAME(t.name) AS table_name,
cc.name AS constraint_name,
cc.is_disabled,
cc.is_not_for_replication,
cc.is_not_trusted,
cc.definition
INTO #DTR_Check_Constraints
FROM sys.check_constraints AS cc
JOIN sys.tables AS t ON t.object_id = cc.parent_object_id
WHERE t.is_ms_shipped = 0
ORDER BY table_name, constraint_name;
GO

------------------------------------------------------------
-- 16c. Sparse Columns Inventory (Optional)
------------------------------------------------------------
PRINT N'▶ 16c. Sparse Columns Inventory (Optional) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_SparseColumns') IS NOT NULL DROP TABLE #DTR_SparseColumns
GO

SELECT
IDENTITY(int) AS RowNumber,
SCHEMA_NAME(t.schema_id) + '.' + t.name AS TableName,
c.name AS ColumnName,
TYPE_NAME(c.user_type_id) AS DataType,
c.is_sparse,
c.is_column_set
INTO #DTR_SparseColumns
FROM sys.tables AS t
JOIN sys.columns AS c ON c.object_id = t.object_id
WHERE t.is_ms_shipped = 0
	AND (c.is_sparse = 1 OR c.is_column_set = 1);
GO

------------------------------------------------------------
-- 17a. Partition Functions
------------------------------------------------------------
PRINT N'▶ 17a. Partition Functions - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_Partition_Functions') IS NOT NULL DROP TABLE #DTR_Partition_Functions
GO

SELECT
IDENTITY(int) AS RowNumber,
pf.name AS partition_function,
pf.type_desc AS boundary_type,
prv.boundary_id,
prv.value AS boundary_value
INTO #DTR_Partition_Functions
FROM sys.partition_functions AS pf
LEFT JOIN sys.partition_range_values AS prv ON prv.function_id = pf.function_id
ORDER BY pf.name, prv.boundary_id;
GO

------------------------------------------------------------
-- 17b. Partition Schemes
------------------------------------------------------------
PRINT N'▶ 17b. Partition Schemes - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_Partition_Schemes') IS NOT NULL DROP TABLE #DTR_Partition_Schemes
GO

SELECT
IDENTITY(int) AS RowNumber,
partition_scheme = ps.name,
partition_scheme_id = ps.data_space_id,
partition_function = pf.name,
partition_number = dds.destination_id,
filegroup = ds.name
INTO #DTR_Partition_Schemes
FROM sys.partition_schemes AS ps
JOIN sys.partition_functions AS pf ON pf.function_id = ps.function_id
JOIN sys.destination_data_spaces AS dds ON dds.partition_scheme_id = ps.data_space_id
JOIN sys.data_spaces AS ds ON ds.data_space_id = dds.data_space_id
ORDER BY ps.name, dds.destination_id;
GO

------------------------------------------------------------
-- 17c. Partitioned Objects
------------------------------------------------------------
PRINT N'▶ 17c. Partitioned Objects - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_Partitioned_Objects') IS NOT NULL DROP TABLE #DTR_Partitioned_Objects
GO

SELECT
IDENTITY(int) AS RowNumber,
QUOTENAME(OBJECT_SCHEMA_NAME(o.object_id)) + '.' + QUOTENAME(o.name) AS object_name,
i.name AS index_name,
ps.name AS partition_scheme,
i.data_space_id,
p.partition_number,
p.rows
INTO #DTR_Partitioned_Objects
FROM sys.objects AS o
JOIN sys.indexes AS i ON i.object_id = o.object_id
JOIN sys.partition_schemes AS ps ON ps.data_space_id = i.data_space_id
JOIN sys.partitions AS p ON p.object_id = o.object_id AND p.index_id = i.index_id
WHERE o.type IN ('U','V')
ORDER BY object_name, index_name, p.partition_number;
GO

------------------------------------------------------------
-- 17d. Non-Aligned Indexes on Partitioned Tables
------------------------------------------------------------
PRINT N'▶ 17d. Non-Aligned Indexes on Partitioned Tables - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_NonAlignedIndexes') IS NOT NULL DROP TABLE #DTR_NonAlignedIndexes
GO

;WITH base AS ( -- base heap/cluster placement (partition scheme-backed)
	SELECT
	o.object_id,
	i.data_space_id AS base_ds_id
	FROM sys.objects AS o
	JOIN sys.indexes AS i ON i.object_id = o.object_id AND i.index_id IN (0,1)
	JOIN sys.data_spaces AS ds ON ds.data_space_id = i.data_space_id AND ds.type_desc = 'PARTITION_SCHEME'
	WHERE o.is_ms_shipped = 0 AND o.type = 'U'
),
idx AS (
	SELECT
	i.object_id,
	i.index_id,
	i.name AS index_name,
	i.type_desc,
	i.data_space_id AS idx_ds_id
	FROM sys.indexes AS i
	WHERE i.index_id > 0 AND EXISTS (
		SELECT
		1
		FROM base
		WHERE base.object_id = i.object_id
	)
)
SELECT
IDENTITY(int) AS RowNumber,
SCHEMA_NAME(o.schema_id) + '.' + o.name AS TableName,
x.index_name,
x.type_desc,
CASE WHEN ds_idx.type_desc <> 'PARTITION_SCHEME' THEN 1 ELSE 0 END AS IndexNotPartitioned,
CASE WHEN x.idx_ds_id <> b.base_ds_id THEN 1 ELSE 0 END AS IsNonAligned
INTO #DTR_NonAlignedIndexes
FROM base AS b
JOIN sys.objects AS o ON o.object_id = b.object_id
JOIN idx AS x ON x.object_id = b.object_id
JOIN sys.data_spaces AS ds_idx ON ds_idx.data_space_id = x.idx_ds_id
WHERE (ds_idx.type_desc <> 'PARTITION_SCHEME' OR x.idx_ds_id <> b.base_ds_id);
GO

------------------------------------------------------------
-- 17e. Partition Skew Summary (Base Table/Cluster Only)
------------------------------------------------------------
PRINT N'▶ 17e. Partition Skew Summary (Base Table/Cluster Only) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_PartitionSkew') IS NOT NULL DROP TABLE #DTR_PartitionSkew
GO

;WITH ps AS (
	SELECT
	object_id,
	partition_number,
	SUM(row_count) AS row_count
FROM sys.dm_db_partition_stats
WHERE index_id IN (0,1) -- heap/clustered index only
	GROUP BY object_id, partition_number
),
t AS (
	SELECT
	o.object_id,
	SCHEMA_NAME(o.schema_id) + '.' + o.name AS TableName,
	COUNT(*) AS partition_count,
	SUM(ps.row_count) AS total_rows,
	MAX(ps.row_count) AS max_rows,
	MIN(ps.row_count) AS min_rows,
	CONVERT(decimal(18,2), CASE WHEN COUNT(*) > 0 THEN (1.0 * MAX(ps.row_count)) / NULLIF(AVG(CONVERT(decimal(38,0), ps.row_count)),0) ELSE NULL END) AS max_to_avg_ratio
	FROM ps
	JOIN sys.objects AS o ON o.object_id = ps.object_id
	WHERE o.is_ms_shipped = 0
	GROUP BY o.object_id, o.schema_id, o.name
)
SELECT
IDENTITY(int) AS RowNumber,
TableName,
partition_count,
total_rows,
max_rows,
min_rows,
max_to_avg_ratio
INTO #DTR_PartitionSkew
FROM t
WHERE partition_count > 1;
GO

------------------------------------------------------------
-- 18a. Index Key Width & Column Counts
------------------------------------------------------------
PRINT N'▶ 18a. Index Key Width & Column Counts - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_Index_KeySize') IS NOT NULL DROP TABLE #DTR_Index_KeySize
GO

SELECT
IDENTITY(int) AS RowNumber,
QUOTENAME(OBJECT_SCHEMA_NAME(i.object_id)) + '.' + QUOTENAME(OBJECT_NAME(i.object_id)) AS object_name,
i.name AS index_name,
SUM(CASE WHEN ic.is_included_column = 0 THEN 1 ELSE 0 END) AS key_columns,
SUM(CASE WHEN ic.is_included_column = 1 THEN 1 ELSE 0 END) AS include_columns,
SUM(CASE WHEN ic.is_included_column = 0 THEN COALESCE(COL_LENGTH(i.object_id, c.name), 0) ELSE 0 END) AS approx_key_bytes
INTO #DTR_Index_KeySize
FROM sys.indexes AS i
JOIN sys.index_columns AS ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
JOIN sys.columns AS c ON c.object_id = i.object_id AND c.column_id = ic.column_id
JOIN sys.objects AS o ON o.object_id = i.object_id
WHERE i.index_id > 0
	AND o.is_ms_shipped = 0
GROUP BY i.object_id, i.name
ORDER BY approx_key_bytes DESC;
GO

------------------------------------------------------------
-- 18b. Index Option Anomalies
------------------------------------------------------------
PRINT N'▶ 18b. Index Option Anomalies - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_IndexOptionAnomalies') IS NOT NULL DROP TABLE #DTR_IndexOptionAnomalies
GO

SELECT
IDENTITY(int) AS RowNumber,
s.name AS schema_name,
o.name AS object_name,
i.name AS index_name,
i.type_desc,
i.fill_factor,
i.is_padded,
i.ignore_dup_key,
i.allow_row_locks,
i.allow_page_locks,
i.has_filter,
i.is_hypothetical,
pc.data_compression_desc
INTO #DTR_IndexOptionAnomalies
FROM sys.indexes AS i
JOIN sys.objects AS o ON o.object_id = i.object_id
JOIN sys.schemas AS s ON s.schema_id = o.schema_id
OUTER APPLY (
	SELECT TOP (1)
	p.data_compression_desc
	FROM sys.partitions AS p
	WHERE p.object_id = i.object_id AND p.index_id = i.index_id
) AS pc
WHERE o.is_ms_shipped = 0
	AND i.index_id > 0
	AND (
		i.fill_factor NOT IN (0,100)
	OR i.is_padded = 1
	OR i.ignore_dup_key = 1
	OR i.allow_row_locks = 0
	OR i.allow_page_locks = 0
	OR i.is_hypothetical = 1
	OR i.has_filter = 1
	OR (pc.data_compression_desc IS NOT NULL AND pc.data_compression_desc <> 'NONE')
	);
GO

------------------------------------------------------------
-- 18c. Indexes with Non-Default Fillfactor / PAD_INDEX
------------------------------------------------------------
PRINT N'▶ 18c. Indexes with Non-Default Fillfactor / PAD_INDEX - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_NonDefaultFillfactor') IS NOT NULL DROP TABLE #DTR_NonDefaultFillfactor
GO

SELECT
IDENTITY(int) AS RowNumber,
SCHEMA_NAME(o.schema_id) + '.' + o.name AS TableName,
i.name AS IndexName,
i.type_desc,
i.fill_factor,
i.is_padded
INTO #DTR_NonDefaultFillfactor
FROM sys.indexes AS i
JOIN sys.objects AS o ON o.object_id = i.object_id
WHERE o.is_ms_shipped = 0
	AND i.index_id > 0
	AND (i.fill_factor NOT IN (0,100) OR i.is_padded = 1);
GO

------------------------------------------------------------
-- 18d. Hypothetical Indexes (Leftover Dta)
------------------------------------------------------------
PRINT N'▶ 18d. Hypothetical Indexes (Leftover Dta) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_HypotheticalIndexes') IS NOT NULL DROP TABLE #DTR_HypotheticalIndexes
GO

SELECT
IDENTITY(int) AS RowNumber,
SCHEMA_NAME(o.schema_id) + '.' + o.name AS TableName,
i.name AS IndexName,
i.type_desc,
i.is_hypothetical,
i.is_disabled
INTO #DTR_HypotheticalIndexes
FROM sys.indexes AS i
JOIN sys.objects AS o ON o.object_id = i.object_id
WHERE o.is_ms_shipped = 0
	AND i.is_hypothetical = 1;
GO

------------------------------------------------------------
-- 19a. Compression Candidates
------------------------------------------------------------
PRINT N'▶ 19a. Compression Candidates - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_Compression_Candidates') IS NOT NULL DROP TABLE #DTR_Compression_Candidates
GO

SELECT
IDENTITY(int) AS RowNumber,
QUOTENAME(OBJECT_SCHEMA_NAME(o.object_id)) + '.' + QUOTENAME(o.name) AS object_name,
i.name AS index_name,
ips.index_type_desc,
ips.page_count,
ips.avg_page_space_used_in_percent
INTO #DTR_Compression_Candidates
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'SAMPLED') AS ips
JOIN sys.objects AS o ON o.object_id = ips.object_id
JOIN sys.indexes AS i ON i.object_id = ips.object_id AND i.index_id = ips.index_id
WHERE ips.page_count >= 1000 AND ips.avg_page_space_used_in_percent < 75
ORDER BY ips.avg_page_space_used_in_percent ASC, ips.page_count DESC;
GO

------------------------------------------------------------
-- 19b. Compression Inventory (Rowstore Partitions)
------------------------------------------------------------
PRINT N'▶ 19b. Compression Inventory (Rowstore Partitions) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_CompressionInventory') IS NOT NULL DROP TABLE #DTR_CompressionInventory;
GO

SELECT
IDENTITY(int) AS RowNumber,
s.name AS schema_name,
o.name AS object_name,
CASE WHEN i.index_id = 0 THEN '(HEAP)' ELSE i.name END AS index_name,
i.type_desc AS index_type_desc,
p.partition_number,
p.rows AS row_count,
p.data_compression_desc,
CAST(SUM(au.total_pages) * 8.0 / 1024.0 AS decimal(18,2)) AS size_mb
INTO #DTR_CompressionInventory
FROM sys.partitions AS p
JOIN sys.indexes AS i ON i.object_id = p.object_id AND i.index_id = p.index_id
JOIN sys.objects AS o ON o.object_id = p.object_id
JOIN sys.schemas AS s ON s.schema_id = o.schema_id
JOIN sys.allocation_units AS au ON au.container_id = p.partition_id
WHERE o.is_ms_shipped = 0 AND i.type NOT IN (5, 6)
GROUP BY
s.name,
o.name,
CASE WHEN i.index_id = 0 THEN '(HEAP)' ELSE i.name END,
i.type_desc,
p.partition_number,
p.rows,
p.data_compression_desc;
GO

------------------------------------------------------------
-- 20a. Object Types
------------------------------------------------------------
PRINT N'▶ 20a. Object Types - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_Object_Types') IS NOT NULL DROP TABLE #DTR_Object_Types
GO

SELECT
IDENTITY(int) AS RowNumber,
type_desc,
COUNT(*) AS count_objects
INTO #DTR_Object_Types
FROM sys.objects
GROUP BY type_desc
ORDER BY type_desc;
GO

------------------------------------------------------------
-- 20b. CLR Assemblies (Permission Set)
------------------------------------------------------------
PRINT N'▶ 20b. CLR Assemblies (Permission Set) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_CLR_Assemblies') IS NOT NULL DROP TABLE #DTR_CLR_Assemblies
GO

SELECT
IDENTITY(int) AS RowNumber,
a.name,
a.is_user_defined,
a.permission_set_desc,
a.create_date,
a.modify_date
INTO #DTR_CLR_Assemblies
FROM sys.assemblies AS a
WHERE a.is_user_defined = 1;
GO

------------------------------------------------------------
-- 20c. Lock Escalation Settings
------------------------------------------------------------
PRINT N'▶ 20c. Lock Escalation Settings - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_LockEscalation') IS NOT NULL DROP TABLE #DTR_LockEscalation;
GO

SELECT
IDENTITY(int) AS RowNumber,
SCHEMA_NAME(t.schema_id) + '.' + t.name AS TableName,
t.lock_escalation_desc AS LockEscalation
INTO #DTR_LockEscalation
FROM sys.tables AS t
WHERE t.is_ms_shipped = 0;
GO

------------------------------------------------------------
-- 21a. Top Procedures ExecStats
------------------------------------------------------------
PRINT N'▶ 21a. Top Procedures ExecStats - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_Top_Procedures_ExecStats') IS NOT NULL DROP TABLE #DTR_Top_Procedures_ExecStats
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT TOP (100)
	IDENTITY(int) AS RowNumber,
	DB_NAME(st.dbid) AS db_name,
	OBJECT_SCHEMA_NAME(st.objectid, st.dbid) AS schema_name,
	OBJECT_NAME(st.objectid, st.dbid) AS object_name,
	qs.total_worker_time / 1000 AS total_cpu_ms,
	qs.total_logical_reads AS total_reads,
	qs.total_logical_writes AS total_writes,
	qs.total_elapsed_time / 1000 AS total_duration_ms,
	qs.last_execution_time AS last_execution_time,
	qs.execution_count AS executions,
	(qs.total_worker_time / 1000) / NULLIF(qs.execution_count, 0) AS avg_cpu_ms,
	(qs.total_elapsed_time / 1000) / NULLIF(qs.execution_count, 0) AS avg_duration_ms
	INTO #DTR_Top_Procedures_ExecStats
	FROM sys.dm_exec_procedure_stats AS qs
	OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
	WHERE st.dbid = DB_ID() AND qs.object_id IS NOT NULL
	ORDER BY qs.total_worker_time DESC;
END
GO

------------------------------------------------------------
-- 21b. Top Triggers ExecStats
------------------------------------------------------------
PRINT N'▶ 21b. Top Triggers ExecStats - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_Top_Triggers_ExecStats') IS NOT NULL DROP TABLE #DTR_Top_Triggers_ExecStats
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT TOP (100)
	IDENTITY(int) AS RowNumber,
	DB_NAME(st.dbid) AS db_name,
	OBJECT_SCHEMA_NAME(st.objectid, st.dbid) AS schema_name,
	OBJECT_NAME(st.objectid, st.dbid) AS object_name,
	o.type_desc AS trigger_type,
	qs.total_worker_time / 1000 AS total_cpu_ms,
	qs.total_logical_reads AS total_reads,
	qs.total_logical_writes AS total_writes,
	qs.total_elapsed_time / 1000 AS total_duration_ms,
	qs.last_execution_time AS last_execution_time,
	qs.execution_count AS executions,
	(qs.total_worker_time / 1000) / NULLIF(qs.execution_count, 0) AS avg_cpu_ms,
	(qs.total_elapsed_time / 1000) / NULLIF(qs.execution_count, 0) AS avg_duration_ms
	INTO #DTR_Top_Triggers_ExecStats
	FROM sys.dm_exec_trigger_stats AS qs
	OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
	JOIN sys.objects AS o ON o.object_id = qs.object_id
	WHERE st.dbid = DB_ID()
	ORDER BY qs.total_worker_time DESC;
END
GO

------------------------------------------------------------
-- 21c. Procedures with Recompile (Definition or Option)
------------------------------------------------------------
PRINT N'▶ 21c. Procedures with Recompile (Definition or Option) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_ProcRecompile') IS NOT NULL DROP TABLE #DTR_ProcRecompile
GO

;WITH m AS (
	SELECT
	p.object_id,
	m.definition
	FROM sys.procedures AS p
	JOIN sys.sql_modules AS m ON m.object_id = p.object_id
	WHERE p.is_ms_shipped = 0
)
SELECT
IDENTITY(int) AS RowNumber,
SCHEMA_NAME(p.schema_id) + '.' + p.name AS ProcName,
	CASE WHEN OBJECTPROPERTY(p.object_id, 'IsRecompiled') = 1 THEN 1 ELSE 0 END AS with_recompile,
	CASE WHEN m.definition LIKE '%OPTION%RECOMPILE%' THEN 1 ELSE 0 END AS has_option_recompile
 INTO #DTR_ProcRecompile
 FROM sys.procedures AS p
 JOIN m ON m.object_id = p.object_id;
GO

------------------------------------------------------------
-- 21d. Disabled Triggers (Ddl & Dml)
------------------------------------------------------------
PRINT N'▶ 21d. Disabled Triggers (Ddl & Dml) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_DisabledTriggers') IS NOT NULL DROP TABLE #DTR_DisabledTriggers;
GO

SELECT
IDENTITY(int) AS RowNumber,
tr.name AS TriggerName,
CASE tr.parent_class WHEN 0 THEN 'DATABASE' WHEN 1 THEN 'OBJECT' ELSE CONVERT(nvarchar(10), tr.parent_class) END AS ParentClass,
CASE WHEN tr.parent_class = 1 THEN SCHEMA_NAME(o.schema_id) + '.' + o.name ELSE DB_NAME() END AS ParentName,
tr.is_disabled AS IsDisabled
INTO #DTR_DisabledTriggers
FROM sys.triggers AS tr
LEFT JOIN sys.objects AS o ON tr.parent_class = 1 AND o.object_id = tr.parent_id
WHERE tr.is_disabled = 1;
GO

------------------------------------------------------------
-- 21e. Modules Referencing xp_cmdshell / Ole Automation
------------------------------------------------------------
PRINT N'▶ 21e. Modules Referencing xp_cmdshell / Ole Automation - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_ModulesRiskyCalls') IS NOT NULL DROP TABLE #DTR_ModulesRiskyCalls;
GO

SELECT
IDENTITY(int) AS RowNumber,
OBJECT_SCHEMA_NAME(m.object_id) AS SchemaName,
OBJECT_NAME(m.object_id) AS ObjectName,
o.type_desc AS ObjectType,
CASE WHEN m.definition LIKE '%xp_cmdshell%' COLLATE DATABASE_DEFAULT THEN 1 ELSE 0 END AS Calls_xp_cmdshell,
CASE WHEN m.definition LIKE '%sp_OA%' COLLATE DATABASE_DEFAULT THEN 1 ELSE 0 END AS Calls_OLEAutomation
INTO #DTR_ModulesRiskyCalls
FROM sys.sql_modules AS m
JOIN sys.objects AS o ON o.object_id = m.object_id
WHERE o.is_ms_shipped = 0
	AND (m.definition LIKE '%xp_cmdshell%' COLLATE DATABASE_DEFAULT
		OR m.definition LIKE '%sp_OA%' COLLATE DATABASE_DEFAULT);
GO

------------------------------------------------------------
-- 21f. Modules Using Nolock / Readuncommitted
------------------------------------------------------------
PRINT N'▶ 21f. Modules Using Nolock / Readuncommitted - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_ModulesNoLock') IS NOT NULL DROP TABLE #DTR_ModulesNoLock;
GO

SELECT
IDENTITY(int) AS RowNumber,
OBJECT_SCHEMA_NAME(m.object_id) AS SchemaName,
OBJECT_NAME(m.object_id) AS ObjectName,
o.type_desc AS ObjectType,
CASE WHEN m.definition LIKE '%WITH (NOLOCK)%' COLLATE DATABASE_DEFAULT
		OR m.definition LIKE '%READUNCOMMITTED%' COLLATE DATABASE_DEFAULT
	THEN 1 ELSE 0 END AS UsesNoLock
INTO #DTR_ModulesNoLock
FROM sys.sql_modules AS m
JOIN sys.objects AS o ON o.object_id = m.object_id
WHERE o.is_ms_shipped = 0
	AND (m.definition LIKE '%WITH (NOLOCK)%' COLLATE DATABASE_DEFAULT
		OR m.definition LIKE '%READUNCOMMITTED%' COLLATE DATABASE_DEFAULT);
GO

------------------------------------------------------------
-- 21g. Modules With Legacy SET Options (QUOTED_IDENTIFIER OFF / ANSI_NULLS OFF)
------------------------------------------------------------
PRINT N'▶ 21g. Modules With Legacy SET Options (QUOTED_IDENTIFIER OFF / ANSI_NULLS OFF) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_ModulesLegacySetOptions') IS NOT NULL DROP TABLE #DTR_ModulesLegacySetOptions
GO

SELECT
IDENTITY(int) AS RowNumber,
SCHEMA_NAME(o.schema_id) + '.' + QUOTENAME(o.name) AS object_name,
o.type_desc,
m.uses_quoted_identifier,
m.uses_ansi_nulls,
o.create_date,
o.modify_date
INTO #DTR_ModulesLegacySetOptions
FROM sys.sql_modules AS m
JOIN sys.objects AS o ON o.object_id = m.object_id
WHERE o.type IN ('P','FN','IF','TF','V','TR') -- procedures, scalar/inline/MS TVFs, views, triggers
	AND (m.uses_quoted_identifier = 0 OR m.uses_ansi_nulls = 0)
	AND o.is_ms_shipped = 0;
GO

------------------------------------------------------------
-- 21h. Loaded Binary Modules (Non-Microsoft)
------------------------------------------------------------
PRINT N'▶ 21h. Loaded Binary Modules (Non-Microsoft) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_LoadedModules') IS NOT NULL DROP TABLE #DTR_LoadedModules;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	name,
	company,
	description,
	file_version,
	product_version,
	debug,
	patched,
	prerelease,
	private_build,
	special_build,
	language,
	base_address
	INTO #DTR_LoadedModules
	FROM sys.dm_os_loaded_modules
	WHERE company NOT LIKE 'Microsoft%';
END
GO

------------------------------------------------------------
-- 22a. Security Policies
------------------------------------------------------------
PRINT N'▶ 22a. Security Policies - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_Security_Policies') IS NOT NULL DROP TABLE #DTR_Security_Policies
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH src AS (
		SELECT
		feature = 'CDC',
		detail = CONVERT(NVARCHAR(256), d.name) COLLATE DATABASE_DEFAULT,
		is_enabled = CONVERT(INT, d.is_cdc_enabled)
		FROM sys.databases AS d
		WHERE d.database_id = DB_ID()
		UNION ALL
		SELECT
		feature = 'TDE',
		detail = CONVERT(NVARCHAR(256), NULL) COLLATE DATABASE_DEFAULT,
		is_enabled = CONVERT(INT, CASE WHEN EXISTS (
			SELECT
			1
			FROM sys.dm_database_encryption_keys
			WHERE database_id = DB_ID()
		) THEN 1 ELSE 0 END)
	)
	SELECT
	ROW_NUMBER() OVER (ORDER BY feature, detail) AS RowNumber,
	feature,
	detail,
	is_enabled
	INTO #DTR_Security_Policies
	FROM src;
END
GO

------------------------------------------------------------
-- 22b. Encryption (TDE) Status
------------------------------------------------------------
PRINT N'▶ 22b. Encryption (TDE) Status - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_TDE_Status') IS NOT NULL DROP TABLE #DTR_TDE_Status
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	CASE WHEN cfg.SafeMode = 0 THEN DB_NAME(dek.database_id) ELSE '[SafeMode]' END AS database_name,
	dek.encryption_state,
	dek.percent_complete,
	dek.key_algorithm,
	dek.key_length
	INTO #DTR_TDE_Status
	FROM sys.dm_database_encryption_keys AS dek
	CROSS JOIN #DT_Config AS cfg
	WHERE dek.database_id = DB_ID();
END
GO

------------------------------------------------------------
-- 22c. SQL Audit (Server)
------------------------------------------------------------
PRINT N'▶ 22c. SQL Audit (Server) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_AuditServer') IS NOT NULL DROP TABLE #DTR_AuditServer
GO

-- Server-level audits/specs
IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	CASE WHEN cfg.SafeMode = 0 THEN a.name ELSE '[SafeMode]' END AS audit_name,
	a.audit_guid,
	a.type_desc AS audit_type_desc,
	a.is_state_enabled,
	a.queue_delay,
	a.on_failure_desc,
	CASE
		WHEN cfg.SafeMode = 0 THEN sas.name
		WHEN sas.name IS NULL THEN NULL
		ELSE '[SafeMode]'
	END AS specification_name,
	sas.is_state_enabled AS specification_enabled,
	ISNULL(ssd.action_count, 0) AS action_count,
	das.status_desc,
	CASE
		WHEN cfg.SafeMode = 0 THEN das.audit_file_path
		WHEN das.audit_file_path IS NULL THEN NULL
		ELSE '[SafeMode]'
	END AS audit_file_path,
	das.audit_file_size
	INTO #DTR_AuditServer
	FROM sys.server_audits AS a
	CROSS JOIN #DT_Config AS cfg
	LEFT JOIN sys.server_audit_specifications AS sas ON sas.audit_guid = a.audit_guid
	LEFT JOIN (
		SELECT
		server_specification_id,
		COUNT(*) AS action_count
		FROM sys.server_audit_specification_details
		GROUP BY server_specification_id
	) AS ssd ON ssd.server_specification_id = sas.server_specification_id
	LEFT JOIN sys.dm_server_audit_status AS das ON das.audit_id = a.audit_id;
END

GO

------------------------------------------------------------
-- 22d. Service Broker (DB Status & Queues)
------------------------------------------------------------
PRINT N'▶ 22d. Service Broker (DB Status & Queues) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_ServiceBroker') IS NOT NULL DROP TABLE #DTR_ServiceBroker
GO

;WITH dbs AS (
	SELECT
	is_broker_enabled
	FROM sys.databases
	WHERE database_id = DB_ID()
),
q AS (
	SELECT
	name,
	is_activation_enabled,
	is_receive_enabled,
	is_enqueue_enabled,
	is_poison_message_handling_enabled
	FROM sys.service_queues
)
SELECT
IDENTITY(int) AS RowNumber,
DB_NAME() AS DatabaseName,
(SELECT is_broker_enabled FROM dbs) AS is_broker_enabled,
q.name AS QueueName,
q.is_activation_enabled,
q.is_receive_enabled,
q.is_enqueue_enabled,
q.is_poison_message_handling_enabled
INTO #DTR_ServiceBroker
FROM q;
GO

------------------------------------------------------------
-- 22e. TDE Certificate & Expiration (Current DB)
------------------------------------------------------------
PRINT N'▶ 22e. TDE Certificate & Expiration (Current DB) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_TDE_Cert') IS NOT NULL DROP TABLE #DTR_TDE_Cert
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	CASE WHEN cfg.SafeMode = 0 THEN DB_NAME() ELSE '[SafeMode]' END AS DatabaseName,
	dek.key_algorithm,
	dek.key_length,
	CASE WHEN cfg.SafeMode = 0 THEN dek.encryptor_thumbprint ELSE NULL END AS encryptor_thumbprint,
	CASE
		WHEN cfg.SafeMode = 0 THEN c.name
		WHEN c.name IS NULL THEN NULL
		ELSE '[SafeMode]'
	END AS certificate_name,
	c.expiry_date
	INTO #DTR_TDE_Cert
	FROM sys.dm_database_encryption_keys AS dek
	CROSS JOIN #DT_Config AS cfg
	LEFT JOIN master.sys.certificates AS c ON c.thumbprint = dek.encryptor_thumbprint
	WHERE dek.database_id = DB_ID();
END
GO

------------------------------------------------------------
-- 22f. Orphaned Users (Instance-Mapped vs. Contained)
------------------------------------------------------------
PRINT N'▶ 22f. Orphaned Users (Instance-Mapped vs. Contained) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_OrphanedUsers') IS NOT NULL DROP TABLE #DTR_OrphanedUsers
GO

;WITH u AS (
	SELECT
	name,
	type_desc,
	principal_id,
	sid
	FROM sys.database_principals
	WHERE principal_id > 4
	AND type_desc LIKE '%USER%'
	AND name NOT IN ('dbo','guest','INFORMATION_SCHEMA','sys')
)
SELECT
IDENTITY(int) AS RowNumber,
 CASE WHEN cfg.SafeMode = 0 THEN u.name ELSE '[SafeMode]' END AS UserName,
 u.type_desc,
 CASE WHEN sp.sid IS NULL THEN 0 ELSE 1 END AS has_matching_server_login
 INTO #DTR_OrphanedUsers
 FROM u
 CROSS JOIN #DT_Config AS cfg
 LEFT JOIN sys.server_principals AS sp ON sp.sid = u.sid;
GO

------------------------------------------------------------
-- 22g. High-Privilege Role Members (db_owner, Securityadmin, Accessadmin, Ddladmin, Backupoperator)
------------------------------------------------------------
PRINT N'▶ 22g. High-Privilege Role Members (db_owner, Securityadmin, Accessadmin, Ddladmin, Backupoperator) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_HighPrivRoleMembers') IS NOT NULL DROP TABLE #DTR_HighPrivRoleMembers
GO

;WITH roles AS (
	SELECT
	principal_id,
	name
	FROM sys.database_principals
	WHERE name IN ('db_owner','db_securityadmin','db_accessadmin','db_ddladmin','db_backupoperator')
),
m AS (
	SELECT
	drm.role_principal_id,
	drm.member_principal_id
	FROM sys.database_role_members AS drm
)
SELECT
IDENTITY(int) AS RowNumber,
 r.name AS RoleName,
 CASE WHEN cfg.SafeMode = 0 THEN mp.name ELSE '[SafeMode]' END AS MemberName,
 mp.type_desc AS MemberType
 INTO #DTR_HighPrivRoleMembers
 FROM roles AS r
 JOIN m ON m.role_principal_id = r.principal_id
 JOIN sys.database_principals AS mp ON mp.principal_id = m.member_principal_id
 CROSS JOIN #DT_Config AS cfg;
GO

------------------------------------------------------------
-- 22h. Credentials (Server & DB Scopes)
------------------------------------------------------------
PRINT N'▶ 22h. Credentials (Server & DB Scopes) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_Credentials') IS NOT NULL DROP TABLE #DTR_Credentials;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	'SERVER' AS scope_desc,
	CAST(NULL AS sysname) AS database_name,
	CASE WHEN cfg.SafeMode = 0 THEN c.name COLLATE DATABASE_DEFAULT ELSE '[SafeMode]' END AS CredentialName,
	CASE WHEN cfg.SafeMode = 0 THEN c.credential_identity COLLATE DATABASE_DEFAULT ELSE '[SafeMode]' END AS IdentityName,
	c.create_date AS CreateDate,
	c.modify_date AS ModifyDate
	INTO #DTR_Credentials
	FROM sys.credentials AS c
	CROSS JOIN #DT_Config AS cfg;
END
GO

------------------------------------------------------------
-- 22i. High-Risk Server-Level Permissions
------------------------------------------------------------
PRINT N'▶ 22i. High-Risk Server-Level Permissions - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_ServerHighRiskPerms') IS NOT NULL DROP TABLE #DTR_ServerHighRiskPerms;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	CASE WHEN cfg.SafeMode = 0 THEN sp.name ELSE '[SafeMode]' END AS PrincipalName,
	sp.type_desc AS PrincipalType,
	sp.is_disabled AS PrincipalDisabled,
	perm.class_desc AS ClassDesc,
	perm.permission_name,
	perm.state_desc AS GrantState
	INTO #DTR_ServerHighRiskPerms
	FROM sys.server_permissions AS perm
	CROSS JOIN #DT_Config AS cfg
	JOIN sys.server_principals AS sp ON sp.principal_id = perm.grantee_principal_id
	WHERE perm.permission_name IN (
		'CONTROL SERVER', 'IMPERSONATE ANY LOGIN',
		'ALTER ANY LOGIN', 'ALTER ANY DATABASE',
		'UNSAFE ASSEMBLY', 'EXTERNAL ACCESS ASSEMBLY'
	);
END
GO

------------------------------------------------------------
-- 22j. Explicit GRANTs to PUBLIC (DB Scope)
------------------------------------------------------------
PRINT N'▶ 22j. Explicit GRANTs to PUBLIC (DB Scope) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_PublicDbGrants') IS NOT NULL DROP TABLE #DTR_PublicDbGrants;
GO

SELECT
IDENTITY(int) AS RowNumber,
dp.class_desc,
dp.permission_name,
dp.state_desc,
dp.major_id,
 CASE
 	WHEN cfg.SafeMode = 0 THEN OBJECT_SCHEMA_NAME(dp.major_id)
 	WHEN OBJECT_SCHEMA_NAME(dp.major_id) IS NULL THEN NULL
 	ELSE '[SafeMode]'
 END AS ObjectSchema,
 CASE
 	WHEN cfg.SafeMode = 0 THEN OBJECT_NAME(dp.major_id)
 	WHEN OBJECT_NAME(dp.major_id) IS NULL THEN NULL
 	ELSE '[SafeMode]'
 END AS ObjectName
 INTO #DTR_PublicDbGrants
 FROM sys.database_permissions AS dp
 CROSS JOIN #DT_Config AS cfg
 JOIN sys.database_principals AS pr ON pr.principal_id = dp.grantee_principal_id
 WHERE pr.name = 'public';
GO

------------------------------------------------------------
-- 22k. Audit Targets (File/Queue)
------------------------------------------------------------
PRINT N'▶ 22k. Audit Targets (File/Queue) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_AuditTargets') IS NOT NULL DROP TABLE #DTR_AuditTargets;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	CASE WHEN cfg.SafeMode = 0 THEN sa.name ELSE '[SafeMode]' END AS AuditName,
	sa.audit_id,
	sa.type_desc AS AuditType,
	sa.on_failure_desc,
	CASE
		WHEN cfg.SafeMode = 0 THEN ds.audit_file_path
		WHEN ds.audit_file_path IS NULL THEN NULL
		ELSE '[SafeMode]'
	END AS FilePath,
	sa.queue_delay,
	ds.status_desc AS Status
	INTO #DTR_AuditTargets
	FROM sys.server_audits AS sa
	CROSS JOIN #DT_Config AS cfg
	LEFT JOIN sys.dm_server_audit_status AS ds ON ds.audit_id = sa.audit_id
END
GO

------------------------------------------------------------
-- 22l. Database Audit Specifications (Bound to Audits)
------------------------------------------------------------
PRINT N'▶ 22l. Database Audit Specifications (Bound to Audits) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_DatabaseAuditSpecs') IS NOT NULL DROP TABLE #DTR_DatabaseAuditSpecs;
GO

-- DB audit specifications bound to audits
IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	CASE WHEN cfg.SafeMode = 0 THEN das.name ELSE '[SafeMode]' END AS DbAuditSpecName,
	das.audit_guid,
	das.create_date,
	das.modify_date
	INTO #DTR_DatabaseAuditSpecs
	FROM sys.database_audit_specifications AS das
	CROSS JOIN #DT_Config AS cfg;
END
GO

------------------------------------------------------------
-- 22m. SQL Audit (Database Specifications)
------------------------------------------------------------
PRINT N'▶ 22m. SQL Audit (Database Specifications) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_AuditDatabase') IS NOT NULL DROP TABLE #DTR_AuditDatabase;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH das AS (
		SELECT
		database_specification_id,
		name AS DbAuditSpecName,
		is_state_enabled,
		create_date,
		modify_date
		FROM sys.database_audit_specifications
	),
	dasd AS (
		SELECT
		database_specification_id,
		COUNT(*) AS action_count
		FROM sys.database_audit_specification_details
		GROUP BY database_specification_id
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	CASE WHEN cfg.SafeMode = 0 THEN das.DbAuditSpecName ELSE '[SafeMode]' END AS DbAuditSpecName,
	das.is_state_enabled,
	das.create_date,
	das.modify_date,
	ISNULL(dasd.action_count,0) AS action_count
	INTO #DTR_AuditDatabase
	FROM das
	CROSS JOIN #DT_Config AS cfg
	LEFT JOIN dasd ON dasd.database_specification_id = das.database_specification_id;
END
GO

------------------------------------------------------------
-- 23a. Filestream Files
------------------------------------------------------------
PRINT N'▶ 23a. Filestream Files - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_FileStream') IS NOT NULL DROP TABLE #DTR_FileStream;
GO

SELECT
IDENTITY(int) AS RowNumber,
df.name AS FileName,
df.type_desc AS FileTypeDesc,
 CASE WHEN cfg.SafeMode = 0 THEN df.physical_name ELSE '[SafeMode]' END AS PhysicalName,
df.state_desc AS StateDesc
INTO #DTR_FileStream
FROM sys.database_files AS df
CROSS JOIN #DT_Config AS cfg
WHERE df.type_desc = 'FILESTREAM';
GO

------------------------------------------------------------
-- 23b. Synonyms & Their Targets (Server/DB/Schema/Object)
------------------------------------------------------------
PRINT N'▶ 23b. Synonyms & Their Targets (Server/DB/Schema/Object) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_SynonymTargets') IS NOT NULL DROP TABLE #DTR_SynonymTargets;
GO

SELECT
IDENTITY(int) AS RowNumber,
CASE WHEN cfg.SafeMode = 0 THEN s.name ELSE '[SafeMode]' END AS SynonymName,
CASE WHEN cfg.SafeMode = 0 THEN s.base_object_name ELSE '[SafeMode]' END AS BaseObjectName,
CASE
	WHEN cfg.SafeMode = 0 THEN PARSENAME(s.base_object_name, 4)
	WHEN PARSENAME(s.base_object_name, 4) IS NULL THEN NULL
	ELSE '[SafeMode]'
END AS ServerName,
CASE
	WHEN cfg.SafeMode = 0 THEN PARSENAME(s.base_object_name, 3)
	WHEN PARSENAME(s.base_object_name, 3) IS NULL THEN NULL
	ELSE '[SafeMode]'
END AS DatabaseName,
CASE
	WHEN cfg.SafeMode = 0 THEN PARSENAME(s.base_object_name, 2)
	WHEN PARSENAME(s.base_object_name, 2) IS NULL THEN NULL
	ELSE '[SafeMode]'
END AS SchemaName,
CASE
	WHEN cfg.SafeMode = 0 THEN PARSENAME(s.base_object_name, 1)
	WHEN PARSENAME(s.base_object_name, 1) IS NULL THEN NULL
	ELSE '[SafeMode]'
END AS ObjectName
INTO #DTR_SynonymTargets
FROM sys.synonyms AS s
CROSS JOIN #DT_Config AS cfg;
GO

------------------------------------------------------------
-- 23c. Full-Text Posture
------------------------------------------------------------
PRINT N'▶ 23c. Full-Text Posture - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_FullTextCatalogs') IS NOT NULL DROP TABLE #DTR_FullTextCatalogs;
GO

SELECT
IDENTITY(int) AS RowNumber,
ftc.fulltext_catalog_id,
ftc.name,
ftc.is_default,
ftc.is_accent_sensitivity_on
INTO #DTR_FullTextCatalogs
FROM sys.fulltext_catalogs AS ftc;
GO

------------------------------------------------------------
-- 23d. Full-Text - Catalogs & Indexes Posture
------------------------------------------------------------
PRINT N'▶ 23d. Full-Text - Catalogs & Indexes Posture - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_FullTextIndexes') IS NOT NULL DROP TABLE #DTR_FullTextIndexes;
GO

SELECT
IDENTITY(int) AS RowNumber,
OBJECT_SCHEMA_NAME(fti.object_id) AS SchemaName,
OBJECT_NAME(fti.object_id) AS TableName,
fti.is_enabled,
fti.change_tracking_state,
fti.change_tracking_state_desc,
fti.stoplist_id
INTO #DTR_FullTextIndexes
FROM sys.fulltext_indexes AS fti;
GO

------------------------------------------------------------
-- 23e. In-Memory OLTP Candidate Tables (Heuristic)
------------------------------------------------------------
PRINT N'▶ 23e. In-Memory OLTP Candidate Tables (Heuristic) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO
IF OBJECT_ID('tempdb..#DTR_InMemoryCandidates') IS NOT NULL DROP TABLE #DTR_InMemoryCandidates;
GO

	IF (SELECT IsSysAdmin FROM #DT_Config) = 1
	BEGIN
		;WITH usage AS (
			SELECT
			i.object_id,
			reads = SUM(ISNULL(us.user_seeks,0) + ISNULL(us.user_scans,0) + ISNULL(us.user_lookups,0)),
			writes = SUM(ISNULL(us.user_updates,0))
			FROM sys.indexes AS i
			LEFT JOIN sys.dm_db_index_usage_stats AS us
				ON us.database_id = DB_ID()
				AND us.object_id = i.object_id
				AND us.index_id = i.index_id
			WHERE OBJECTPROPERTY(i.object_id, 'IsUserTable') = 1
			GROUP BY i.object_id
	),
	ps AS (
		SELECT
		object_id,
		row_count = SUM(CASE WHEN index_id IN (0,1) THEN row_count ELSE 0 END),
		data_mb = SUM(CASE WHEN index_id IN (0,1) THEN (used_page_count*8.0/1024.0) ELSE 0 END)
		FROM sys.dm_db_partition_stats
		GROUP BY object_id
	),
	cols AS (
		SELECT
		t.object_id,
		has_lob = MAX(CASE WHEN (c.system_type_id IN (35,99,34,241)) OR ((c.system_type_id IN (167,231,165)) AND c.max_length = -1) THEN 1 ELSE 0 END),
		has_rowversion = MAX(CASE WHEN c.system_type_id = 189 THEN 1 ELSE 0 END),
		has_computed = MAX(CASE WHEN c.is_computed = 1 THEN 1 ELSE 0 END)
		FROM sys.tables AS t
		JOIN sys.columns AS c ON c.object_id = t.object_id
		WHERE t.is_ms_shipped = 0
		GROUP BY t.object_id
	),
	mi AS (
		SELECT
		mid.object_id
		FROM sys.dm_db_missing_index_groups AS g
		JOIN sys.dm_db_missing_index_group_stats AS gs ON gs.group_handle = g.index_group_handle
		JOIN sys.dm_db_missing_index_details AS mid ON mid.index_handle = g.index_handle
		WHERE mid.database_id = DB_ID()
		GROUP BY mid.object_id
),
score AS (
	SELECT
	t.object_id,
	reads = ISNULL(u.reads,0),
	writes = ISNULL(u.writes,0),
	total_accesses = ISNULL(u.reads,0) + ISNULL(u.writes,0),
	write_pct = CONVERT(decimal(5,2), CASE WHEN (ISNULL(u.reads,0)+ISNULL(u.writes,0)) > 0 THEN (100.0 * ISNULL(u.writes,0)) / (ISNULL(u.reads,0)+ISNULL(u.writes,0)) END),
	row_count = ISNULL(p.row_count,0),
	data_mb = CONVERT(decimal(18,2), ISNULL(p.data_mb,0)),
	has_lob = ISNULL(c.has_lob,0),
	has_rowversion = ISNULL(c.has_rowversion,0),
	has_computed = ISNULL(c.has_computed,0),
	has_missing_index = CASE WHEN EXISTS (SELECT 1 FROM mi WHERE mi.object_id = t.object_id) THEN 1 ELSE 0 END
	FROM sys.tables AS t
	LEFT JOIN usage AS u ON u.object_id = t.object_id
	LEFT JOIN ps AS p ON p.object_id = t.object_id
	LEFT JOIN cols AS c ON c.object_id = t.object_id
	WHERE t.is_ms_shipped = 0
)
	SELECT
	IDENTITY(int) AS RowNumber,
	SchemaName = CASE WHEN cfg.SafeMode = 0 THEN SCHEMA_NAME(t.schema_id) ELSE '[SafeMode]' END,
	TableName = CASE WHEN cfg.SafeMode = 0 THEN t.name ELSE '[SafeMode]' END,
	ObjectName = CASE WHEN cfg.SafeMode = 0 THEN QUOTENAME(SCHEMA_NAME(t.schema_id)) + '.' + QUOTENAME(t.name) ELSE '[SafeMode]' END,
	reads = s.reads,
	writes = s.writes,
	total_accesses = s.total_accesses,
	write_pct = s.write_pct,
	row_count = s.row_count,
	data_mb = s.data_mb,
	has_missing_index = s.has_missing_index,
	has_lob = s.has_lob,
	has_computed = s.has_computed,
	has_rowversion = s.has_rowversion,
	CandidateScore = CONVERT(decimal(9,2),
		ISNULL((CASE WHEN s.write_pct IS NOT NULL THEN (s.write_pct/10.0) ELSE 0 END),0)
		+ CASE WHEN s.total_accesses >= 1000 THEN 5 ELSE 0 END
		+ CASE WHEN s.has_missing_index = 1 THEN 2 ELSE 0 END
		+ CASE WHEN s.has_lob = 1 THEN -3 ELSE 0 END
	)
 INTO #DTR_InMemoryCandidates
 FROM score AS s
 JOIN sys.tables AS t ON t.object_id = s.object_id
 CROSS JOIN #DT_Config AS cfg
 WHERE s.total_accesses IS NOT NULL
 ;
 END
 GO

------------------------------------------------------------
-- 24a. Duplicate Overlap Index Heuristic
------------------------------------------------------------
PRINT N'▶ 24a. Duplicate Overlap Index Heuristic - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_Duplicate_Overlap_Index_Heuristic') IS NOT NULL DROP TABLE #DTR_Duplicate_Overlap_Index_Heuristic
GO

;WITH idx_cols AS (
	SELECT
	i.object_id,
	i.index_id,
	i.name AS index_name,
	key_cols = STUFF((
		SELECT
		',' + QUOTENAME(c.name) + CASE WHEN ic.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END
		FROM sys.index_columns AS ic
		JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
		WHERE ic.object_id = i.object_id
		AND ic.index_id = i.index_id
		AND ic.is_included_column = 0
		ORDER BY ic.key_ordinal
		FOR XML PATH(''), TYPE
		).value('.', 'NVARCHAR(MAX)'), 1, 1, ''),
	include_cols = STUFF((
		SELECT
		',' + QUOTENAME(c.name)
		FROM sys.index_columns AS ic
		JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
		WHERE ic.object_id = i.object_id
		AND ic.index_id = i.index_id
		AND ic.is_included_column = 1
		ORDER BY ic.key_ordinal
		FOR XML PATH(''), TYPE
		).value('.', 'NVARCHAR(MAX)'), 1, 1, '')
	FROM sys.indexes AS i
	WHERE i.type_desc <> 'HEAP'
	AND OBJECTPROPERTY(i.object_id, 'IsUserTable') = 1
)
	SELECT
	IDENTITY(int) AS RowNumber,
	a.object_id,
	table_name = QUOTENAME(OBJECT_SCHEMA_NAME(a.object_id)) + '.' + QUOTENAME(OBJECT_NAME(a.object_id)),
	index_a = a.index_name,
	a_keys = a.key_cols,
	a_includes = a.include_cols,
	index_b = b.index_name,
	b_keys = b.key_cols,
b_includes = b.include_cols
INTO #DTR_Duplicate_Overlap_Index_Heuristic
FROM idx_cols a
JOIN idx_cols b ON a.object_id = b.object_id AND a.index_id < b.index_id
WHERE a.key_cols = b.key_cols OR (LEN(a.key_cols) <= LEN(b.key_cols) AND b.key_cols LIKE a.key_cols + ',%') -- b extends a
ORDER BY table_name;
GO

------------------------------------------------------------
-- 25a. Computed NotPersisted
------------------------------------------------------------
PRINT N'▶ 25a. Computed NotPersisted - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_Computed_NotPersisted') IS NOT NULL DROP TABLE #DTR_Computed_NotPersisted
GO

SELECT
IDENTITY(int) AS RowNumber,
table_name = QUOTENAME(SCHEMA_NAME(t.schema_id)) + '.' + QUOTENAME(t.name),
column_name = c.name,
is_persisted = CAST(ISNULL(COLUMNPROPERTY(c.object_id, c.name, 'IsPersisted'), 0) AS INT)
INTO #DTR_Computed_NotPersisted
FROM sys.tables AS t
JOIN sys.columns AS c ON c.object_id = t.object_id
WHERE c.is_computed = 1 AND ISNULL(COLUMNPROPERTY(c.object_id, c.name, 'IsPersisted'), 0) = 0
ORDER BY table_name, column_name;
GO

------------------------------------------------------------
-- 25b. Scalar UDFs
------------------------------------------------------------
PRINT N'▶ 25b. Scalar UDFs - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_Scalar_UDFs') IS NOT NULL DROP TABLE #DTR_Scalar_UDFs
GO

SELECT DISTINCT
IDENTITY(int) AS RowNumber,
udf_name = QUOTENAME(OBJECT_SCHEMA_NAME(o.object_id)) + '.' + QUOTENAME(o.name),
o.type_desc
INTO #DTR_Scalar_UDFs
FROM sys.objects AS o
WHERE o.type IN ('FN', 'FS');
GO

------------------------------------------------------------
-- 25c. Multi-Statement TVFs (Type = Tf)
------------------------------------------------------------
PRINT N'▶ 25c. Multi-Statement TVFs (Type = Tf) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_MSTVFs') IS NOT NULL DROP TABLE #DTR_MSTVFs
GO

SELECT
IDENTITY(int) AS RowNumber,
SCHEMA_NAME(o.schema_id) + '.' + o.name AS FunctionName,
o.create_date,
o.modify_date
INTO #DTR_MSTVFs
FROM sys.objects AS o
WHERE o.type = 'TF' AND o.is_ms_shipped = 0;
GO

------------------------------------------------------------
-- 26a. Table RowCounts
------------------------------------------------------------
PRINT N'▶ 26a. Table RowCounts - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_Table_RowCounts') IS NOT NULL DROP TABLE #DTR_Table_RowCounts
GO

SELECT
IDENTITY(int) AS RowNumber,
table_name = QUOTENAME(OBJECT_SCHEMA_NAME(p.object_id)) + '.' + QUOTENAME(OBJECT_NAME(p.object_id)),
rows = SUM(p.rows)
INTO #DTR_Table_RowCounts
FROM sys.partitions AS p
JOIN sys.objects AS o ON o.object_id = p.object_id
WHERE o.type = 'U' AND p.index_id IN (0, 1)
GROUP BY p.object_id
ORDER BY rows DESC;
GO

------------------------------------------------------------
-- 26b. CPU Utilization (SQL vs Idle vs Other)
------------------------------------------------------------
PRINT N'▶ 26b. CPU Utilization (SQL vs Idle vs Other) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO
IF OBJECT_ID('tempdb..#DTR_CPU_Utilization') IS NOT NULL DROP TABLE #DTR_CPU_Utilization;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH rb AS (
		SELECT TOP (1)
		CAST(record AS xml) AS record,
		[timestamp] AS ts
		FROM sys.dm_os_ring_buffers
		WHERE ring_buffer_type = 'RING_BUFFER_SCHEDULER_MONITOR' AND record LIKE '%<SystemHealth>%'
		ORDER BY [timestamp] DESC
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	sysinfo.sqlserver_start_time,
	DATEDIFF(HOUR, sysinfo.sqlserver_start_time, SYSDATETIME()) AS uptime_hours,
	CASE WHEN rb.record.exist('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]') = 1
		THEN rb.record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') END AS SQLProcessUtilization,
	CASE WHEN rb.record.exist('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]') = 1
		THEN rb.record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') END AS SystemIdle,
	CASE WHEN rb.record.exist('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]') = 1
		AND rb.record.exist('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]') = 1
		THEN 100
				- rb.record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int')
				- rb.record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int')
		END AS OtherProcessUtilization,
	DATEADD(ms, -1 * (sysinfo.ms_ticks - rb.ts), SYSDATETIME()) AS event_time
	INTO #DTR_CPU_Utilization
	FROM rb
	CROSS JOIN sys.dm_os_sys_info AS sysinfo;
END
GO

------------------------------------------------------------
-- 26c. Latch Contention Hotspots (Aggregated)
------------------------------------------------------------
PRINT N'▶ 26c. Latch Contention Hotspots (Aggregated) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO
IF OBJECT_ID('tempdb..#DTR_LatchStats') IS NOT NULL DROP TABLE #DTR_LatchStats;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH x AS
	(
		SELECT
		latch_class,
		waiting_requests_count,
		wait_time_ms,
		max_wait_time_ms
		FROM sys.dm_os_latch_stats
		WHERE waiting_requests_count > 0
	)
	SELECT TOP (50)
	IDENTITY(int) AS RowNumber,
	x.latch_class,
	x.waiting_requests_count,
	x.wait_time_ms,
	CAST(x.wait_time_ms / 1000.0 AS decimal(18, 3)) AS wait_time_s,
	x.max_wait_time_ms,
	CAST(100.0 * x.wait_time_ms / NULLIF(SUM(x.wait_time_ms) OVER (), 0) AS decimal(6, 2)) AS pct_of_total_wait_ms
	INTO #DTR_LatchStats
	FROM x
	ORDER BY x.wait_time_ms DESC;
END
GO

------------------------------------------------------------
-- 27a. Server TopWaits
------------------------------------------------------------
PRINT N'▶ 27a. Server TopWaits - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_Server_TopWaits') IS NOT NULL DROP TABLE #DTR_Server_TopWaits
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT TOP (15)
	IDENTITY(int) AS RowNumber,
	wait_type,
	waiting_tasks_count,
	wait_time_ms,
	signal_wait_time_ms,
	CAST(100.0 * signal_wait_time_ms / NULLIF(wait_time_ms, 0) AS decimal(5, 2)) AS pct_signal
	INTO #DTR_Server_TopWaits
	FROM sys.dm_os_wait_stats
	WHERE wait_type NOT IN (
		'SLEEP_TASK', 'BROKER_TASK_STOP', 'BROKER_TO_FLUSH', 'SQLTRACE_BUFFER_FLUSH', 'CLR_AUTO_EVENT', 'CLR_MANUAL_EVENT',
		'LAZYWRITER_SLEEP', 'SLEEP_SYSTEMTASK', 'SLEEP_BPOOL_FLUSH', 'BROKER_EVENTHANDLER', 'XE_DISPATCHER_WAIT', 'XE_TIMER_EVENT',
		'XE_DISPATCHER_JOIN', 'FT_IFTS_SCHEDULER_IDLE_WAIT', 'FT_IFTSHC_MUTEX', 'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', 'DIRTY_PAGE_POLL',
		'HADR_FILESTREAM_IOMGR_IOCOMPLETION', 'HADR_WORK_QUEUE', 'SP_SERVER_DIAGNOSTICS_SLEEP', 'HADR_TIMER_TASK', 'HADR_CLUSAPI_CALL',
		'LAST_GASP_TASK', 'BROKER_TRANSMITTER', 'CHECKPOINT_QUEUE', 'REQUEST_FOR_DEADLOCK_SEARCH', 'LOGMGR_QUEUE', 'REPLICATION_AGENT',
		'DISPATCHER_QUEUE_SEMAPHORE', 'XE_LIVE_TARGET_TVF'
	)
	ORDER BY wait_time_ms DESC;
END
GO

------------------------------------------------------------
-- 27b. Server Schedulers
------------------------------------------------------------
PRINT N'▶ 27b. Server Schedulers - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_Server_Schedulers') IS NOT NULL DROP TABLE #DTR_Server_Schedulers
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	scheduler_id,
	cpu_id,
	is_online,
	current_tasks_count,
	runnable_tasks_count,
	active_workers_count,
	work_queue_count
	INTO #DTR_Server_Schedulers
	FROM sys.dm_os_schedulers
	ORDER BY scheduler_id;
END
GO

------------------------------------------------------------
-- 27c. Server MemoryClerks
------------------------------------------------------------
PRINT N'▶ 27c. Server MemoryClerks - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_Server_MemoryClerks') IS NOT NULL DROP TABLE #DTR_Server_MemoryClerks
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	DECLARE @sql nvarchar(max);

	IF COL_LENGTH('sys.dm_os_memory_clerks', 'pages_kb') IS NOT NULL
	BEGIN
		SET @sql = N'
			SELECT TOP (15)
			IDENTITY(int) AS RowNumber,
			type,
			pages_kb,
			virtual_memory_committed_kb,
			shared_memory_committed_kb,
			awe_allocated_kb
			INTO #DTR_Server_MemoryClerks
			FROM sys.dm_os_memory_clerks
			ORDER BY pages_kb DESC;'
	END

	IF COL_LENGTH('sys.dm_os_memory_clerks', 'pages_kb') IS NULL
	BEGIN
		SET @sql = N'
			SELECT TOP (15)
			IDENTITY(int) AS RowNumber,
			type,
			(single_pages_kb + multi_pages_kb) AS pages_kb,
			virtual_memory_committed_kb,
			shared_memory_committed_kb,
			awe_allocated_kb
			INTO #DTR_Server_MemoryClerks
			FROM sys.dm_os_memory_clerks
			ORDER BY (single_pages_kb + multi_pages_kb) DESC;'
	END

	EXEC sys.sp_executesql
	@sql;
END
GO

------------------------------------------------------------
-- 27d. Tempdb Layout
------------------------------------------------------------
PRINT N'▶ 27d. Tempdb Layout - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_Tempdb_Layout') IS NOT NULL DROP TABLE #DTR_Tempdb_Layout
GO

SELECT
IDENTITY(int) AS RowNumber, name,
type_desc,
size_mb = CONVERT(DECIMAL(18,1), size/128.0),
growth_desc = CASE WHEN is_percent_growth = 1 THEN CAST(growth AS VARCHAR(20)) + '%' ELSE CAST((growth*8)/1024 AS VARCHAR(20)) + ' MB' END,
file_id,
CASE WHEN cfg.SafeMode = 0 THEN physical_name ELSE '[SafeMode]' END AS physical_name
INTO #DTR_Tempdb_Layout
FROM tempdb.sys.database_files
CROSS JOIN #DT_Config AS cfg
ORDER BY type_desc, file_id;
GO

------------------------------------------------------------
-- 27e. Waits Roll-Up (Filtered Categories)
------------------------------------------------------------
PRINT N'▶ 27e. Waits Roll-Up (Filtered Categories) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_WaitsRollup') IS NOT NULL DROP TABLE #DTR_WaitsRollup
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH raw AS (
		SELECT
		wait_type,
		wait_time_ms,
		signal_wait_time_ms
		FROM sys.dm_os_wait_stats
		WHERE wait_type NOT IN (
			'SLEEP_TASK','SLEEP_SYSTEMTASK','LAZYWRITER_SLEEP','BROKER_TASK_STOP','BROKER_EVENTHANDLER',
			'BROKER_TO_FLUSH','XE_TIMER_EVENT','XE_DISPATCHER_WAIT','FT_IFTS_SCHEDULER_IDLE_WAIT',
			'CLR_MANUAL_EVENT','CLR_AUTO_EVENT','REQUEST_FOR_DEADLOCK_SEARCH'
		)
	),
	m AS (
		SELECT
		CASE
			WHEN wait_type IN ('SOS_SCHEDULER_YIELD','CXCONSUMER','CXPACKET') THEN 'CPU/Parallelism'
			WHEN wait_type LIKE 'PAGEIOLATCH%' OR wait_type IN ('IO_COMPLETION','ASYNC_IO_COMPLETION','WRITELOG','LOGBUFFER') THEN 'I/O'
			WHEN wait_type LIKE 'LCK_%' THEN 'Locks'
			WHEN wait_type LIKE 'LATCH_%' OR wait_type LIKE 'PAGELATCH_%' THEN 'Latch'
			WHEN wait_type LIKE 'NETWORK_IO' THEN 'Network'
			WHEN wait_type IN ('RESOURCE_SEMAPHORE','CMEMTHREAD','MEMORY_ALLOCATION_EXT','MEMORY_GRANT_UPDATE') THEN 'Memory'
			ELSE 'Other'
		END AS category,
		wait_time_ms,
		signal_wait_time_ms
		FROM raw
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	category,
	SUM(wait_time_ms) AS total_wait_ms,
	SUM(signal_wait_time_ms) AS total_signal_ms
	INTO #DTR_WaitsRollup
	FROM m
	GROUP BY category;
END
GO

------------------------------------------------------------
-- 27f. Wait Categories (Signal-Only Roll-Up)
------------------------------------------------------------
PRINT N'▶ 27f. Wait Categories (Signal-Only Roll-Up) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_WaitCategories') IS NOT NULL DROP TABLE #DTR_WaitCategories;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH ws AS (
		SELECT
		wait_type,
		waiting_tasks_count,
		wait_time_ms - signal_wait_time_ms AS resource_wait_ms,
		signal_wait_time_ms
		FROM sys.dm_os_wait_stats
		WHERE wait_type NOT LIKE 'SLEEP_%' AND wait_type NOT IN ('BROKER_TASK_STOP','BROKER_TO_FLUSH','CLR_AUTO_EVENT','CLR_MANUAL_EVENT','LAZYWRITER_SLEEP','XE_TIMER_EVENT','XE_DISPATCHER_WAIT')
	),
	m AS (
		SELECT
		CASE
			WHEN wait_type IN ('CXPACKET','CXCONSUMER') THEN 'Parallelism'
			WHEN wait_type LIKE 'PAGEIOLATCH_%' OR wait_type IN ('IO_COMPLETION','ASYNC_IO_COMPLETION','READLOG','WRITELOG','BACKUPIO') THEN 'I/O'
			WHEN wait_type LIKE 'PAGELATCH_%' OR wait_type LIKE 'LATCH_%' THEN 'Latch'
			WHEN wait_type LIKE 'LCK_%' THEN 'Lock'
			WHEN wait_type IN ('RESOURCE_SEMAPHORE','RESOURCE_SEMAPHORE_QUERY_COMPILE','CMEMTHREAD','CMEMPAGE','MEMORY_ALLOCATION_EXT') THEN 'Memory'
			WHEN wait_type = 'ASYNC_NETWORK_IO' THEN 'Network'
			ELSE 'Other'
		END AS category,
		waiting_tasks_count,
		resource_wait_ms,
		signal_wait_time_ms
		FROM ws
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	category,
	SUM(waiting_tasks_count) AS total_waits,
	SUM(resource_wait_ms) AS total_resource_ms,
	SUM(signal_wait_time_ms) AS total_signal_ms
	INTO #DTR_WaitCategories
	FROM m
	GROUP BY category
	ORDER BY SUM(resource_wait_ms) DESC;
END
GO

------------------------------------------------------------
-- 27g. Spinlock Hotspots
------------------------------------------------------------
PRINT N'▶ 27g. Spinlock Hotspots - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_SpinlockHotspots') IS NOT NULL DROP TABLE #DTR_SpinlockHotspots
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	s.name,
	s.collisions,
	s.spins,
	s.spins_per_collision,
	s.backoffs,
	s.sleep_time
	INTO #DTR_SpinlockHotspots
	FROM sys.dm_os_spinlock_stats AS s
	WHERE (s.collisions > 0 OR s.spins > 0 OR s.backoffs > 0);
END
GO

------------------------------------------------------------
-- 27h. Signal vs. Resource Waits (Since Startup)
------------------------------------------------------------
PRINT N'▶ 27h. Signal vs. Resource Waits (Since Startup) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_SignalVsResourceWaits') IS NOT NULL DROP TABLE #DTR_SignalVsResourceWaits
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH w AS (
		SELECT
		wait_type,
		wait_time_ms,
		signal_wait_time_ms
		FROM sys.dm_os_wait_stats
		WHERE wait_type NOT IN (
			'SLEEP_TASK','SLEEP_SYSTEMTASK','LAZYWRITER_SLEEP',
			'BROKER_TASK_STOP','BROKER_TO_FLUSH','BROKER_EVENTHANDLER',
			'XE_TIMER_EVENT','XE_DISPATCHER_WAIT','FT_IFTS_SCHEDULER_IDLE_WAIT',
			'REQUEST_FOR_DEADLOCK_SEARCH','CLR_MANUAL_EVENT','CLR_AUTO_EVENT'
		)
	),
	a AS (
		SELECT
		SUM(CASE WHEN wait_time_ms > signal_wait_time_ms THEN (wait_time_ms - signal_wait_time_ms) ELSE 0 END) AS resource_wait_ms,
		SUM(signal_wait_time_ms) AS signal_wait_ms,
		SUM(wait_time_ms) AS total_wait_ms
		FROM w
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	total_wait_ms,
	resource_wait_ms,
	signal_wait_ms,
	CONVERT(decimal(6,2), CASE WHEN total_wait_ms > 0 THEN (100.0 * signal_wait_ms) / total_wait_ms END) AS signal_pct,
	CONVERT(decimal(6,2), CASE WHEN total_wait_ms > 0 THEN (100.0 * resource_wait_ms) / total_wait_ms END) AS resource_pct
	INTO #DTR_SignalVsResourceWaits
	FROM a;
END
GO

------------------------------------------------------------
-- 27i. Buffer Node Page Life Expectancy (by Numa)
------------------------------------------------------------
PRINT N'▶ 27i. Buffer Node Page Life Expectancy (by Numa) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_PLEByNode') IS NOT NULL DROP TABLE #DTR_PLEByNode;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	pc.instance_name AS NodeName,
	pc.cntr_value AS PLESeconds
	INTO #DTR_PLEByNode
	FROM sys.dm_os_performance_counters AS pc
	WHERE pc.object_name LIKE '%Buffer Node%' AND pc.counter_name = 'Page life expectancy';
END
GO

------------------------------------------------------------
-- 28a. CheckDB Recency
------------------------------------------------------------
PRINT N'▶ 28a. CheckDB Recency - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_CheckDB_Recency') IS NOT NULL DROP TABLE #DTR_CheckDB_Recency
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	DECLARE @db sysname = DB_NAME();
	DECLARE @pattern nvarchar(4000) = N'%DBCC CHECKDB%(' + @db + N')%';

	IF OBJECT_ID('tempdb..#DTR_XpErrorLog') IS NOT NULL DROP TABLE #DTR_XpErrorLog;
	CREATE TABLE #DTR_XpErrorLog (LogDate datetime, ProcessInfo nvarchar(50), [Text] nvarchar(max));

	INSERT #DTR_XpErrorLog
	EXEC master.dbo.xp_readerrorlog 0, 1, N'DBCC CHECKDB', @db, NULL, NULL, 'desc';

	SELECT TOP (5)
	IDENTITY(int) AS RowNumber,
	LogDate,
	[Text]
	INTO #DTR_CheckDB_Recency
	FROM #DTR_XpErrorLog
	WHERE [Text] LIKE @pattern
	ORDER BY LogDate DESC;

	IF OBJECT_ID('tempdb..#DTR_XpErrorLog') IS NOT NULL DROP TABLE #DTR_XpErrorLog;
END
GO

------------------------------------------------------------
-- 28b. Suspect Pages (Current DB)
------------------------------------------------------------
PRINT N'▶ 28b. Suspect Pages (Current DB) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_SuspectPages') IS NOT NULL DROP TABLE #DTR_SuspectPages
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT TOP (200)
	IDENTITY(int) AS RowNumber,
	event_type,
	database_id,
	file_id,
	page_id,
	last_update_date,
	error_count
	INTO #DTR_SuspectPages
	FROM msdb.dbo.suspect_pages
	WHERE database_id = DB_ID()
	ORDER BY last_update_date DESC;
END
GO

------------------------------------------------------------
-- 29a. File FreeSpace
------------------------------------------------------------
PRINT N'▶ 29a. File FreeSpace - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_File_FreeSpace') IS NOT NULL DROP TABLE #DTR_File_FreeSpace
GO

SELECT
IDENTITY(int) AS RowNumber,
name,
type_desc,
size_mb = size / 128.0,
space_used_mb = FILEPROPERTY(name, 'SpaceUsed') / 128.0,
free_mb = (size - FILEPROPERTY(name, 'SpaceUsed')) / 128.0,
free_pct = CAST(100.0 * (size - FILEPROPERTY(name, 'SpaceUsed')) / NULLIF(size, 0) AS DECIMAL(5, 2)),
growth_desc = CASE
	WHEN is_percent_growth = 1 THEN CAST(growth AS VARCHAR(20)) + '%'
	ELSE CAST((growth * 8) / 1024 AS VARCHAR(20)) + ' MB'
	END,
max_size_mb = CASE WHEN max_size = -1 THEN NULL ELSE max_size / 128.0 END
INTO #DTR_File_FreeSpace
FROM sys.database_files
ORDER BY type_desc, name;
GO

------------------------------------------------------------
-- 29b. Files with Percentage Growth
------------------------------------------------------------
PRINT N'▶ 29b. Files with Percentage Growth - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_FilesPercentGrowth') IS NOT NULL DROP TABLE #DTR_FilesPercentGrowth
GO

	SELECT
	IDENTITY(int) AS RowNumber,
	mf.type_desc AS file_type,
	mf.file_id AS file_id,
	mf.name AS logical_name,
	CASE WHEN cfg.SafeMode = 0 THEN mf.physical_name ELSE '[SafeMode]' END AS physical_name,
	CONVERT(decimal(18,1), mf.size/128.0) AS size_mb,
	mf.growth AS growth_raw,
	mf.is_percent_growth
 INTO #DTR_FilesPercentGrowth
 FROM sys.database_files AS mf
 CROSS JOIN #DT_Config AS cfg
 WHERE mf.is_percent_growth = 1
 ORDER BY mf.type_desc, mf.file_id;
  GO

------------------------------------------------------------
-- 29c. Files with Tiny Fixed Autogrowth (< 64 Mb)
------------------------------------------------------------
PRINT N'▶ 29c. Files with Tiny Fixed Autogrowth (< 64 Mb) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_FilesSmallGrowth') IS NOT NULL DROP TABLE #DTR_FilesSmallGrowth
GO

	SELECT
	IDENTITY(int) AS RowNumber,
	df.file_id,
	df.type_desc,
	df.name AS LogicalName,
	(df.size * 8) / 1024 AS size_mb,
	df.growth AS growth_pages,
	(df.growth * 8) / 1024 AS growth_mb,
	df.is_percent_growth
INTO #DTR_FilesSmallGrowth
FROM sys.database_files AS df
WHERE df.is_percent_growth = 0
	AND (df.growth * 8) / 1024 < 64;
GO

------------------------------------------------------------
-- 30a. AgentJobs ReferencingDB
------------------------------------------------------------
PRINT N'▶ 30a. AgentJobs ReferencingDB - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_AgentJobs_ReferencingDB') IS NOT NULL DROP TABLE #DTR_AgentJobs_ReferencingDB
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	j.job_id AS job_id,
	j.name AS job_name,
	j.enabled AS job_enabled,
	j.description AS job_description,
	s.step_id,
	s.step_name,
	s.database_name,
	s.subsystem,
	LEFT(s.command, 4000) AS command_snippet,
	CASE
		WHEN s.database_name = DB_NAME() THEN 'database_name'
		WHEN s.command LIKE '%[' + DB_NAME() + ']%' THEN 'command_contains_[dbname]'
		WHEN s.command LIKE '%''' + DB_NAME() + '''%' THEN 'command_contains_''dbname'''
		WHEN s.subsystem = 'CmdExec'
			AND s.command LIKE '%-d %' + DB_NAME() + '%' THEN 'cmdexec_-d_dbname'
		WHEN s.command LIKE '%' + DB_NAME() + '%' THEN 'command_contains_dbname'
		ELSE 'unknown'
	END AS match_source
	INTO #DTR_AgentJobs_ReferencingDB
	FROM msdb.dbo.sysjobs AS j
	JOIN msdb.dbo.sysjobsteps AS s ON s.job_id = j.job_id
	WHERE
		s.database_name = DB_NAME()
		OR s.command LIKE '%' + DB_NAME() + '%'
	ORDER BY
		j.name,
		s.step_id;
END
GO

------------------------------------------------------------
-- 30b. SQL Agent Alerts (Msdb)
------------------------------------------------------------
PRINT N'▶ 30b. SQL Agent Alerts (Msdb) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_AgentAlerts') IS NOT NULL DROP TABLE #DTR_AgentAlerts
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	CAST(sa.id AS int) AS alert_id,
	sa.name,
	sa.event_source,
	sa.event_category_id,
	sa.severity,
	sa.message_id,
	sa.database_name,
	sa.[enabled],
	sa.delay_between_responses,
	sa.occurrence_count,
	sa.last_occurrence_date,
	sa.last_occurrence_time,
	sa.last_response_date,
	sa.last_response_time,
	CASE
		WHEN sa.severity BETWEEN 19 AND 25 OR sa.message_id IN (823, 824, 825) THEN CAST(1 AS bit)
		ELSE CAST(0 AS bit)
	END AS is_critical_corruption_or_severity,
	j.name AS job_name,
	o.id AS operator_id,
	o.name AS operator_name,
	CASE WHEN sn.notification_method & 1 = 1 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS notify_email,
	CASE WHEN sn.notification_method & 2 = 2 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS notify_pager,
	CASE WHEN sn.notification_method & 4 = 4 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS notify_netsend,
	CASE WHEN sn.notification_method IS NULL OR sn.notification_method = 0 THEN CAST(0 AS bit) ELSE CAST(1 AS bit) END AS has_notification
	INTO #DTR_AgentAlerts
	FROM msdb.dbo.sysalerts AS sa
	LEFT JOIN msdb.dbo.sysjobs AS j ON j.job_id = sa.job_id AND sa.job_id <> 0x00
	LEFT JOIN msdb.dbo.sysnotifications AS sn ON sn.alert_id = sa.id
	LEFT JOIN msdb.dbo.sysoperators AS o ON o.id = sn.operator_id
	ORDER BY sa.name, o.name;
END
GO

------------------------------------------------------------
-- 30c. SQL Agent Operators (Msdb)
------------------------------------------------------------
PRINT N'▶ 30c. SQL Agent Operators (Msdb) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_AgentOperators') IS NOT NULL DROP TABLE #DTR_AgentOperators
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	CAST(o.id AS int) AS operator_id,
	o.name,
	o.enabled,
	o.email_address,
	o.pager_address,
	o.netsend_address,
	CASE WHEN o.email_address IS NOT NULL AND LTRIM(RTRIM(o.email_address)) <> '' THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS has_email,
	CASE WHEN o.pager_address IS NOT NULL AND LTRIM(RTRIM(o.pager_address)) <> '' THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS has_pager,
	CASE WHEN o.netsend_address IS NOT NULL AND LTRIM(RTRIM(o.netsend_address)) <> '' THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS has_netsend,
	o.weekday_pager_start_time,
	o.weekday_pager_end_time,
	o.saturday_pager_start_time,
	o.saturday_pager_end_time,
	o.sunday_pager_start_time,
	o.sunday_pager_end_time,
	o.pager_days
	INTO #DTR_AgentOperators
	FROM msdb.dbo.sysoperators AS o
	ORDER BY o.name;
END
GO

------------------------------------------------------------
-- 30d. SQL Agent Proxies (Enabled + Usage)
------------------------------------------------------------
PRINT N'▶ 30d. SQL Agent Proxies (Enabled + Usage) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_AgentProxies') IS NOT NULL DROP TABLE #DTR_AgentProxies
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH p AS (
		SELECT
		proxy_id,
		name,
		credential_id,
		enabled
		FROM msdb.dbo.sysproxies
	),
	subs AS (
		SELECT
		proxy_id,
		COUNT(*) AS subsystem_count
		FROM msdb.dbo.sysproxysubsystem
		GROUP BY proxy_id
	),
	logins AS (
		SELECT
		proxy_id,
		COUNT(*) AS login_mappings
		FROM msdb.dbo.sysproxylogin
		GROUP BY proxy_id
	),
	step_usage AS (
		SELECT
		s.proxy_id,
		COUNT(*) AS step_usage_count,
		COUNT(DISTINCT s.job_id) AS jobs_using_proxy
		FROM msdb.dbo.sysjobsteps AS s
		WHERE s.proxy_id IS NOT NULL
		GROUP BY
		s.proxy_id
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	p.name AS ProxyName,
	p.enabled,
	p.credential_id,
	ISNULL(subs.subsystem_count, 0) AS subsystem_count,
	ISNULL(logins.login_mappings, 0) AS login_mappings,
	ISNULL(step_usage.step_usage_count, 0) AS step_usage_count,
	ISNULL(step_usage.jobs_using_proxy, 0) AS jobs_using_proxy,
	CASE WHEN ISNULL(subs.subsystem_count, 0) > 0 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS has_subsystems,
	CASE WHEN ISNULL(logins.login_mappings, 0) > 0 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS has_logins
	INTO #DTR_AgentProxies
	FROM p
	LEFT JOIN subs ON subs.proxy_id = p.proxy_id
	LEFT JOIN logins ON logins.proxy_id = p.proxy_id
	LEFT JOIN step_usage ON step_usage.proxy_id = p.proxy_id;
END
GO

------------------------------------------------------------
-- 30e. SQL Agent Jobs - Last Outcome (Msdb)
------------------------------------------------------------
PRINT N'▶ 30e. SQL Agent Jobs - Last Outcome (Msdb) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_AgentJobLastRun') IS NOT NULL DROP TABLE #DTR_AgentJobLastRun
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH h AS (
		SELECT
		jh.job_id,
		jh.run_status,
		jh.run_duration,
		msdb.dbo.agent_datetime(jh.run_date, jh.run_time) AS run_dt,
		ROW_NUMBER() OVER (PARTITION BY jh.job_id ORDER BY jh.instance_id DESC) AS rn
		FROM msdb.dbo.sysjobhistory AS jh
		WHERE jh.step_id = 0
	),
	last0 AS (
		SELECT
		job_id,
		run_status,
		run_dt,
		((run_duration / 10000) * 3600)
			+ (((run_duration % 10000) / 100) * 60)
			+ (run_duration % 100) AS run_duration_sec
		FROM h
		WHERE rn = 1
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	j.job_id,
	j.name AS JobName,
	j.enabled,
	l.run_status AS last_run_status,
	CASE l.run_status
		WHEN 0 THEN 'Failed'
		WHEN 1 THEN 'Succeeded'
		WHEN 2 THEN 'Retry'
		WHEN 3 THEN 'Canceled'
		WHEN 4 THEN 'In Progress'
		ELSE NULL
	END AS last_run_status_desc,
	CASE WHEN l.run_status = 1 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS last_run_succeeded,
	CASE WHEN l.run_status = 0 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS last_run_failed,
	l.run_dt AS last_run_datetime,
	l.run_duration_sec
	INTO #DTR_AgentJobLastRun
	FROM msdb.dbo.sysjobs AS j
	LEFT JOIN last0 AS l ON l.job_id = j.job_id;
END
GO

------------------------------------------------------------
-- 30f. Jobs with No Schedule or Only Disabled Schedules
------------------------------------------------------------
PRINT N'▶ 30f. Jobs with No Schedule or Only Disabled Schedules - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_JobsNoSchedule') IS NOT NULL DROP TABLE #DTR_JobsNoSchedule;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	j.job_id AS JobId,
	j.name AS JobName,
	j.enabled AS JobEnabled,
	COUNT(js.schedule_id) AS ScheduleCount,
	SUM(CASE WHEN sc.enabled = 1 THEN 1 ELSE 0 END) AS EnabledSchedules,
	SUM(CASE WHEN sc.enabled = 0 THEN 1 ELSE 0 END) AS DisabledSchedules,
	CASE WHEN COUNT(js.schedule_id) = 0 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS HasNoSchedule,
	CASE WHEN COUNT(js.schedule_id) > 0
			AND SUM(CASE WHEN sc.enabled = 1 THEN 1 ELSE 0 END) = 0
		THEN CAST(1 AS bit)
		ELSE CAST(0 AS bit)
	END AS OnlyDisabledSchedules,
	MAX(CASE WHEN js.next_run_date > 0 THEN msdb.dbo.agent_datetime(js.next_run_date, js.next_run_time) END) AS next_run_datetime,
	CASE
		WHEN MAX(CASE WHEN js.next_run_date > 0 THEN msdb.dbo.agent_datetime(js.next_run_date, js.next_run_time) END) IS NOT NULL
			AND MAX(CASE WHEN js.next_run_date > 0 THEN msdb.dbo.agent_datetime(js.next_run_date, js.next_run_time) END) < DATEADD(DAY, 7, GETDATE())
			THEN CAST(1 AS bit)
		ELSE CAST(0 AS bit)
	END AS next_run_within_7d
	INTO #DTR_JobsNoSchedule
	FROM msdb.dbo.sysjobs AS j
	LEFT JOIN msdb.dbo.sysjobschedules AS js ON js.job_id = j.job_id
	LEFT JOIN msdb.dbo.sysschedules AS sc ON sc.schedule_id = js.schedule_id
	GROUP BY
	j.job_id,
	j.name,
	j.enabled
	HAVING COUNT(js.schedule_id) = 0 OR SUM(CASE WHEN sc.enabled = 0 THEN 1 ELSE 0 END) > 0;
END
GO

------------------------------------------------------------
-- 30g. SQL Agent - Job Owner Posture
------------------------------------------------------------
PRINT N'▶ 30g. SQL Agent - Job Owner Posture - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_JobOwnerPosture') IS NOT NULL DROP TABLE #DTR_JobOwnerPosture;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH owners AS (
		SELECT
		s.name AS OwnerName,
		s.principal_id,
		s.sid,
		s.type_desc AS OwnerType,
		s.is_disabled
		FROM sys.server_principals AS s
	),
	sysadmin_role AS (
		SELECT m.member_principal_id
		FROM sys.server_role_members AS m
		JOIN sys.server_principals AS r ON r.principal_id = m.role_principal_id AND r.name = 'sysadmin'
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	j.job_id,
	j.name AS JobName,
	CASE WHEN cfg.SafeMode = 0 THEN o.OwnerName ELSE '[SafeMode]' END AS OwnerName,
	o.principal_id AS OwnerPrincipalId,
	o.sid AS OwnerSid,
	o.OwnerType,
	o.is_disabled AS OwnerDisabled,
	CASE WHEN o.principal_id IN (SELECT member_principal_id FROM sysadmin_role) THEN 1 ELSE 0 END AS OwnerIsSysadmin,
	CASE WHEN o.OwnerType IN ('SQL_LOGIN', 'WINDOWS_LOGIN', 'WINDOWS_GROUP') THEN 1 ELSE 0 END AS OwnerIsLoginLike,
	CASE WHEN o.OwnerType = 'SQL_LOGIN' AND o.OwnerName NOT IN ('sa') THEN 1 ELSE 0 END AS OwnerIsSqlLoginNonSa,
	CASE WHEN o.OwnerType IN ('WINDOWS_LOGIN', 'WINDOWS_GROUP') THEN 1 ELSE 0 END AS OwnerIsWindowsPrincipal
	INTO #DTR_JobOwnerPosture
	FROM msdb.dbo.sysjobs AS j
	CROSS JOIN #DT_Config AS cfg
	LEFT JOIN owners AS o ON o.sid = j.owner_sid;
END
GO

------------------------------------------------------------
-- 30h. Database Mail Profiles
------------------------------------------------------------
PRINT N'▶ 30h. Database Mail Profiles - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_DbMailProfiles') IS NOT NULL DROP TABLE #DTR_DbMailProfiles;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	CAST(p.profile_id AS int) AS profile_id,
	p.name AS profile_name,
	p.description AS profile_description,
	ISNULL(pa.account_count, 0) AS account_count,
	pa.primary_account_id,
	CASE WHEN ISNULL(pa.account_count, 0) > 0 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS has_accounts,
	CASE WHEN ISNULL(pa.account_count, 0) > 1 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS has_multiple_accounts,
	CASE WHEN pa.primary_account_id IS NOT NULL THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS has_primary_account
	INTO #DTR_DbMailProfiles
	FROM msdb.dbo.sysmail_profile AS p
	LEFT JOIN (
		SELECT
		pa.profile_id,
		COUNT(DISTINCT pa.account_id) AS account_count,
		MIN(CASE WHEN pa.sequence_number = 1 THEN pa.account_id END) AS primary_account_id
		FROM msdb.dbo.sysmail_profileaccount AS pa
		GROUP BY pa.profile_id
	) AS pa ON p.profile_id = pa.profile_id
	ORDER BY p.name;
END
GO

------------------------------------------------------------
-- 30i. Database Mail Accounts
------------------------------------------------------------
PRINT N'▶ 30i. Database Mail Accounts - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_DbMailAccounts') IS NOT NULL DROP TABLE #DTR_DbMailAccounts;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	CAST(a.account_id AS int) AS account_id,
	a.name AS account_name,
	a.description AS account_description,
	CASE WHEN cfg.SafeMode = 0 THEN a.email_address ELSE '[SafeMode]' END AS email_address,
	CASE WHEN cfg.SafeMode = 0 THEN a.display_name ELSE '[SafeMode]' END AS display_name,
	CASE WHEN cfg.SafeMode = 0 THEN a.replyto_address ELSE '[SafeMode]' END AS replyto_address,
	CASE WHEN cfg.SafeMode = 0 THEN s.servername ELSE '[SafeMode]' END AS servername,
	s.port,
	CASE WHEN cfg.SafeMode = 0 THEN s.username ELSE '[SafeMode]' END AS username,
	s.use_default_credentials,
	s.enable_ssl,
	CASE WHEN s.use_default_credentials = 1 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS uses_default_credentials,
	CASE WHEN s.use_default_credentials = 0 AND s.username IS NOT NULL THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS uses_explicit_credentials,
	CASE WHEN s.enable_ssl = 1 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS uses_ssl
	INTO #DTR_DbMailAccounts
	FROM msdb.dbo.sysmail_account AS a
	JOIN msdb.dbo.sysmail_server AS s ON a.account_id = s.account_id
	CROSS JOIN #DT_Config AS cfg
	ORDER BY a.name;
END
GO

------------------------------------------------------------
-- 30j. Database Mail Profile Accounts
------------------------------------------------------------
PRINT N'▶ 30j. Database Mail Profile Accounts - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_DbMailProfileAccounts') IS NOT NULL DROP TABLE #DTR_DbMailProfileAccounts;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	CAST(pa.profile_id AS int) AS profile_id,
	CAST(pa.account_id AS int) AS account_id,
	pa.sequence_number,
	CASE WHEN pa.sequence_number = 1 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS is_primary_account
	INTO #DTR_DbMailProfileAccounts
	FROM msdb.dbo.sysmail_profileaccount AS pa;
END
GO

------------------------------------------------------------
-- 30k. Maintenance Plans - Plans/Subplans + Jobs/Schedules
------------------------------------------------------------
PRINT N'▶ 30k. Maintenance Plans - Plans/Subplans + Jobs/Schedules - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_MaintPlans_Subplans') IS NOT NULL DROP TABLE #DTR_MaintPlans_Subplans
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	p.id AS plan_id,
	CASE WHEN cfg.SafeMode = 0 THEN p.name ELSE '[SafeMode]' END AS plan_name,
	sp.subplan_id,
	CASE WHEN cfg.SafeMode = 0 THEN sp.subplan_name ELSE '[SafeMode]' END AS subplan_name,
	j.job_id,
	CASE WHEN cfg.SafeMode = 0 THEN j.name ELSE '[SafeMode]' END AS job_name,
	j.enabled AS job_enabled,
	js.schedule_id,
	CASE WHEN cfg.SafeMode = 0 THEN s.name ELSE '[SafeMode]' END AS schedule_name,
	s.enabled AS schedule_enabled,
	s.freq_type,
	s.freq_interval,
	s.freq_subday_type,
	s.freq_subday_interval,
	s.freq_relative_interval,
	s.freq_recurrence_factor,
	s.active_start_date,
	s.active_start_time,
	msdb.dbo.agent_datetime(js.next_run_date, js.next_run_time) AS next_run_datetime
	INTO #DTR_MaintPlans_Subplans
	FROM msdb.dbo.sysmaintplan_plans AS p
	CROSS JOIN #DT_Config AS cfg
	JOIN msdb.dbo.sysmaintplan_subplans AS sp ON sp.plan_id = p.id
	LEFT JOIN msdb.dbo.sysjobs AS j ON j.job_id = sp.job_id
	LEFT JOIN msdb.dbo.sysjobschedules AS js ON js.job_id = j.job_id
	LEFT JOIN msdb.dbo.sysschedules AS s ON s.schedule_id = js.schedule_id;
END
GO

------------------------------------------------------------
-- 30l. Maintenance Plans - Run Summary
------------------------------------------------------------
PRINT N'▶ 30l. Maintenance Plans - Run Summary - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_MaintPlans_RunSummary') IS NOT NULL DROP TABLE #DTR_MaintPlans_RunSummary
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	p.id AS plan_id,
	CASE WHEN cfg.SafeMode = 0 THEN p.name ELSE '[SafeMode]' END AS plan_name,
	sp.subplan_id,
	CASE WHEN cfg.SafeMode = 0 THEN sp.subplan_name ELSE '[SafeMode]' END AS subplan_name,
	ISNULL(agg.total_runs, 0) AS total_runs,
	ISNULL(agg.succeeded_runs, 0) AS succeeded_runs,
	ISNULL(agg.failed_runs, 0) AS failed_runs,
	agg.first_run_start_time,
	agg.last_run_start_time,
	agg.last_run_end_time,
	agg.success_rate_pct,
	agg.days_since_last_run
	INTO #DTR_MaintPlans_RunSummary
	FROM msdb.dbo.sysmaintplan_plans AS p
	CROSS JOIN #DT_Config AS cfg
	JOIN msdb.dbo.sysmaintplan_subplans AS sp ON sp.plan_id = p.id
	LEFT JOIN (
		SELECT
		l.plan_id,
		l.subplan_id,
		MIN(l.start_time) AS first_run_start_time,
		MAX(l.start_time) AS last_run_start_time,
		MAX(l.end_time) AS last_run_end_time,
		COUNT(*) AS total_runs,
		SUM(CASE WHEN l.succeeded = 1 THEN 1 ELSE 0 END) AS succeeded_runs,
		SUM(CASE WHEN l.succeeded = 0 THEN 1 ELSE 0 END) AS failed_runs,
		CAST(CASE WHEN COUNT(*) = 0 THEN NULL ELSE 100.0 * SUM(CASE WHEN l.succeeded = 1 THEN 1 ELSE 0 END) / COUNT(*) END AS decimal(5, 2)) AS success_rate_pct,
		CASE WHEN MAX(l.start_time) IS NULL THEN NULL ELSE DATEDIFF(day, MAX(l.start_time), SYSUTCDATETIME()) END AS days_since_last_run
		FROM msdb.dbo.sysmaintplan_log AS l
		GROUP BY
		l.plan_id,
		l.subplan_id
	) AS agg ON agg.plan_id = p.id AND agg.subplan_id = sp.subplan_id
	ORDER BY
	p.name,
	sp.subplan_name;
END
GO

------------------------------------------------------------
-- 30m. Maintenance Plans - Last Run per Subplan (Log)
------------------------------------------------------------
PRINT N'▶ 30m. Maintenance Plans - Last Run per Subplan (Log) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_MaintPlanLastRun') IS NOT NULL DROP TABLE #DTR_MaintPlanLastRun
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH last_run AS (
		SELECT
		plan_id,
		subplan_id,
		MAX(start_time) AS last_start_time
		FROM msdb.dbo.sysmaintplan_log
		GROUP BY
		plan_id,
		subplan_id
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	p.id AS plan_id,
	CASE WHEN cfg.SafeMode = 0 THEN p.name ELSE '[SafeMode]' END AS plan_name,
	sp.subplan_id,
	CASE WHEN cfg.SafeMode = 0 THEN sp.subplan_name ELSE '[SafeMode]' END AS subplan_name,
	j.job_id,
	CASE WHEN cfg.SafeMode = 0 THEN j.name ELSE '[SafeMode]' END AS job_name,
	l.start_time AS last_start_time,
	l.end_time AS last_end_time,
	DATEDIFF(second, l.start_time, l.end_time) AS duration_seconds,
	l.succeeded AS last_succeeded,
	l.task_detail_id
	INTO #DTR_MaintPlanLastRun
	FROM last_run AS x
	JOIN msdb.dbo.sysmaintplan_log AS l ON l.plan_id = x.plan_id AND l.subplan_id = x.subplan_id AND l.start_time = x.last_start_time
	JOIN msdb.dbo.sysmaintplan_subplans AS sp ON sp.subplan_id = l.subplan_id
	JOIN msdb.dbo.sysmaintplan_plans AS p ON p.id = sp.plan_id
	CROSS JOIN #DT_Config AS cfg
	LEFT JOIN msdb.dbo.sysjobs AS j ON j.job_id = sp.job_id
	ORDER BY
	p.name,
	sp.subplan_name;
END
GO

------------------------------------------------------------
-- 30n. Maintenance Plans - Recent Task-Level Log Detail
------------------------------------------------------------
PRINT N'▶ 30n. Maintenance Plans - Recent Task-Level Log Detail - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_MaintPlanLogDetail') IS NOT NULL DROP TABLE #DTR_MaintPlanLogDetail
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT TOP (200)
	IDENTITY(int) AS RowNumber,
	CASE WHEN cfg.SafeMode = 0 THEN p.name ELSE '[SafeMode]' END AS plan_name,
	CASE WHEN cfg.SafeMode = 0 THEN sp.subplan_name ELSE '[SafeMode]' END AS subplan_name,
	CASE WHEN cfg.SafeMode = 0 THEN ld.server_name ELSE '[SafeMode]' END AS server_name,
	ld.start_time,
	ld.end_time,
	ld.succeeded,
	CASE WHEN cfg.SafeMode = 0 THEN ld.line1 ELSE '[SafeMode]' END AS line1,
	CASE WHEN cfg.SafeMode = 0 THEN ld.line2 ELSE '[SafeMode]' END AS line2,
	CASE WHEN cfg.SafeMode = 0 THEN ld.line3 ELSE '[SafeMode]' END AS line3,
	CASE WHEN cfg.SafeMode = 0 THEN ld.line4 ELSE '[SafeMode]' END AS line4,
	CASE WHEN cfg.SafeMode = 0 THEN ld.line5 ELSE '[SafeMode]' END AS line5,
	CASE WHEN cfg.SafeMode = 0 THEN LEFT(ld.command, 4000) ELSE '[SafeMode]' END AS command,
	ld.error_number,
	CASE WHEN cfg.SafeMode = 0 THEN LEFT(ld.error_message, 4000) ELSE '[SafeMode]' END AS error_message
	INTO #DTR_MaintPlanLogDetail
	FROM msdb.dbo.sysmaintplan_plans AS p
	CROSS JOIN #DT_Config AS cfg
	JOIN msdb.dbo.sysmaintplan_subplans AS sp ON sp.plan_id = p.id
	JOIN msdb.dbo.sysmaintplan_log AS l ON l.plan_id = p.id AND l.subplan_id = sp.subplan_id
	JOIN msdb.dbo.sysmaintplan_logdetail AS ld ON ld.task_detail_id = l.task_detail_id
	ORDER BY
	ld.start_time DESC;
END
GO

------------------------------------------------------------
-- 30o. Maintenance Plans - Health Flags & Orphans
------------------------------------------------------------
PRINT N'▶ 30o. Maintenance Plans - Health Flags & Orphans - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_MaintPlan_Health') IS NOT NULL DROP TABLE #DTR_MaintPlan_Health
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	p.id AS plan_id,
	CASE WHEN cfg.SafeMode = 0 THEN p.name ELSE '[SafeMode]' END AS plan_name,
	sp.subplan_id,
	CASE WHEN cfg.SafeMode = 0 THEN sp.subplan_name ELSE '[SafeMode]' END AS subplan_name,
	CASE WHEN j.job_id IS NULL THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS is_orphan_job,
	CASE WHEN j.job_id IS NOT NULL AND j.enabled = 0 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS job_disabled,
	CASE WHEN js.job_id IS NULL THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS has_no_schedule,
	CASE WHEN js.job_id IS NOT NULL AND s.enabled = 0 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS schedule_disabled,
	lr.last_start_time,
	lr.last_succeeded
	INTO #DTR_MaintPlan_Health
	FROM msdb.dbo.sysmaintplan_plans AS p
	CROSS JOIN #DT_Config AS cfg
	JOIN msdb.dbo.sysmaintplan_subplans AS sp ON sp.plan_id = p.id
	LEFT JOIN msdb.dbo.sysjobs AS j ON j.job_id = sp.job_id
	LEFT JOIN msdb.dbo.sysjobschedules AS js ON js.job_id = j.job_id
	LEFT JOIN msdb.dbo.sysschedules AS s ON s.schedule_id = js.schedule_id
	OUTER APPLY (
		SELECT TOP (1)
		l.start_time AS last_start_time,
		l.succeeded AS last_succeeded
		FROM msdb.dbo.sysmaintplan_log AS l
		WHERE l.plan_id = p.id AND l.subplan_id = sp.subplan_id
		ORDER BY
		l.start_time DESC
	) AS lr
	ORDER BY
	p.name,
	sp.subplan_name;
END
GO

------------------------------------------------------------
-- 30p. SQL Agent - Jobs Without Failure Notifications
------------------------------------------------------------
PRINT N'▶ 30p. SQL Agent - Jobs Without Failure Notifications - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_JobNotifications') IS NOT NULL DROP TABLE #DTR_JobNotifications
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	j.job_id,
	CASE WHEN cfg.SafeMode = 0 THEN j.name ELSE '[SafeMode]' END AS job_name,
	j.enabled AS job_enabled,
	j.notify_level_email,
	CASE j.notify_level_email
		WHEN 0 THEN 'Never'
		WHEN 1 THEN 'On Success'
		WHEN 2 THEN 'On Failure'
		WHEN 3 THEN 'On Completion'
	END AS notify_level_email_desc,
	j.notify_email_operator_id,
	CASE WHEN cfg.SafeMode = 0 THEN eo.name ELSE '[SafeMode]' END AS email_operator_name,
	CASE WHEN j.notify_level_email = 2 AND j.notify_email_operator_id IS NOT NULL THEN 1 ELSE 0 END AS email_on_failure_exact,
	CASE WHEN j.notify_level_email IN (2, 3) AND j.notify_email_operator_id IS NOT NULL THEN 1 ELSE 0 END AS email_includes_failure,
	j.notify_level_page,
	CASE WHEN cfg.SafeMode = 0 THEN po.name ELSE '[SafeMode]' END AS pager_operator_name,
	CASE WHEN j.notify_level_page = 2 AND j.notify_page_operator_id IS NOT NULL THEN 1 ELSE 0 END AS pager_on_failure_exact,
	j.notify_level_netsend,
	CASE WHEN cfg.SafeMode = 0 THEN no.name ELSE '[SafeMode]' END AS netsend_operator_name,
	CASE WHEN j.notify_level_netsend = 2 AND j.notify_netsend_operator_id IS NOT NULL THEN 1 ELSE 0 END AS netsend_on_failure_exact,
	CASE
		WHEN (j.notify_level_email IN (2, 3) AND j.notify_email_operator_id IS NOT NULL)
			OR (j.notify_level_page = 2 AND j.notify_page_operator_id IS NOT NULL)
			OR (j.notify_level_netsend = 2 AND j.notify_netsend_operator_id IS NOT NULL)
			THEN CAST(1 AS bit)
		ELSE CAST(0 AS bit)
	END AS has_failure_notification
	INTO #DTR_JobNotifications
	FROM msdb.dbo.sysjobs AS j
	CROSS JOIN #DT_Config AS cfg
	LEFT JOIN msdb.dbo.sysoperators AS eo ON eo.id = j.notify_email_operator_id
	LEFT JOIN msdb.dbo.sysoperators AS po ON po.id = j.notify_page_operator_id
	LEFT JOIN msdb.dbo.sysoperators AS no ON no.id = j.notify_netsend_operator_id
	ORDER BY
	j.name;
END
GO

------------------------------------------------------------
-- 30q. SQL Agent - Job Step Security Posture (Subsystems & Proxies)
------------------------------------------------------------
PRINT N'▶ 30q. SQL Agent - Job Step Security Posture (Subsystems & Proxies) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_JobStepSecurity') IS NOT NULL DROP TABLE #DTR_JobStepSecurity
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	CASE WHEN cfg.SafeMode = 0 THEN j.name ELSE '[SafeMode]' END AS job_name,
	j.enabled AS job_enabled,
	s.step_id,
	CASE WHEN cfg.SafeMode = 0 THEN s.step_name ELSE '[SafeMode]' END AS step_name,
	s.subsystem,
	CASE WHEN cfg.SafeMode = 0 THEN s.database_name ELSE '[SafeMode]' END AS database_name,
	CASE WHEN cfg.SafeMode = 0 THEN LEFT(s.command, 1000) ELSE '[SafeMode]' END AS command_snippet,
	s.proxy_id,
	CASE WHEN cfg.SafeMode = 0 THEN p.name ELSE '[SafeMode]' END AS proxy_name,
	CASE WHEN s.subsystem IN ('CmdExec', 'PowerShell', 'ActiveScripting', 'SSIS') THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS is_risky_subsystem,
	CASE WHEN s.subsystem IN ('CmdExec', 'PowerShell', 'ActiveScripting', 'SSIS') AND s.proxy_id IS NULL THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS risky_without_proxy
	INTO #DTR_JobStepSecurity
	FROM msdb.dbo.sysjobs AS j
	CROSS JOIN #DT_Config AS cfg
	JOIN msdb.dbo.sysjobsteps AS s ON s.job_id = j.job_id
	LEFT JOIN msdb.dbo.sysproxies AS p ON p.proxy_id = s.proxy_id
	ORDER BY
	j.name,
	s.step_id;
END
GO

------------------------------------------------------------
-- 30r. Database Mail - Recent Errors (with Message Context)
------------------------------------------------------------
PRINT N'▶ 30r. Database Mail - Recent Errors (with Message Context) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_DbMailRecentErrors') IS NOT NULL DROP TABLE #DTR_DbMailRecentErrors
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT TOP (200)
	IDENTITY(int) AS RowNumber,
	l.log_id,
	l.event_type,
	l.log_date,
	a.mailitem_id,
	a.sent_status,
	CASE WHEN cfg.SafeMode = 0 THEN a.recipients ELSE '[SafeMode]' END AS recipients,
	CASE WHEN cfg.SafeMode = 0 THEN a.subject ELSE '[SafeMode]' END AS subject,
	CASE WHEN cfg.SafeMode = 0 THEN LEFT(l.description, 4000) ELSE '[SafeMode]' END AS description
	INTO #DTR_DbMailRecentErrors
	FROM msdb.dbo.sysmail_event_log AS l
	CROSS JOIN #DT_Config AS cfg
	LEFT JOIN msdb.dbo.sysmail_allitems AS a ON a.mailitem_id = l.mailitem_id
	WHERE l.event_type IN ('error', 'warning')
	ORDER BY
	l.log_date DESC;
END
GO

------------------------------------------------------------
-- 30s. msdb Housekeeping - Top Tables by Size
------------------------------------------------------------
PRINT N'▶ 30s. msdb Housekeeping - Top Tables by Size - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_msdbTopTables') IS NOT NULL DROP TABLE #DTR_msdbTopTables
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH sizes AS (
		SELECT
		s.name AS schema_name,
		t.name AS table_name,
		SUM(a.total_pages) AS total_pages,
		SUM(a.used_pages) AS used_pages,
		SUM(p.rows) AS row_count
		FROM msdb.sys.tables AS t
		JOIN msdb.sys.schemas AS s ON s.schema_id = t.schema_id
		JOIN msdb.sys.indexes AS i ON i.object_id = t.object_id
		JOIN msdb.sys.partitions AS p ON p.object_id = i.object_id AND p.index_id = i.index_id
		JOIN msdb.sys.allocation_units AS a ON a.container_id = CASE WHEN a.type IN (1, 3) THEN p.hobt_id ELSE p.partition_id END
		GROUP BY
		s.name,
		t.name
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	schema_name + '.' + table_name AS table_name,
	row_count,
	CAST(used_pages * 8.0 / 1024.0 AS decimal(18, 2)) AS used_mb,
	CAST(total_pages * 8.0 / 1024.0 AS decimal(18, 2)) AS reserved_mb,
	CAST((total_pages - used_pages) * 8.0 / 1024.0 AS decimal(18, 2)) AS unused_mb
	INTO #DTR_msdbTopTables
	FROM sizes
	ORDER BY
	used_mb DESC,
	table_name;
END
GO

------------------------------------------------------------
-- 31a. CompilePressure SingleUseAdhoc
------------------------------------------------------------
PRINT N'▶ 31a. CompilePressure SingleUseAdhoc - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_CompilePressure_SingleUseAdhoc') IS NOT NULL DROP TABLE #DTR_CompilePressure_SingleUseAdhoc
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	objtype,
	cacheobjtype,
	total_plans = COUNT(*),
	single_use_plans = SUM(CASE WHEN usecounts = 1 THEN 1 ELSE 0 END),
	multi_use_plans = SUM(CASE WHEN usecounts > 1 THEN 1 ELSE 0 END),
	total_cache_mb = CAST(SUM(size_in_bytes) / 1048576.0 AS decimal(18, 2)),
	single_use_cache_mb = CAST(SUM(CASE WHEN usecounts = 1 THEN size_in_bytes ELSE 0 END) / 1048576.0 AS decimal(18, 2)),
	CAST(100.0 * SUM(CASE WHEN usecounts = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS decimal(5, 2)) AS pct_single_use,
	CAST(100.0 * SUM(CASE WHEN usecounts = 1 THEN size_in_bytes ELSE 0 END) / NULLIF(SUM(size_in_bytes), 0) AS decimal(5, 2)) AS pct_single_use_cache,
	CASE WHEN CAST(100.0 * SUM(CASE WHEN usecounts = 1 THEN size_in_bytes ELSE 0 END) / NULLIF(SUM(size_in_bytes), 0) AS decimal(5, 2)) >= 50.0 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS is_high_single_use_cache
	INTO #DTR_CompilePressure_SingleUseAdhoc
	FROM sys.dm_exec_cached_plans AS cp
	OUTER APPLY sys.dm_exec_sql_text(cp.plan_handle) AS st
	WHERE st.dbid = DB_ID()
	GROUP BY objtype, cacheobjtype
	ORDER BY pct_single_use DESC;
END
GO

------------------------------------------------------------
-- 31b. CompilePressure MultiPlanByHash
------------------------------------------------------------
PRINT N'▶ 31b. CompilePressure MultiPlanByHash - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_CompilePressure_MultiPlanByHash') IS NOT NULL DROP TABLE #DTR_CompilePressure_MultiPlanByHash
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT TOP (200)
	IDENTITY(int) AS RowNumber,
	qs.query_hash,
	COUNT(DISTINCT qs.query_plan_hash) AS distinct_plans,
	COUNT(*) AS plans_cached,
	SUM(qs.execution_count) AS total_execs
	INTO #DTR_CompilePressure_MultiPlanByHash
	FROM sys.dm_exec_query_stats AS qs
	OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
	WHERE st.dbid = DB_ID()
	GROUP BY qs.query_hash
	HAVING COUNT(DISTINCT qs.query_plan_hash) > 1
	ORDER BY distinct_plans DESC, total_execs DESC;
END
GO

------------------------------------------------------------
-- 31c. Plan Guides (Enabled/Disabled)
------------------------------------------------------------
PRINT N'▶ 31c. Plan Guides (Enabled/Disabled) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_PlanGuides') IS NOT NULL DROP TABLE #DTR_PlanGuides
GO

	SELECT
	IDENTITY(int) AS RowNumber,
	name,
	is_disabled,
	scope_type_desc,
	CASE WHEN scope_type_desc = 'OBJECT' THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS is_object_scope,
	CASE WHEN scope_type_desc = 'SQL' THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS is_sql_scope,
	CASE WHEN scope_type_desc = 'TEMPLATE' THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS is_template_scope,
	scope_object_id,
	OBJECT_SCHEMA_NAME(scope_object_id, DB_ID()) AS scope_schema_name,
	OBJECT_NAME(scope_object_id, DB_ID()) AS scope_object_name,
	create_date,
	modify_date
INTO #DTR_PlanGuides
FROM sys.plan_guides;
GO

------------------------------------------------------------
-- 31d. Plan Cache Memory Breakdown (by Cache Objtype)
------------------------------------------------------------
PRINT N'▶ 31d. Plan Cache Memory Breakdown (by Cache Objtype) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_PlanCacheBreakdown') IS NOT NULL DROP TABLE #DTR_PlanCacheBreakdown
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	cp.cacheobjtype,
	cp.objtype,
	COUNT(*) AS plan_count,
	CAST(SUM(CAST(cp.size_in_bytes AS bigint)) / 1048576.0 AS decimal(19, 2)) AS total_size_mb,
	CAST(100.0 * SUM(CAST(cp.size_in_bytes AS bigint)) / NULLIF(SUM(SUM(CAST(cp.size_in_bytes AS bigint))) OVER (), 0) AS decimal(5, 2)) AS pct_of_cache
	INTO #DTR_PlanCacheBreakdown
	FROM sys.dm_exec_cached_plans AS cp
	GROUP BY cp.cacheobjtype, cp.objtype;
END
GO

------------------------------------------------------------
-- 31e. Plan Cache Memory by Database (Mb)
------------------------------------------------------------
PRINT N'▶ 31e. Plan Cache Memory by Database (Mb) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_PlanCacheByDb') IS NOT NULL DROP TABLE #DTR_PlanCacheByDb;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	COALESCE(DB_NAME(CAST(pa.value AS int)), '<unknown>') AS database_name,
	CAST(pa.value AS int) AS database_id,
	COUNT_BIG(*) AS total_plans,
	CAST(SUM(CAST(cp.size_in_bytes AS bigint)) / 1048576.0 AS decimal(19, 2)) AS total_size_mb,
	CAST(100.0 * SUM(CAST(cp.size_in_bytes AS bigint)) / NULLIF(totals.total_size_bytes, 0) AS decimal(5, 2)) AS pct_of_cache
	INTO #DTR_PlanCacheByDb
	FROM sys.dm_exec_cached_plans AS cp
	CROSS APPLY sys.dm_exec_plan_attributes(cp.plan_handle) AS pa
	CROSS JOIN (
		SELECT
		SUM(CAST(size_in_bytes AS bigint)) AS total_size_bytes
		FROM sys.dm_exec_cached_plans
		WHERE cacheobjtype = 'Compiled Plan'
			AND objtype IN ('Adhoc', 'Prepared', 'Proc')
	) AS totals
	WHERE pa.attribute = 'dbid'
		AND cp.cacheobjtype = 'Compiled Plan'
		AND cp.objtype IN ('Adhoc', 'Prepared', 'Proc')
		AND CAST(pa.value AS int) = DB_ID()
	GROUP BY CAST(pa.value AS int), totals.total_size_bytes
	ORDER BY total_size_mb DESC, database_name;
END
GO

------------------------------------------------------------
-- 32a. Index Operational Stats
------------------------------------------------------------
PRINT N'▶ 32a. Index Operational Stats - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_Index_Operational_Stats') IS NOT NULL DROP TABLE #DTR_Index_Operational_Stats
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH writes AS (
		SELECT TOP (20)
		i.object_id,
		i.index_id,
		SUM(ISNULL(s.user_updates, 0)) AS writes
		FROM sys.indexes AS i
		LEFT JOIN sys.dm_db_index_usage_stats AS s ON s.object_id = i.object_id AND s.index_id = i.index_id AND s.database_id = DB_ID()
		WHERE OBJECTPROPERTY(i.object_id, 'IsUserTable') = 1
			AND i.type_desc <> 'HEAP'
		GROUP BY
		i.object_id,
		i.index_id
		ORDER BY SUM(ISNULL(s.user_updates, 0)) DESC
	),
	ios AS (
		SELECT
		ios.object_id,
		ios.index_id,
		SUM(ios.leaf_insert_count) AS leaf_insert_count,
		SUM(ios.leaf_update_count) AS leaf_update_count,
		SUM(ios.leaf_delete_count) AS leaf_delete_count,
		SUM(ios.range_scan_count) AS range_scan_count,
		SUM(ios.row_lock_count) AS row_lock_count,
		SUM(ios.row_lock_wait_count) AS row_lock_wait_count,
		SUM(ios.page_lock_count) AS page_lock_count,
		SUM(ios.page_lock_wait_count) AS page_lock_wait_count,
		SUM(ios.index_lock_promotion_attempt_count) AS index_lock_promotion_attempt_count,
		SUM(ios.index_lock_promotion_count) AS index_lock_promotion_count
		FROM sys.dm_db_index_operational_stats(DB_ID(), NULL, NULL, NULL) AS ios
		GROUP BY
		ios.object_id,
		ios.index_id
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	i.object_id,
	i.index_id,
	QUOTENAME(OBJECT_SCHEMA_NAME(i.object_id)) + '.' + QUOTENAME(OBJECT_NAME(i.object_id)) AS table_name,
	i.name AS index_name,
	ios.leaf_insert_count,
	ios.leaf_update_count,
	ios.leaf_delete_count,
	ios.range_scan_count,
	ios.row_lock_count,
	ios.row_lock_wait_count,
	ios.page_lock_count,
	ios.page_lock_wait_count,
	ios.row_lock_wait_count + ios.page_lock_wait_count AS total_lock_wait_count,
	ios.index_lock_promotion_attempt_count,
	ios.index_lock_promotion_count
	INTO #DTR_Index_Operational_Stats
	FROM writes AS w
	JOIN sys.indexes AS i ON i.object_id = w.object_id AND i.index_id = w.index_id
	JOIN ios ON ios.object_id = w.object_id AND ios.index_id = w.index_id
	ORDER BY (ios.row_lock_wait_count + ios.page_lock_wait_count) DESC, ios.range_scan_count DESC;
END
GO

------------------------------------------------------------
-- 32b. Index Operational Hot-Spots (Leaf Mods & Ghosts)
------------------------------------------------------------
PRINT N'▶ 32b. Index Operational Hot-Spots (Leaf Mods & Ghosts) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_IndexOperationalHotspots') IS NOT NULL DROP TABLE #DTR_IndexOperationalHotspots
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH ios AS (
		SELECT
		ios.object_id,
		ios.index_id,
		SUM(leaf_insert_count) AS leaf_insert_count,
		SUM(leaf_update_count) AS leaf_update_count,
		SUM(leaf_delete_count) AS leaf_delete_count,
		SUM(leaf_ghost_count) AS leaf_ghost_count
		FROM sys.dm_db_index_operational_stats(DB_ID(), NULL, NULL, NULL) AS ios
		GROUP BY ios.object_id, ios.index_id
	),
	ix AS (
		SELECT
		i.object_id,
		i.index_id,
		i.name AS index_name,
		i.type_desc
		FROM sys.indexes AS i
	)
		SELECT
		IDENTITY(int) AS RowNumber,
		ios.object_id,
		ios.index_id,
		SCHEMA_NAME(o.schema_id) + '.' + o.name AS TableName,
		x.index_name,
		x.type_desc,
		ios.leaf_insert_count,
		ios.leaf_update_count,
		ios.leaf_delete_count,
		ios.leaf_ghost_count,
		ios.leaf_insert_count + ios.leaf_update_count + ios.leaf_delete_count AS total_leaf_mods,
		ios.leaf_ghost_count + ios.leaf_delete_count AS total_ghost_and_deletes
	INTO #DTR_IndexOperationalHotspots
	FROM ios
	JOIN ix AS x ON x.object_id = ios.object_id AND x.index_id = ios.index_id
	JOIN sys.objects AS o ON o.object_id = ios.object_id
	WHERE o.is_ms_shipped = 0
		AND (ios.leaf_insert_count + ios.leaf_update_count + ios.leaf_delete_count + ios.leaf_ghost_count) > 0;
END
GO

------------------------------------------------------------
-- 33a. Deadlock Xml
------------------------------------------------------------
PRINT N'▶ 33a. Deadlock Xml - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_Deadlock_XML') IS NOT NULL DROP TABLE #DTR_Deadlock_XML
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH x AS (
		SELECT
		CAST(xet.target_data AS XML) AS target_data
		FROM sys.dm_xe_session_targets AS xet
		JOIN sys.dm_xe_sessions AS xe ON xe.address = xet.event_session_address
		WHERE xe.name = 'system_health'
			AND xet.target_name = 'ring_buffer'
	)
	SELECT TOP (10)
	IDENTITY(int) AS RowNumber,
	n.value('@timestamp', 'DATETIME2(3)') AS DeadlockUtcTime,
	CASE WHEN cfg.SafeMode = 0 THEN CONVERT(NVARCHAR(MAX), n.query('.')) ELSE NULL END AS deadlock_xml
	INTO #DTR_Deadlock_XML
	FROM x
	CROSS JOIN #DT_Config AS cfg
	CROSS APPLY target_data.nodes('//event[@name="xml_deadlock_report"]') AS q(n)
	ORDER BY DeadlockUtcTime DESC;
END
GO

------------------------------------------------------------
-- 33b. Deadlock Summary (system_health)
------------------------------------------------------------
PRINT N'▶ 33b. Deadlock Summary (system_health) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_DeadlockSummaryXE') IS NOT NULL DROP TABLE #DTR_DeadlockSummaryXE
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH src AS (
		SELECT
		CAST(st.target_data AS XML) AS target_data
		FROM sys.dm_xe_session_targets AS st
		JOIN sys.dm_xe_sessions AS s ON s.address = st.event_session_address
		WHERE s.name = 'system_health' AND st.target_name = 'ring_buffer'
	),
	evt AS (
		SELECT
		xed.value('@timestamp[1]','datetime2') AS utc_ts,
		CAST(xed.value('(data/value)[1]','varchar(max)') AS xml) AS dl_xml
		FROM src
		CROSS APPLY target_data.nodes('//RingBufferTarget/event[@name="xml_deadlock_report"]') AS X(xed)
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	e.utc_ts AS DeadlockUtcTime,
	COALESCE(
		e.dl_xml.value('(deadlock/victim-list/victimProcess/@id)[1]','varchar(100)'),
		e.dl_xml.value('(/event/data/value/deadlock/victim-list/victimProcess/@id)[1]','varchar(100)')
	) AS VictimProcessId,
	e.dl_xml.value('count(//process-list/process)','int') AS ProcessCount,
	e.dl_xml.value('count(//resource-list/*)','int') AS ResourceCount,
	CASE WHEN e.dl_xml.value('count(//process-list/process)','int') > 2 THEN CONVERT(bit,1) ELSE CONVERT(bit,0) END AS HasMultipleProcesses,
	CASE WHEN e.dl_xml.value('count(//resource-list/*)','int') > 1 THEN CONVERT(bit,1) ELSE CONVERT(bit,0) END AS HasMultipleResources
	INTO #DTR_DeadlockSummaryXE
	FROM evt AS e
	WHERE e.dl_xml IS NOT NULL;
END
GO

------------------------------------------------------------
-- 34a. UDFs In Hot Queries
------------------------------------------------------------
PRINT N'▶ 34a. UDFs In Hot Queries - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_UDFs_In_Hot_Queries') IS NOT NULL DROP TABLE #DTR_UDFs_In_Hot_Queries
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH hot AS (
		SELECT TOP (200)
		qs.query_hash,
		SUBSTRING(
			st.text,
			(qs.statement_start_offset / 2) + 1,
			((CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(st.text) ELSE qs.statement_end_offset END)
				- qs.statement_start_offset) / 2 + 1
		) AS sql_text
		FROM sys.dm_exec_query_stats AS qs
		OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
		WHERE st.dbid = DB_ID()
		ORDER BY qs.total_worker_time DESC
	),
	udfs AS (
		SELECT
		o.object_id AS udf_object_id,
		o.type AS udf_type,
		o.type_desc AS udf_type_desc,
		CASE WHEN o.type IN ('FN', 'FS') THEN CONVERT(bit,1) ELSE CONVERT(bit,0) END AS IsScalarUdf,
		QUOTENAME(OBJECT_SCHEMA_NAME(o.object_id)) + '.' + QUOTENAME(o.name) AS udf_name
		FROM sys.objects AS o
		WHERE o.type IN ('FN', 'FS', 'TF', 'IF')
	)
	SELECT DISTINCT TOP (500)
	IDENTITY(int) AS RowNumber,
	h.query_hash,
	u.udf_object_id,
	u.udf_type,
	u.udf_type_desc,
	u.IsScalarUdf,
	u.udf_name,
	CASE WHEN cfg.SafeMode = 0 THEN LEFT(h.sql_text, 4000) ELSE '[SafeMode]' END AS query_snippet
	INTO #DTR_UDFs_In_Hot_Queries
	FROM hot AS h
	CROSS JOIN #DT_Config AS cfg
	JOIN udfs AS u ON h.sql_text COLLATE DATABASE_DEFAULT LIKE '%' + (u.udf_name COLLATE DATABASE_DEFAULT) + '%';
END
GO

-- 35a. Change Data Capture (CDC) Status
------------------------------------------------------------
PRINT N'▶ 35a. Change Data Capture (CDC) Status - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_CDCStatus') IS NOT NULL DROP TABLE #DTR_CDCStatus
GO

SELECT
IDENTITY(int) AS RowNumber,
DB_NAME() AS DatabaseName,
d.is_cdc_enabled,
CASE WHEN d.is_cdc_enabled = 1 THEN 'Enabled' ELSE 'Disabled' END AS cdc_status_desc
INTO #DTR_CDCStatus
FROM sys.databases AS d
WHERE d.database_id = DB_ID();
GO

------------------------------------------------------------
-- 35b. Replication Posture (Current DB)
------------------------------------------------------------
PRINT N'▶ 35b. Replication Posture (Current DB) - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_ReplPublications') IS NOT NULL DROP TABLE #DTR_ReplPublications;
GO

SELECT
IDENTITY(int) AS RowNumber,
DB_NAME() AS DatabaseName,
d.is_published,
d.is_subscribed,
d.is_merge_published,
d.is_distributor,
CASE
	WHEN d.is_published = 1 OR d.is_subscribed = 1 OR d.is_merge_published = 1 THEN CONVERT(bit, 1)
	ELSE CONVERT(bit, 0)
END AS is_replication_participant
INTO #DTR_ReplPublications
FROM sys.databases AS d
WHERE d.database_id = DB_ID();
GO

------------------------------------------------------------
-- 36a. Identity Near Max
------------------------------------------------------------
PRINT N'▶ 36a. Identity Near Max - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_Identity_Near_Max') IS NOT NULL DROP TABLE #DTR_Identity_Near_Max
GO

WITH x AS (
	SELECT
	table_name = QUOTENAME(OBJECT_SCHEMA_NAME(t.object_id)) + '.' + QUOTENAME(t.name),
	column_name = c.name,
	data_type = tp.name,
	seed_value = CAST(ic.seed_value AS decimal(38, 0)),
	increment_value = CAST(ic.increment_value AS decimal(38, 0)),
	last_value_num = CASE
		WHEN SQL_VARIANT_PROPERTY(ic.last_value, 'BaseType') IN ('tinyint', 'smallint', 'int', 'bigint') THEN CONVERT(decimal(38, 0), ic.last_value)
		WHEN SQL_VARIANT_PROPERTY(ic.last_value, 'BaseType') IN ('numeric', 'decimal') AND ISNULL(SQL_VARIANT_PROPERTY(ic.last_value, 'Scale'), 0) = 0 THEN CONVERT(decimal(38, 0), ic.last_value)
		ELSE NULL
	END,
	max_value = CASE
		WHEN tp.name = 'tinyint' THEN CAST(255 AS decimal(38, 0))
		WHEN tp.name = 'smallint' THEN CAST(32767 AS decimal(38, 0))
		WHEN tp.name = 'int' THEN CAST(2147483647 AS decimal(38, 0))
		WHEN tp.name = 'bigint' THEN CAST(9223372036854775807 AS decimal(38, 0))
		WHEN tp.name IN ('numeric', 'decimal') AND c.scale = 0
			THEN CONVERT(decimal(38, 0), POWER(CAST(10 AS decimal(38, 0)), c.precision) - 1)
	END
	FROM sys.identity_columns AS ic
	JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
	JOIN sys.tables AS t ON t.object_id = ic.object_id
	JOIN sys.types AS tp ON c.user_type_id = tp.user_type_id
	WHERE (tp.name IN ('tinyint', 'smallint', 'int', 'bigint') OR (tp.name IN ('numeric', 'decimal') AND c.scale = 0))
)
SELECT TOP (100)
IDENTITY(int) AS RowNumber,
table_name,
column_name,
data_type,
seed_value,
increment_value,
last_value = last_value_num,
max_value,
percent_of_max = CASE
	WHEN last_value_num IS NULL OR max_value = 0 THEN NULL
	ELSE CAST(last_value_num * 100.0 / max_value AS decimal(5, 2))
END
INTO #DTR_Identity_Near_Max
FROM x
WHERE
last_value_num IS NOT NULL
AND max_value IS NOT NULL
AND last_value_num >= 0.9 * max_value
ORDER BY percent_of_max DESC, last_value_num DESC;
GO

------------------------------------------------------------
-- 37a. Lookup / Enum Shortlist (Heuristic)
------------------------------------------------------------
PRINT N'▶ 37a. Lookup / Enum Shortlist (Heuristic) - ' + CONVERT(char(8), GETDATE(), 108)
GO

IF OBJECT_ID('tempdb..#DTR_LookupShortlist') IS NOT NULL DROP TABLE #DTR_LookupShortlist
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	;WITH t AS ( -- user tables
		SELECT
		tb.object_id,
		SCHEMA_NAME(tb.schema_id) AS schema_name,
		tb.name AS table_name
		FROM sys.tables AS tb
		WHERE tb.is_ms_shipped = 0
	),
	rc AS ( -- heap/clustered row count
		SELECT
		p.object_id,
		SUM(p.row_count) AS row_count
		FROM sys.dm_db_partition_stats AS p
		WHERE p.index_id IN (0,1)
		GROUP BY p.object_id
	),
	fi_in AS ( -- inbound FK reuse (distinct parents)
		SELECT
		fk.referenced_object_id AS object_id,
		COUNT(DISTINCT fk.parent_object_id) AS fk_in_distinct_parents
		FROM sys.foreign_keys AS fk
		GROUP BY fk.referenced_object_id
	),
	fi_out AS ( -- outbound FKs from the table
		SELECT
		fk.parent_object_id AS object_id,
		COUNT(*) AS outbound_fk_count
		FROM sys.foreign_keys AS fk
		GROUP BY fk.parent_object_id
	),
	pk AS ( -- simple, int-like PK?
		SELECT
		kc.parent_object_id AS object_id,
		COUNT(*) AS pk_cols,
		MAX(CASE WHEN ty.name IN ('tinyint','smallint','int','bigint') THEN 1 ELSE 0 END) AS pk_is_intlike
		FROM sys.key_constraints AS kc
		JOIN sys.index_columns AS ic ON ic.object_id = kc.parent_object_id AND ic.index_id = kc.unique_index_id
		JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
		JOIN sys.types AS ty ON ty.user_type_id = c.user_type_id
		WHERE kc.type = 'PK'
		GROUP BY kc.parent_object_id
	),
	uq AS ( -- unique index that includes Code/Name
		SELECT
		i.object_id,
		MAX(CASE WHEN i.is_unique = 1 AND LOWER(c.name) LIKE '%code%' THEN 1 ELSE 0 END) AS has_unique_code,
		MAX(CASE WHEN i.is_unique = 1 AND LOWER(c.name) LIKE '%name%' THEN 1 ELSE 0 END) AS has_unique_name
		FROM sys.indexes AS i
		JOIN sys.index_columns AS ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
		JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
		GROUP BY i.object_id
	),
	col AS ( -- column count
		SELECT
		c.object_id,
		COUNT(*) AS col_count
		FROM sys.columns AS c
		GROUP BY c.object_id
	),
	lob AS ( -- LOB-ish columns (MAX types / legacy LOBs / XML, spatial, etc.)
		SELECT
		c.object_id,
		SUM(CASE
			WHEN (t.name IN ('varchar','nvarchar','varbinary') AND c.max_length = -1)
				OR t.name IN ('text','ntext','image','xml','geography','geometry','hierarchyid')
			THEN 1 ELSE 0
		END) AS lob_cols
		FROM sys.columns AS c
		JOIN sys.types AS t ON t.user_type_id = c.user_type_id
		GROUP BY c.object_id
	),
	hint AS ( -- table name looks like a lookup?
		SELECT
		t.object_id,
		CASE
			WHEN LOWER(t.table_name) LIKE '%status%'
				OR LOWER(t.table_name) LIKE '%state%'
				OR LOWER(t.table_name) LIKE '%type%'
				OR LOWER(t.table_name) LIKE '%code%'
				OR LOWER(t.table_name) LIKE '%category%'
				OR LOWER(t.table_name) LIKE '%reason%'
				OR LOWER(t.table_name) LIKE '%outcome%'
				OR LOWER(t.table_name) LIKE '%stage%'
				OR LOWER(t.table_name) LIKE '%lookup%'
				OR LOWER(t.table_name) LIKE '%enum%'
			THEN 1 ELSE 0
		END AS name_looks_lookup
		FROM t
	)
	SELECT
	IDENTITY(int) AS RowNumber,
	t.schema_name + '.' + t.table_name AS TableName,
	ISNULL(rc.row_count, 0) AS [RowCount],
	ISNULL(fi_in.fk_in_distinct_parents, 0) AS InboundFKDistinctParents,
	hint.name_looks_lookup AS NameLooksLookup,
	ISNULL(uq.has_unique_code, 0) AS HasUniqueCode,
	ISNULL(uq.has_unique_name, 0) AS HasUniqueName,
	ISNULL(pk.pk_cols, 0) AS PKCols,
	ISNULL(pk.pk_is_intlike, 0) AS PKIsIntLike,
	lbl.LabelColGuess,
	s.Score
	INTO #DTR_LookupShortlist
	FROM t
	LEFT JOIN rc ON rc.object_id = t.object_id
	LEFT JOIN fi_in ON fi_in.object_id = t.object_id
	LEFT JOIN fi_out ON fi_out.object_id = t.object_id
	LEFT JOIN pk ON pk.object_id = t.object_id
	LEFT JOIN uq ON uq.object_id = t.object_id
	LEFT JOIN col ON col.object_id = t.object_id
	LEFT JOIN lob ON lob.object_id = t.object_id
	LEFT JOIN hint ON hint.object_id = t.object_id
	OUTER APPLY (
		SELECT TOP (1)
		c.name AS LabelColGuess
		FROM sys.columns AS c
		WHERE c.object_id = t.object_id
			AND (
				LOWER(c.name) LIKE '%status%'
				OR LOWER(c.name) LIKE '%name%'
				OR LOWER(c.name) LIKE '%code%'
				OR LOWER(c.name) LIKE '%desc%'
				OR LOWER(c.name) LIKE '%description%'
			)
		ORDER BY CASE
			WHEN LOWER(c.name) LIKE '%status%' THEN 1
			WHEN LOWER(c.name) LIKE '%name%' THEN 2
			WHEN LOWER(c.name) LIKE '%code%' THEN 3
			WHEN LOWER(c.name) LIKE '%desc%' OR LOWER(c.name) LIKE '%description%' THEN 4
			ELSE 5
		END
	) AS lbl
	CROSS APPLY ( -- single Score computation
		SELECT
		CASE
			WHEN ISNULL(rc.row_count, 0) <= 20 THEN 8
			WHEN ISNULL(rc.row_count, 0) <= 200 THEN 4
			ELSE 0
		END
		+ CASE
			WHEN ISNULL(fi_in.fk_in_distinct_parents, 0) >= 3 THEN 4
			WHEN ISNULL(fi_in.fk_in_distinct_parents, 0) >= 1 THEN 2
			ELSE 0
		END
		+ CASE WHEN hint.name_looks_lookup = 1 THEN 2 ELSE 0 END
		+ CASE WHEN ISNULL(uq.has_unique_code, 0) = 1 OR ISNULL(uq.has_unique_name, 0) = 1 THEN 1 ELSE 0 END
		+ CASE WHEN ISNULL(pk.pk_cols, 0) = 1 AND ISNULL(pk.pk_is_intlike, 0) = 1 THEN 1 ELSE 0 END
		+ CASE WHEN ISNULL(col.col_count, 0) <= 6 THEN 1 ELSE 0 END
	) AS s(Score)
	WHERE s.Score >= 12
		AND ISNULL(rc.row_count, 0) > 0
		/* Drop likely false-positives unless table looks like a lookup by name */
		AND (
			hint.name_looks_lookup = 1
			OR (
				ISNULL(lob.lob_cols, 0) = 0
				AND ISNULL(fi_out.outbound_fk_count, 0) <= 1
				AND ISNULL(col.col_count, 0) <= 6
			)
		)
	ORDER BY s.Score DESC, [RowCount] ASC, [InboundFKDistinctParents] DESC
END
GO

------------------------------------------------------------
-- 37b. TDE Encryption Status
------------------------------------------------------------
PRINT N'▶ 37b. TDE Encryption Status - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

IF OBJECT_ID('tempdb..#DTR_TDEStatus') IS NOT NULL DROP TABLE #DTR_TDEStatus;
GO

IF (SELECT IsSysAdmin FROM #DT_Config) = 1
BEGIN
	SELECT
	IDENTITY(int) AS RowNumber,
	DB_NAME() AS DatabaseName,
	dek.encryption_state,
	CASE dek.encryption_state
		WHEN 0 THEN 'No database encryption key present'
		WHEN 1 THEN 'Unencrypted'
		WHEN 2 THEN 'Encryption in progress'
		WHEN 3 THEN 'Encrypted'
		WHEN 4 THEN 'Key change in progress'
		WHEN 5 THEN 'Decryption in progress'
		WHEN 6 THEN 'Protection change in progress'
		ELSE 'Unknown (' + CONVERT(varchar(20), dek.encryption_state) + ')'
	END AS encryption_state_desc,
	dek.key_algorithm,
	dek.key_length,
	CASE WHEN cfg.SafeMode = 0 THEN c.name ELSE '[SafeMode]' END AS certificate_name,
	CASE WHEN cfg.SafeMode = 0 THEN c.expiry_date ELSE NULL END AS certificate_expiry,
	CASE WHEN cfg.SafeMode = 0 THEN c.thumbprint ELSE NULL END AS certificate_thumbprint
	INTO #DTR_TDEStatus
  	FROM sys.dm_database_encryption_keys AS dek
	CROSS JOIN #DT_Config AS cfg
  	LEFT JOIN master.sys.certificates AS c ON dek.encryptor_thumbprint = c.thumbprint
  	WHERE dek.database_id = DB_ID();
END
GO

------------------------------------------------------------
-- #AppendLine - used to print a single line with handling for over 4000 characters
------------------------------------------------------------
IF OBJECT_ID('tempdb..#AppendLine') IS NOT NULL DROP PROCEDURE #AppendLine;
GO
CREATE PROCEDURE #AppendLine
	@s  nvarchar(max),
	@p1 nvarchar(max)=NULL,@p2 nvarchar(max)=NULL,@p3 nvarchar(max)=NULL,@p4 nvarchar(max)=NULL,
	@p5 nvarchar(max)=NULL,@p6 nvarchar(max)=NULL,@p7 nvarchar(max)=NULL,@p8 nvarchar(max)=NULL
AS
BEGIN
	IF @s IS NULL SET @s = '';

	-- Base text normalized to DB default
	DECLARE @t nvarchar(max) = @s COLLATE DATABASE_DEFAULT;

	-- Token→value map (only non-NULL values kept), all collated the same
	DECLARE @kv TABLE(
		token nvarchar(10) COLLATE DATABASE_DEFAULT,
		val   nvarchar(max) COLLATE DATABASE_DEFAULT
	);

	INSERT INTO @kv(token, val)
	SELECT v.token COLLATE DATABASE_DEFAULT, v.val COLLATE DATABASE_DEFAULT
	FROM (VALUES
		('{%1}', @p1),
		('{%2}', @p2),
		('{%3}', @p3),
		('{%4}', @p4),
		('{%5}', @p5),
		('{%6}', @p6),
		('{%7}', @p7),
		('{%8}', @p8)
	) AS v(token, val)
	WHERE v.val IS NOT NULL;

	-- Apply replacements (one pass per provided token)
	WHILE EXISTS (SELECT 1 FROM @kv)
	BEGIN
		DECLARE @k nvarchar(10), @v nvarchar(max);

		SELECT TOP (1)
		@k = token,
		@v = val
		FROM @kv;

		SET @t = REPLACE(@t, @k, @v);
		DELETE TOP (1) FROM @kv WHERE token = @k AND val = @v;
	END

	-- Long-line safe print (preserves whitespace)
	DECLARE @i int = 1, @chunk int = 4000, @len int = DATALENGTH(@t)/2;
	WHILE @i <= @len
	BEGIN
		PRINT SUBSTRING(@t, @i, @chunk);
		SET @i += @chunk;
	END
	IF @len = 0 PRINT '';
END;
GO

------------------------------------------------------------
-- #AppendCsv - used to print a table of data in csv format (within a `csv` block)
------------------------------------------------------------
IF OBJECT_ID('tempdb..#AppendCsv') IS NOT NULL DROP PROCEDURE #AppendCsv;
GO
CREATE PROCEDURE #AppendCsv
	@TableName sysname
AS
BEGIN
	DECLARE @objid int = OBJECT_ID('tempdb..' + @TableName);
	IF @objid IS NULL
	BEGIN
		EXEC #AppendLine '> No results available for this section.';
		EXEC #AppendLine '';
	RETURN;
	END

	DECLARE @OrderBy nvarchar(max) = NULL;
	IF EXISTS (SELECT 1 FROM tempdb.sys.columns WHERE object_id=@objid AND name='RowNumber')
	SET @OrderBy = '[RowNumber]';

	EXEC #AppendLine '```csv';

	-- Build header deterministically (one line), COLLATE everything to DB default
	DECLARE @header nvarchar(max) = '';
	SELECT @header =
			@header
			+ CASE WHEN @header COLLATE DATABASE_DEFAULT = '' COLLATE DATABASE_DEFAULT
				THEN '' COLLATE DATABASE_DEFAULT
				ELSE ',' COLLATE DATABASE_DEFAULT
			END
			+ '"' + (c.name COLLATE DATABASE_DEFAULT) + '"'
	FROM tempdb.sys.columns AS c
	WHERE c.object_id = @objid
	ORDER BY c.column_id;

	EXEC #AppendLine @header;

	-- Row expression: quote fields, escape quotes, CR/LF -> '' (empty), NULL -> ""
	DECLARE @delim nvarchar(10) = ' + '','' + ';
	DECLARE @colExpr nvarchar(max) = '';

	SELECT @colExpr =
	STUFF((
		SELECT @delim +
				'(NCHAR(34) + '
			+ 'REPLACE(REPLACE(REPLACE('
			+ 'COALESCE(CONVERT(nvarchar(max),' + QUOTENAME(c.name) + ') COLLATE DATABASE_DEFAULT, ''''), '
			+ 'NCHAR(34), NCHAR(34)+NCHAR(34)), CHAR(13), ''''), CHAR(10), '''')'
			+ ' + NCHAR(34))'
		FROM tempdb.sys.columns AS c
		WHERE c.object_id = @objid
		ORDER BY c.column_id
		FOR XML PATH(''), TYPE).value('.','nvarchar(max)'), 1, LEN(@delim), '');

	DECLARE @sql nvarchar(max) =
	'SELECT ' + @colExpr + ' AS csv_line FROM ' + QUOTENAME(@TableName)
		+ CASE WHEN @OrderBy IS NOT NULL THEN ' ORDER BY ' + @OrderBy ELSE '' END;

	IF OBJECT_ID('tempdb..#csv_buffer') IS NOT NULL DROP TABLE #csv_buffer;
	CREATE TABLE #csv_buffer (id int IDENTITY(1,1) PRIMARY KEY, csv_line nvarchar(max));

	INSERT INTO #csv_buffer(csv_line)
	EXEC sys.sp_executesql @sql;

	DECLARE @line nvarchar(max);
	DECLARE c CURSOR LOCAL FAST_FORWARD FOR SELECT csv_line FROM #csv_buffer ORDER BY id;

	OPEN c;
	FETCH NEXT FROM c INTO @line;
	WHILE @@FETCH_STATUS = 0
	BEGIN
		EXEC #AppendLine @line;
		FETCH NEXT FROM c INTO @line;
	END
	CLOSE c; DEALLOCATE c;

	DROP TABLE #csv_buffer;

	EXEC #AppendLine '```';
END;
GO

------------------------------------------------------------
-- #AppendSql - used to print a table of schema information in their original format (within a `sql` block)
------------------------------------------------------------
IF OBJECT_ID('tempdb..#AppendSql') IS NOT NULL DROP PROCEDURE #AppendSql
GO
CREATE PROCEDURE #AppendSql
	@TableName  sysname,   -- e.g. '#DTR_Modules'
	@BodyColumn sysname,   -- e.g. 'definition'
	@TypeCol    sysname = NULL,   -- type label column (or NULL to omit)
	@SchemaCol  sysname = NULL,   -- schema column (or NULL)
	@NameCol    sysname = NULL -- name/path column (or NULL)
AS
BEGIN
	DECLARE @objid int = OBJECT_ID('tempdb..' + @TableName)
	IF @objid IS NULL RETURN

	-- Validate body column exists
	IF NOT EXISTS (SELECT 1 FROM tempdb.sys.columns WHERE object_id=@objid AND name = @BodyColumn)
	BEGIN
		EXEC #AppendLine '> AppendSql: body column [{%1}] not found in {%2}.', @BodyColumn, @TableName
		EXEC #AppendLine ''
		RETURN
	END

	-- Order: only RowNumber if present
	DECLARE @OrderByClause nvarchar(max) = ''
	IF EXISTS (SELECT 1 FROM tempdb.sys.columns WHERE object_id=@objid AND name = 'RowNumber')
	SET @OrderByClause = ' ORDER BY [RowNumber]'

	-- Title column presence (tolerate NULLs)
	DECLARE @HasType bit   = CASE WHEN @TypeCol   IS NOT NULL AND EXISTS (SELECT 1 FROM tempdb.sys.columns WHERE object_id=@objid AND name = @TypeCol)   THEN 1 ELSE 0 END
	DECLARE @HasSch  bit   = CASE WHEN @SchemaCol IS NOT NULL AND EXISTS (SELECT 1 FROM tempdb.sys.columns WHERE object_id=@objid AND name = @SchemaCol) THEN 1 ELSE 0 END
	DECLARE @HasName bit   = CASE WHEN @NameCol   IS NOT NULL AND EXISTS (SELECT 1 FROM tempdb.sys.columns WHERE object_id=@objid AND name = @NameCol)   THEN 1 ELSE 0 END

	-- Build dynamic SELECT for titles + body (compat-safe)
	DECLARE @sql nvarchar(max) = 'SELECT ' +
		CASE WHEN @HasType=1 THEN 'CONVERT(nvarchar(max),' + QUOTENAME(@TypeCol)   + ')' ELSE 'CAST(NULL AS nvarchar(max))' END + ' AS __t, ' +
		CASE WHEN @HasSch =1 THEN 'CONVERT(nvarchar(max),' + QUOTENAME(@SchemaCol) + ')' ELSE 'CAST(NULL AS nvarchar(max))' END + ' AS __s, ' +
		CASE WHEN @HasName=1 THEN 'CONVERT(nvarchar(max),' + QUOTENAME(@NameCol)   + ')' ELSE 'CAST(NULL AS nvarchar(max))' END + ' AS __n, ' +
		'CONVERT(nvarchar(max),' + QUOTENAME(@BodyColumn) + ') AS __body ' +
		'FROM ' + QUOTENAME(@TableName) + @OrderByClause

	IF OBJECT_ID('tempdb..#DTR_SqlBuffer') IS NOT NULL DROP TABLE #DTR_SqlBuffer
	
	CREATE TABLE #DTR_SqlBuffer (
		id int IDENTITY(1,1) PRIMARY KEY,
		__t nvarchar(max),
		__s nvarchar(max),
		__n nvarchar(max),
		__body nvarchar(max)
	)

	INSERT INTO #DTR_SqlBuffer(__t, __s, __n, __body)
	EXEC sys.sp_executesql @sql

	IF NOT EXISTS (SELECT 1 FROM #DTR_SqlBuffer)
	BEGIN
		EXEC #AppendLine '> No rows.'
		EXEC #AppendLine ''
		DROP TABLE #DTR_SqlBuffer
		RETURN
	END

	DECLARE @t nvarchar(max), @s nvarchar(max), @n nvarchar(max), @b nvarchar(max)
	
	DECLARE d CURSOR LOCAL FAST_FORWARD FOR
	SELECT __t, __s, __n, __body
	FROM #DTR_SqlBuffer
	ORDER BY id
	
	OPEN d
	FETCH NEXT FROM d INTO @t,@s,@n,@b
	WHILE @@FETCH_STATUS = 0
	BEGIN
		-- Optional title line: "### <type> [schema].[name]"
		IF @t IS NOT NULL OR @s IS NOT NULL OR @n IS NOT NULL
		BEGIN
			DECLARE @title nvarchar(max) =
				ISNULL(@t + ' ', '') +
				CASE
					WHEN @s IS NOT NULL AND @n IS NOT NULL THEN '[' + @s + '].' + '[' + @n + ']'
					WHEN @n IS NOT NULL THEN '[' + @n + ']'
					ELSE ISNULL(@s, '')
				END
			IF LEN(@title) > 0 EXEC #AppendLine '### {%1}', @title
		END
		
		-- Trim trailing CR/LF from body so the code fence stays tight
		WHILE @b IS NOT NULL
			AND LEN(@b) > 0
			AND UNICODE(RIGHT(@b, 1)) IN (10, 13)
		BEGIN
			SET @b = LEFT(@b, LEN(@b) - 1);
		END
		
		EXEC #AppendLine '```sql'
		EXEC #AppendLine @b
		EXEC #AppendLine '```'
		EXEC #AppendLine ''
		
		FETCH NEXT FROM d INTO @t,@s,@n,@b
	END
	CLOSE d;
	DEALLOCATE d;

	DROP TABLE #DTR_SqlBuffer
END
GO

------------------------------------------------------------
-- Export Markdown to {OutputDir}\dt_legacy_report ({TargetDB} - {Version}).md
------------------------------------------------------------
PRINT 'Exporting Markdown' + CASE WHEN '$(ExportSchema)' = '1' THEN ' and Schema' ELSE '' END + ' - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

:OUT $(OutputDir)"\dt_legacy_report ("$(TargetDB)" - "$(Version)").md"

DECLARE @DatabaseName nvarchar(128) = DB_NAME()
DECLARE @ServerName nvarchar(128) = CONVERT(nvarchar(128), SERVERPROPERTY('ServerName'))
DECLARE @SysDateTime nvarchar(30) = CONVERT(nvarchar(30),  SYSDATETIME(), 126)
DECLARE @ExportSchema nvarchar(3) = CASE WHEN '$(ExportSchema)' = '1' THEN 'On' ELSE 'Off' END;
DECLARE @SafeMode nvarchar(3) = CASE WHEN '$(SafeMode)' = '1' THEN 'On' ELSE 'Off' END;

-- Database Tuner Legacy Report
EXEC #AppendLine '# Database Tuner Legacy Report'
EXEC #AppendLine ''
EXEC #AppendLine '- **Version:** $(Version)'
EXEC #AppendLine '- **Target DB:** [{%1}]', @DatabaseName
EXEC #AppendLine '- **Server:** {%1}', @ServerName
EXEC #AppendLine '- **Generated (local):** {%1}', @SysDateTime
EXEC #AppendLine '- **Export Schema:** {%1}', @ExportSchema
EXEC #AppendLine '- **Safe Mode:** {%1}', @SafeMode
EXEC #AppendLine ''

-- Assistant Brief (for GPT)
EXEC #AppendLine '# Assistant Brief - How to use (LLM response protocol)';
EXEC #AppendLine '';
EXEC #AppendLine 'Non-exhaustive: This Assistant Brief defines the LLM response protocol (for example first screen, menus, navigation). The Assistant Brief Table children are hints, not hard rules; if MD evidence supports an action not listed, recommend it and cite the evidence.';
EXEC #AppendLine '';
EXEC #AppendLine '## Core operating principles';
EXEC #AppendLine '';
EXEC #AppendLine '- Evidence beats priors. Always ground recommendations in this MD export''s #DTR_* evidence. If any table value or prior (including ''Priority'') conflicts with observed evidence, ignore the prior, note the conflict briefly, and proceed with the evidence-backed path.';
EXEC #AppendLine '- Version/compat are soft gates. Use SQL Server and SQL Compat to avoid suggesting unsupported features. If blocked by version or edition, propose the nearest safe alternative and explain the trade-off.';
EXEC #AppendLine '- Effort vs. impact (tie-break rule). Prefer DB-only (EDMX = No) changes when expected impact is comparable. Do not suppress higher-gain actions that require EDMX changes; include them when evidence indicates materially better outcomes, and call out delivery implications.';
EXEC #AppendLine '- Notes = search tokens, not guards. Use tokens in Notes to find signals fast (for example: CONVERT_IMPLICIT, spill, PAGEIOLATCH, CXPACKET/CXCONSUMER, THREADPOOL, WRITELOG, TempDB). Preconditions are carried only in the Preconditions column.';
EXEC #AppendLine '- Numeric fields are heuristics. Treat any numeric priors (for example: confidence, reach, plan stability, risk, priority) as weak hints, not mandates.';
EXEC #AppendLine '- Cite precisely. When recommending an action, reference the strongest concrete evidence: query IDs, object names, wait names, stats age, or relevant CSV blocks.';
EXEC #AppendLine '- Export Schema = user configuration setting which appends a database schema export section to the end of the markdown report.';
EXEC #AppendLine '- Safe Mode = user configuration setting which redacts sensitive information (shows [SafeMode] for redacted values).';
EXEC #AppendLine '- Low-signal behaviour (applies to all main-menu items and helpers). If a primary runtime slice (for example Top Queries, Missing Indexes, Wait Stats) is empty or low-signal, acknowledge this once in a short, matter-of-fact sentence, then immediately pivot to other evidence that does exist: schema, index definitions, statistics, module code, file layout, backup history, server configuration, or cross-database patterns. Never make \"no signals\", \"nothing to tune\", \"re-run the collector\", or \"enable feature X and try again\" the main story or the primary recommendation; it is fine to mention instrumentation gaps (for example low activity or limited permissions) or \"collect more data later\" as a secondary note near the end, not as the headline.';
EXEC #AppendLine '- Dev / test / low-activity databases. Treat dev or low-usage databases the same as production in terms of analysis quality: a quiet DMV surface does not mean there is nothing to improve. When DMVs are empty because the database is not under load, assume the stored procedures, views, and functions represent the intended future workload. From that code and schema alone, still evaluate predicates and joins for SARGability (functions on filter columns, CASE in joins, non-searchable patterns) and infer likely missing or suboptimal indexes based on common join/filter columns and key order, even if the missing-index DMVs return no rows.';
EXEC #AppendLine '- Prefer concrete, object-level actions. Frame recommendations as specific changes to objects: for example \"rewrite this view predicate for SARGability\", \"drop or consolidate these unused indexes\", \"change these file growth settings\", \"add these targeted statistics\". Where possible, show example T-SQL or exact settings, and tie each recommendation to the evidence you have. It is better to give a small number of well-reasoned, schema-driven recommendations than many vague or generic ones.';
EXEC #AppendLine '- Use cross-area thinking. If one evidence stream is empty, lean on others: for example, when there are no hot queries, look at T-SQL modules, indexes, and statistics and propose SARGable rewrites or better index designs; when missing-index DMVs are empty, use schema plus index usage to propose obvious new indexes or index consolidation; when waits are quiet, look at file layout, autogrowth, backup strategy, CHECKDB posture, and maintenance patterns. Connect dots across areas when helpful (for example a heavy trigger pattern in T-SQL plus slow log IO plus autogrowth settings).';
EXEC #AppendLine '- Keep the tone pragmatic. Avoid blaming the environment or the script for lack of signals. Assume the user already knows the environment is quiet or dev; focus on what they can improve today and what will make future troubleshooting easier. Be clear about risk versus impact, but stay practical and action-oriented.';
EXEC #AppendLine '';
EXEC #AppendLine '## First interaction (Main Menu)';
EXEC #AppendLine '';
EXEC #AppendLine 'When the MD is opened for the first time in a conversation (including generic prompts like \"process the md\", \"analyse this\", or \"summarise this report\"), you should normally display the header and home screen (Main Menu 1-7, Helpers 11-20, Top-10 (T1-10)). The Main Menu home screen must always include Helpers (11-20) and Top-10 (T1-10); do not omit these sections. Only when the user''s very first message gives explicit instructions to investigate a specific object or condition may you skip the home screen and follow that request directly.';
EXEC #AppendLine '';
EXEC #AppendLine '- Print the header block:';
EXEC #AppendLine '  - `## Database Tuner Legacy Report {Version}`';
EXEC #AppendLine '  - `Target DB: [{TargetDB}] on {Instance}`';
EXEC #AppendLine '  - `SQL Server Version: {SQL Server version/edition}`';
EXEC #AppendLine '  - `Database Compat: {compat level}`';
EXEC #AppendLine '  - `---`';
EXEC #AppendLine '  - `### 1-7. Main Menu`';
EXEC #AppendLine '- Menu (1-7): show these parents with bold title line, then one-line plain-text description on its own line:';
EXEC #AppendLine '  1) Workload and Optimizer Plans';
EXEC #AppendLine '     Workload patterns, execution plans, plan cache.';
EXEC #AppendLine '  2) T-SQL Modules (Views/Procs/UDFs)';
EXEC #AppendLine '     Views, stored procedures, functions, and triggers behaviour.';
EXEC #AppendLine '  3) Indexes and Statistics';
EXEC #AppendLine '     Index design, fragmentation, and statistics health.';
EXEC #AppendLine '  4) Schema and Data Modeling';
EXEC #AppendLine '     Table design, keys, relationships, and very large tables.';
EXEC #AppendLine '  5) Concurrency and Transactions';
EXEC #AppendLine '     Blocking, isolation, versioning, and transaction patterns.';
EXEC #AppendLine '  6) Platform, Storage and Maintenance';
EXEC #AppendLine '     Server configuration, memory, files, IO, and maintenance posture.';
EXEC #AppendLine '  7) Full Analysis';
EXEC #AppendLine '     Prioritised recommendations across all areas based on this snapshot.';
EXEC #AppendLine '- Helpers (11-20): positioned between Menu and Top-10. Currently defined:';
EXEC #AppendLine '  11) Top 5 queries / T-SQL modules to tune (SARGable - from DMVs and schema).';
EXEC #AppendLine '  12) Top 5 index actions (missing / redundant / covering - from DMVs and schema).';
EXEC #AppendLine '- Top-10 evidence-backed low-risk opportunities (T1-10):';
EXEC #AppendLine '  - Low-risk quick wins (target Risk <= 4.0; allow slightly higher only if clearly very high-impact, low effort, and there are not enough clean low-risk candidates). If no meaningful workload/DMV data, still populate T1-10 from schema-driven analysis (views/procs/indexes/statistics) rather than defaulting to maintenance-only items.';
EXEC #AppendLine '  - Make the Top-10 primarily about changes to objects in the target database: SARGable rewrites of views and stored procedures, function/trigger simplifications, and targeted index/statistics changes that keep semantics intact. Aim for several view optimisations (e.g., 2-3), a couple of procedure optimisations (e.g., 1-2), a couple of index/statistics optimisations or new indexes (e.g., 1-2), and only a small number of database-level enhancements. When both schema and configuration changes are available, prefer the schema changes for the higher slots.';
EXEC #AppendLine '  - Do not surface instance-level or server-wide setting changes (for example MAXDOP, cost threshold, max server memory, backup compression, IFI) in the top of the list. Reserve such settings for parent 6 / 7 drill-downs, or at most for the last few Top-10 slots (for example T9-T10) when they are strongly evidence-backed and low-risk; do not allow settings tweaks to displace strong schema optimisation opportunities from T1-T8.';
EXEC #AppendLine '  - Exclude medium/high-risk actions (for example compatibility level changes, major schema or audit-framework refactors, large partitioning schemes) from T1-10; surface those instead in drill-downs or Full Analysis with appropriate higher Risk scores.';
EXEC #AppendLine '  - Do not risk-filter the Menu parents or their drill-down content; per-parent views may surface a mix of low, medium, and high-risk items with Impact/Risk scores shown so the operator can choose how deep to go.';
EXEC #AppendLine '- Top-10 layout on the Main Menu:';
EXEC #AppendLine '  - Each item Tn renders:';
EXEC #AppendLine '';
EXEC #AppendLine '    - `Tn. <Title>` where the title starts with the primary object being changed, then a short, 2-5 word action phrase (for example `T1. dbo.GetAgentJobProblemAggregated - make JobDate filter SARGable`).';
EXEC #AppendLine '    - Impact line: marker, then label and score (for example `[G] Impact: 8.4`).';
EXEC #AppendLine '    - Risk line: marker, then label and score (for example `[A] Risk: 3.5`).';
EXEC #AppendLine '    - Then very short explanation paragraphs grouped as Reason, Additional context, and Solution (for example 1-2 short sentences total or one short sentence per aspect). Each group should be its own paragraph with a blank line between groups so the Main Menu stays concise.';
EXEC #AppendLine '    - Use simple ASCII markers that map to coloured-circle emoji; prefer the emoji form when supported and fall back to the ASCII markers when not. `[G]` = U+1F7E2 (green), `[A]` = U+1F7E0 (amber), `[R]` = U+1F534 (red), `[N]` = U+26AA (neutral/white).';
EXEC #AppendLine '    - These code points are the preferred rendering when emoji is available; otherwise show the ASCII markers.';
EXEC #AppendLine '    - Impact (higher is better): 0.0-4.0 low (neutral), 4.1-7.0 medium (amber/yellow), 7.1-10.0 high (green).';
EXEC #AppendLine '    - Risk (higher is worse): 0.0-4.0 low (green), 4.1-7.0 medium (amber/yellow), 7.1-10.0 high (red).';
EXEC #AppendLine '  - Avoid long paragraphs directly under T1-T10 on the Main Menu; put detail only when the user drills into `Tn`.';
EXEC #AppendLine '- Commands (no numeric prefix):';
EXEC #AppendLine '  - Say 1-7 (or the parent name) to pick a Menu category.';
EXEC #AppendLine '  - Say 11-20 to run a helper (where defined).';
EXEC #AppendLine '  - Say T1-10 (or the opportunity title) to drill into a Top-10 item.';
EXEC #AppendLine '  - Type ''main menu'' at any time to return to the Main Menu.';
EXEC #AppendLine '  - If you already have a specific table, view, procedure, or optimisation in mind, just name it and start there.';
EXEC #AppendLine '';
EXEC #AppendLine '## Impact and Risk';
EXEC #AppendLine '';
EXEC #AppendLine '- Impact: 0.0 to 10.0 (1 decimal), estimated performance upside; low 0.0-4.0, medium 4.1-7.0, high 7.1-10.0.';
EXEC #AppendLine '- Risk: 0.0 to 10.0 (1 decimal), delivery risk/complexity; low 0.0-4.0, medium 4.1-7.0, high 7.1-10.0.';
EXEC #AppendLine '- Where to show: only on concrete result lists (Top-10, per-parent child lists, other multi-result recommendations), not on the Menu or Helpers.';
EXEC #AppendLine '- Layout for T1-10: Title line; Impact line (with marker); Risk line (with marker); keep each rationale clause very short.';
EXEC #AppendLine '- Sorting: Top-10 sorted by Impact descending with a bias toward low-risk quick wins.';
EXEC #AppendLine '- Apply the same marker/banding semantics across Top-10, drill-downs, and helpers so Impact/Risk always read the same way.';
EXEC #AppendLine '';
EXEC #AppendLine '## Parent drill-downs (Menu 1-7)';
EXEC #AppendLine '';
EXEC #AppendLine '- Use a flat, numbered list (1-10) of optimisation candidates (no 2.1/2.2 sub-numbering).';
EXEC #AppendLine '- Each item should have: a short title line that starts with the primary object (view/proc/table) or a tight object group (e.g. `PortalJobDetailsView / PortalJobsView`), followed by a sharp 2-5 word action phrase (avoid generic conceptual labels); separate Impact and Risk lines (same style as Top-10, no risk filter here); then a reasonably detailed suggested action and rationale.';
EXEC #AppendLine '- Order the list so low-risk, practical fixes appear first (for example SARGable rewrites, targeted index changes); place more structural or higher-risk ideas later in the list.';
EXEC #AppendLine '- Keep each suggestion aligned with its parent category and relevant Assistant Brief children (for example parent 2 should focus on views/procs/UDFs, parent 3 on indexes/stats, parent 6 on platform/storage/maintenance), but allow GPT to propose additional, well-justified ideas in that theme beyond the explicit children when the MD evidence supports them.';
EXEC #AppendLine '- When composing drill-down items, treat each suggestion as a mini research-first pass: draw on multiple relevant evidence sources from the MD and external docs where needed, and provide clearly separated paragraphs for Reason, Additional context, and Solution/implementation steps, with a blank line between each group, rather than a single shallow sentence.';
EXEC #AppendLine '- You may include a very short, unnumbered Overview/Summary, but keep it brief and unnumbered; avoid repeating global environment facts unless they directly affect an item.';
EXEC #AppendLine '- When collector data is sparse or low-signal, especially on dev databases, still analyse schema/metadata for that area and use Brief hints plus GPT knowledge to propose likely optimisations (avoid returning ''nothing to do'' or only maintenance/instrumentation items).';
EXEC #AppendLine '- Treat the Assistant Brief Table children as hints, not a checklist; combine them with MD evidence and GPT knowledge to surface the best candidates.';
EXEC #AppendLine '- End with an unnumbered Actions footer:';
EXEC #AppendLine '  - Remind the user they can select 1-10 to drill deeper into a specific suggestion.';
EXEC #AppendLine '  - Offer ''More results'' to expand the list to the top 20 (items 11-20 should use the same title/Impact/Risk/action pattern).';
EXEC #AppendLine '  - Remind them `main menu` returns to the Main Menu.';
EXEC #AppendLine '  - Invite free-form requests (for example any table, query, or setting to investigate for optimisation).';
EXEC #AppendLine '- Soft gates. Do not suggest features blocked by version/compat/edition. When blocked, propose the nearest safe alternative.';
EXEC #AppendLine '- Be concise and factual. Keep advice ASCII-clean and actionable.';
EXEC #AppendLine '- Explain overrides. When you override table guidance, say why (for example: different waits dominate, stats age severe, spills observed).';
EXEC #AppendLine '- Delivery clarity. If a recommendation requires an EDMX change, call this out explicitly so the team can coordinate with the app model.';
EXEC #AppendLine '';
EXEC #AppendLine '## Assistant Brief Table';
EXEC #AppendLine '';
EXEC #AppendLine 'Columns are self-descriptive and used as hints to guide, not constrain:';
EXEC #AppendLine '';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Parent | Child | EDMX Change | SQL Server | SQL Compat | Type of Fix | Preconditions | Notes';
EXEC #AppendLine 'Workload & Optimizer Plans | Parameter Sensitivity - stabilize with hints or refactors | No | 2008+ | 100+ | T-SQL |  | Tokens: forwarded record; Where: Index/Heap review';
EXEC #AppendLine 'Workload & Optimizer Plans | Memory-Grant Spills - fix estimates; reshape operators | No | 2008+ | 100+ | Config |  | Tokens: spill, Hash Warning, Sort Warning; Where: Plan Warnings; Top Queries';
EXEC #AppendLine 'Workload & Optimizer Plans | Wait Stats Triage - map dominant waits to actions | No | 2008+ | 100+ | Analysis |  | Tokens: stale stats, rows_modified, steps; Where: Stats health';
EXEC #AppendLine 'Workload & Optimizer Plans | Implicit Conversion Hotspots - align types to remove scans | No | 2008+ | 100+ | Analysis |  | Tokens: CONVERT_IMPLICIT on partition key; Where: Plan Warnings; Partitioning';
EXEC #AppendLine 'Workload & Optimizer Plans | Reduce PAGEIOLATCH via plan read reduction; validate I/O latency | No | 2008+ | 100+ | Analysis |  | Tokens: PAGEIOLATCH_*, READ latency, WRITE latency; Where: File I/O; Waits; Top Queries';
EXEC #AppendLine 'Workload & Optimizer Plans | Map Queries to Tables/Indexes - cross-map hotspots to objects | No | 2008+ | 100+ | Index/Stats |  | Tokens: Key Lookup, RID Lookup; Where: Top Queries; Execution plans';
EXEC #AppendLine 'Workload & Optimizer Plans | Row Goal Detection - remove row-goals; adjust plan shape | No | 2008+ | 100+ | Schema |  | Tokens: Row Goal; Where: Plan Warnings/Operators';
EXEC #AppendLine 'Workload & Optimizer Plans | Correlate wait stats with IO latency to separate CPU vs storage | No | 2008+ | 100+ | Index/Stats |  | Tokens: SOS_SCHEDULER_YIELD; Where: Waits; Compilations; Top Queries';
EXEC #AppendLine 'Workload & Optimizer Plans | Overview & Top Hashes - rank top statements and objects | No | 2008+ | 100+ | Analysis |  | Tokens: spill, Hash Warning, Sort Warning; Where: Plan Warnings; Top Queries';
EXEC #AppendLine 'Workload & Optimizer Plans | Parallelism sanity - cost threshold & MAXDOP; per-query overrides | No | 2008+ | 100+ | Config |  | Tokens: Key Lookup, RID Lookup; Where: Top Queries; Execution plans';
EXEC #AppendLine 'Workload & Optimizer Plans | Diagnose parallelism skew via exchange row imbalance | No | 2008+ | 100+ | Analysis |  | Tokens: CXPACKET, CXCONSUMER; Where: Waits; Top Queries';
EXEC #AppendLine 'Workload & Optimizer Plans | OPTION(RECOMPILE) for highly variable predicates | No | 2008+ | 100+ | Config |  | Tokens: CONVERT_IMPLICIT, implicit convert; Where: Plan Warnings; Top Queries';
EXEC #AppendLine 'Workload & Optimizer Plans | Standardize session SET options to stabilize plan reuse | No | 2008+ | 100+ | Config |  | Tokens: ARITHABORT, set_options; Where: Plan cache; Module inventory';
EXEC #AppendLine 'Workload & Optimizer Plans | Diagnose ASYNC_NETWORK_IO via client fetch rate and row size | No | 2008+ | 100+ | Analysis |  | Tokens: ASYNC_NETWORK_IO; Where: Waits; Top Queries';
EXEC #AppendLine 'Workload & Optimizer Plans | Triage CPU pressure using SOS_SCHEDULER_YIELD waits | No | 2008+ | 100+ | Analysis |  | Tokens: SOS_SCHEDULER_YIELD; Where: Waits; Compilations; Top Queries';
EXEC #AppendLine 'Workload & Optimizer Plans | Spool Mitigation - index to remove eager/lazy spools | No | 2008+ | 100+ | Index/Stats |  | Tokens: missing index; Where: Missing Indexes; Top Queries';
EXEC #AppendLine 'Workload & Optimizer Plans | Ad-hoc plan bloat: enable ''optimize for ad hoc'' (instance) | No | 2008+ | 100+ | Config |  | Tokens: Adhoc, single-use plan, CACHESTORE_SQLCP; Where: Plan cache';
EXEC #AppendLine 'Workload & Optimizer Plans | Capture hash/sort spill events; record count and spill bytes | No | 2008+ | 100+ | Analysis |  | Tokens: spill, Hash Warning, Sort Warning; Where: Plan Warnings; Top Queries';
EXEC #AppendLine 'Workload & Optimizer Plans | Baseline performance using DMVs - waits, I/O, CPU; include scheduler CPU trends | No | 2008+ | 100+ | Analysis |  | Tokens: SOS_SCHEDULER_YIELD; Where: Waits; Compilations; Top Queries';
EXEC #AppendLine 'Workload & Optimizer Plans | Compare memory grants vs used via DMVs and Extended Events | No | 2008+ | 100+ | Analysis |  | Tokens: spill, Hash Warning, Sort Warning; Where: Plan Warnings; Top Queries';
EXEC #AppendLine 'Workload & Optimizer Plans | Enable forced parameterization when literals cause bloat | No | 2008+ | 100+ | Config |  | Tokens: parameter sniffing, variable selectivity; Where: Top Queries; Plan cache';
EXEC #AppendLine 'Workload & Optimizer Plans | Detect optimizer timeouts; simplify or split complex queries | No | 2008+ | 100+ | T-SQL |  | Tokens: Key Lookup, RID Lookup; Where: Top Queries; Execution plans';
EXEC #AppendLine 'Workload & Optimizer Plans | Capture statement recompiles; log root-cause fields | No | 2008+ | 100+ | Analysis |  | ';
EXEC #AppendLine 'Workload & Optimizer Plans | Parallelism waits: focus CXPACKET; ignore CXCONSUMER | No | 2008+ | 100+ | Analysis |  | Tokens: CXPACKET, CXCONSUMER; Where: Waits; Top Queries';
EXEC #AppendLine 'Workload & Optimizer Plans | Log session and wait snapshots to table using DMVs | No | 2008+ | 100+ | Analysis |  | Tokens: AFTER INSERT/UPDATE on hot table; Where: Module inventory';
EXEC #AppendLine 'Workload & Optimizer Plans | Use DISABLE_OPTIMIZER_ROWGOAL hint to remove row goal | No | 2008+ | 100+ | Analysis |  | Tokens: Row Goal; Where: Plan Warnings/Operators';
EXEC #AppendLine 'Workload & Optimizer Plans | Full-scope analysis - beyond listed checks | No | 2008+ | 100+ | Analysis |  | ';
EXEC #AppendLine 'T-SQL Modules (Views/Procs/UDFs) | Propose PERSISTED Computed Columns - deterministic CASE logic | Yes | 2008+ | 100+ | Schema |  | ';
EXEC #AppendLine 'T-SQL Modules (Views/Procs/UDFs) | Module hygiene & SARGability - joins/predicates | No | 2008+ | 100+ | T-SQL |  | Tokens: CONVERT_IMPLICIT, implicit convert; Where: Plan Warnings; Top Queries';
EXEC #AppendLine 'T-SQL Modules (Views/Procs/UDFs) | Cursor/RBAR Detection - replace with set-based operations | No | 2008+ | 100+ | T-SQL |  | Tokens: forwarded record; Where: Index/Heap review';
EXEC #AppendLine 'T-SQL Modules (Views/Procs/UDFs) | Detect Helper Views - tiny lookups; 1:1 projections | No | 2008+ | 100+ | T-SQL |  | Tokens: Key Lookup, RID Lookup; Where: Top Queries; Execution plans';
EXEC #AppendLine 'T-SQL Modules (Views/Procs/UDFs) | Surface UDFs in Hot Plans - inline or refactor | No | 2008+ | 100+ | Analysis |  | Tokens: Key Lookup, RID Lookup; Where: Top Queries; Execution plans';
EXEC #AppendLine 'T-SQL Modules (Views/Procs/UDFs) | Parameterize dynamic SQL (sp_executesql) to reuse plans | No | 2008+ | 100+ | T-SQL |  | Tokens: parameter sniffing, variable selectivity; Where: Top Queries; Plan cache';
EXEC #AppendLine 'T-SQL Modules (Views/Procs/UDFs) | Rewrite MSTVFs to inline TVFs where possible | Maybe | 2008+ | 100+ | T-SQL |  | Tokens: PAGEIOLATCH_*, READ latency, WRITE latency; Where: File I/O; Waits; Top Queries';
EXEC #AppendLine 'T-SQL Modules (Views/Procs/UDFs) | Refactor correlated subqueries to APPLY/joins for set-based execution | No | 2008+ | 100+ | T-SQL |  | Tokens: Key Lookup, RID Lookup; Where: Top Queries; Execution plans';
EXEC #AppendLine 'T-SQL Modules (Views/Procs/UDFs) | Select needed columns only - enable covering indexes | No | 2008+ | 100+ | Index/Stats |  | Tokens: missing index; Where: Missing Indexes; Top Queries';
EXEC #AppendLine 'T-SQL Modules (Views/Procs/UDFs) | SARGable dates - half-open range (>= start, < end) | No | 2008+ | 100+ | T-SQL |  | ';
EXEC #AppendLine 'T-SQL Modules (Views/Procs/UDFs) | Linked Server Review - push predicates and aggregation down | No | 2008+ | 100+ | T-SQL |  | Tokens: CONVERT_IMPLICIT, implicit convert; Where: Plan Warnings; Top Queries';
EXEC #AppendLine 'T-SQL Modules (Views/Procs/UDFs) | Replace MERGE with UPSERT + HOLDLOCK | No | 2008+ | 100+ | T-SQL |  | Tokens: forwarded record; Where: Index/Heap review';
EXEC #AppendLine 'T-SQL Modules (Views/Procs/UDFs) | Prefer NOT EXISTS over NOT IN with NULLs | No | 2008+ | 100+ | T-SQL |  | Tokens: forwarded record; Where: Index/Heap review';
EXEC #AppendLine 'T-SQL Modules (Views/Procs/UDFs) | Avoid NOLOCK hints - prefer RCSI for correctness | No | 2008+ | 100+ | T-SQL | RCSI=ON | Tokens: NOLOCK, READUNCOMMITTED; Where: T-SQL review';
EXEC #AppendLine 'T-SQL Modules (Views/Procs/UDFs) | Avoid large IN lists - TVP or temp table | Yes | 2008+ | 100+ | T-SQL |  | Tokens: AFTER INSERT/UPDATE on hot table; Where: Module inventory';
EXEC #AppendLine 'T-SQL Modules (Views/Procs/UDFs) | Triggers on Hot Writes - audit cost and row safety | No | 2008+ | 100+ | T-SQL |  | Tokens: AFTER INSERT/UPDATE on hot table; Where: Module inventory';
EXEC #AppendLine 'T-SQL Modules (Views/Procs/UDFs) | Batch DML using TVPs to reduce round trips | Yes | 2008+ | 100+ | T-SQL |  | Tokens: Batch Mode on Rowstore; Where: Plan properties';
EXEC #AppendLine 'T-SQL Modules (Views/Procs/UDFs) | SET XACT_ABORT ON for ETL batches | No | 2008+ | 100+ | Config |  | Tokens: Batch Mode on Rowstore; Where: Plan properties';
EXEC #AppendLine 'T-SQL Modules (Views/Procs/UDFs) | Remove unnecessary DISTINCT/sorts - dedupe correctly; avoid spills | No | 2008+ | 100+ | T-SQL |  | Tokens: spill, Hash Warning, Sort Warning; Where: Plan Warnings; Top Queries';
EXEC #AppendLine 'T-SQL Modules (Views/Procs/UDFs) | Top-N per group via APPLY with ordered index | No | 2008+ | 100+ | Index/Stats |  | Tokens: missing index; Where: Missing Indexes; Top Queries';
EXEC #AppendLine 'T-SQL Modules (Views/Procs/UDFs) | Index temp tables in procs when measurement shows benefit | No | 2008+ | 100+ | Index/Stats |  | Tokens: missing index; Where: Missing Indexes; Top Queries';
EXEC #AppendLine 'T-SQL Modules (Views/Procs/UDFs) | Rewrite OR to UNION ALL when selective | No | 2008+ | 100+ | T-SQL |  | Tokens: PAGEIOLATCH_*, READ latency, WRITE latency; Where: File I/O; Waits; Top Queries';
EXEC #AppendLine 'T-SQL Modules (Views/Procs/UDFs) | Remove ORDER BY in views/subqueries unless TOP | No | 2008+ | 100+ | T-SQL |  | Tokens: Key Lookup, RID Lookup; Where: Top Queries; Execution plans';
EXEC #AppendLine 'T-SQL Modules (Views/Procs/UDFs) | Full-scope analysis - beyond listed checks | No | 2008+ | 100+ | T-SQL |  | ';
EXEC #AppendLine 'Indexes & Statistics | Near-Miss Coverage - add INCLUDEs; adjust key order | No | 2008+ | 100+ | Index/Stats |  | Tokens: Key Lookup, RID Lookup; Where: Top Queries; Execution plans';
EXEC #AppendLine 'Indexes & Statistics | Filtered Indexes - hot slices, selective predicates | No | 2008+ | 100+ | Index/Stats |  | Tokens: missing index; Where: Missing Indexes; Top Queries';
EXEC #AppendLine 'Indexes & Statistics | Filtered Stats - cover hot slices; fix misestimates | No | 2008+ | 100+ | Index/Stats |  | Tokens: Key Lookup, RID Lookup; Where: Top Queries; Execution plans';
EXEC #AppendLine 'Indexes & Statistics | Index persisted computed columns used in predicates | No | 2008+ | 100+ | Index/Stats |  | Tokens: CONVERT_IMPLICIT, implicit convert; Where: Plan Warnings; Top Queries';
EXEC #AppendLine 'Indexes & Statistics | Overlap/Duplicate Consolidation - merge or drop redundant indexes | No | 2008+ | 100+ | Index/Stats |  | Tokens: unused index, duplicate, overlap; Where: Index Usage; Index List';
EXEC #AppendLine 'Indexes & Statistics | Ensure AUTO_CREATE/UPDATE STATISTICS enabled | No | 2008+ | 100+ | Config |  | Tokens: AFTER INSERT/UPDATE on hot table; Where: Module inventory';
EXEC #AppendLine 'Indexes & Statistics | FK Support Indexes - narrow child indexes for joins | No | 2008+ | 100+ | Index/Stats |  | Tokens: missing index; Where: Missing Indexes; Top Queries';
EXEC #AppendLine 'Indexes & Statistics | Targeted Stats Updates - per-object; FULLSCAN for skew | No | 2008+ | 100+ | Index/Stats |  | Tokens: CXPACKET, CXCONSUMER; Where: Waits; Top Queries';
EXEC #AppendLine 'Indexes & Statistics | Order-Preserving Indexes - satisfy ORDER BY without sort | No | 2008+ | 100+ | Index/Stats |  | Tokens: missing index; Where: Missing Indexes; Top Queries';
EXEC #AppendLine 'Indexes & Statistics | Wide Clustering Key - reduce key width; avoid bloat | No | 2008+ | 100+ | Index/Stats |  | Tokens: Key Lookup, RID Lookup; Where: Top Queries; Execution plans';
EXEC #AppendLine 'Indexes & Statistics | Detect Stale or Missing Stats - prioritize predicate columns | No | 2008+ | 100+ | Index/Stats |  | Tokens: stale stats, rows_modified, steps; Where: Stats health';
EXEC #AppendLine 'Indexes & Statistics | Partition Elimination - align predicates with partitions | No | 2008+ | 100+ | Index/Stats |  | Tokens: CONVERT_IMPLICIT, implicit convert; Where: Plan Warnings; Top Queries';
EXEC #AppendLine 'Indexes & Statistics | Detect ascending-key patterns causing underestimates | No | 2008+ | 100+ | Analysis |  | Tokens: increasing key, last step; Where: Stats histogram';
EXEC #AppendLine 'Indexes & Statistics | Indexed views for stable aggregates/joins | No | 2008+ | 100+ | Index/Stats |  | Tokens: missing index; Where: Missing Indexes; Top Queries';
EXEC #AppendLine 'Indexes & Statistics | Partition key implicit conversions - block elimination | No | 2008+ | 100+ | Index/Stats |  | Tokens: CONVERT_IMPLICIT on partition key; Where: Plan Warnings; Partitioning';
EXEC #AppendLine 'Indexes & Statistics | Unused Index Cleanup - deprecate safely with rollback plan | No | 2008+ | 100+ | Index/Stats |  | Tokens: unused index, duplicate, overlap; Where: Index Usage; Index List';
EXEC #AppendLine 'Indexes & Statistics | Stats on Computed Columns - create where predicates depend | No | 2008+ | 100+ | Index/Stats |  | Tokens: CONVERT_IMPLICIT, implicit convert; Where: Plan Warnings; Top Queries';
EXEC #AppendLine 'Indexes & Statistics | Group By coverage - index group keys; include aggregates | No | 2008+ | 100+ | Index/Stats |  | Tokens: Key Lookup, RID Lookup; Where: Top Queries; Execution plans';
EXEC #AppendLine 'Indexes & Statistics | Refresh critical statistics immediately after bulk loads | No | 2008+ | 100+ | Index/Stats |  | Tokens: AFTER INSERT/UPDATE on hot table; Where: Module inventory';
EXEC #AppendLine 'Indexes & Statistics | Multi-column stats for correlated predicates | No | 2008+ | 100+ | Index/Stats |  | Tokens: CONVERT_IMPLICIT, implicit convert; Where: Plan Warnings; Top Queries';
EXEC #AppendLine 'Indexes & Statistics | Filtered index on IS NULL / IS NOT NULL | Yes | 2008+ | 100+ | Index/Stats |  | Tokens: missing index; Where: Missing Indexes; Top Queries';
EXEC #AppendLine 'Indexes & Statistics | Partition sliding window - boundaries, staging partition, bulk TABLOCK switch-in/out; archive filegroups READ_ONLY | No | 2008+ | 100+ | Index/Stats |  | Tokens: PAGEIOLATCH_*, READ latency, WRITE latency; Where: File I/O; Waits; Top Queries';
EXEC #AppendLine 'Indexes & Statistics | Index for window functions - POC partition/order/cover | No | 2008+ | 100+ | Index/Stats |  | Tokens: missing index; Where: Missing Indexes; Top Queries';
EXEC #AppendLine 'Indexes & Statistics | Equality then range columns in key order | No | 2008+ | 100+ | Index/Stats |  | Tokens: Key Lookup, RID Lookup; Where: Top Queries; Execution plans';
EXEC #AppendLine 'Indexes & Statistics | Inspect stats histogram for skew and step boundaries | No | 2008+ | 100+ | Analysis |  | Tokens: CXPACKET, CXCONSUMER; Where: Waits; Top Queries';
EXEC #AppendLine 'Indexes & Statistics | Target stale stats using dm_db_stats_properties age | No | 2008+ | 100+ | Analysis |  | Tokens: stale stats, rows_modified, steps; Where: Stats health';
EXEC #AppendLine 'Indexes & Statistics | Set FILLFACTOR/PAD_INDEX for hot update workloads | No | 2008+ | 100+ | Index/Stats |  | Tokens: AFTER INSERT/UPDATE on hot table; Where: Module inventory';
EXEC #AppendLine 'Indexes & Statistics | Confirm dynamic auto-stats thresholds (TF 2371 behavior) | No | 2008+ | 100+ | Analysis |  | Tokens: stale stats, rows_modified, steps; Where: Stats health';
EXEC #AppendLine 'Indexes & Statistics | Avoid NORECOMPUTE; reset where it harms estimates | No | 2008+ | 100+ | Index/Stats |  | ';
EXEC #AppendLine 'Indexes & Statistics | Async Stats Review - evaluate AUTO_UPDATE_STATISTICS_ASYNC | No | 2008+ | 100+ | Index/Stats |  | Tokens: stale stats, rows_modified, steps; Where: Stats health';
EXEC #AppendLine 'Indexes & Statistics | Full-scope analysis - beyond listed checks | No | 2008+ | 100+ | Analysis |  | ';
EXEC #AppendLine 'Schema & Data Modeling | Persist Useful Computed Columns - deterministic, persisted CASEs | Yes | 2008+ | 100+ | Schema |  | ';
EXEC #AppendLine 'Schema & Data Modeling | Fix Implicit Conversions - align parameter and column types | Yes | 2008+ | 100+ | Schema |  | Tokens: CONVERT_IMPLICIT, implicit convert; Where: Plan Warnings; Top Queries';
EXEC #AppendLine 'Schema & Data Modeling | Clustered key: narrow, static, ever-increasing; avoid GUID | Yes | 2008+ | 100+ | Schema |  | Tokens: increasing key, last step; Where: Stats histogram';
EXEC #AppendLine 'Schema & Data Modeling | Row width & sparsity - fit types/scale; consider SPARSE | No | 2008+ | 100+ | T-SQL |  | Tokens: Row Goal; Where: Plan Warnings/Operators';
EXEC #AppendLine 'Schema & Data Modeling | FK Health - ensure referential integrity and indexes | No | 2008+ | 100+ | Index/Stats |  | Tokens: missing index; Where: Missing Indexes; Top Queries';
EXEC #AppendLine 'Schema & Data Modeling | Specify NOT NULL when safe - better plans possible | Yes | 2008+ | 100+ | Schema |  | Tokens: nullability, IS NULL; Where: Schema export; Module inventory';
EXEC #AppendLine 'Schema & Data Modeling | Collation Mismatches - align collations; avoid conversions | No | 2008+ | 100+ | Schema |  | Tokens: CONVERT_IMPLICIT, implicit convert; Where: Plan Warnings; Top Queries';
EXEC #AppendLine 'Schema & Data Modeling | Enable join elimination via trusted FKs and CHECK constraints | No | 2008+ | 100+ | Schema |  | ';
EXEC #AppendLine 'Schema & Data Modeling | Find and fix untrusted constraints from NOCHECK | No | 2008+ | 100+ | Schema |  | ';
EXEC #AppendLine 'Schema & Data Modeling | Normalize or denormalize based on join cost vs row size | Yes | 2008+ | 100+ | Schema |  | Tokens: Row Goal; Where: Plan Warnings/Operators';
EXEC #AppendLine 'Schema & Data Modeling | Refactor EAV hot paths - typed columns or summaries | Yes | 2008+ | 100+ | Schema |  | Tokens: Key Lookup, RID Lookup; Where: Top Queries; Execution plans';
EXEC #AppendLine 'Schema & Data Modeling | Use ROWVERSION to detect write conflicts (optimistic concurrency) | Yes | 2008+ | 100+ | T-SQL |  | Tokens: PAGEIOLATCH_*, READ latency, WRITE latency; Where: File I/O; Waits; Top Queries';
EXEC #AppendLine 'Schema & Data Modeling | Replace TEXT/NTEXT/IMAGE with (VAR)MAX types | Yes | 2008+ | 100+ | Schema |  | Tokens: forwarded record; Where: Index/Heap review';
EXEC #AppendLine 'Schema & Data Modeling | LOB I/O impact - row-overflow; consider FILESTREAM/externalization | No | 2008+ | 100+ | Analysis |  | Tokens: Row Goal; Where: Plan Warnings/Operators';
EXEC #AppendLine 'Schema & Data Modeling | Detect identity near max; widen data type early | Yes | 2008+ | 100+ | Schema |  | ';
EXEC #AppendLine 'Schema & Data Modeling | Full-scope analysis - beyond listed checks | No | 2008+ | 100+ | Analysis |  | ';
EXEC #AppendLine 'Concurrency & Transactions | RCSI Review - evaluate benefits; document trade-offs | No | 2008+ | 100+ | Config | RCSI=ON | ';
EXEC #AppendLine 'Concurrency & Transactions | Deadlock Analysis - graphs, ordering, index fixes | No | 2008+ | 100+ | Analysis |  | Tokens: missing index; Where: Missing Indexes; Top Queries';
EXEC #AppendLine 'Concurrency & Transactions | Mitigations - batching, retry logic, predicate indexing | No | 2008+ | 100+ | Analysis |  | Tokens: missing index; Where: Missing Indexes; Top Queries';
EXEC #AppendLine 'Concurrency & Transactions | Contention triage - PAGELATCH/tempdb; include spinlock checks | No | 2008+ | 100+ | Analysis |  | Tokens: PAGELATCH_% in tempdb, version store; Where: Tempdb; Waits';
EXEC #AppendLine 'Concurrency & Transactions | Implicit transactions - turn OFF to reduce blocking | No | 2008+ | 100+ | Config |  | Tokens: CONVERT_IMPLICIT, implicit convert; Where: Plan Warnings; Top Queries';
EXEC #AppendLine 'Concurrency & Transactions | THREADPOOL mitigation - reduce fan-out; adjust worker threads last | No | 2008+ | 100+ | Analysis |  | Tokens: THREADPOOL; Where: Waits';
EXEC #AppendLine 'Concurrency & Transactions | Top Blockers/Victims - summarize blocking graph and causes | No | 2008+ | 100+ | Analysis |  | ';
EXEC #AppendLine 'Concurrency & Transactions | Detect and resolve sp_getapplock blocking patterns | No | 2008+ | 100+ | T-SQL |  | Tokens: increasing key, last step; Where: Stats histogram';
EXEC #AppendLine 'Concurrency & Transactions | Queue Table Pattern - READPAST/UPDLOCK ordered seeks | No | 2008+ | 100+ | T-SQL |  | Tokens: PAGEIOLATCH_*, READ latency, WRITE latency; Where: File I/O; Waits; Top Queries';
EXEC #AppendLine 'Concurrency & Transactions | Long Transactions - identify sessions and lock/latch impact | No | 2008+ | 100+ | Analysis |  | Tokens: last-page latch, hotspot key; Where: Hot append indexes';
EXEC #AppendLine 'Concurrency & Transactions | Set DEADLOCK_PRIORITY and implement retry/backoff logic | No | 2008+ | 100+ | T-SQL |  | ';
EXEC #AppendLine 'Concurrency & Transactions | Capture Extended Events blocked_process_report and sample waits | No | 2008+ | 100+ | Analysis |  | ';
EXEC #AppendLine 'Concurrency & Transactions | Full-scope analysis - beyond listed checks | No | 2008+ | 100+ | Analysis |  | ';
EXEC #AppendLine 'Platform, Storage & Maintenance | TempDB Review - file count, size uniformity, contention | No | 2008+ | 100+ | T-SQL |  | Tokens: PAGELATCH_% in tempdb, version store; Where: Tempdb; Waits';
EXEC #AppendLine 'Platform, Storage & Maintenance | Concurrency controls - MAXDOP/cost threshold | No | 2008+ | 100+ | Config |  | ';
EXEC #AppendLine 'Platform, Storage & Maintenance | Enable Instant File Initialization to speed data file growth | No | 2008+ | 100+ | Config |  | Tokens: IFI, Perform Volume Maintenance Tasks; Where: Startup/Config checks';
EXEC #AppendLine 'Platform, Storage & Maintenance | Log health - pre-size to peak, large growth, single log file, batch bulk-loads, VLF churn tracking | No | 2008+ | 100+ | T-SQL |  | Tokens: Row Goal; Where: Plan Warnings/Operators';
EXEC #AppendLine 'Platform, Storage & Maintenance | Heaps with Forwarded Records - cluster or rebuild heaps | No | 2008+ | 100+ | T-SQL |  | Tokens: forwarded record; Where: Index/Heap review';
EXEC #AppendLine 'Platform, Storage & Maintenance | Max server memory headroom - leave OS 20-25% | No | 2008+ | 100+ | T-SQL |  | Tokens: spill, Hash Warning, Sort Warning; Where: Plan Warnings; Top Queries';
EXEC #AppendLine 'Platform, Storage & Maintenance | I/O latency hotspots - top files by read/write latency | No | 2008+ | 100+ | T-SQL |  | Tokens: PAGEIOLATCH_*, READ latency, WRITE latency; Where: File I/O; Waits; Top Queries';
EXEC #AppendLine 'Platform, Storage & Maintenance | Correlate IO latency with wait stats over time | No | 2008+ | 100+ | Index/Stats |  | Tokens: PAGEIOLATCH_*, READ latency, WRITE latency; Where: File I/O; Waits; Top Queries';
EXEC #AppendLine 'Platform, Storage & Maintenance | Queries Impacting TempDB - identify heavy tempdb users | No | 2008+ | 100+ | T-SQL |  | Tokens: Key Lookup, RID Lookup; Where: Top Queries; Execution plans';
EXEC #AppendLine 'Platform, Storage & Maintenance | Compression candidates - test row/page; estimate CPU/IO trade-offs | No | 2008+ | 100+ | Index/Stats | Edition supports compression | Tokens: compression, row/page; Where: Schema export; Index list';
EXEC #AppendLine 'Platform, Storage & Maintenance | Standardize session options - keep packet size default unless tested | No | 2008+ | 100+ | T-SQL |  | ';
EXEC #AppendLine 'Platform, Storage & Maintenance | Backup policy - CHECKSUM, compression, striping/blocksize | No | 2008+ | 100+ | T-SQL |  | ';
EXEC #AppendLine 'Platform, Storage & Maintenance | Disable AUTO_SHRINK; planned sizing, one-off targeted shrink | No | 2008+ | 100+ | Config |  | Tokens: AUTO_SHRINK, shrink; Where: Database configurations';
EXEC #AppendLine 'Platform, Storage & Maintenance | Windows power plan - High performance; avoid core parking | No | 2008+ | 100+ | Analysis |  | Tokens: Balanced power plan; Where: Server checks';
EXEC #AppendLine 'Platform, Storage & Maintenance | Report tempdb space per session and per task | No | 2008+ | 100+ | T-SQL |  | Tokens: PAGELATCH_% in tempdb, version store; Where: Tempdb; Waits';
EXEC #AppendLine 'Platform, Storage & Maintenance | Disable AUTO_CLOSE; align recovery, page verify | No | 2008+ | 100+ | Config |  | Tokens: last-page latch, hotspot key; Where: Hot append indexes';
EXEC #AppendLine 'Platform, Storage & Maintenance | Configure AV exclusions for data, log, tempdb, backup folders | No | 2008+ | 100+ | Config |  | Tokens: PAGELATCH_% in tempdb, version store; Where: Tempdb; Waits';
EXEC #AppendLine 'Platform, Storage & Maintenance | Batch large deletes; monitor and clear ghost cleanup backlog | No | 2008+ | 100+ | T-SQL |  | Tokens: Batch Mode on Rowstore; Where: Plan properties';
EXEC #AppendLine 'Platform, Storage & Maintenance | Consider Lock Pages in Memory with max server memory set | No | 2008+ | 100+ | T-SQL |  | Tokens: spill, Hash Warning, Sort Warning; Where: Plan Warnings; Top Queries';
EXEC #AppendLine 'Platform, Storage & Maintenance | Remove legacy trace flags now defaulted | No | 2008+ | 100+ | Config |  | ';
EXEC #AppendLine 'Platform, Storage & Maintenance | Check if SQL Server instance has service pack / CU updates available | No | 2008+ | 100+ | Config |  | Tokens: service pack, cumulative update, build number; Where: server version / build info';
EXEC #AppendLine 'Platform, Storage & Maintenance | Full-scope analysis - beyond listed checks | No | 2008+ | 100+ | Analysis |  | ';
EXEC #AppendLine 'Full Analysis | Best-judgment tuning sweep - no category limits | No | 2008+ | 100+ | Analysis |  | ';
EXEC #AppendLine '```';
EXEC #AppendLine ''

-- 00a. Metadata
EXEC #AppendLine '## 00a. Metadata'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: SERVERPROPERTY (ServerName, InstanceName, MachineName, ComputerNamePhysicalNetBIOS, Collation, ProductVersion, ProductBuild, ProductBuildType, ProductLevel, ProductMajorVersion, ProductMinorVersion, ProductUpdateLevel, ProductUpdateReference, Edition, EngineEdition, InstanceDefaultBackup/Data/Log paths, IsClustered, IsHadrEnabled, HadrManagerStatus, Filestream*, BuildClrVersion)'
EXEC #AppendLine 'Source: DATABASEPROPERTYEX (DB_NAME(), Collation | Recovery | Status | Updateability | UserAccess | IsReadCommittedSnapshotOn | IsSnapshotIsolationOn | IsAuto* stats + AUTO_CLOSE/SHRINK | IsParameterizationForced | IsAnsiNullDefault | IsAnsiWarningsOn | IsArithAbortOn | IsBrokerEnabled | IsSyncWithBackup | LastBackupTime | LastLogBackupTime | LastGoodCheckDbTime)'
EXEC #AppendLine 'Why: SERVERPROPERTY surfaces build lineage, default paths, HA posture, and file-stream status for the host instance; DATABASEPROPERTYEX captures the TargetDB''s automation/isolation toggles that frequently drive performance or concurrency regressions.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_Metadata'
EXEC #AppendLine ''

-- 00b. Database Configurations
EXEC #AppendLine '## 00b. Database Configurations'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.databases (compatibility_level, recovery_model_desc, page_verify_option_desc, user_access_desc, state_desc, is_read_only, is_encrypted, snapshot_isolation_state_desc, is_read_committed_snapshot_on, is_auto_close_on, is_auto_shrink_on, is_auto_create_stats_on, is_auto_update_stats_on, is_auto_update_stats_async_on, is_parameterization_forced, is_cdc_enabled, is_broker_enabled, is_trustworthy_on, is_db_chaining_on, log_reuse_wait_desc, log_reuse_wait)'
EXEC #AppendLine 'Why: sys.databases snapshot of compatibility, stats/tuning toggles, log reuse wait, and other per-database state that influences plan shape.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_DatabaseConfigurations'
EXEC #AppendLine ''

-- 00c. Instance-Level Configurations
EXEC #AppendLine '## 00c. Instance-Level Configurations'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.configurations (selected columns: configuration_id | minimum | maximum | value | value_in_use | is_dynamic | is_advanced | description)'
EXEC #AppendLine 'Why: All instance-level options from sys.configurations with run vs config values, ranges, flags (dynamic/advanced), and the documented option description.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_InstanceConfigs'
EXEC #AppendLine ''

-- 00d. Server Environment
EXEC #AppendLine '## 00d. Server Environment'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.dm_os_sys_info (cpu_count, scheduler_count, hyperthread_ratio, max_workers_count, sqlserver_start_time, physical_memory_in_bytes, virtual_memory_in_bytes, bpool_committed/bpool_commit_target/bpool_visible, affinity_type_desc, time_source_desc)'
EXEC #AppendLine 'Why: CPU/scheduler posture, memory sizing and buffer pool targets, and time source; useful context for interpreting waits, plan cache, and memory pressure signals.'
EXEC #AppendLine 'Gate: Requires sysadmin'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_ServerInfo'
EXEC #AppendLine ''

-- 00e. Extended Events Sessions (Defined vs Running)
EXEC #AppendLine '## 00e. Extended Events Sessions (Defined vs Running)'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.server_event_sessions; sys.dm_xe_sessions; sys.server_event_session_targets; sys.server_event_session_fields; sys.dm_xe_session_targets'
EXEC #AppendLine 'Why: Compare stored XE definitions to live sessions, including buffer usage, start time, event_file target sizing, and whether sessions are lossy or dropping events.'
EXEC #AppendLine 'Gate: Requires sysadmin (VIEW SERVER STATE).'
EXEC #AppendLine 'Notes: Flags is_lossy_mode and is_dropping_events surface lossy retention modes and non-zero dropped_event_count / dropped_buffer_count for quick risk triage.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_XESessions'
EXEC #AppendLine ''

-- 00f. Resource Governor (Configuration & State)
EXEC #AppendLine '## 00f. Resource Governor (Configuration & State)'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.resource_governor_configuration; sys.dm_resource_governor_configuration'
EXEC #AppendLine 'Why: Compare stored vs effective classifier function settings and whether a reconfiguration is pending to explain workload governance.'
EXEC #AppendLine 'Gate: Requires sysadmin'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_RG_Config'
EXEC #AppendLine ''

-- 00g. Linked Servers (Inventory)
EXEC #AppendLine '## 00g. Linked Servers (Inventory)'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.servers; sys.linked_logins'
EXEC #AppendLine 'Why: Surface RPC/data-access settings, replication roles, and login mapping counts (self vs explicit) for each remote server.'
EXEC #AppendLine 'Notes: server_id = 0 is filtered (local instance).'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_LinkedServers'
EXEC #AppendLine ''

-- 00h. Active Trace Flags
EXEC #AppendLine '## 00h. Active Trace Flags'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: DBCC TRACESTATUS(-1)'
EXEC #AppendLine 'Why: Capture currently enabled trace flags (global vs session) for troubleshooting context (e.g., TF 1117, 3226).'
EXEC #AppendLine 'Notes: Requires DBCC TRACESTATUS(-1) permissions (often granted only to sysadmin); if DBCC TRACESTATUS is not permitted for the current login, this slice will return a permission error instead of being gated.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_TraceFlags'
EXEC #AppendLine ''

-- 00i. Connection Encryption & Protocol Mix (Server & Target DB)
EXEC #AppendLine '## 00i. Connection Encryption & Protocol Mix (Server & Target DB)'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.dm_exec_connections; sys.sysprocesses'
EXEC #AppendLine 'Why: Measure net transport, auth scheme, TLS status, and protocol version for all sessions vs. TargetDB sessions.'
EXEC #AppendLine 'Gate: Requires sysadmin'
EXEC #AppendLine 'Notes: dm_exec_* views require VIEW SERVER STATE; the gate keeps db_owner runs clean.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_ConnEncryptionMix'
EXEC #AppendLine ''

-- 00j. HADR Endpoint (Database Mirroring) - Encryption & Auth
EXEC #AppendLine '## 00j. HADR Endpoint (Database Mirroring) - Encryption & Auth'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.database_mirroring_endpoints; sys.endpoints; sys.tcp_endpoints; master.sys.certificates; sys.server_principals'
EXEC #AppendLine 'Why: Show endpoint owner, role, encryption algorithm, auth scheme, message forwarding, and TCP port settings for mirroring transport.'
EXEC #AppendLine 'Notes: Requires endpoint and certificate metadata permissions (typically VIEW SERVER STATE). Endpoint metadata helps confirm AES usage and certificate expiry; TCP fields may differ if SQL Server Configuration Manager overrides the listener.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_HadrEndpoint'
EXEC #AppendLine ''

-- 00k. Server Configuration - Focused Risk Summary
EXEC #AppendLine '## 00k. Server Configuration - Focused Risk Summary';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.configurations';
EXEC #AppendLine 'Why: All instance-level options with configured vs in-use values, ranges (min/max), and dynamic/advanced flags; includes description for each option.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_ServerConfigRisk';
EXEC #AppendLine '';

-- 00l. Collation Posture (Server vs. Target DB)
EXEC #AppendLine '## 00l. Collation Posture (Server vs. Target DB)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: SERVERPROPERTY(''Collation''); DATABASEPROPERTYEX(DB_NAME(), ''Collation''); DATABASEPROPERTYEX(''tempdb'', ''Collation'')';
EXEC #AppendLine 'Why: Surface server vs. DB collation and sensitivity (CI/CS, AS/AI) to avoid cross-collation surprises.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_CollationPosture';
EXEC #AppendLine '';

-- 00m. Endpoints Inventory (TLS/Port/Type)
EXEC #AppendLine '## 00m. Endpoints Inventory (TLS/Port/Type)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.endpoints; sys.tcp_endpoints; sys.database_mirroring_endpoints; sys.service_broker_endpoints; sys.server_principals';
EXEC #AppendLine 'Notes: Requires endpoint metadata permissions (typically VIEW SERVER STATE). For mirroring/Service Broker endpoints, prefer AES algorithms over RC4; dynamic TCP ports have port=0 with is_dynamic_port=1.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_Endpoints';
EXEC #AppendLine '';

-- 00n. Effective Parallelism Posture (Server/DB/RG)
EXEC #AppendLine '## 00n. Effective Parallelism Posture (Server/DB/RG)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.configurations; sys.dm_resource_governor_workload_groups; sys.resource_governor_configuration; sys.databases';
EXEC #AppendLine 'Why: Effective parallelism posture (MAXDOP and cost threshold) across server and Resource Governor default workload group; includes DB compat level.';
EXEC #AppendLine 'Gate: Requires sysadmin';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_EffectiveParallelism';
EXEC #AppendLine '';

-- 00o. TempDB File Space Usage (DB)
EXEC #AppendLine '## 00o. TempDB File Space Usage (DB)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: tempdb.sys.dm_db_file_space_usage';
EXEC #AppendLine 'Why: Snapshot of version store and TempDB file usage';
EXEC #AppendLine 'Gate: Requires sysadmin';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_TempdbFileSpace';
EXEC #AppendLine '';

-- 00p. TempDB Session Space Usage (Top 50)
EXEC #AppendLine '## 00p. TempDB Session Space Usage (Top 50)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: tempdb.sys.dm_db_session_space_usage; sys.dm_exec_sessions';
EXEC #AppendLine 'Why: Who is using TempDB space (sessions)';
EXEC #AppendLine 'Gate: Requires sysadmin';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_TempdbSessionSpace';
EXEC #AppendLine '';

-- 00q. TempDB Task Space Usage (Top 50)
EXEC #AppendLine '## 00q. TempDB Task Space Usage (Top 50)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: tempdb.sys.dm_db_task_space_usage';
EXEC #AppendLine 'Why: Which tasks/statements are consuming TempDB';
EXEC #AppendLine 'Gate: Requires sysadmin';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_TempdbTaskSpace';
EXEC #AppendLine '';

-- 01a. Waiting Tasks (Target DB Snapshot)
EXEC #AppendLine '## 01a. Waiting Tasks (Target DB Snapshot)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_os_waiting_tasks; sys.dm_exec_requests; sys.dm_exec_sessions; sys.dm_exec_sql_text';
EXEC #AppendLine 'Why: Live snapshot of what sessions in this database are waiting on.';
EXEC #AppendLine 'Gate: Requires sysadmin';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_WaitingTasks';
EXEC #AppendLine '';

-- 01b. OS Tasks by Scheduler (Target DB)
EXEC #AppendLine '## 01b. OS Tasks by Scheduler (Target DB)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_os_tasks; sys.dm_os_schedulers; sys.dm_exec_requests';
EXEC #AppendLine 'Why: Finds scheduler hotspots (runnable pressure) for this DB.';
EXEC #AppendLine 'Gate: Requires sysadmin';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_OSTasksByScheduler';
EXEC #AppendLine '';

-- 01c. OS Workers - Top Pressure (Target DB)
EXEC #AppendLine '## 01c. OS Workers - Top Pressure (Target DB)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_os_workers; sys.dm_os_tasks; sys.dm_exec_requests; sys.dm_os_schedulers';
EXEC #AppendLine 'Why: Finds preemptive workers / high pending IO / high context-switch workers impacting this DB.';
EXEC #AppendLine 'Gate: Requires sysadmin';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_OSWorkersTop';
EXEC #AppendLine '';

-- 01d. Memory Grant Semaphores (Grant Pressure)
EXEC #AppendLine '## 01d. Memory Grant Semaphores (Grant Pressure)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_query_resource_semaphores (+ sys.dm_resource_governor_resource_pools).';
EXEC #AppendLine 'Why: Detects memory grant pressure (waiters, timeouts, forced grants).';
EXEC #AppendLine 'Gate: VIEW SERVER STATE (<=2019) or VIEW SERVER PERFORMANCE STATE (2022+).';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_ResourceSemaphores';
EXEC #AppendLine '';

-- 01e. SQL Server Process Memory (Snapshot)
EXEC #AppendLine '## 01e. SQL Server Process Memory (Snapshot)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_os_process_memory (+ sys.configurations for max server memory).';
EXEC #AppendLine 'Why: Quick health snapshot (working set, VA space, locked/large pages, commit headroom).';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine 'Notes: DMV access otherwise needs VIEW SERVER STATE (<=2019) or VIEW SERVER PERFORMANCE STATE (2022+).';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_ProcessMemory';
EXEC #AppendLine '';

-- 01f. OS Memory Snapshot & Headroom
EXEC #AppendLine '## 01f. OS Memory Snapshot & Headroom';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_os_sys_memory; sys.dm_os_process_memory; sys.dm_os_sys_info.';
EXEC #AppendLine 'Why: Compare OS-level memory availability and signals to SQL Server process and buffer pool targets to spot headroom or pressure.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_OsSysMemory';
EXEC #AppendLine '';

-- 01g. NUMA Nodes & Scheduler Distribution
EXEC #AppendLine '## 01g. NUMA Nodes & Scheduler Distribution';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_os_nodes; sys.dm_os_schedulers.';
EXEC #AppendLine 'Why: Surface NUMA layout and per-node scheduler pressure to spot imbalances or offline nodes.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine 'Notes: DMV access otherwise needs VIEW SERVER STATE.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_NumaNodes';
EXEC #AppendLine '';

-- 01h. Resource Governor Pools (Definitions)
EXEC #AppendLine '## 01h. Resource Governor Pools (Definitions)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.resource_governor_resource_pools (catalog).';
EXEC #AppendLine 'Why: Show configured pools/limits to compare with runtime utilization and catch misconfiguration.';
EXEC #AppendLine 'Gate: Present on all supported versions; returns empty table if RG not initialized.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_RG_PoolDefs';
EXEC #AppendLine '';

-- 01i. Resource Governor Workload Groups (Definitions)
EXEC #AppendLine '## 01i. Resource Governor Workload Groups (Definitions)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.resource_governor_workload_groups (catalog).';
EXEC #AppendLine 'Why: Inventory of groups and request caps aids in diagnosing throttling/queuing.';
EXEC #AppendLine 'Gate: Returns empty table if RG not initialized.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_RG_WGDefs';
EXEC #AppendLine '';

-- 02a. Files, Size & Growth
EXEC #AppendLine '## 02a. Files, Size & Growth'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.database_files(type_desc | file_id | name | physical_name | size | is_percent_growth | growth | max_size)'
EXEC #AppendLine 'Why: Current database files with size (MB), growth setting (percent or MB), and max size.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_FilesSizeGrowth'
EXEC #AppendLine ''

-- 02b. File IO Stalls
EXEC #AppendLine '## 02b. File IO Stalls'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.dm_io_virtual_file_stats; sys.database_files'
EXEC #AppendLine 'Why: Per-file I/O counters and stall times since instance start; joined to file metadata for names and sizes.'
EXEC #AppendLine 'Gate: Requires sysadmin.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_FileIOStalls'
EXEC #AppendLine ''

-- 02c. Recent Autogrowth Events
EXEC #AppendLine '## 02c. Recent Autogrowth Events'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.traces; sys.fn_trace_gettable'
EXEC #AppendLine 'Why: Last 5 autogrowth events for this database from the default trace (returns empty if default trace is disabled/unavailable).'
EXEC #AppendLine 'Gate: Requires sysadmin.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_AutogrowthEvents'
EXEC #AppendLine ''

-- 02d. Log Space Usage Snapshot
EXEC #AppendLine '## 02d. Log Space Usage Snapshot';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: DBCC SQLPERF(LOGSPACE) (per-database log size and percent used).';
EXEC #AppendLine 'Why: point-in-time fullness and estimated used MB for the target DB.';
EXEC #AppendLine 'Gate: Requires sysadmin';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_LogSpaceUsage';
EXEC #AppendLine '';

-- 02e. Pending I/O Requests (Current DB)
EXEC #AppendLine '## 02e. Pending I/O Requests (Current DB)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_io_pending_io_requests; sys.dm_os_sys_info; sys.dm_io_virtual_file_stats; sys.database_files';
EXEC #AppendLine 'Why: Outstanding I/O against files in this DB';
EXEC #AppendLine 'Gate: Requires sysadmin';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_PendingIO';
EXEC #AppendLine '';

-- 02f. Volume Stats for Database Files
EXEC #AppendLine '## 02f. Volume Stats for Database Files';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.master_files; master..xp_fixeddrives (drive free space MB).';
EXEC #AppendLine 'Why: Per-file drive letter and drive free space (best-effort replacement for dm_os_volume_stats).';
EXEC #AppendLine 'Gate: Requires sysadmin';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_VolumeStats';
EXEC #AppendLine '';

-- 03a. Object Sizes & Rowcounts
EXEC #AppendLine '## 03a. Object Sizes & Rowcounts'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.dm_db_partition_stats (size metrics by partition)'
EXEC #AppendLine 'Source: sys.objects (object names and type; filter is_ms_shipped=0)'
EXEC #AppendLine 'Why: User tables and indexed views with rowcounts plus reserved/used/data/in-row/LOB/row-overflow MB.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_ObjectSizes'
EXEC #AppendLine ''

-- 04a. Index Usage
EXEC #AppendLine '## 04a. Index Usage'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.indexes; sys.dm_db_index_usage_stats; sys.dm_db_partition_stats'
EXEC #AppendLine 'Why: Usage counts for user indexes (reads/writes and last activity times) scoped to the current DB.'
EXEC #AppendLine 'Gate: Requires sysadmin.'
EXEC #AppendLine 'Notes: Counters reset whenever sqlserver_start_time (sys.dm_os_sys_info) changes or a DB detaches.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_IndexUsage'
EXEC #AppendLine ''

-- 04b. Index Fragmentation
EXEC #AppendLine '## 04b. Index Fragmentation'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, ''LIMITED'')'
EXEC #AppendLine 'Source: sys.indexes (names & mapping)'
EXEC #AppendLine 'Why: Per-index fragmentation and page/fragment counts using LIMITED mode.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_IndexFragmentation'
EXEC #AppendLine ''

-- 04c. Unused Indexes
EXEC #AppendLine '## 04c. Unused Indexes'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.indexes; sys.dm_db_index_usage_stats; sys.dm_db_index_physical_stats'
EXEC #AppendLine 'Why: User indexes with zero reads since last stats reset; includes update counts.'
EXEC #AppendLine 'Gate: Requires sysadmin.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_UnusedIndexes'
EXEC #AppendLine ''

-- 04d. Disabled Indexes
EXEC #AppendLine '## 04d. Disabled Indexes'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.indexes (is_disabled = 1)'
EXEC #AppendLine 'Source: sys.objects (modify_date)'
EXEC #AppendLine 'Why: User indexes currently disabled, with last object modify date and any filter definition.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_DisabledIndexes'
EXEC #AppendLine ''

-- 05a. Statistics Staleness
EXEC #AppendLine '## 05a. Statistics Staleness'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.stats; sys.sysindexes; STATS_DATE(object_id, stats_id).'
EXEC #AppendLine 'Why: Table/index statistics with last update time, row counts, and modification counter, plus a heuristic threshold for when auto-update would trigger.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_StatisticsStaleness'
EXEC #AppendLine ''

-- 05b. Statistics with NORECOMPUTE (Auto Update OFF)
EXEC #AppendLine '## 05b. Statistics with NORECOMPUTE (Auto Update OFF)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.stats; sys.sysindexes; STATS_DATE(object_id, stats_id).';
EXEC #AppendLine 'Why: Stats with auto update OFF can lead to stale estimates; review and re-enable when safe.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_StatsNoRecompute';
EXEC #AppendLine '';

-- 05c. Filtered Statistics (Definitions + Recency)
EXEC #AppendLine '## 05c. Filtered Statistics (Definitions + Recency)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.stats; sys.sysindexes; STATS_DATE(object_id, stats_id).';
EXEC #AppendLine 'Why: Filtered stats can drift; shows definitions and recency for safe upkeep.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_FilteredStats';
EXEC #AppendLine '';

-- 05d. Predicate Columns Without Standalone Stats (Index Non-Leading/Included)
EXEC #AppendLine '## 05d. Predicate Columns Without Standalone Stats (Index Non-Leading/Included)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.indexes; sys.index_columns; sys.columns; sys.stats; sys.stats_columns.';
EXEC #AppendLine 'Why: Non-leading/index-included columns used in predicates benefit from per-column stats for better selectivity estimates.';
EXEC #AppendLine 'Notes: SQL Server 2008+; no additional permissions required.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_NoStandaloneStats';
EXEC #AppendLine '';

-- 06a. Missing Indexes
EXEC #AppendLine '## 06a. Missing Indexes'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.dm_db_missing_index_group_stats; sys.dm_db_missing_index_groups; sys.dm_db_missing_index_details'
EXEC #AppendLine 'Why: Recommended indexes from missing-index DMVs with impact and a suggested CREATE INDEX statement.'
EXEC #AppendLine 'Gate: Requires sysadmin.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_MissingIndexes'
EXEC #AppendLine ''

-- 06b. Missing Index Suggestions (Dedup & Combined Impact)
EXEC #AppendLine '## 06b. Missing Index Suggestions (Dedup & Combined Impact)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_db_missing_index_* (groups, stats, details); sys.objects';
EXEC #AppendLine 'Why: Deduplicates suggestions by (eq/ineq/include) and computes Microsoft''s combined benefit score.';
EXEC #AppendLine 'Gate: Requires sysadmin';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_MissingIndexDedup';
EXEC #AppendLine '';

-- 07a. Plan Cache Hotspots & Bloat
EXEC #AppendLine '## 07a. Plan Cache Hotspots & Bloat'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.dm_exec_cached_plans; sys.dm_exec_sql_text (filtered to current DB)'
EXEC #AppendLine 'Why: Distribution of cached plans per objtype/cacheobjtype and percentage of single-use plans (per current DB).'
EXEC #AppendLine 'Gate: Requires sysadmin.'
EXEC #AppendLine 'Notes: Includes total_size_kb and single_use_size_kb (KB of all plans and single-use plans per objtype/cacheobjtype).'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_PlanCacheBloat'
EXEC #AppendLine ''

-- 07b. Multi-Plan by Query Hash
EXEC #AppendLine '## 07b. Multi-Plan by Query Hash'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.dm_exec_query_stats; sys.dm_exec_sql_text (filtered to current DB)'
EXEC #AppendLine 'Why: Query hashes that have multiple distinct plan hashes, with executions and ranking.'
EXEC #AppendLine 'Gate: Requires sysadmin.'
EXEC #AppendLine 'Notes: Includes total_worker_time and total_logical_reads aggregated across all cached plans per query_hash.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_MultiPlanByQueryHash'
EXEC #AppendLine ''

-- 07c. Plan Cache - Trivial & Early-Abort Summary
EXEC #AppendLine '## 07c. Plan Cache - Trivial & Early-Abort Summary';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_query_stats; sys.dm_exec_query_plan (default Showplan namespace).';
EXEC #AppendLine 'Columns: total_cpu_ms; trivial_stmt_count; early_abort_count (GoodEnoughPlanFound); total_stmt_count.';
EXEC #AppendLine 'Scope: Top 300 cached plans by CPU in this DB (st.dbid = DB_ID()).';
EXEC #AppendLine 'Gate: Requires sysadmin';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_CacheTrivialEarlyAbort';
EXEC #AppendLine '';

-- 07d. Active Cursor Usage (by Session)
EXEC #AppendLine '## 07d. Active Cursor Usage (by Session)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_cursors(0) + sys.dm_exec_sessions + sys.dm_exec_sql_text (DMF/DMV).';
EXEC #AppendLine 'Why: Identify expensive or lingering cursors causing CPU/locking; sorted by worker time and reads, with sql_text for investigation.';
EXEC #AppendLine 'Gate: Requires sysadmin';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_ActiveCursors';
EXEC #AppendLine '';

-- 08a. Top Queries - CPU
EXEC #AppendLine '## 08a. Top Queries - CPU'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.dm_exec_query_stats; sys.dm_exec_sql_text'
EXEC #AppendLine 'Why: Top statements by total CPU with counts, averages, last execution time, object name, and sample text.'
EXEC #AppendLine 'Gate: Requires sysadmin.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_TopQueries_CPU'
EXEC #AppendLine ''

-- 08b. Top Queries - Reads
EXEC #AppendLine '## 08b. Top Queries - Reads'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.dm_exec_query_stats; sys.dm_exec_sql_text'
EXEC #AppendLine 'Why: Top statements by total logical reads, including averages and metadata.'
EXEC #AppendLine 'Gate: Requires sysadmin.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_TopQueries_Reads'
EXEC #AppendLine ''

-- 08c. Top Queries - Duration
EXEC #AppendLine '## 08c. Top Queries - Duration'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.dm_exec_query_stats; sys.dm_exec_sql_text'
EXEC #AppendLine 'Why: Top statements by total elapsed time with averages and metadata.'
EXEC #AppendLine 'Gate: Requires sysadmin.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_TopQueries_Duration'
EXEC #AppendLine ''

-- 08d. Top Queries - Writes
EXEC #AppendLine '## 08d. Top Queries - Writes'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.dm_exec_query_stats; sys.dm_exec_sql_text'
EXEC #AppendLine 'Why: Top statements by total logical writes with averages and metadata.'
EXEC #AppendLine 'Gate: Requires sysadmin.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_TopQueries_Writes'
EXEC #AppendLine ''

-- 08e. Parameter-Sensitivity Candidates (Multi-Plan by query_hash + Spread)
EXEC #AppendLine '## 08e. Parameter-Sensitivity Candidates (Multi-Plan by query_hash + Spread)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_query_stats; sys.dm_exec_sql_text (runtime spread heuristic, scoped to current DB).';
EXEC #AppendLine 'Gate: Requires sysadmin';
EXEC #AppendLine 'Notes: Includes min_duration_ms, max_duration_ms, and elapsed_spread_ratio to highlight both absolute runtimes and variability across plans per query_hash.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_ParamSensitivity';
EXEC #AppendLine '';

-- 08f. Top Queries - Memory Grants (Cached Plans)
EXEC #AppendLine '## 08f. Top Queries - Memory Grants (Cached Plans)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_query_stats; sys.dm_exec_query_plan; sys.dm_exec_sql_text (top 300 by CPU).';
EXEC #AppendLine 'Why: Large grants can starve concurrency or indicate overestimation; MaxUsed << Granted often means waste.';
EXEC #AppendLine 'Gate: Requires sysadmin; reads plan XML MemoryGrantInfo; limited to top 300.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_TopQueries_MemoryGrants';
EXEC #AppendLine '';

-- 08g. Operator Prevalence (Text Plans, Top CPU)
EXEC #AppendLine '## 08g. Operator Prevalence (Text Plans, Top CPU)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_query_stats; sys.dm_exec_text_query_plan; sys.dm_exec_sql_text.';
EXEC #AppendLine 'Why: Highlights which operators (scans/lookups/hash/parallelism) dominate the highest-CPU cached statements.';
EXEC #AppendLine 'Gate: Requires sysadmin (sys.dm_exec_query_stats access).';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_TextPlanOperators';
EXEC #AppendLine '';

-- 09a. Plan Warnings
EXEC #AppendLine '## 09a. Plan Warnings';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_query_stats; sys.dm_exec_sql_text; sys.dm_exec_query_plan.';
EXEC #AppendLine 'Why: Surfaces cached statements with spill, memory-grant, plan-affecting convert, or missing-join warnings among the top resource consumers.';
EXEC #AppendLine 'Gate: Requires sysadmin (dm_exec_* DMV access).';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_PlanWarnings'
EXEC #AppendLine ''

-- 09b. Plan Warning Details
EXEC #AppendLine '## 09b. Plan Warning Details';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_query_stats; sys.dm_exec_query_plan; sys.dm_exec_sql_text.';
EXEC #AppendLine 'Why: Provides per-plan warning flags (spills, row goals, FAST hints, spool counts, batch vs row mode) for the hottest cached statements.';
EXEC #AppendLine 'Gate: Requires sysadmin (dm_exec_* DMV access).';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_PlanWarningDetails'
EXEC #AppendLine ''

-- 09c. Row-Goal Proxies (Top Operators & FAST Hint) in Cached Plans
EXEC #AppendLine '## 09c. Row-Goal Proxies (Top Operators & FAST Hint) in Cached Plans';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_query_stats; sys.dm_exec_query_plan; sys.dm_exec_sql_text.';
EXEC #AppendLine 'Why: Finds cached statements with TOP operators or FAST hints that imply row goals and distort estimates.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_RowGoalProxies';
EXEC #AppendLine '';

-- 09d. Spool Builder/Consumer Summary (Table/Index/RowCount Spools)
EXEC #AppendLine '## 09d. Spool Builder/Consumer Summary (Table/Index/RowCount Spools)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_query_stats; sys.dm_exec_sql_text; sys.dm_exec_query_plan.';
EXEC #AppendLine 'Why: Counts table/index/rowcount spools (and nested-loop parents) in the highest-CPU cached plans to flag potential indexing gaps.';
EXEC #AppendLine 'Gate: Requires sysadmin (dm_exec_* DMV access).';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_SpoolSummary';
EXEC #AppendLine '';

-- 09e. Spool Builder/Consumer Pairs (Cached Plans)
EXEC #AppendLine '## 09e. Spool Builder/Consumer Pairs (Cached Plans)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_query_stats; sys.dm_exec_sql_text; sys.dm_exec_query_plan.';
EXEC #AppendLine 'Why: Pairs spool consumers to their builders (WithStack, nested loops) so we can fix the predicates/indexes causing tempdb pressure.';
EXEC #AppendLine 'Gate: Requires sysadmin (dm_exec_* DMV access).';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_SpoolPairs';
EXEC #AppendLine '';

-- 09f. Plan Spill Warnings Summary (Cached Plans)
EXEC #AppendLine '## 09f. Plan Spill Warnings Summary (Cached Plans)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_query_stats; sys.dm_exec_sql_text; sys.dm_exec_query_plan (SpillToTempDb warnings).';
EXEC #AppendLine 'Why: Quantifies cached statements with spill warnings/writes so memory grants or stats can be tuned.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_PlanSpillSummary';
EXEC #AppendLine '';

-- 09g. Memory Grant Utilization Summary (Cached Plans)
EXEC #AppendLine '## 09g. Memory Grant Utilization Summary (Cached Plans)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_query_stats; sys.dm_exec_sql_text; sys.dm_exec_query_plan (MemoryGrantInfo).';
EXEC #AppendLine 'Why: Compares requested/granted/used memory for the hottest cached plans to spot over/under grants.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_MemoryGrantUtil';
EXEC #AppendLine '';

-- 09h. Lookup Hotspots (Key/Rid) in Cached Plans
EXEC #AppendLine '## 09h. Lookup Hotspots (Key/Rid) in Cached Plans';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_query_stats; sys.dm_exec_sql_text; sys.dm_exec_query_plan.';
EXEC #AppendLine 'Why: Tallies key/RID lookups (and nested-loop parents) in top cached plans to prioritize missing INCLUDEs.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_LookupHotspots';
EXEC #AppendLine '';

-- 09i. Parallelism Summary (Dop & NonParallelPlanReason)
EXEC #AppendLine '## 09i. Parallelism Summary (Dop & NonParallelPlanReason)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_query_stats; sys.dm_exec_sql_text; sys.dm_exec_query_plan.';
EXEC #AppendLine 'Why: Shows DOP, NonParallelPlanReason, and parallel operator counts for the top CPU cached plans.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_ParallelismSummary';
EXEC #AppendLine '';

-- 09j. Residual Predicate Summary (Seeks)
EXEC #AppendLine '## 09j. Residual Predicate Summary (Seeks)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_query_stats; sys.dm_exec_sql_text; sys.dm_exec_query_plan.';
EXEC #AppendLine 'Why: Counts seek predicates vs residual predicates to expose filtered seeks that still scan rows.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_ResidualPredicates';
EXEC #AppendLine '';

-- 09k. Plan Warning Variants (NoJoinPredicate, ColumnsWithNoStatistics, UnmatchedIndexes, PlanAffectingConvert)
EXEC #AppendLine '## 09k. Plan Warning Variants (NoJoinPredicate, ColumnsWithNoStatistics, UnmatchedIndexes, PlanAffectingConvert)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_query_stats; sys.dm_exec_sql_text; sys.dm_exec_query_plan.';
EXEC #AppendLine 'Why: Tallies specific plan-warning nodes (NoJoinPredicate, ColumnsWithNoStatistics, UnmatchedIndexes, PlanAffectingConvert) for the top cached plans.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_PlanWarningVariants';
EXEC #AppendLine '';

-- 09l. Join-Shape Mix (Loops/Hash/Merge) in Cached Plans
EXEC #AppendLine '## 09l. Join-Shape Mix (Loops/Hash/Merge) in Cached Plans';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_query_stats, sys.dm_exec_query_plan.';
EXEC #AppendLine 'Why: Quick fingerprint of join operators (Loops/Hash/Merge) across top CPU plans.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_JoinShapeMix';
EXEC #AppendLine '';

-- 09m. Exchange Operators (Parallelism) Summary
EXEC #AppendLine '## 09m. Exchange Operators (Parallelism) Summary';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_query_stats; sys.dm_exec_sql_text; sys.dm_exec_query_plan.';
EXEC #AppendLine 'Why: Counts Repartition/Distribute/Gather Streams operators to surface exchange pressure in hot plans.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_ExchangesSummary';
EXEC #AppendLine '';

-- 09n. Memory Grant Warnings Summary (Cached Plans)
EXEC #AppendLine '## 09n. Memory Grant Warnings Summary (Cached Plans)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_query_stats + sys.dm_exec_query_plan (Showplan //Warnings/MemoryGrantWarning)';
EXEC #AppendLine 'Why: Fast triage of grant sizing problems (excessive/insufficient/wait).';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_MemGrantWarnings';
EXEC #AppendLine '';

-- 09o. Memory Broker Pressure & Targets
EXEC #AppendLine '## 09o. Memory Broker Pressure & Targets';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_os_memory_brokers.';
EXEC #AppendLine 'Why: Highlights where memory is distributed (CACHE / STEAL / RESERVE), how far allocations are from targets/limits, and the broker''s current recommendation.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_MemoryBrokers';
EXEC #AppendLine '';

-- 09p. Resource Governor Pools - Runtime Utilization
EXEC #AppendLine '## 09p. Resource Governor Pools - Runtime Utilization';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_resource_governor_resource_pools (DMV).';
EXEC #AppendLine 'Why: Snapshot of target/used memory and CPU governance per pool to spot pressure or skew.';
EXEC #AppendLine 'Gate: Requires sysadmin; DMV returns no rows when Resource Governor is not enabled.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_RG_PoolRuntime';
EXEC #AppendLine '';

-- 09q. Memory Objects Breakdown (dm_os_memory_objects)
EXEC #AppendLine '## 09q. Memory Objects Breakdown (dm_os_memory_objects)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_os_memory_objects (DMV).';
EXEC #AppendLine 'Why: Deep visibility into internal allocations; useful when clerks view is inconclusive.';
EXEC #AppendLine 'Notes: Only shows objects with allocated bytes >= 256 KB.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_MemoryObjects';
EXEC #AppendLine '';

-- 10a. Memory Grants
EXEC #AppendLine '## 10a. Memory Grants';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_query_memory_grants; sys.dm_exec_sql_text.';
EXEC #AppendLine 'Why: Lists the largest/oldest active memory grants (with text) to diagnose grant starvation.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_MemoryGrants'
EXEC #AppendLine ''

-- 10b. TempDB Session Space
EXEC #AppendLine '## 10b. TempDB Session Space';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_db_session_space_usage; sys.dm_exec_sessions; sys.dm_exec_requests.';
EXEC #AppendLine 'Why: Breaks down user/internal TempDB consumption per session scoped to the current database.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_TempdbSessionSpace_10b'
EXEC #AppendLine ''

-- 10c. Top Buffer Pool Consumers
EXEC #AppendLine '## 10c. Top Buffer Pool Consumers';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_os_buffer_descriptors; sys.allocation_units; sys.partitions.';
EXEC #AppendLine 'Why: Shows which objects consume the most buffer cache in the current database.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_TopBufferPool'
EXEC #AppendLine ''

-- 10d. Top Objects in Buffer Pool (by Pages)
EXEC #AppendLine '## 10d. Top Objects in Buffer Pool (by Pages)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_os_buffer_descriptors; sys.objects; sys.indexes';
EXEC #AppendLine 'Why: Identify objects dominating memory to guide tuning/capacity.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_BufferPoolTopObjects';
EXEC #AppendLine '';

-- 10e. Recent User Commands (Active Sessions - Input Buffer)
EXEC #AppendLine '## 10e. Recent User Commands (Active Sessions - Input Buffer)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_requests; sys.dm_exec_sessions; DBCC INPUTBUFFER.';
EXEC #AppendLine 'Why: Quick snapshot of last statements executed by users active in the target DB to aid incident triage.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_RecentUserCommands';
EXEC #AppendLine '';

-- 11a. Blocking
EXEC #AppendLine '## 11a. Blocking';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_requests; sys.dm_exec_sql_text.';
EXEC #AppendLine 'Why: Surfaces currently blocked requests in this database with waits, elapsed time, and statement text.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_BlockingActive'
EXEC #AppendLine ''

-- 11b. Long Transactions
EXEC #AppendLine '## 11b. Long Transactions';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_tran_active_transactions; sys.dm_tran_session_transactions; sys.dm_tran_database_transactions; sys.dm_exec_requests; sys.dm_exec_sql_text.';
EXEC #AppendLine 'Why: Lists the oldest transactions in this database with waits, IO, and SQL text for cleanup decisions.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_LongTransactions'
EXEC #AppendLine ''

-- 11c. Top Blocked Objects
EXEC #AppendLine '## 11c. Top Blocked Objects';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_tran_locks; sys.dm_exec_sessions; sys.dm_exec_requests.';
EXEC #AppendLine 'Why: Aggregates the most frequently blocked resources inside the current database.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_TopBlockedObjects'
EXEC #AppendLine ''

-- 11d. Long Snapshot Transactions (Version Store Consumers)
EXEC #AppendLine '## 11d. Long Snapshot Transactions (Version Store Consumers)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_tran_active_snapshot_database_transactions (scoped to current DB).';
EXEC #AppendLine 'Columns: session_id, elapsed_time_seconds, max/avg version chain traversed.';
EXEC #AppendLine 'Why: Detect long-running snapshot readers that bloat version store and cause long scans.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_LongSnapshotXacts';
EXEC #AppendLine '';

-- 11e. Top Version-Store Generators (by Table)
EXEC #AppendLine '## 11e. Top Version-Store Generators (by Table)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_tran_top_version_generators; sys.partitions; sys.objects';
EXEC #AppendLine 'Why: Identify top contributors to tempdb version store under snapshot/RCSI.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_TopVersionGenerators';
EXEC #AppendLine '';

-- 12a. Heaps & Forwarded Records
EXEC #AppendLine '## 12a. Heaps & Forwarded Records'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.dm_db_partition_stats (row_count); sys.dm_db_index_physical_stats (SAMPLED; index_id = 0)'
EXEC #AppendLine 'Why: Large user heaps with forwarded records and page usage stats (current database).'
EXEC #AppendLine 'Gate: User tables only; considers heaps with row_count >= 100000 and at most the top 200 heaps by row_count.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_HeapsForwarded'
EXEC #AppendLine ''

-- 12b. All Heaps
EXEC #AppendLine '## 12b. All Heaps'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.tables'
EXEC #AppendLine 'Source: sys.indexes (clustered index check)'
EXEC #AppendLine 'Why: User tables without a clustered index (heaps).'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_AllHeaps'
EXEC #AppendLine ''

-- 13a. Foreign Keys Without Supporting Indexes
EXEC #AppendLine '## 13a. Foreign Keys Without Supporting Indexes'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.foreign_keys, sys.foreign_key_columns'
EXEC #AppendLine 'Source: sys.tables, sys.columns'
EXEC #AppendLine 'Source: sys.indexes, sys.index_columns'
EXEC #AppendLine 'Why: Child-side FK columns lacking a supporting index on the child table.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_FK_NoIndex_Child'
EXEC #AppendLine ''

-- 13b. Foreign Keys Without Supporting Indexes
EXEC #AppendLine '## 13b. Foreign Keys Without Supporting Indexes'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.foreign_keys, sys.foreign_key_columns'
EXEC #AppendLine 'Source: sys.tables, sys.columns'
EXEC #AppendLine 'Source: sys.indexes, sys.index_columns'
EXEC #AppendLine 'Why: Parent-side referenced columns and whether a supporting index exists on the parent table.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_FK_NoIndex_Parent'
EXEC #AppendLine ''

-- 13c. Fk Type / Collation Mismatch (Heuristic)
EXEC #AppendLine '## 13c. Fk Type / Collation Mismatch (Heuristic)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.foreign_key_columns; sys.columns; sys.types; sys.objects.';
EXEC #AppendLine 'Why: Flags FK/PK column mismatches that often cause implicit conversions and unsargable joins.';
EXEC #AppendLine 'Notes: Heuristic only - review each row before remediation.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_FK_TypeCollationMismatch';
EXEC #AppendLine '';

-- 13d. Untrusted or Disabled Foreign Keys
EXEC #AppendLine '## 13d. Untrusted or Disabled Foreign Keys';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.foreign_keys (is_not_trusted, is_disabled).';
EXEC #AppendLine 'Why: Untrusted or disabled FKs can prevent join elimination and other optimizations.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_UntrustedFKs';
EXEC #AppendLine '';

-- 13e. Foreign Keys with Cascades & Trust Status
EXEC #AppendLine '## 13e. Foreign Keys with Cascades & Trust Status';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.foreign_keys; sys.objects; sys.schemas';
EXEC #AppendLine 'Why: Cascades can surprise workloads; untrusted FKs break optimizer assumptions.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_FKCascades';
EXEC #AppendLine '';

-- 14a. Top Objects by Reads/Writes
EXEC #AppendLine '## 14a. Top Objects by Reads/Writes';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.indexes; sys.dm_db_index_usage_stats.';
EXEC #AppendLine 'Why: Summarizes per-object read/write counts to highlight IO-heavy tables.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_TopObjectsReadWrite'
EXEC #AppendLine ''


-- 15a. Backup History
EXEC #AppendLine '## 15a. Backup History'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: msdb.dbo.backupset'
EXEC #AppendLine 'Source: msdb.dbo.backupmediafamily'
EXEC #AppendLine 'Why: Most recent backup for the current database (type, size, device).'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_BackupHistoryTop1'
EXEC #AppendLine ''

-- 15b. Vlf Count
EXEC #AppendLine '## 15b. Vlf Count';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: DBCC LOGINFO (current DB).';
EXEC #AppendLine 'Why: Reports VLF counts which affect recovery time and log growth management.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_VLF_Count'
EXEC #AppendLine ''

-- 15c. Log Stats
EXEC #AppendLine '## 15c. Log Stats';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: DBCC SQLPERF(LOGSPACE); sys.databases.';
EXEC #AppendLine 'Why: Provides current log size vs used percent for the target database.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_LogStats'
EXEC #AppendLine ''

-- 15d. Backup Compression Ratio (Last 14 Days)
EXEC #AppendLine '## 15d. Backup Compression Ratio (Last 14 Days)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: msdb.dbo.backupset.';
EXEC #AppendLine 'Why: Show recent backup compression savings by database.';
EXEC #AppendLine 'Notes: Reads msdb backup history (requires permissions).';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_BackupCompression';
EXEC #AppendLine '';

-- 15e. Backup Checksums (Last 14 Days)
EXEC #AppendLine '## 15e. Backup Checksums (Last 14 Days)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: msdb.dbo.backupset';
EXEC #AppendLine 'Why: Confirm backup checksums usage and highlight damaged or copy-only backups for the current database.';
EXEC #AppendLine 'Notes: Reads msdb backup history (requires permissions).';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_BackupChecksums';
EXEC #AppendLine '';

-- 16a. Lob Usage
EXEC #AppendLine '## 16a. Lob Usage';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.tables, sys.partitions, sys.allocation_units (LOB_DATA, ROW_OVERFLOW_DATA).';
EXEC #AppendLine 'Why: Identify tables with heavy LOB and row-overflow usage to guide cleanup and compression decisions.';
EXEC #AppendLine 'Notes: Top 50 tables in the current database by total LOB/ROW_OVERFLOW reserved space (MB).';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_LOB_Usage'
EXEC #AppendLine ''

-- 16b. Check Constraints
EXEC #AppendLine '## 16b. Check Constraints';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.check_constraints, sys.tables.';
EXEC #AppendLine 'Why: Show check constraints per user table (disabled, untrusted, not-for-replication state, definition).';
EXEC #AppendLine 'Notes: Excludes system tables (t.is_ms_shipped = 0).';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_Check_Constraints';
EXEC #AppendLine ''

-- 16c. Sparse Columns Inventory (Optional)
EXEC #AppendLine '## 16c. Sparse Columns Inventory (Optional)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.columns (is_sparse, is_column_set).';
EXEC #AppendLine 'Why: Helps assess sparse and column-set usage and potential storage/CPU trade-offs.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_SparseColumns';
EXEC #AppendLine '';

-- 17a. Partition Functions
EXEC #AppendLine '## 17a. Partition Functions'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.partition_functions; sys.partition_range_values'
EXEC #AppendLine 'Why: Partition functions, boundary type, and boundary values.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_Partition_Functions'
EXEC #AppendLine ''

-- 17b. Partition Schemes
EXEC #AppendLine '## 17b. Partition Schemes'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.partition_schemes; sys.destination_data_spaces; sys.filegroups'
EXEC #AppendLine 'Why: Partition schemes and target filegroups.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_Partition_Schemes'
EXEC #AppendLine ''

-- 17c. Partitioned Objects
EXEC #AppendLine '## 17c. Partitioned Objects'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.objects; sys.indexes; sys.partition_schemes; sys.partitions'
EXEC #AppendLine 'Why: Partitioned tables/indexes with scheme, partition number, and row counts.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_Partitioned_Objects'
EXEC #AppendLine ''

-- 17d. Non-Aligned Indexes on Partitioned Tables
EXEC #AppendLine '## 17d. Non-Aligned Indexes on Partitioned Tables';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.objects; sys.indexes; sys.data_spaces.';
EXEC #AppendLine 'Why: Find indexes not aligned with the base table''s partition scheme (maintenance/perf).';
EXEC #AppendLine 'Notes: Requires partitioned tables/schemes to exist.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_NonAlignedIndexes';
EXEC #AppendLine '';

-- 17e. Partition Skew Summary (Base Table/Cluster Only)
EXEC #AppendLine '## 17e. Partition Skew Summary (Base Table/Cluster Only)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_db_partition_stats; sys.objects.';
EXEC #AppendLine 'Why: Find skewed partitions causing hotspots & uneven maintenance.';
EXEC #AppendLine 'Notes: Requires partitioned objects to exist.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_PartitionSkew';
EXEC #AppendLine '';

-- 18a. Index Key Width & Column Counts
EXEC #AppendLine '## 18a. Index Key Width & Column Counts'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.indexes; sys.index_columns; sys.columns'
EXEC #AppendLine 'Why: Key vs include column counts and approximate total key bytes per index.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_Index_KeySize'
EXEC #AppendLine ''

-- 18b. Index Option Anomalies
EXEC #AppendLine '## 18b. Index Option Anomalies';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.indexes; sys.partitions; sys.objects; sys.schemas.';
EXEC #AppendLine 'Why: Lists indexes using non-default options (fillfactor, PAD_INDEX, IGNORE_DUP_KEY, compression, filters, hypothetical).';
EXEC #AppendLine 'Notes: Reports user indexes with non-default options.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_IndexOptionAnomalies';
EXEC #AppendLine '';

-- 18c. Indexes with Non-Default Fillfactor / PAD_INDEX
EXEC #AppendLine '## 18c. Indexes with Non-Default Fillfactor / PAD_INDEX';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.indexes; sys.objects';
EXEC #AppendLine 'Why: Inventory explicit fillfactor/padding choices that shape IO and splits.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_NonDefaultFillfactor';
EXEC #AppendLine '';

-- 18d. Hypothetical Indexes (Leftover Dta)
EXEC #AppendLine '## 18d. Hypothetical Indexes (Leftover Dta)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.indexes; sys.objects';
EXEC #AppendLine 'Why: Inventories hypothetical indexes left by tuning tools; they are not usable and add clutter.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_HypotheticalIndexes';
EXEC #AppendLine '';

-- 19a. Compression Candidates
EXEC #AppendLine '## 19a. Compression Candidates'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.dm_db_index_physical_stats (SAMPLED); sys.objects; sys.indexes'
EXEC #AppendLine 'Why: Low page density indexes (page_count threshold + avg page space used).'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_Compression_Candidates'
EXEC #AppendLine ''

-- 19b. Compression Inventory (Rowstore Partitions)
EXEC #AppendLine '## 19b. Compression Inventory (Rowstore Partitions)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.partitions; sys.indexes; sys.objects; sys.schemas; sys.allocation_units';
EXEC #AppendLine 'Why: Shows what is already compressed (ROW/PAGE) and footprint per partition.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_CompressionInventory';
EXEC #AppendLine '';

-- 20a. Object Types
EXEC #AppendLine '## 20a. Object Types'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.objects'
EXEC #AppendLine 'Why: Count of objects by type in the current database.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_Object_Types'
EXEC #AppendLine ''

-- 20b. CLR Assemblies (Permission Set)
EXEC #AppendLine '## 20b. CLR Assemblies (Permission Set)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.assemblies.';
EXEC #AppendLine 'Why: Surface CLR assemblies & permission sets for governance/security.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_CLR_Assemblies';
EXEC #AppendLine '';

-- 20c. Lock Escalation Settings
EXEC #AppendLine '## 20c. Lock Escalation Settings';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.tables';
EXEC #AppendLine 'Why: Inventory of table-level lock escalation posture (disabling alters blocking & memory).';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_LockEscalation';
EXEC #AppendLine '';

-- 21a. Top Procedures ExecStats
EXEC #AppendLine '## 21a. Top Procedures ExecStats';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_procedure_stats; sys.dm_exec_sql_text.';
EXEC #AppendLine 'Why: Top stored procedures by CPU/time/reads/writes plus averages and last execution time for the current database.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_Top_Procedures_ExecStats'
EXEC #AppendLine ''

-- 21b. Top Triggers ExecStats
EXEC #AppendLine '## 21b. Top Triggers ExecStats';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_trigger_stats; sys.dm_exec_sql_text.';
EXEC #AppendLine 'Why: Shows triggers with highest cumulative CPU/time/reads/writes plus averages and last execution time in this DB.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_Top_Triggers_ExecStats'
EXEC #AppendLine ''

-- 21c. Procedures with Recompile (Definition or Option)
EXEC #AppendLine '## 21c. Procedures with Recompile (Definition or Option)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.procedures; sys.sql_modules.';
EXEC #AppendLine 'Why: RECOMPILE can mask parameter sniffing but increase CPU/compile.';
EXEC #AppendLine 'Notes: Highlights procedures defined WITH RECOMPILE or executed WITH RECOMPILE.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_ProcRecompile';
EXEC #AppendLine '';

-- 21d. Disabled Triggers (Ddl & Dml)
EXEC #AppendLine '## 21d. Disabled Triggers (Ddl & Dml)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.triggers; sys.objects';
EXEC #AppendLine 'Why: Disabled triggers silently bypass DDL/DML guards; good to surface.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_DisabledTriggers';
EXEC #AppendLine '';

-- 21e. Modules Referencing xp_cmdshell / Ole Automation
EXEC #AppendLine '## 21e. Modules Referencing xp_cmdshell / Ole Automation';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.sql_modules; sys.objects.';
EXEC #AppendLine 'Why: Highlight modules that reference xp_cmdshell or OLE Automation routines for hardening and security review.';
EXEC #AppendLine 'Notes: Limited to user modules; visibility follows VIEW DEFINITION permissions.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_ModulesRiskyCalls';
EXEC #AppendLine '';

-- 21f. Modules Using Nolock / Readuncommitted
EXEC #AppendLine '## 21f. Modules Using Nolock / Readuncommitted';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.sql_modules; sys.objects.';
EXEC #AppendLine 'Why: Surface modules that use NOLOCK/READUNCOMMITTED hints, which can return inconsistent reads.';
EXEC #AppendLine 'Notes: Limited to user modules; visibility follows VIEW DEFINITION permissions.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_ModulesNoLock';
EXEC #AppendLine '';

-- 21g. Modules With Legacy SET Options (QUOTED_IDENTIFIER OFF / ANSI_NULLS OFF)
EXEC #AppendLine '## 21g. Modules With Legacy SET Options (QUOTED_IDENTIFIER OFF / ANSI_NULLS OFF)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.sql_modules joined to sys.objects';
EXEC #AppendLine 'Why: QUOTED_IDENTIFIER OFF and ANSI_NULLS OFF can block filtered indexes and indexed views and can change query semantics.';
EXEC #AppendLine 'Notes: Action is to normalize SET options (CREATE/ALTER with proper SET options) before enabling certain features.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_ModulesLegacySetOptions';
EXEC #AppendLine '';

-- 21h. Loaded Binary Modules (Non-Microsoft)
EXEC #AppendLine '## 21h. Loaded Binary Modules (Non-Microsoft)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_os_loaded_modules (DMV).';
EXEC #AppendLine 'Why: Surface third-party DLLs/extended components for audit/perf investigations.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_LoadedModules';
EXEC #AppendLine '';

-- 22a. Security Policies
EXEC #AppendLine '## 22a. Security Policies';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.databases; sys.dm_database_encryption_keys.';
EXEC #AppendLine 'Why: Reports CDC and TDE enablement for this database.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_Security_Policies'
EXEC #AppendLine ''

-- 22b. Encryption (TDE) Status
EXEC #AppendLine '## 22b. Encryption (TDE) Status';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_database_encryption_keys.';
EXEC #AppendLine 'Why: Shows TDE encryption state/progress for the current database.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_TDE_Status';
EXEC #AppendLine '';

-- 22c. SQL Audit (Server)
EXEC #AppendLine '## 22c. SQL Audit (Server)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.server_audits; sys.server_audit_specifications; sys.server_audit_specification_details; sys.dm_server_audit_status.';
EXEC #AppendLine 'Why: Show SQL Audit posture at server scope.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_AuditServer';
EXEC #AppendLine '';

-- 22d. Service Broker (DB Status & Queues)
EXEC #AppendLine '## 22d. Service Broker (DB Status & Queues)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.databases; sys.service_queues.';
EXEC #AppendLine 'Why: Shows whether Service Broker is enabled and queue activation/poisoning settings.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_ServiceBroker';
EXEC #AppendLine '';

-- 22e. TDE Certificate & Expiration (Current DB)
EXEC #AppendLine '## 22e. TDE Certificate & Expiration (Current DB)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_database_encryption_keys; master.sys.certificates.';
EXEC #AppendLine 'Why: Provides the TDE encryptor thumbprint, algorithm, and certificate expiration.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_TDE_Cert';
EXEC #AppendLine '';

-- 22f. Orphaned Users (Instance-Mapped vs. Contained)
EXEC #AppendLine '## 22f. Orphaned Users (Instance-Mapped vs. Contained)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.database_principals; sys.server_principals.';
EXEC #AppendLine 'Why: Highlights database users lacking matching server logins (SID mismatch) to fix orphaned access.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_OrphanedUsers';
EXEC #AppendLine '';

-- 22g. High-Privilege Role Members (db_owner, Securityadmin, Accessadmin, Ddladmin, Backupoperator)
EXEC #AppendLine '## 22g. High-Privilege Role Members (db_owner, Securityadmin, Accessadmin, Ddladmin, Backupoperator)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.database_principals; sys.database_role_members.';
EXEC #AppendLine 'Why: Lists members of high-privilege DB roles to enforce least privilege.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_HighPrivRoleMembers';
EXEC #AppendLine '';

-- 22h. Credentials (Server & DB Scopes)
EXEC #AppendLine '## 22h. Credentials (Server & DB Scopes)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.credentials.';
EXEC #AppendLine 'Why: Credentials inventory (no secrets) at server scope.';
EXEC #AppendLine 'Gate: Requires sysadmin. SafeMode redacts names.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_Credentials';
EXEC #AppendLine '';

-- 22i. High-Risk Server-Level Permissions
EXEC #AppendLine '## 22i. High-Risk Server-Level Permissions';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.server_permissions; sys.server_principals.';
EXEC #AppendLine 'Why: Inventory principals granted powerful server-level permissions (CONTROL SERVER, IMPERSONATE ANY LOGIN, ALTER ANY LOGIN/DATABASE, unsafe/external assemblies) for least-privilege review.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_ServerHighRiskPerms';
EXEC #AppendLine '';

-- 22j. Explicit GRANTs to PUBLIC (DB Scope)
EXEC #AppendLine '## 22j. Explicit GRANTs to PUBLIC (DB Scope)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.database_permissions; sys.database_principals.';
EXEC #AppendLine 'Why: Find explicit GRANTs to PUBLIC that broaden access beyond expectations.';
EXEC #AppendLine 'Notes: Visibility follows database metadata permissions; some grants may be hidden without VIEW DEFINITION.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_PublicDbGrants';
EXEC #AppendLine '';

-- 22k. Audit Targets (File/Queue)
EXEC #AppendLine '## 22k. Audit Targets (File/Queue)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.server_audits; sys.dm_server_audit_status.';
EXEC #AppendLine 'Why: Show where server audits write (file/URL/queue) and whether they are currently running.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_AuditTargets';
EXEC #AppendLine '';

-- 22l. Database Audit Specifications (Bound to Audits)
EXEC #AppendLine '## 22l. Database Audit Specifications (Bound to Audits)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.database_audit_specifications.';
EXEC #AppendLine 'Why: Inventory database audit specifications bound to audits for this database.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_DatabaseAuditSpecs';
EXEC #AppendLine '';

-- 22m. SQL Audit (Database Specifications)
EXEC #AppendLine '## 22m. SQL Audit (Database Specifications)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.database_audit_specifications; sys.database_audit_specification_details.';
EXEC #AppendLine 'Why: Show database-level audit specification posture (current DB).';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_AuditDatabase';
EXEC #AppendLine '';

-- 23a. Filestream Files
EXEC #AppendLine '## 23a. Filestream Files';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.database_files.';
EXEC #AppendLine 'Why: Shows FILESTREAM files in the current database.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_FileStream';
EXEC #AppendLine '';

-- 23b. Synonyms & Their Targets (Server/DB/Schema/Object)
EXEC #AppendLine '## 23b. Synonyms & Their Targets (Server/DB/Schema/Object)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.synonyms.';
EXEC #AppendLine 'Why: Expands synonym targets (server/db/schema/object) to reveal cross-database hops.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_SynonymTargets';
EXEC #AppendLine '';

-- 23c. Full-Text Posture
EXEC #AppendLine '## 23c. Full-Text Posture';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.fulltext_catalogs; sys.fulltext_indexes';
EXEC #AppendLine 'Why: Show full-text catalogs and indexes present in the database for search posture review.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_FullTextCatalogs';
EXEC #AppendLine '';

-- 23d. Full-Text - Catalogs & Indexes Posture
EXEC #AppendLine '## 23d. Full-Text - Catalogs & Indexes Posture';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.fulltext_indexes.';
EXEC #AppendLine 'Why: Detail full-text indexes (tables/columns, key, change tracking) for tuning and validation.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_FullTextIndexes';
EXEC #AppendLine '';

-- 23e. In-Memory OLTP Candidate Tables (Heuristic)
EXEC #AppendLine '## 23e. In-Memory OLTP Candidate Tables (Heuristic)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.tables; sys.dm_db_index_usage_stats; sys.dm_db_partition_stats; sys.columns; sys.dm_db_missing_index_* DMVs.';
EXEC #AppendLine 'Why: Heuristic to flag write-heavy, latch-prone tables that might benefit from In-Memory OLTP.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_InMemoryCandidates';
EXEC #AppendLine '';

-- 24a. Duplicate Overlap Index Heuristic
EXEC #AppendLine '## 24a. Duplicate Overlap Index Heuristic'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.indexes; sys.index_columns; sys.columns'
EXEC #AppendLine 'Why: Heuristic to spot duplicate/overlapping indexes via key/include column lists.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_Duplicate_Overlap_Index_Heuristic'
EXEC #AppendLine ''

-- 25a. Computed NotPersisted
EXEC #AppendLine '## 25a. Computed NotPersisted'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.tables; sys.columns; COLUMNPROPERTY(IsPersisted)'
EXEC #AppendLine 'Why: Computed columns that are not persisted (potential performance considerations).'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_Computed_NotPersisted'
EXEC #AppendLine ''

-- 25b. Scalar UDFs
EXEC #AppendLine '## 25b. Scalar UDFs'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.objects (FN, FS)'
EXEC #AppendLine 'Why: Scalar user-defined functions present in the database.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_Scalar_UDFs'
EXEC #AppendLine ''

-- 25c. Multi-Statement TVFs (Type = Tf)
EXEC #AppendLine '## 25c. Multi-Statement TVFs (Type = Tf)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.objects (type = TF).';
EXEC #AppendLine 'Why: Multi-statement TVFs often have row-estimation issues.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_MSTVFs';
EXEC #AppendLine '';

-- 26a. Table RowCounts
EXEC #AppendLine '## 26a. Table RowCounts'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.tables; sys.partitions (if used); sys.indexes (if used)'
EXEC #AppendLine 'Why: Per-table approximate rowcounts (from catalog/DMVs), for inventory and size cues.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_Table_RowCounts'
EXEC #AppendLine ''

-- 26b. CPU Utilization (SQL vs Idle vs Other)
EXEC #AppendLine '## 26b. CPU Utilization (SQL vs Idle vs Other)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_os_ring_buffers (RING_BUFFER_SCHEDULER_MONITOR/SystemHealth); sys.dm_os_sys_info.';
EXEC #AppendLine 'Why: Cumulative CPU breakdown since service start (SQL vs Idle vs Other) plus uptime anchor.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_CPU_Utilization';
EXEC #AppendLine '';

-- 26c. Latch Contention Hotspots (Aggregated)
EXEC #AppendLine '## 26c. Latch Contention Hotspots (Aggregated)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_os_latch_stats.';
EXEC #AppendLine 'Why: Surfaces non-buffer latch hotspots by cumulative wait time to aid in diagnosing internal contention.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine 'Notes: Reset with DBCC SQLPERF (''sys.dm_os_latch_stats'', CLEAR).';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_LatchStats';
EXEC #AppendLine '';

-- 27a. Server TopWaits
EXEC #AppendLine '## 27a. Server TopWaits';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_os_wait_stats.';
EXEC #AppendLine 'Why: Highlight top waits since last restart to guide mitigation focus.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_Server_TopWaits'
EXEC #AppendLine ''

-- 27b. Server Schedulers
EXEC #AppendLine '## 27b. Server Schedulers';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_os_schedulers.';
EXEC #AppendLine 'Why: Shows runnable tasks/queue depth and online/offline state per scheduler for CPU pressure triage and offline-core detection.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_Server_Schedulers'
EXEC #AppendLine ''

-- 27c. Server MemoryClerks
EXEC #AppendLine '## 27c. Server MemoryClerks';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_os_memory_clerks.';
EXEC #AppendLine 'Why: Lists top memory clerk consumers (pages KB, VM) for server-level memory diagnostics.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_Server_MemoryClerks'
EXEC #AppendLine ''

-- 27d. Tempdb Layout
EXEC #AppendLine '## 27d. Tempdb Layout'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: tempdb.sys.database_files'
EXEC #AppendLine 'Why: TempDB files layout (count, size); useful for contention checks.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_Tempdb_Layout'
EXEC #AppendLine ''

-- 27e. Waits Roll-Up (Filtered Categories)
EXEC #AppendLine '## 27e. Waits Roll-Up (Filtered Categories)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_os_wait_stats.';
EXEC #AppendLine 'Why: Buckets waits into CPU/I/O/Locks/Latch/Memory/Other after filtering benign types.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_WaitsRollup';
EXEC #AppendLine '';

-- 27f. Wait Categories (Signal-Only Roll-Up)
EXEC #AppendLine '## 27f. Wait Categories (Signal-Only Roll-Up)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_os_wait_stats.';
EXEC #AppendLine 'Why: Maps waits to categories (Parallelism/I-O/Latch/Lock/Memory/Network/Other) emphasizing signal waits.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_WaitCategories';
EXEC #AppendLine '';

-- 27g. Spinlock Hotspots
EXEC #AppendLine '## 27g. Spinlock Hotspots';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_os_spinlock_stats.';
EXEC #AppendLine 'Why: Highlights spinlock types with high collisions/backoffs for advanced CPU diagnostics.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_SpinlockHotspots';
EXEC #AppendLine '';

-- 27h. Signal vs. Resource Waits (Since Startup)
EXEC #AppendLine '## 27h. Signal vs. Resource Waits (Since Startup)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_os_wait_stats.';
EXEC #AppendLine 'Why: Compares cumulative signal vs resource waits to flag CPU pressure.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_SignalVsResourceWaits';
EXEC #AppendLine '';

-- 27i. Buffer Node Page Life Expectancy (by Numa)
EXEC #AppendLine '## 27i. Buffer Node Page Life Expectancy (by Numa)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_os_performance_counters (Buffer Node).';
EXEC #AppendLine 'Why: Reports Page Life Expectancy per NUMA node to expose uneven memory pressure.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_PLEByNode';
EXEC #AppendLine '';

-- 28a. CheckDB Recency
EXEC #AppendLine '## 28a. CheckDB Recency'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: master.dbo.xp_readerrorlog (DBCC CHECKDB entries).'
EXEC #AppendLine 'Why: Shows recent DBCC CHECKDB messages for this database from the server error log.'
EXEC #AppendLine 'Gate: Requires sysadmin.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_CheckDB_Recency'
EXEC #AppendLine ''

-- 28b. Suspect Pages (Current DB)
EXEC #AppendLine '## 28b. Suspect Pages (Current DB)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: msdb.dbo.suspect_pages.';
EXEC #AppendLine 'Why: Detect page corruption history affecting this database.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_SuspectPages';
EXEC #AppendLine '';

-- 29a. File FreeSpace
EXEC #AppendLine '## 29a. File FreeSpace'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.database_files; FILEPROPERTY (SpaceUsed)'
EXEC #AppendLine 'Why: File free space and growth headroom (size, max_size, growth settings) for the current database.'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_File_FreeSpace'
EXEC #AppendLine ''

-- 29b. Files with Percentage Growth
EXEC #AppendLine '## 29b. Files with Percentage Growth';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.database_files.';
EXEC #AppendLine 'Why: Percent growth can cause many small growth events; flag files to review.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_FilesPercentGrowth';
EXEC #AppendLine '';

-- 29c. Files with Tiny Fixed Autogrowth (< 64 Mb)
EXEC #AppendLine '## 29c. Files with Tiny Fixed Autogrowth (< 64 Mb)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.database_files';
EXEC #AppendLine 'Why: Tiny fixed growth can cause churn and fragmentation; raise to MB-sized steps.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_FilesSmallGrowth';
EXEC #AppendLine '';

-- 30a. AgentJobs ReferencingDB
EXEC #AppendLine '## 30a. AgentJobs ReferencingDB';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: msdb.dbo.sysjobs; msdb.dbo.sysjobsteps.';
EXEC #AppendLine 'Why: Lists SQL Agent jobs whose steps reference this database by step database_name, bracketed/quoted names, or CmdExec -d switches.';
EXEC #AppendLine 'Gate: Requires sysadmin and msdb present.';
EXEC #AppendLine 'Notes: Includes job id, job name, enabled flag, job description, step id/name, target database, subsystem, a 4K command snippet, and match_source describing which pattern matched (database_name, [dbname], ''dbname'', cmdexec_-d_dbname, or command_contains_dbname).';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_AgentJobs_ReferencingDB'
EXEC #AppendLine ''

-- 30b. SQL Agent Alerts (Msdb)
EXEC #AppendLine '## 30b. SQL Agent Alerts (Msdb)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: msdb.dbo.sysalerts; msdb.dbo.sysnotifications; msdb.dbo.sysoperators; msdb.dbo.sysjobs.';
EXEC #AppendLine 'Why: Alerts that raise on critical errors (823/824/825) and severities, including their notification posture.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine 'Notes: Includes alert id/name, severity/message filters, response delay/counts, mapped job, operator, and notification methods plus is_critical_corruption_or_severity and notify_email/pager/netsend/has_notification flags for GPT-friendly analysis.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_AgentAlerts';
EXEC #AppendLine '';

-- 30c. SQL Agent Operators (Msdb)
EXEC #AppendLine '## 30c. SQL Agent Operators (Msdb)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: msdb.dbo.sysoperators.';
EXEC #AppendLine 'Why: Operators configured for Agent notifications.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine 'Notes: Surfaces email/pager/net send addresses, has_email/has_pager/has_netsend flags, plus weekday/weekend window start/end times and pager day masks.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_AgentOperators';
EXEC #AppendLine '';

-- 30d. SQL Agent Proxies (Enabled + Usage)
EXEC #AppendLine '## 30d. SQL Agent Proxies (Enabled + Usage)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: msdb.dbo.sysproxies; msdb.dbo.sysproxysubsystem; msdb.dbo.sysproxylogin.';
EXEC #AppendLine 'Why: Shows which Agent proxies are enabled, which subsystems they cover, how many logins map to each, and how many jobs/steps actually use them.';
EXEC #AppendLine 'Gate: msdb present and Agent proxy metadata available.';
EXEC #AppendLine 'Notes: Columns surface credential id, subsystem_count, login_mappings, step_usage_count, jobs_using_proxy, and has_subsystems/has_logins flags to highlight unused or over-privileged or unused proxies.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_AgentProxies';
EXEC #AppendLine '';

-- 30e. SQL Agent Jobs - Last Outcome (Msdb)
EXEC #AppendLine '## 30e. SQL Agent Jobs - Last Outcome (Msdb)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: msdb.dbo.sysjobs; msdb.dbo.sysjobhistory.';
EXEC #AppendLine 'Why: Shows last execution outcome and duration per job (step 0) for maintenance hygiene and alerting.';
EXEC #AppendLine 'Gate: msdb present and job history metadata available.';
EXEC #AppendLine 'Notes: Reports job id, job enabled flag, numeric last_run_status, textual last_run_status_desc, last_run_succeeded/last_run_failed flags, last execution timestamp (msdb agent_datetime), and last run duration in seconds (run_duration_sec) computed from msdb run_duration.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_AgentJobLastRun';
EXEC #AppendLine '';

-- 30f. Jobs with No Schedule or Only Disabled Schedules
EXEC #AppendLine '## 30f. Jobs with No Schedule or Only Disabled Schedules';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: msdb.dbo.sysjobs; msdb.dbo.sysjobschedules; msdb.dbo.sysschedules.';
EXEC #AppendLine 'Why: Flag jobs that never run or have only disabled schedules, with explicit counts, posture flags, and next run metadata for GPT/automation.';
EXEC #AppendLine 'Gate: Requires sysadmin and msdb present.';
EXEC #AppendLine 'Notes: Outputs job id/enabled state, schedule/enable/disable counts plus HasNoSchedule and OnlyDisabledSchedules bits, next_run_datetime, and next_run_within_7d so you can triage orphaned, paused, or mis-scheduled jobs programmatically.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_JobsNoSchedule';
EXEC #AppendLine '';

-- 30g. SQL Agent - Job Owner Posture
EXEC #AppendLine '## 30g. SQL Agent - Job Owner Posture';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: msdb.dbo.sysjobs; sys.server_principals.';
EXEC #AppendLine 'Why: Highlights job owners, whether the principal is disabled, whether the owner is a sysadmin, and whether the owner looks like a login or Windows principal.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine 'Notes: Includes job id, owner name/type/principal id/sid plus flags OwnerIsSysadmin, OwnerIsLoginLike, OwnerIsSqlLoginNonSa, and OwnerIsWindowsPrincipal to support automated posture analysis and remediation.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_JobOwnerPosture';
EXEC #AppendLine '';

-- 30h. Database Mail Profiles
EXEC #AppendLine '## 30h. Database Mail Profiles';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: msdb.dbo.sysmail_profile; msdb.dbo.sysmail_profileaccount.';
EXEC #AppendLine 'Why: Inventory Database Mail profiles, their descriptions, and account coverage to validate notification routing.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine 'Notes: Includes profile_id/name/description plus account_count, primary_account_id and flags has_accounts, has_multiple_accounts, and has_primary_account to support automated mail-profile posture checks.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_DbMailProfiles';
EXEC #AppendLine '';

-- 30i. Database Mail Accounts
EXEC #AppendLine '## 30i. Database Mail Accounts';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: msdb.dbo.sysmail_account; msdb.dbo.sysmail_server.';
EXEC #AppendLine 'Why: Lists Database Mail accounts (identity, email/display/reply-to) and their SMTP server configuration.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine 'Notes: Includes account_id, description, addresses plus servername, port, username, credential usage, SSL flag and posture bits uses_default_credentials, uses_explicit_credentials, and uses_ssl so GPT/automation can classify mail account risk.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_DbMailAccounts';
EXEC #AppendLine '';

-- 30j. Database Mail Profile Accounts
EXEC #AppendLine '## 30j. Database Mail Profile Accounts';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: msdb.dbo.sysmail_profileaccount.';
EXEC #AppendLine 'Why: Shows which accounts (and sequence order) are bound to each Database Mail profile.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine 'Notes: Outputs profile_id/account_id/sequence_number and is_primary_account flag so GPT/automation can reason about primary vs secondary accounts in failover chains.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_DbMailProfileAccounts';
EXEC #AppendLine '';

-- 30k. Maintenance Plans - Plans/Subplans + Jobs/Schedules
EXEC #AppendLine '## 30k. Maintenance Plans - Plans/Subplans + Jobs/Schedules';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: msdb.dbo.sysmaintplan_plans; msdb.dbo.sysmaintplan_subplans; msdb.dbo.sysjobs; msdb.dbo.sysjobschedules; msdb.dbo.sysschedules.';
EXEC #AppendLine 'Why: Inventory maintenance plans and subplans and show which jobs and schedules drive them, including next run time.';
EXEC #AppendLine 'Gate: Requires sysadmin. SafeMode redacts plan/job/schedule names.';
EXEC #AppendLine 'Notes: Surfaces plan/subplan ids and names, job id/name/enabled flag, schedule id/name/enabled flag, frequency properties, and next_run_datetime (via msdb.dbo.agent_datetime) so GPT/automation can reason about maintenance coverage and cadence.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_MaintPlans_Subplans';
EXEC #AppendLine '';

-- 30l. Maintenance Plans - Run Summary
EXEC #AppendLine '## 30l. Maintenance Plans - Run Summary';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: msdb.dbo.sysmaintplan_plans; msdb.dbo.sysmaintplan_subplans; msdb.dbo.sysmaintplan_log.';
EXEC #AppendLine 'Why: Summarize maintenance-plan run history per plan and subplan, including total runs, failures, last run, success rate, and days since last run.';
EXEC #AppendLine 'Gate: Requires sysadmin. SafeMode redacts plan/subplan names.';
EXEC #AppendLine 'Notes: Surfaces plan/subplan ids and names, aggregated run counts, first/last run timestamps, success_rate_pct, and days_since_last_run so GPT/automation can spot failing or stale maintenance plans.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_MaintPlans_RunSummary';
EXEC #AppendLine '';

-- 30m. Maintenance Plans - Last Run per Subplan (Log)
EXEC #AppendLine '## 30m. Maintenance Plans - Last Run per Subplan (Log)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: msdb.dbo.sysmaintplan_log; msdb.dbo.sysmaintplan_plans; msdb.dbo.sysmaintplan_subplans; msdb.dbo.sysjobs.';
EXEC #AppendLine 'Why: Expose the most recent maintenance-plan run outcome (success/failure, duration) per subplan directly from the maintenance-plan log (not just Agent job history).';
EXEC #AppendLine 'Gate: Requires sysadmin. SafeMode redacts plan/subplan/job names.';
EXEC #AppendLine 'Notes: Returns plan/subplan and job identity, last_start_time/last_end_time, duration_seconds, last_succeeded flag, and task_detail_id so GPT/automation can focus on current failures or long-running plans.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_MaintPlanLastRun';
EXEC #AppendLine '';

-- 30n. Maintenance Plans - Recent Task-Level Log Detail
EXEC #AppendLine '## 30n. Maintenance Plans - Recent Task-Level Log Detail';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: msdb.dbo.sysmaintplan_plans; msdb.dbo.sysmaintplan_subplans; msdb.dbo.sysmaintplan_log; msdb.dbo.sysmaintplan_logdetail.';
EXEC #AppendLine 'Why: Provide recent task-level maintenance-plan log detail (commands, messages, success/failure) so you can diagnose failing steps and assess MSDB log bloat.';
EXEC #AppendLine 'Gate: Requires sysadmin. SafeMode redacts names and text columns.';
EXEC #AppendLine 'Notes: Returns plan_name, subplan_name, server_name, timestamps, succeeded flag, up to five log lines, command snippet, and error number/message for the 200 most recent entries ordered by start_time.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_MaintPlanLogDetail';
EXEC #AppendLine '';

-- 30o. Maintenance Plans - Health Flags & Orphans
EXEC #AppendLine '## 30o. Maintenance Plans - Health Flags & Orphans';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: msdb.dbo.sysmaintplan_plans; msdb.dbo.sysmaintplan_subplans; msdb.dbo.sysjobs; msdb.dbo.sysjobschedules; msdb.dbo.sysschedules; msdb.dbo.sysmaintplan_log.';
EXEC #AppendLine 'Why: Highlight orphaned maintenance-plan subplans (no job), disabled jobs, missing or disabled schedules, and subplans with no recent run history.';
EXEC #AppendLine 'Gate: Requires sysadmin. SafeMode redacts plan/subplan names.';
EXEC #AppendLine 'Notes: Includes flags is_orphan_job, job_disabled, has_no_schedule, schedule_disabled plus last_start_time and last_succeeded so GPT/automation can focus on broken or stale maintenance plans.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_MaintPlan_Health';
EXEC #AppendLine '';

-- 30p. SQL Agent - Jobs Without Failure Notifications
EXEC #AppendLine '## 30p. SQL Agent - Jobs Without Failure Notifications';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: msdb.dbo.sysjobs; msdb.dbo.sysoperators.';
EXEC #AppendLine 'Why: Ensure jobs notify an operator on failure (or completion).';
EXEC #AppendLine 'Gate: Requires sysadmin. SafeMode redacts job/operator names.';
EXEC #AppendLine 'Notes: Surfaces job notification configuration, including email/pager/netsend operators, email_includes_failure flag, and has_failure_notification bit so GPT/automation can highlight jobs that will not alert on failure.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_JobNotifications';
EXEC #AppendLine '';

-- 30q. SQL Agent - Job Step Security Posture (Subsystems & Proxies)
EXEC #AppendLine '## 30q. SQL Agent - Job Step Security Posture (Subsystems & Proxies)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: msdb.dbo.sysjobsteps; msdb.dbo.sysjobs; msdb.dbo.sysproxies.';
EXEC #AppendLine 'Why: Surface OS-level execution and whether a proxy is used for CmdExec/PowerShell/ActiveScripting/SSIS steps.';
EXEC #AppendLine 'Gate: Requires sysadmin. SafeMode redacts names and command text.';
EXEC #AppendLine 'Notes: Returns job_name, step_id, subsystem, database_name, command_snippet, proxy_name, and flags is_risky_subsystem and risky_without_proxy so GPT/automation can highlight risky OS-level steps running without proxies.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_JobStepSecurity';
EXEC #AppendLine '';

-- 30r. Database Mail - Recent Errors (with Message Context)
EXEC #AppendLine '## 30r. Database Mail - Recent Errors (with Message Context)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: msdb.dbo.sysmail_event_log; msdb.dbo.sysmail_allitems.';
EXEC #AppendLine 'Why: Triage DB Mail failures quickly; ensures alert channels are healthy.';
EXEC #AppendLine 'Gate: Requires sysadmin. SafeMode redacts recipients/subject/description.';
EXEC #AppendLine 'Notes: Shows the last 200 error/warning events with log_date, mailitem_id, sent_status, recipients, subject, and a description snippet for troubleshooting mail issues.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_DbMailRecentErrors';
EXEC #AppendLine '';

-- 30s. msdb Housekeeping - Top Tables by Size
EXEC #AppendLine '## 30s. msdb Housekeeping - Top Tables by Size';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: msdb.sys.tables; msdb.sys.schemas; msdb.sys.indexes; msdb.sys.partitions; msdb.sys.allocation_units.';
EXEC #AppendLine 'Why: Identify msdb growth drivers (backup/mail/maintenance logs, etc.) to plan safe retention/purge.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine 'Notes: Returns table_name, row_count, and used_mb/reserved_mb/unused_mb so GPT/automation can rank msdb tables by space usage and highlight bloat drivers.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_msdbTopTables';
EXEC #AppendLine '';

-- 31a. CompilePressure SingleUseAdhoc
EXEC #AppendLine '## 31a. CompilePressure SingleUseAdhoc';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_cached_plans; sys.dm_exec_sql_text.';
EXEC #AppendLine 'Why: Measures single-use ad hoc plan volume (compile pressure indicator).';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_CompilePressure_SingleUseAdhoc'
EXEC #AppendLine ''

-- 31b. CompilePressure MultiPlanByHash
EXEC #AppendLine '## 31b. CompilePressure MultiPlanByHash';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_query_stats; sys.dm_exec_sql_text.';
EXEC #AppendLine 'Why: Highlights query_hashes compiling multiple distinct plans (compile pressure indicator).';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine 'Notes: Surfaces distinct_plans, plans_cached, total_execs per query_hash for GPT/automation to assess high-variance queries.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_CompilePressure_MultiPlanByHash'
EXEC #AppendLine ''

-- 31c. Plan Guides (Enabled/Disabled)
EXEC #AppendLine '## 31c. Plan Guides (Enabled/Disabled)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.plan_guides.';
EXEC #AppendLine 'Why: Plan guides can force plans/hints and surprise compiles; inventory with scope schema/object names and scope-type flags helps triage.';
EXEC #AppendLine 'Notes: Includes is_object_scope, is_sql_scope, is_template_scope plus scope_schema_name/scope_object_name to make joins and GPT posture analysis easier.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_PlanGuides';
EXEC #AppendLine '';

-- 31d. Plan Cache Memory Breakdown (by Cache Objtype)
EXEC #AppendLine '## 31d. Plan Cache Memory Breakdown (by Cache Objtype)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_cached_plans.';
EXEC #AppendLine 'Why: Understand memory footprint by cache object type; helps explain pressure/bloat by cacheobjtype/objtype and pct_of_cache.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine 'Notes: Includes plan_count, total_size_mb, pct_of_cache so GPT/automation can rank cache stores by memory share.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_PlanCacheBreakdown';
EXEC #AppendLine '';

-- 31e. Plan Cache Memory by Database (Mb)
EXEC #AppendLine '## 31e. Plan Cache Memory by Database (Mb)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_cached_plans; sys.dm_exec_plan_attributes';
EXEC #AppendLine 'Why: Quantify plan cache memory by DB; useful when one DB dominates or ad-hoc bloat is scoped.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_PlanCacheByDb';
EXEC #AppendLine '';

-- 32a. Index Operational Stats
EXEC #AppendLine '## 32a. Index Operational Stats';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_db_index_operational_stats; sys.indexes.';
EXEC #AppendLine 'Why: Surface leaf/page splits and lock pressure on the busiest indexes.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine 'Notes: Includes object_id, index_id, table_name, index_name and total_lock_wait_count so GPT/automation can join to other index slices and sort by lock pressure.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_Index_Operational_Stats'
EXEC #AppendLine ''

-- 32b. Index Operational Hot-Spots (Leaf Mods & Ghosts)
EXEC #AppendLine '## 32b. Index Operational Hot-Spots (Leaf Mods & Ghosts)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_db_index_operational_stats.';
EXEC #AppendLine 'Why: Highlight indexes with high leaf modifications and ghost records.';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine 'Notes: Includes object_id, index_id, TableName, index_name plus total_leaf_mods and total_ghost_and_deletes so GPT/automation can join to other index slices and quantify modification/ghost pressure.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_IndexOperationalHotspots';
EXEC #AppendLine '';

-- 33a. Deadlock Xml
EXEC #AppendLine '## 33a. Deadlock Xml';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_xe_session_targets; sys.dm_xe_sessions (system_health).';
EXEC #AppendLine 'Why: Capture recent xml_deadlock_report events for root-cause analysis.';
EXEC #AppendLine 'Gate: Requires sysadmin. SafeMode redacts deadlock_xml.';
EXEC #AppendLine 'Notes: Returns UTC timestamp plus the full deadlock XML payload (converted to NVARCHAR) for up to 10 recent events.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_Deadlock_XML'
EXEC #AppendLine ''

-- 33b. Deadlock Summary (system_health)
EXEC #AppendLine '## 33b. Deadlock Summary (system_health)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.fn_xe_file_target_read_file / system_health.';
EXEC #AppendLine 'Why: Summarize deadlocks captured by system_health (XE).';
EXEC #AppendLine 'Gate: Requires sysadmin.';
EXEC #AppendLine 'Notes: Surfaces deadlock UTC time, victimProcessId, counts of processes/resources, plus HasMultipleProcesses and HasMultipleResources flags to help GPT/automation classify more complex deadlocks.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_DeadlockSummaryXE';
EXEC #AppendLine '';

-- 34a. UDFs In Hot Queries
EXEC #AppendLine '## 34a. UDFs In Hot Queries';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_exec_query_stats; sys.dm_exec_sql_text; sys.objects.';
EXEC #AppendLine 'Why: Highlight top CPU queries that call scalar/table UDFs.';
EXEC #AppendLine 'Gate: Requires sysadmin. SafeMode redacts query snippets.';
EXEC #AppendLine 'Notes: Outputs query_hash plus UDF object id/type (with IsScalarUdf flag), UDF name, and 4K query snippets from the top 200 worker-time queries so GPT/automation can join UDF usage to other query-hash based slices and prioritize tuning.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_UDFs_In_Hot_Queries'
EXEC #AppendLine ''

-- 35a. Change Data Capture (CDC) Status
EXEC #AppendLine '## 35a. Change Data Capture (CDC) Status';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.databases (is_cdc_enabled).';
EXEC #AppendLine 'Why: Show CDC enabled/disabled at the database level (affects log growth and Agent jobs).';
EXEC #AppendLine 'Notes: Database-level posture only (no per-table capture instances) to keep SQL2008-compatible without object-existence gating.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_CDCStatus';
EXEC #AppendLine '';

-- 35b. Replication Posture (Current DB)
EXEC #AppendLine '## 35b. Replication Posture (Current DB)';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.databases (replication flags).';
EXEC #AppendLine 'Why: Show database-level replication posture flags (published/subscribed/merge/distributor).';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_ReplPublications';
EXEC #AppendLine '';

-- 36a. Identity Near Max
EXEC #AppendLine '## 36a. Identity Near Max'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.identity_columns; COLUMNPROPERTY(MAXIMUM_VALUE)'
EXEC #AppendLine 'Why: Identity columns nearing maximum values (risk of exhaustion).'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_Identity_Near_Max'
EXEC #AppendLine ''

-- 37a. Lookup / Enum Shortlist (Heuristic)
EXEC #AppendLine '## 37a. Lookup / Enum Shortlist (Heuristic)'
EXEC #AppendLine '```text'
EXEC #AppendLine 'Source: sys.tables / sys.columns / sys.types'
EXEC #AppendLine 'Source: sys.dm_db_partition_stats (row counts for heap/clustered)'
EXEC #AppendLine 'Source: sys.foreign_keys (inbound/outbound FK counts)'
EXEC #AppendLine 'Source: sys.key_constraints / sys.indexes / sys.index_columns (PK & unique Code/Name)'
EXEC #AppendLine 'Why: Metadata-only heuristic that shortlists small enum-like and lookup tables (candidates for hard-coded labels or replacing helper-view joins). No row data is read.'
EXEC #AppendLine 'Scoring (Score):'
EXEC #AppendLine '+8 if RowCount <= 20 (strict enum); else +4 if RowCount <= 200 (small lookup)'
EXEC #AppendLine '+4 if >=3 distinct parent tables reference it by FK; +2 if >=1'
EXEC #AppendLine '+2 if table name looks like a lookup (status/state/type/code/category/reason/outcome/stage/lookup/enum)'
EXEC #AppendLine '+1 if any unique index includes a column named like Code or Name'
EXEC #AppendLine '+1 if PK is single-column and int-like (tinyint/smallint/int/bigint)'
EXEC #AppendLine '+1 if total columns <= 6'
EXEC #AppendLine 'Inclusion rule:'
EXEC #AppendLine 'Score >= 12 and RowCount > 0; and either (a) NameLooksLookup = 1, or (b) shape looks lookup-ish (no LOB columns, <=1 outbound FK, <=6 columns).'
EXEC #AppendLine 'Columns:'
EXEC #AppendLine 'TableName, RowCount, InboundFKDistinctParents, NameLooksLookup, HasUniqueCode/HasUniqueName, PKCols, PKIsIntLike, LabelColGuess (first label-like column), Score'
EXEC #AppendLine '```'
EXEC #AppendCsv  '#DTR_LookupShortlist'
EXEC #AppendLine ''

-- 37b. TDE Encryption Status
EXEC #AppendLine '## 37b. TDE Encryption Status';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.dm_database_encryption_keys; master.sys.certificates (thumbprint match).';
EXEC #AppendLine 'Why: Confirms Transparent Data Encryption state and certificate ownership for the target database.';
EXEC #AppendLine 'Gate: Requires sysadmin. SafeMode redacts certificate fields.';
EXEC #AppendLine 'Notes: Columns include encryption_state (textified), key algorithm/length, and certificate thumbprint/expiry when present.';
EXEC #AppendLine '```';
EXEC #AppendCsv  '#DTR_TDEStatus';
EXEC #AppendLine '';

------------------------------------------------------------
-- Export Schema to {OutputDir}\dt_legacy_report ({TargetDB} - {Version}).md
------------------------------------------------------------
GO

IF (SELECT ExportSchema FROM #DT_Config) = 1
BEGIN
	DECLARE @ExportDatabaseName nvarchar(128) = DB_NAME()
	DECLARE @ExportServerName nvarchar(128) = CONVERT(nvarchar(128), SERVERPROPERTY('ServerName'))
	DECLARE @ExportSysDateTime nvarchar(30) = CONVERT(nvarchar(30), SYSDATETIME(), 126)

	-- Title and basic info
	EXEC #AppendLine '# Database Schema for [{%1}]', @ExportDatabaseName;
	EXEC #AppendLine '';
	EXEC #AppendLine '- **Target DB:** [{%1}]', @ExportDatabaseName;
	EXEC #AppendLine '- **Server:** {%1}', @ExportServerName;
	EXEC #AppendLine '- **Generated:** {%1}', @ExportSysDateTime;
	EXEC #AppendLine '';

	-- Modules (procedures, views, functions)
	IF OBJECT_ID('tempdb..#DTR_S_Modules') IS NOT NULL DROP TABLE #DTR_S_Modules;
	SELECT
	IDENTITY(int) AS RowNumber,
	object_type = CASE o.type
		WHEN 'P' THEN 'PROCEDURE'
		WHEN 'V' THEN 'VIEW'
		WHEN 'FN' THEN 'FUNCTION'
		WHEN 'TF' THEN 'FUNCTION'
		WHEN 'IF' THEN 'FUNCTION'
		ELSE o.type_desc
		END,
	schema_name = s.name,
	object_name = o.name,
	definition = m.definition
	INTO #DTR_S_Modules
	FROM sys.objects AS o
	JOIN sys.schemas AS s ON s.schema_id = o.schema_id
	JOIN sys.sql_modules AS m ON m.object_id = o.object_id
	WHERE o.type IN ('P','V','FN','TF','IF')
		AND OBJECTPROPERTY(o.object_id, 'IsEncrypted') = 0
	ORDER BY object_type, schema_name, object_name;

	EXEC #AppendLine '## Programmability - Module definitions';
	EXEC #AppendLine '```text';
	EXEC #AppendLine 'Source: sys.objects (P, V, FN, TF, IF); sys.sql_modules; sys.schemas';
	EXEC #AppendLine 'Why: Source code of stored procedures, views, and user-defined functions.';
	EXEC #AppendLine '```';
	EXEC #AppendSql @TableName='#DTR_S_Modules', @BodyColumn='definition', @TypeCol='object_type', @SchemaCol='schema_name', @NameCol='object_name';
	EXEC #AppendLine '';

	-- Triggers
	IF OBJECT_ID('tempdb..#DTR_S_Triggers') IS NOT NULL DROP TABLE #DTR_S_Triggers;
	SELECT
	IDENTITY(int) AS RowNumber,
	object_type = 'TRIGGER',
	schema_name = s.name,
	object_name = tr.name,
	definition = m.definition
	INTO #DTR_S_Triggers
	FROM sys.triggers AS tr
	JOIN sys.objects AS o ON o.object_id = tr.object_id
	JOIN sys.schemas AS s ON s.schema_id = o.schema_id
	JOIN sys.sql_modules AS m ON m.object_id = tr.object_id
	ORDER BY schema_name, object_name;

	EXEC #AppendLine '## Triggers - Definitions';
	EXEC #AppendLine '```text';
	EXEC #AppendLine 'Source: sys.triggers; sys.sql_modules; sys.objects; sys.schemas';
	EXEC #AppendLine 'Why: Definition of DML triggers (on tables) and DDL triggers (on database).';
	EXEC #AppendLine '```';
	EXEC #AppendSql @TableName='#DTR_S_Triggers', @BodyColumn='definition', @TypeCol='object_type', @SchemaCol='schema_name', @NameCol='object_name';
	EXEC #AppendLine '';

	-- Tables
	IF OBJECT_ID('tempdb..#DTR_S_Tables') IS NOT NULL DROP TABLE #DTR_S_Tables;
	SELECT
	IDENTITY(int) AS RowNumber,
	schema_name = s.name,
	table_name = t.name,
	object_id = t.object_id
	INTO #DTR_S_Tables
	FROM sys.tables AS t
	JOIN sys.schemas AS s ON s.schema_id = t.schema_id
	WHERE t.is_ms_shipped = 0
	ORDER BY schema_name, table_name;

	EXEC #AppendLine '## Tables';
	EXEC #AppendLine '```text';
	EXEC #AppendLine 'Source: sys.tables; sys.schemas';
	EXEC #AppendLine 'Why: All user tables.';
	EXEC #AppendLine '```';
	EXEC #AppendCsv  '#DTR_S_Tables';
	EXEC #AppendLine '';

	-- Columns
	IF OBJECT_ID('tempdb..#DTR_S_Columns') IS NOT NULL DROP TABLE #DTR_S_Columns;
	SELECT
	IDENTITY(int) AS RowNumber,
	schema_name = s.name,
	table_name = t.name,
	column_name = c.name,
	ordinal_position = c.column_id,
	data_type = TYPE_NAME(c.user_type_id),
	max_length = c.max_length,
	precision = c.precision,
	scale = c.scale,
	is_nullable = c.is_nullable,
	is_identity = c.is_identity,
	is_computed = c.is_computed,
	is_persisted = cc.is_persisted
	INTO #DTR_S_Columns
	FROM sys.tables AS t
	JOIN sys.schemas AS s ON s.schema_id = t.schema_id
	JOIN sys.columns AS c ON c.object_id = t.object_id
	LEFT JOIN sys.computed_columns AS cc ON cc.object_id = c.object_id AND cc.column_id = c.column_id
	WHERE t.is_ms_shipped = 0
	ORDER BY s.name, t.name, c.column_id;

	EXEC #AppendLine '## Columns';
	EXEC #AppendLine '```text';
	EXEC #AppendLine 'Source: sys.columns; sys.tables; sys.computed_columns; sys.schemas';
	EXEC #AppendLine 'Why: Columns of user tables with data types and properties (nullability, identity, computed).';
	EXEC #AppendLine '```';
	EXEC #AppendCsv  '#DTR_S_Columns';
	EXEC #AppendLine '';

	-- Types (user-defined)
	IF OBJECT_ID('tempdb..#DTR_S_Types') IS NOT NULL DROP TABLE #DTR_S_Types;
	SELECT
	IDENTITY(int) AS RowNumber,
	schema_name = s.name,
	type_name = t.name,
	base_system_type = TYPE_NAME(t.system_type_id),
	max_length = t.max_length,
	precision = t.precision,
	scale = t.scale,
	is_table_type = t.is_table_type,
	is_assembly_type = t.is_assembly_type,
	assembly_name = asm.name
	INTO #DTR_S_Types
	FROM sys.types AS t
	JOIN sys.schemas AS s ON s.schema_id = t.schema_id
	LEFT JOIN sys.assembly_types AS at ON at.user_type_id = t.user_type_id AND t.is_assembly_type = 1
	LEFT JOIN sys.assemblies AS asm ON asm.assembly_id = at.assembly_id AND t.is_assembly_type = 1
	WHERE t.is_user_defined = 1
	ORDER BY s.name, t.name;

	EXEC #AppendLine '## Types (User-Defined)';
	EXEC #AppendLine '```text';
	EXEC #AppendLine 'Source: sys.types; sys.schemas; sys.assemblies';
	EXEC #AppendLine 'Why: User-defined types with base system type and CLR/TVP flags.';
	EXEC #AppendLine '```';
	EXEC #AppendCsv  '#DTR_S_Types';
	EXEC #AppendLine '';

	-- Table types
	IF OBJECT_ID('tempdb..#DTR_S_TableTypes') IS NOT NULL DROP TABLE #DTR_S_TableTypes;
	SELECT
	IDENTITY(int) AS RowNumber,
	schema_name = s.name,
	table_type_name = tt.name
	INTO #DTR_S_TableTypes
	FROM sys.table_types AS tt
	JOIN sys.schemas AS s ON s.schema_id = tt.schema_id
	WHERE tt.is_user_defined = 1
	ORDER BY s.name, tt.name;

	EXEC #AppendLine '## Table Types';
	EXEC #AppendLine '```text';
	EXEC #AppendLine 'Source: sys.table_types; sys.schemas';
	EXEC #AppendLine 'Why: Table-valued parameter types and their properties.';
	EXEC #AppendLine '```';
	EXEC #AppendCsv  '#DTR_S_TableTypes';
	EXEC #AppendLine '';

	-- Table type columns
	IF OBJECT_ID('tempdb..#DTR_S_TableTypeColumns') IS NOT NULL DROP TABLE #DTR_S_TableTypeColumns;
	SELECT
	IDENTITY(int) AS RowNumber,
	schema_name = s.name,
	table_type_name = tt.name,
	column_name = c.name,
	column_id = c.column_id,
	data_type = TYPE_NAME(c.user_type_id),
	max_length = c.max_length,
	precision = c.precision,
	scale = c.scale,
	is_nullable = c.is_nullable,
	is_identity = c.is_identity,
	is_computed = c.is_computed,
	is_persisted = cc.is_persisted
	INTO #DTR_S_TableTypeColumns
	FROM sys.table_types AS tt
	JOIN sys.schemas AS s ON s.schema_id = tt.schema_id
	JOIN sys.columns AS c ON c.object_id = tt.type_table_object_id
	LEFT JOIN sys.computed_columns AS cc ON cc.object_id = c.object_id AND cc.column_id = c.column_id
	WHERE tt.is_user_defined = 1
	ORDER BY s.name, tt.name, c.column_id;

	EXEC #AppendLine '## Table Type Columns';
	EXEC #AppendLine '```text';
	EXEC #AppendLine 'Source: sys.table_types; sys.columns; sys.computed_columns; sys.schemas';
	EXEC #AppendLine 'Why: Columns of user-defined table types (TVPs) with data types and properties.';
	EXEC #AppendLine '```';
	EXEC #AppendCsv  '#DTR_S_TableTypeColumns';
	EXEC #AppendLine '';

	-- Constraints (PK, UNIQUE, FK, CHECK, DEFAULT)
	IF OBJECT_ID('tempdb..#DTR_S_Constraints') IS NOT NULL DROP TABLE #DTR_S_Constraints;
	SELECT
	IDENTITY(int) AS RowNumber,
	schema_name = s.name,
	table_name = t.name,
	constraint_name = kc.name,
	constraint_type = CASE kc.type
		WHEN 'PK' THEN 'PRIMARY KEY'
		WHEN 'UQ' THEN 'UNIQUE'
		WHEN 'F' THEN 'FOREIGN KEY'
		WHEN 'C' THEN 'CHECK'
		WHEN 'D' THEN 'DEFAULT'
		ELSE kc.type_desc
		END
	INTO #DTR_S_Constraints
	FROM sys.objects AS t
	JOIN sys.schemas AS s ON s.schema_id = t.schema_id
	JOIN sys.objects AS kc ON kc.parent_object_id = t.object_id
	WHERE t.type = 'U'
		AND kc.type IN ('PK','UQ','F','C','D')
	ORDER BY s.name, t.name, constraint_type, kc.name;

	EXEC #AppendLine '## Constraints - Inventory';
	EXEC #AppendLine '```text';
	EXEC #AppendLine 'Source: sys.objects (constraints by type PK, UQ, F, C, D); sys.schemas';
	EXEC #AppendLine 'Why: Inventory of primary key, unique, foreign key, check, and default constraints on tables.';
	EXEC #AppendLine '```';
	EXEC #AppendCsv  '#DTR_S_Constraints';
	EXEC #AppendLine '';

	-- Check constraint definitions
	IF OBJECT_ID('tempdb..#DTR_S_CheckConstraintDefs') IS NOT NULL DROP TABLE #DTR_S_CheckConstraintDefs;
	SELECT
	IDENTITY(int) AS RowNumber,
	object_type = 'CHECK CONSTRAINT',
	schema_name = OBJECT_SCHEMA_NAME(c.parent_object_id),
	object_name = c.name,
	definition = c.definition
	INTO #DTR_S_CheckConstraintDefs
	FROM sys.check_constraints AS c
	ORDER BY schema_name, object_name;

	EXEC #AppendLine '## Constraint definitions - CHECK';
	EXEC #AppendLine '```text';
	EXEC #AppendLine 'Source: sys.check_constraints';
	EXEC #AppendLine 'Why: Definition of CHECK constraint expressions.';
	EXEC #AppendLine '```';
	EXEC #AppendSql @TableName='#DTR_S_CheckConstraintDefs', @BodyColumn='definition', @TypeCol='object_type', @SchemaCol='schema_name', @NameCol='object_name';
	EXEC #AppendLine '';

	-- Default constraint definitions
	IF OBJECT_ID('tempdb..#DTR_S_DefaultConstraintDefs') IS NOT NULL DROP TABLE #DTR_S_DefaultConstraintDefs;
	SELECT
	IDENTITY(int) AS RowNumber,
	object_type = 'DEFAULT CONSTRAINT',
	schema_name = OBJECT_SCHEMA_NAME(d.parent_object_id),
	object_name = d.name,
	definition = d.definition
	INTO #DTR_S_DefaultConstraintDefs
	FROM sys.default_constraints AS d
	ORDER BY schema_name, object_name;

	EXEC #AppendLine '## Constraint definitions - DEFAULT';
	EXEC #AppendLine '```text';
	EXEC #AppendLine 'Source: sys.default_constraints';
	EXEC #AppendLine 'Why: Definition of DEFAULT constraint expressions.';
	EXEC #AppendLine '```';
	EXEC #AppendSql @TableName='#DTR_S_DefaultConstraintDefs', @BodyColumn='definition', @TypeCol='object_type', @SchemaCol='schema_name', @NameCol='object_name';
	EXEC #AppendLine '';

	-- Computed column definitions
	IF OBJECT_ID('tempdb..#DTR_S_ComputedColumnDefs') IS NOT NULL DROP TABLE #DTR_S_ComputedColumnDefs;
	SELECT
	IDENTITY(int) AS RowNumber,
	object_type = 'COLUMN COMPUTED',
	schema_name = s.name,
	object_name = t.name,
	name_path = t.name + '].[' + c.name,
	definition = cc.definition
	INTO #DTR_S_ComputedColumnDefs
	FROM sys.tables AS t
	JOIN sys.schemas AS s ON s.schema_id = t.schema_id
	JOIN sys.columns AS c ON c.object_id = t.object_id
	JOIN sys.computed_columns AS cc ON cc.object_id = c.object_id AND cc.column_id = c.column_id
	ORDER BY s.name, t.name, name_path;

	EXEC #AppendLine '## Computed columns - Expressions';
	EXEC #AppendLine '```text';
	EXEC #AppendLine 'Source: sys.computed_columns; sys.columns; sys.tables; sys.schemas';
	EXEC #AppendLine 'Why: Definitions of computed column expressions.';
	EXEC #AppendLine '```';
	EXEC #AppendSql @TableName='#DTR_S_ComputedColumnDefs', @BodyColumn='definition', @TypeCol='object_type', @SchemaCol='schema_name', @NameCol='name_path';
	EXEC #AppendLine '';

	-- Indexes
	IF OBJECT_ID('tempdb..#DTR_S_Indexes') IS NOT NULL DROP TABLE #DTR_S_Indexes;
	SELECT
	IDENTITY(int) AS RowNumber,
	schema_name = s.name,
	table_name = t.name,
	index_name = i.name,
	index_type_desc = i.type_desc,
	is_unique = i.is_unique,
	is_primary_key = i.is_primary_key,
	is_unique_constraint = i.is_unique_constraint,
	has_filter = i.has_filter,
	filter_definition = i.filter_definition
	INTO #DTR_S_Indexes
	FROM sys.indexes AS i
	JOIN sys.tables AS t ON t.object_id = i.object_id
	JOIN sys.schemas AS s ON s.schema_id = t.schema_id
	WHERE i.index_id > 0
		AND t.is_ms_shipped = 0
	ORDER BY s.name, t.name, i.name;

	EXEC #AppendLine '## Indexes';
	EXEC #AppendLine '```text';
	EXEC #AppendLine 'Source: sys.indexes; sys.tables; sys.schemas';
	EXEC #AppendLine 'Why: Indexes on user tables with type, uniqueness, and filter (if any).';
	EXEC #AppendLine '```';
	EXEC #AppendCsv  '#DTR_S_Indexes';
	EXEC #AppendLine '';

	-- Index columns (keys and included columns)
	IF OBJECT_ID('tempdb..#DTR_S_IndexColumns') IS NOT NULL DROP TABLE #DTR_S_IndexColumns;
	SELECT
	IDENTITY(int) AS RowNumber,
	schema_name = s.name,
	table_name = t.name,
	index_name = i.name,
	column_name = c.name,
	is_included = ic.is_included_column,
	key_ordinal = ic.key_ordinal,
	is_descending = ic.is_descending_key
	INTO #DTR_S_IndexColumns
	FROM sys.indexes AS i
	JOIN sys.index_columns AS ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
	JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
	JOIN sys.tables AS t ON t.object_id = i.object_id
	JOIN sys.schemas AS s ON s.schema_id = t.schema_id
	WHERE i.index_id > 0
		AND t.is_ms_shipped = 0
	ORDER BY
		s.name,
		t.name,
		i.name,
		CASE WHEN ic.is_included_column = 1 THEN 1 ELSE 0 END,
		ic.key_ordinal,
		c.name;

	EXEC #AppendLine '## Index Columns';
	EXEC #AppendLine '```text';
	EXEC #AppendLine 'Source: sys.index_columns; sys.columns; sys.indexes; sys.tables; sys.schemas';
	EXEC #AppendLine 'Why: Index key columns and included columns with order and sort direction.';
	EXEC #AppendLine '```';
	EXEC #AppendCsv  '#DTR_S_IndexColumns';
	EXEC #AppendLine '';

	-- Foreign Keys
	IF OBJECT_ID('tempdb..#DTR_S_ForeignKeys') IS NOT NULL DROP TABLE #DTR_S_ForeignKeys;
	SELECT
	IDENTITY(int) AS RowNumber,
	schema_name = sp.name,
	table_name = p.name,
	constraint_name = fk.name,
	referenced_schema_name = sr.name,
	referenced_table_name = r.name,
	delete_referential_action_desc = fk.delete_referential_action_desc,
	update_referential_action_desc = fk.update_referential_action_desc,
	column_names = STUFF((
		SELECT
		', ' + c_p.name
		FROM sys.foreign_key_columns AS fkc
		JOIN sys.columns AS c_p ON c_p.object_id = fkc.parent_object_id AND c_p.column_id = fkc.parent_column_id
		WHERE fkc.constraint_object_id = fk.object_id
		ORDER BY fkc.constraint_column_id
		FOR XML PATH(''), TYPE
		).value('(text())[1]','nvarchar(max)'), 1, 2, ''),
	referenced_column_names = STUFF((
		SELECT
		', ' + c_r.name
		FROM sys.foreign_key_columns AS fkc
		JOIN sys.columns AS c_r ON c_r.object_id = fkc.referenced_object_id AND c_r.column_id = fkc.referenced_column_id
		WHERE fkc.constraint_object_id = fk.object_id
		ORDER BY fkc.constraint_column_id
		FOR XML PATH(''), TYPE
		).value('(text())[1]','nvarchar(max)'), 1, 2, '')
	INTO #DTR_S_ForeignKeys
	FROM sys.foreign_keys AS fk
	JOIN sys.tables AS p ON p.object_id = fk.parent_object_id
	JOIN sys.schemas AS sp ON sp.schema_id = p.schema_id
	JOIN sys.tables AS r ON r.object_id = fk.referenced_object_id
	JOIN sys.schemas AS sr ON sr.schema_id = r.schema_id
	WHERE p.is_ms_shipped = 0
	ORDER BY sp.name, p.name, fk.name;

	EXEC #AppendLine '## Foreign Keys';
	EXEC #AppendLine '```text';
	EXEC #AppendLine 'Source: sys.foreign_keys; sys.foreign_key_columns; sys.tables; sys.columns; sys.schemas';
	EXEC #AppendLine 'Why: Foreign key constraints with referencing and referenced columns and actions.';
	EXEC #AppendLine '```';
	EXEC #AppendCsv  '#DTR_S_ForeignKeys';
	EXEC #AppendLine '';

	-- Synonyms
	IF OBJECT_ID('tempdb..#DTR_S_Synonyms') IS NOT NULL DROP TABLE #DTR_S_Synonyms;
	SELECT
	IDENTITY(int) AS RowNumber,
	schema_name = s.name,
	synonym_name = syn.name,
	base_object_name = syn.base_object_name
	INTO #DTR_S_Synonyms
	FROM sys.synonyms AS syn
	JOIN sys.schemas AS s ON s.schema_id = syn.schema_id
	ORDER BY s.name, syn.name;

	EXEC #AppendLine '## Synonyms';
	EXEC #AppendLine '```text';
	EXEC #AppendLine 'Source: sys.synonyms; sys.schemas';
	EXEC #AppendLine 'Why: Synonyms and their referenced base object (which can be in other databases/servers).';
	EXEC #AppendLine '```';
	EXEC #AppendCsv  '#DTR_S_Synonyms';
	EXEC #AppendLine '';

	-- Partition functions (with boundaries)
	IF OBJECT_ID('tempdb..#DTR_S_PartitionFunctions') IS NOT NULL DROP TABLE #DTR_S_PartitionFunctions;
	SELECT
	IDENTITY(int) AS RowNumber,
	function_name = pf.name,
	boundary_type = CASE
		WHEN pf.boundary_value_on_right = 1 THEN 'RIGHT'
		ELSE 'LEFT'
		END,
	data_type = TYPE_NAME(pp.user_type_id),
	max_length = pp.max_length,
	precision = pp.precision,
	scale = pp.scale,
	boundary_values = STUFF((
		SELECT
		', ' + CONVERT(nvarchar(4000), prv.value)
		FROM sys.partition_range_values AS prv
		WHERE prv.function_id = pf.function_id
		ORDER BY prv.boundary_id
		FOR XML PATH(''), TYPE
		).value('(text())[1]','nvarchar(max)'), 1, 2, ''),
	partition_count = pf.fanout
	INTO #DTR_S_PartitionFunctions
	FROM sys.partition_functions AS pf
	JOIN sys.partition_parameters AS pp ON pp.function_id = pf.function_id
	ORDER BY pf.name;

	EXEC #AppendLine '## Partition Functions';
	EXEC #AppendLine '```text';
	EXEC #AppendLine 'Source: sys.partition_functions; sys.partition_parameters; sys.partition_range_values';
	EXEC #AppendLine 'Why: Boundary type, data type, boundary values, and partition count for partition functions.';
	EXEC #AppendLine '```';
	EXEC #AppendCsv  '#DTR_S_PartitionFunctions';
	EXEC #AppendLine '';

	-- Partition schemes
	IF OBJECT_ID('tempdb..#DTR_S_PartitionSchemes') IS NOT NULL DROP TABLE #DTR_S_PartitionSchemes;
	SELECT
	IDENTITY(int) AS RowNumber,
	scheme_name = ps.name,
	function_name = pf.name,
	filegroups = STUFF((
		SELECT
		', ' + fg.name
		FROM sys.destination_data_spaces AS dds
		JOIN sys.filegroups AS fg ON fg.data_space_id = dds.data_space_id
		WHERE dds.partition_scheme_id = ps.data_space_id
		ORDER BY dds.destination_id
		FOR XML PATH(''), TYPE
		).value('(text())[1]','nvarchar(max)'), 1, 2, '')
	INTO #DTR_S_PartitionSchemes
	FROM sys.partition_schemes AS ps
	JOIN sys.partition_functions AS pf ON pf.function_id = ps.function_id
	ORDER BY ps.name;

	EXEC #AppendLine '## Partition Schemes';
	EXEC #AppendLine '```text';
	EXEC #AppendLine 'Source: sys.partition_schemes; sys.partition_functions; sys.destination_data_spaces; sys.filegroups';
	EXEC #AppendLine 'Why: Mapping of partition schemes to partition functions and filegroups.';
	EXEC #AppendLine '```';
	EXEC #AppendCsv  '#DTR_S_PartitionSchemes';
	EXEC #AppendLine '';

	-- Table Partitions
	IF OBJECT_ID('tempdb..#DTR_S_TablePartitions') IS NOT NULL DROP TABLE #DTR_S_TablePartitions;
	SELECT
	IDENTITY(int) AS RowNumber,
	schema_name = s.name,
	table_name = t.name,
	index_name = ISNULL(i.name, '(HEAP)'),
	partition_scheme_name = ps.name,
	partition_function_name = pf.name,
	partition_column = (
		SELECT
		c.name
		FROM sys.index_columns AS ic2
		JOIN sys.columns AS c ON c.object_id = ic2.object_id AND c.column_id = ic2.column_id
		WHERE ic2.object_id = i.object_id
			AND ic2.index_id = i.index_id
			AND ic2.partition_ordinal = 1
	),
	partition_count = (
		SELECT
		COUNT(*)
		FROM sys.partitions AS p
		WHERE p.object_id = i.object_id
			AND p.index_id = i.index_id
	)
	INTO #DTR_S_TablePartitions
	FROM sys.indexes AS i
	JOIN sys.partition_schemes AS ps ON i.data_space_id = ps.data_space_id
	JOIN sys.partition_functions AS pf ON pf.function_id = ps.function_id
	JOIN sys.tables AS t ON t.object_id = i.object_id
	JOIN sys.schemas AS s ON s.schema_id = t.schema_id
	WHERE t.is_ms_shipped = 0
	ORDER BY s.name, t.name, ISNULL(i.name, '(HEAP)');

	EXEC #AppendLine '## Table Partitions';
	EXEC #AppendLine '```text';
	EXEC #AppendLine 'Source: sys.indexes (joined to sys.partition_schemes/functions); sys.partitions; sys.columns; sys.tables; sys.schemas';
	EXEC #AppendLine 'Why: Partitioned tables and indexes with their partition scheme, function, key, and number of partitions.';
	EXEC #AppendLine '```';
	EXEC #AppendCsv  '#DTR_S_TablePartitions';
	EXEC #AppendLine '';

	-- Full-Text Indexes
	IF OBJECT_ID('tempdb..#DTR_S_FullText') IS NOT NULL DROP TABLE #DTR_S_FullText;
	SELECT
	IDENTITY(int) AS RowNumber,
	schema_name = s.name,
	table_name = t.name,
	fulltext_catalog_name = ftc.name,
	unique_index_name = ui.name,
	change_tracking_desc = CASE fti.change_tracking_state
		WHEN 0 THEN 'OFF'
		WHEN 1 THEN 'MANUAL'
		WHEN 2 THEN 'AUTO'
		END,
	column_names = STUFF((
		SELECT
		', ' + c.name
		FROM sys.fulltext_index_columns AS fic
		JOIN sys.columns AS c ON c.object_id = fic.object_id AND c.column_id = fic.column_id
		WHERE fic.object_id = t.object_id
		ORDER BY c.name
		FOR XML PATH(''), TYPE
		).value('(text())[1]','nvarchar(max)'), 1, 2, '')
	INTO #DTR_S_FullText
	FROM sys.fulltext_indexes AS fti
	JOIN sys.tables AS t ON t.object_id = fti.object_id
	JOIN sys.schemas AS s ON s.schema_id = t.schema_id
	LEFT JOIN sys.fulltext_catalogs AS ftc ON ftc.fulltext_catalog_id = fti.fulltext_catalog_id
	LEFT JOIN sys.indexes AS ui ON ui.object_id = t.object_id AND ui.index_id = fti.unique_index_id
	WHERE t.is_ms_shipped = 0
	ORDER BY s.name, t.name;

	EXEC #AppendLine '## Full-Text Indexes';
	EXEC #AppendLine '```text';
	EXEC #AppendLine 'Source: sys.fulltext_indexes; sys.fulltext_catalogs; sys.fulltext_index_columns; sys.indexes; sys.tables; sys.schemas';
	EXEC #AppendLine 'Why: Full-text indexes with unique key index, catalog name, change tracking mode, and indexed columns.';
	EXEC #AppendLine '```';
	EXEC #AppendCsv  '#DTR_S_FullText';
	EXEC #AppendLine '';

	-- XML Schema Collections
	IF OBJECT_ID('tempdb..#DTR_S_XmlSchemaCollections') IS NOT NULL DROP TABLE #DTR_S_XmlSchemaCollections;
	SELECT
	IDENTITY(int) AS RowNumber,
	schema_name = s.name,
	xml_collection_name = x.name
	INTO #DTR_S_XmlSchemaCollections
	FROM sys.xml_schema_collections AS x
	JOIN sys.schemas AS s ON s.schema_id = x.schema_id
	ORDER BY s.name, x.name;

	EXEC #AppendLine '## XML Schema Collections';
	EXEC #AppendLine '```text';
	EXEC #AppendLine 'Source: sys.xml_schema_collections; sys.schemas';
	EXEC #AppendLine 'Why: XML schema collections created in the database.';
	EXEC #AppendLine '```';
	EXEC #AppendCsv  '#DTR_S_XmlSchemaCollections';
	EXEC #AppendLine '';

	-- Assemblies (CLR)
	IF OBJECT_ID('tempdb..#DTR_S_Assemblies') IS NOT NULL DROP TABLE #DTR_S_Assemblies;
	SELECT
	IDENTITY(int) AS RowNumber,
	assembly_name = a.name,
	clr_name = a.clr_name,
	permission_set = a.permission_set_desc,
	is_visible = a.is_visible,
	create_date = a.create_date,
	modify_date = a.modify_date,
	is_user_defined = a.is_user_defined,
	file_count = (
		SELECT
		COUNT(*)
		FROM sys.assembly_files AS af
		WHERE af.assembly_id = a.assembly_id
	)
	INTO #DTR_S_Assemblies
	FROM sys.assemblies AS a
	WHERE a.is_user_defined = 1
	ORDER BY a.name;

	EXEC #AppendLine '## Assemblies';
	EXEC #AppendLine '```text';
	EXEC #AppendLine 'Source: sys.assemblies; sys.assembly_files';
	EXEC #AppendLine 'Why: CLR assemblies loaded in the database with visibility flags, dates, and file count.';
	EXEC #AppendLine '```';
	EXEC #AppendCsv  '#DTR_S_Assemblies';
	EXEC #AppendLine '';

	-- Extended Properties
	IF OBJECT_ID('tempdb..#DTR_S_ExtendedProperties') IS NOT NULL DROP TABLE #DTR_S_ExtendedProperties;
	SELECT
	IDENTITY(int) AS RowNumber,
	class_desc = ep.class_desc,
	schema_name = COALESCE(sc_obj.name, sc_schema.name, sc_type.name, NULL),
	object_name = CASE
		WHEN ep.class_desc = 'DATABASE' THEN DB_NAME()
		WHEN ep.class_desc = 'SCHEMA' THEN sc_schema.name
		WHEN ep.class_desc = 'DATABASE_PRINCIPAL' THEN dp.name
		WHEN ep.class_desc = 'TYPE' THEN typ.name
		ELSE o.name
		END,
	subobject_name = CASE
		WHEN ep.class_desc = 'OBJECT_OR_COLUMN' AND ep.minor_id <> 0 THEN col.name
		WHEN ep.class_desc = 'PARAMETER' THEN '@' + par.name
		WHEN ep.class_desc = 'INDEX' THEN ix.name
		ELSE NULL
		END,
	property_name = ep.name,
	property_value = CONVERT(nvarchar(max), ep.value)
	INTO #DTR_S_ExtendedProperties
	FROM sys.extended_properties AS ep
	LEFT JOIN sys.objects AS o ON ep.class IN (1,2,7) AND ep.major_id = o.object_id
	LEFT JOIN sys.columns AS col ON ep.class = 1 AND ep.minor_id <> 0 AND col.object_id = o.object_id AND col.column_id = ep.minor_id
	LEFT JOIN sys.parameters AS par ON ep.class = 2 AND par.object_id = o.object_id AND par.parameter_id = ep.minor_id
	LEFT JOIN sys.indexes AS ix ON ep.class = 7 AND ix.object_id = o.object_id AND ix.index_id = ep.minor_id
	LEFT JOIN sys.schemas AS sc_obj ON ep.class IN (1,2,7) AND o.schema_id = sc_obj.schema_id
	LEFT JOIN sys.schemas AS sc_schema ON ep.class = 3 AND ep.major_id = sc_schema.schema_id
	LEFT JOIN sys.database_principals AS dp ON ep.class = 4 AND ep.major_id = dp.principal_id
	LEFT JOIN sys.types AS typ ON ep.class = 6 AND ep.major_id = typ.user_type_id
	LEFT JOIN sys.schemas AS sc_type ON ep.class = 6 AND typ.schema_id = sc_type.schema_id
	WHERE ep.class_desc NOT IN ('DATABASE','DATABASE_PRINCIPAL','TYPE','SCHEMA')
		OR (ep.class_desc IN ('DATABASE','DATABASE_PRINCIPAL','TYPE','SCHEMA') AND ep.major_id IS NOT NULL)
	ORDER BY
		CASE WHEN ep.class IN (2,7) THEN 1 ELSE ep.class END,
		COALESCE(sc_obj.name, sc_schema.name, sc_type.name, dp.name, ''),
		CASE
			WHEN ep.class = 1 AND ep.minor_id <> 0 THEN 1
			WHEN ep.class = 2 THEN 1
			WHEN ep.class = 7 THEN 1
			ELSE 0
			END,
		CASE
			WHEN ep.class_desc = 'DATABASE' THEN DB_NAME()
			WHEN ep.class_desc = 'SCHEMA' THEN sc_schema.name
			WHEN ep.class_desc = 'DATABASE_PRINCIPAL' THEN dp.name
			WHEN ep.class_desc = 'TYPE' THEN typ.name
			ELSE o.name
			END,
		CASE
			WHEN ep.class = 1 AND ep.minor_id <> 0 THEN col.name
			WHEN ep.class = 2 THEN par.name
			WHEN ep.class = 7 THEN ix.name
			ELSE ''
			END,
		ep.name;

	EXEC #AppendLine '## Extended Properties';
	EXEC #AppendLine '```text';
	EXEC #AppendLine 'Source: sys.extended_properties; sys.objects / sys.columns / sys.parameters / sys.indexes; sys.schemas / sys.database_principals / sys.types';
	EXEC #AppendLine 'Why: Extended properties (e.g., descriptive comments) defined on various objects.';
	EXEC #AppendLine '```';
	EXEC #AppendCsv  '#DTR_S_ExtendedProperties';
	EXEC #AppendLine '';
END

GO
:OUT STDOUT

------------------------------------------------------------
-- Complete
------------------------------------------------------------
PRINT 'Database Tuner Legacy Complete'
GO

PRINT ''
PRINT 'Markdown Saved: $(OutputDir)\dt_legacy_report ($(TargetDB) - $(Version)).md'
GO


