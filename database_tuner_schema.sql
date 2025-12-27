------------------------------------------------------------
-- Database Tuner Schema v0.01
------------------------------------------------------------
-- Description:
--   Exports a read-only Markdown schema snapshot of a database.
--   Output includes tables, columns, keys, constraints, indexes, and programmable objects.
-- Requirements:
--   SQL Server 2008 / 2008 R2 (compat 100) minimum (not tested with Azure SQL Database).
-- Usage:
--   1) SSMS -> Query -> SQLCMD Mode (enable)
--   2) Update "User Config" section below (ensure OutputDir exists)
--   3) Run and monitor progress from "Messages" tab
--   4) Collect generated file from OutputDir (filename: dt_schema_report ({TargetDB} - {Version}).md)
-- Notes:
--   - Ensure OutputDir exists otherwise output will be printed to the SSMS console instead of to file.
--   - Safe Mode redacts some text output (uses [Redacted] markers).
--   - No database changes are made by this script aside from temp tables which are discarded when the query closes.
--   - Author: Dean Lafferty (laffo16@hotmail.com)

------------------------------------------------------------
-- User Config
------------------------------------------------------------
:SETVAR TargetDB "DatabaseName"
:SETVAR OutputDir "C:\Temp\DatabaseTuner\"
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
	RAISERROR('Database Tuner Schema requires SQL Server 2008 / 2008 R2 (10.x) minimum.', 16, 1);
ELSE IF @CompatLevel < 100
	RAISERROR('Database Tuner Schema requires Database Compatibility Level 100 or higher.', 16, 1);
GO

------------------------------------------------------------
-- Initialisation
------------------------------------------------------------
USE $(TargetDB)

SET NOCOUNT ON
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
SET LOCK_TIMEOUT 15000

DECLARE @ProductVersion varchar(30) = CONVERT(varchar(30), SERVERPROPERTY('ProductVersion'));
DECLARE @ProductMajorVersion int = CONVERT(int, PARSENAME(@ProductVersion, 4));
DECLARE @CompatLevel int = (SELECT compatibility_level FROM sys.databases WHERE name = '$(TargetDB)');
DECLARE @SafeMode bit = CASE WHEN '$(SafeMode)' = '1' THEN 1 ELSE 0 END;

IF OBJECT_ID('tempdb..#DT_Config') IS NOT NULL DROP TABLE #DT_Config;
CREATE TABLE #DT_Config(
	ProductMajorVersion int NOT NULL,
	ProductVersion varchar(30) NOT NULL,
	CompatLevel int NOT NULL,
	SafeMode bit NOT NULL
);

INSERT INTO #DT_Config(ProductMajorVersion, ProductVersion, CompatLevel, SafeMode)
VALUES (@ProductMajorVersion, @ProductVersion, @CompatLevel, @SafeMode);

PRINT 'Database Tuner Schema $(Version)'
GO

------------------------------------------------------------
-- #AppendLine - used to print a single line with handling for over 4000 characters
------------------------------------------------------------
IF OBJECT_ID('tempdb..#AppendLine') IS NOT NULL DROP PROCEDURE #AppendLine;
GO
CREATE PROCEDURE #AppendLine
	@s nvarchar(max),
	@p1 nvarchar(max) = NULL,
	@p2 nvarchar(max) = NULL,
	@p3 nvarchar(max) = NULL,
	@p4 nvarchar(max) = NULL,
	@p5 nvarchar(max) = NULL,
	@p6 nvarchar(max) = NULL,
	@p7 nvarchar(max) = NULL,
	@p8 nvarchar(max) = NULL
AS
BEGIN
	IF @s IS NULL SET @s = '';

	-- Base text normalized to DB default
	DECLARE @t nvarchar(max) = @s COLLATE DATABASE_DEFAULT;

	-- Token→value map (only non-NULL values kept), all collated the same
	DECLARE @kv TABLE(
		token nvarchar(10) COLLATE DATABASE_DEFAULT,
		val nvarchar(max) COLLATE DATABASE_DEFAULT
	);

	INSERT INTO @kv(token, val)
	SELECT
	v.token COLLATE DATABASE_DEFAULT,
	v.val COLLATE DATABASE_DEFAULT
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
	DECLARE @i int = 1, @chunk int = 4000, @len int = DATALENGTH(@t) / 2;
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
	IF EXISTS (SELECT 1 FROM tempdb.sys.columns WHERE object_id = @objid AND name = 'RowNumber')
	SET @OrderBy = '[RowNumber]';

	EXEC #AppendLine '```csv';

	-- Build header deterministically (one line), COLLATE everything to DB default
	DECLARE @header nvarchar(max) = '';
	SELECT
	@header =
		@header
		+ CASE
			WHEN @header COLLATE DATABASE_DEFAULT = '' COLLATE DATABASE_DEFAULT THEN '' COLLATE DATABASE_DEFAULT
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

	SELECT
	@colExpr =
		STUFF((
			SELECT
			@delim +
				'(NCHAR(34) + '
				+ 'REPLACE(REPLACE(REPLACE('
				+ 'COALESCE(CONVERT(nvarchar(max),' + QUOTENAME(c.name) + ') COLLATE DATABASE_DEFAULT, ''''), '
				+ 'NCHAR(34), NCHAR(34)+NCHAR(34)), CHAR(13), ''''), CHAR(10), '''')'
				+ ' + NCHAR(34))'
			FROM tempdb.sys.columns AS c
			WHERE c.object_id = @objid
			ORDER BY c.column_id
			FOR XML PATH(''), TYPE).value('.', 'nvarchar(max)'),
			1,
			LEN(@delim),
			''
		);

	DECLARE @sql nvarchar(max) = 
	'SELECT ' + @colExpr + ' AS csv_line FROM ' + QUOTENAME(@TableName)
		+ CASE WHEN @OrderBy IS NOT NULL THEN ' ORDER BY ' + @OrderBy ELSE '' END;

	IF OBJECT_ID('tempdb..#csv_buffer') IS NOT NULL DROP TABLE #csv_buffer;
	CREATE TABLE #csv_buffer (id int IDENTITY(1, 1) PRIMARY KEY, csv_line nvarchar(max));

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
	@TableName sysname, -- e.g. '#DTR_Modules'
	@BodyColumn sysname, -- e.g. 'definition'
	@TypeCol sysname = NULL, -- type label column (or NULL to omit)
	@SchemaCol sysname = NULL, -- schema column (or NULL)
	@NameCol sysname = NULL -- name/path column (or NULL)
AS
BEGIN
	DECLARE @CfgSafeMode bit = 0;
	IF OBJECT_ID('tempdb..#DT_Config') IS NOT NULL
	BEGIN
		SELECT TOP (1)
		@CfgSafeMode = SafeMode
		FROM #DT_Config;
	END

	DECLARE @objid int = OBJECT_ID('tempdb..' + @TableName)
	IF @objid IS NULL RETURN

	-- Validate body column exists
	IF NOT EXISTS (SELECT 1 FROM tempdb.sys.columns WHERE object_id = @objid AND name = @BodyColumn)
	BEGIN
		EXEC #AppendLine '> AppendSql: body column [{%1}] not found in {%2}.', @BodyColumn, @TableName
		EXEC #AppendLine ''
		RETURN
	END

	-- Order: only RowNumber if present
	DECLARE @OrderByClause nvarchar(max) = ''
	IF EXISTS (SELECT 1 FROM tempdb.sys.columns WHERE object_id = @objid AND name = 'RowNumber')
	SET @OrderByClause = ' ORDER BY [RowNumber]'

	-- Title column presence (tolerate NULLs)
	DECLARE @HasType bit = CASE WHEN @TypeCol IS NOT NULL AND EXISTS (SELECT 1 FROM tempdb.sys.columns WHERE object_id = @objid AND name = @TypeCol) THEN 1 ELSE 0 END
	DECLARE @HasSch bit = CASE WHEN @SchemaCol IS NOT NULL AND EXISTS (SELECT 1 FROM tempdb.sys.columns WHERE object_id = @objid AND name = @SchemaCol) THEN 1 ELSE 0 END
	DECLARE @HasName bit = CASE WHEN @NameCol IS NOT NULL AND EXISTS (SELECT 1 FROM tempdb.sys.columns WHERE object_id = @objid AND name = @NameCol) THEN 1 ELSE 0 END

	-- Build dynamic SELECT for titles + body (compat-safe)
	DECLARE @sql nvarchar(max) = 'SELECT ' +
		CASE WHEN @HasType = 1 THEN 'CONVERT(nvarchar(max),' + QUOTENAME(@TypeCol) + ')' ELSE 'CAST(NULL AS nvarchar(max))' END + ' AS __t, ' +
		CASE WHEN @HasSch = 1 THEN 'CONVERT(nvarchar(max),' + QUOTENAME(@SchemaCol) + ')' ELSE 'CAST(NULL AS nvarchar(max))' END + ' AS __s, ' +
		CASE WHEN @HasName = 1 THEN 'CONVERT(nvarchar(max),' + QUOTENAME(@NameCol) + ')' ELSE 'CAST(NULL AS nvarchar(max))' END + ' AS __n, ' +
		'CONVERT(nvarchar(max),' + QUOTENAME(@BodyColumn) + ') AS __body ' +
		'FROM ' + QUOTENAME(@TableName) + @OrderByClause

	IF OBJECT_ID('tempdb..#DTR_SqlBuffer') IS NOT NULL DROP TABLE #DTR_SqlBuffer
	
	CREATE TABLE #DTR_SqlBuffer (
		id int IDENTITY(1, 1) PRIMARY KEY,
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
	SELECT
	__t,
	__s,
	__n,
	__body
	FROM #DTR_SqlBuffer
	ORDER BY id

	OPEN d
	FETCH NEXT FROM d INTO @t, @s, @n, @b
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
		
		-- SafeMode: never emit body text (force a stable marker)
		IF @CfgSafeMode = 1 AND (@b IS NULL OR LEN(@b) = 0)
		SET @b = '-- [Redacted]';

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
		
		FETCH NEXT FROM d INTO @t, @s, @n, @b
	END
	CLOSE d;
	DEALLOCATE d;

	DROP TABLE #DTR_SqlBuffer
END
GO

------------------------------------------------------------
-- Export Schema to {OutputDir}\dt_schema_report ({TargetDB} - {Version}).md
------------------------------------------------------------
PRINT 'Exporting Schema - ' + CONVERT(nvarchar(8), SYSDATETIME(), 108);
GO

:OUT $(OutputDir)"\dt_schema_report ("$(TargetDB)" - "$(Version)").md"
GO

DECLARE @ExportDatabaseName nvarchar(128) = DB_NAME()
DECLARE @ExportServerName nvarchar(128) = CONVERT(nvarchar(128), SERVERPROPERTY('ServerName'))
DECLARE @ExportSysDateTime nvarchar(30) = CONVERT(nvarchar(30), SYSDATETIME(), 126)
DECLARE @ProductMajorVersion int = (SELECT ProductMajorVersion FROM #DT_Config);
DECLARE @CompatLevel int = (SELECT CompatLevel FROM #DT_Config);
DECLARE @SafeMode bit = (SELECT SafeMode FROM #DT_Config);
DECLARE @ExportSafeMode nvarchar(3) = CASE WHEN @SafeMode = 1 THEN 'On' ELSE 'Off' END;

-- Title and basic info
EXEC #AppendLine '# Database Tuner Schema Report';
EXEC #AppendLine '';
EXEC #AppendLine '- **Version:** $(Version)';
EXEC #AppendLine '- **Target DB:** [{%1}]', @ExportDatabaseName;
EXEC #AppendLine '- **Server:** {%1}', @ExportServerName;
EXEC #AppendLine '- **Generated (local):** {%1}', @ExportSysDateTime;
EXEC #AppendLine '- **Product Major Version:** {%1}', @ProductMajorVersion;
EXEC #AppendLine '- **DB Compat:** {%1}', @CompatLevel;
EXEC #AppendLine '- **Safe Mode:** {%1}', @ExportSafeMode;
EXEC #AppendLine '';
EXEC #AppendLine '## Assistant Brief';
EXEC #AppendLine '';
EXEC #AppendLine 'This export contains schema only.';
EXEC #AppendLine '';
EXEC #AppendLine '- Use it to review tables, views, procedures, functions, relationships, indexes, and module/trigger definitions.';
EXEC #AppendLine '- If Safe Mode is On, module/trigger/constraint definitions are redacted (shown as -- [Redacted]) and extended property values as [Redacted].';
EXEC #AppendLine '';

-- Schemas
IF OBJECT_ID('tempdb..#DTR_S_Schemas') IS NOT NULL DROP TABLE #DTR_S_Schemas;
SELECT
IDENTITY(int) AS RowNumber,
schema_id = s.schema_id,
schema_name = s.name,
owner_name = dp.name
INTO #DTR_S_Schemas
FROM sys.schemas AS s
LEFT JOIN sys.database_principals AS dp ON dp.principal_id = s.principal_id
ORDER BY s.name;

EXEC #AppendLine '## Schemas';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.schemas; sys.database_principals';
EXEC #AppendLine 'Why: Database schemas and owners.';
EXEC #AppendLine '```';
EXEC #AppendCsv '#DTR_S_Schemas';
EXEC #AppendLine '';

-- Modules (procedures, views, functions)
IF OBJECT_ID('tempdb..#DTR_S_Modules') IS NOT NULL DROP TABLE #DTR_S_Modules;
SELECT
IDENTITY(int) AS RowNumber,
object_type =
	CASE o.type
		WHEN 'P' THEN 'PROCEDURE'
		WHEN 'V' THEN 'VIEW'
		WHEN 'FN' THEN 'FUNCTION'
		WHEN 'TF' THEN 'FUNCTION'
		WHEN 'IF' THEN 'FUNCTION'
		ELSE o.type_desc
	END,
schema_name = s.name,
object_name = o.name,
definition = CASE WHEN @SafeMode = 0 THEN m.definition ELSE '-- [Redacted]' END
INTO #DTR_S_Modules
FROM sys.objects AS o
JOIN sys.schemas AS s ON s.schema_id = o.schema_id
JOIN sys.sql_modules AS m ON m.object_id = o.object_id
WHERE o.type IN ('P', 'V', 'FN', 'TF', 'IF')
AND (OBJECTPROPERTY(o.object_id, 'IsEncrypted') = 0)
ORDER BY object_type, schema_name, object_name;

EXEC #AppendLine '## Programmability - Module definitions';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.objects (P, V, FN, TF, IF); sys.sql_modules; sys.schemas';
EXEC #AppendLine 'Why: Source code of stored procedures, views, and user-defined functions.';
EXEC #AppendLine '```';
EXEC #AppendSql @TableName = '#DTR_S_Modules', @BodyColumn = 'definition',
				@TypeCol = 'object_type', @SchemaCol = 'schema_name', @NameCol = 'object_name';
EXEC #AppendLine '';

-- Triggers
IF OBJECT_ID('tempdb..#DTR_S_Triggers') IS NOT NULL DROP TABLE #DTR_S_Triggers;
SELECT
IDENTITY(int) AS RowNumber,
object_type = 'TRIGGER',
schema_name = s.name,
object_name = tr.name,
definition = CASE WHEN @SafeMode = 0 THEN m.definition ELSE '-- [Redacted]' END
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
EXEC #AppendSql @TableName = '#DTR_S_Triggers', @BodyColumn = 'definition',
				@TypeCol = 'object_type', @SchemaCol = 'schema_name', @NameCol = 'object_name';
EXEC #AppendLine '';

-- Tables
IF OBJECT_ID('tempdb..#DTR_S_Tables') IS NOT NULL DROP TABLE #DTR_S_Tables;

CREATE TABLE #DTR_S_Tables (
	RowNumber int IDENTITY(1, 1) NOT NULL,
	schema_name sysname NOT NULL,
	table_name sysname NOT NULL,
	object_id int NOT NULL,
	temporal_type_desc nvarchar(120) NULL,
	is_memory_optimized bit NULL
);

DECLARE @SqlTables nvarchar(max);
DECLARE @TablesTemporalExpr nvarchar(200) =
	CASE
		WHEN COL_LENGTH('sys.tables', 'temporal_type_desc') IS NOT NULL THEN 't.temporal_type_desc'
		ELSE 'CAST(NULL AS nvarchar(120))'
	END;
DECLARE @TablesMemOptExpr nvarchar(200) =
	CASE
		WHEN COL_LENGTH('sys.tables', 'is_memory_optimized') IS NOT NULL THEN 't.is_memory_optimized'
		ELSE 'CAST(NULL AS bit)'
	END;

SET @SqlTables = N'INSERT INTO #DTR_S_Tables (
	schema_name,
	table_name,
	object_id,
	temporal_type_desc,
	is_memory_optimized
)
SELECT
s.name,
t.name,
t.object_id,
' + @TablesTemporalExpr + N',
' + @TablesMemOptExpr + N'
FROM sys.tables AS t
JOIN sys.schemas AS s ON s.schema_id = t.schema_id
WHERE t.is_ms_shipped = 0
ORDER BY s.name, t.name;';

EXEC sys.sp_executesql @SqlTables;

EXEC #AppendLine '## Tables';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.tables; sys.schemas';
EXEC #AppendLine 'Why: All user tables with temporal and memory-optimized flags.';
EXEC #AppendLine '```';
EXEC #AppendCsv '#DTR_S_Tables';
EXEC #AppendLine '';

-- Columns
IF OBJECT_ID('tempdb..#DTR_S_Columns') IS NOT NULL DROP TABLE #DTR_S_Columns;

CREATE TABLE #DTR_S_Columns (
	RowNumber int IDENTITY(1, 1) NOT NULL,
	schema_name sysname NOT NULL,
	table_name sysname NOT NULL,
	column_name sysname NOT NULL,
	ordinal_position int NOT NULL,
	data_type sysname NULL,
	max_length smallint NULL,
	precision tinyint NULL,
	scale tinyint NULL,
	is_nullable bit NULL,
	is_identity bit NULL,
	is_computed bit NULL,
	is_persisted bit NULL,
	vector_base_type tinyint NULL,
	vector_base_type_desc nvarchar(20) NULL,
	vector_dimensions int NULL
);

DECLARE @SqlColumns nvarchar(max);
DECLARE @VectorBaseTypeExpr nvarchar(200) =
	CASE
		WHEN COL_LENGTH('sys.columns', 'vector_base_type') IS NOT NULL THEN 'c.vector_base_type'
		ELSE 'CAST(NULL AS tinyint)'
	END;
DECLARE @VectorBaseTypeDescExpr nvarchar(200) =
	CASE
		WHEN COL_LENGTH('sys.columns', 'vector_base_type_desc') IS NOT NULL THEN 'c.vector_base_type_desc'
		ELSE 'CAST(NULL AS nvarchar(20))'
	END;
DECLARE @VectorDimensionsExpr nvarchar(200) =
	CASE
		WHEN COL_LENGTH('sys.columns', 'vector_dimensions') IS NOT NULL THEN 'c.vector_dimensions'
		ELSE 'CAST(NULL AS int)'
	END;

SET @SqlColumns = N'INSERT INTO #DTR_S_Columns (
	schema_name,
	table_name,
	column_name,
	ordinal_position,
	data_type,
	max_length,
	precision,
	scale,
	is_nullable,
	is_identity,
	is_computed,
	is_persisted,
	vector_base_type,
	vector_base_type_desc,
	vector_dimensions
)
SELECT
s.name,
t.name,
c.name,
c.column_id,
TYPE_NAME(c.user_type_id),
c.max_length,
c.precision,
c.scale,
c.is_nullable,
c.is_identity,
c.is_computed,
cc.is_persisted,
' + @VectorBaseTypeExpr + N',
' + @VectorBaseTypeDescExpr + N',
' + @VectorDimensionsExpr + N'
FROM sys.tables AS t
JOIN sys.schemas AS s ON s.schema_id = t.schema_id
JOIN sys.columns AS c ON c.object_id = t.object_id
LEFT JOIN sys.computed_columns AS cc ON cc.object_id = c.object_id AND cc.column_id = c.column_id
WHERE t.is_ms_shipped = 0
ORDER BY s.name, t.name, c.column_id;';

EXEC sys.sp_executesql @SqlColumns;

EXEC #AppendLine '## Columns';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.columns; sys.tables; sys.computed_columns; sys.schemas';
EXEC #AppendLine 'Why: Columns of user tables with data types and properties (nullability, identity, computed).';
EXEC #AppendLine 'Notes: vector_* columns populate on SQL Server 2025+; NULL on earlier versions.';
EXEC #AppendLine '```';
EXEC #AppendCsv '#DTR_S_Columns';
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
EXEC #AppendCsv '#DTR_S_Types';
EXEC #AppendLine '';

-- Table types
IF OBJECT_ID('tempdb..#DTR_S_TableTypes') IS NOT NULL DROP TABLE #DTR_S_TableTypes;

CREATE TABLE #DTR_S_TableTypes (
	RowNumber int IDENTITY(1, 1) NOT NULL,
	schema_name sysname NOT NULL,
	table_type_name sysname NOT NULL,
	is_memory_optimized bit NULL
);

DECLARE @SqlTableTypes nvarchar(max);
DECLARE @TableTypesMemOptExpr nvarchar(200) =
	CASE
		WHEN COL_LENGTH('sys.table_types', 'is_memory_optimized') IS NOT NULL THEN 'tt.is_memory_optimized'
		ELSE 'CAST(NULL AS bit)'
	END;

SET @SqlTableTypes = N'INSERT INTO #DTR_S_TableTypes (
	schema_name,
	table_type_name,
	is_memory_optimized
)
SELECT
s.name,
tt.name,
' + @TableTypesMemOptExpr + N'
FROM sys.table_types AS tt
JOIN sys.schemas AS s ON s.schema_id = tt.schema_id
WHERE tt.is_user_defined = 1
ORDER BY s.name, tt.name;';

EXEC sys.sp_executesql @SqlTableTypes;

EXEC #AppendLine '## Table Types';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.table_types; sys.schemas';
EXEC #AppendLine 'Why: Table-valued parameter types and their properties (memory-optimized).';
EXEC #AppendLine '```';
EXEC #AppendCsv '#DTR_S_TableTypes';
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
EXEC #AppendCsv '#DTR_S_TableTypeColumns';
EXEC #AppendLine '';

-- Constraints (PK, UNIQUE, FK, CHECK, DEFAULT)
IF OBJECT_ID('tempdb..#DTR_S_Constraints') IS NOT NULL DROP TABLE #DTR_S_Constraints;
SELECT
IDENTITY(int) AS RowNumber,
schema_name = s.name,
table_name = t.name,
constraint_name = kc.name,
constraint_type =
	CASE kc.type
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
WHERE t.type = 'U' AND kc.type IN ('PK', 'UQ', 'F', 'C', 'D')
ORDER BY s.name, t.name, constraint_type, kc.name;

EXEC #AppendLine '## Constraints - Inventory';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.objects (constraints by type PK, UQ, F, C, D); sys.schemas';
EXEC #AppendLine 'Why: Inventory of primary key, unique, foreign key, check, and default constraints on tables.';
EXEC #AppendLine '```';
EXEC #AppendCsv '#DTR_S_Constraints';
EXEC #AppendLine '';

-- Check constraint definitions
IF OBJECT_ID('tempdb..#DTR_S_CheckConstraintDefs') IS NOT NULL DROP TABLE #DTR_S_CheckConstraintDefs;
SELECT
IDENTITY(int) AS RowNumber,
object_type = 'CHECK CONSTRAINT',
schema_name = OBJECT_SCHEMA_NAME(c.parent_object_id),
object_name = c.name,
definition = CASE WHEN @SafeMode = 0 THEN c.definition ELSE '-- [Redacted]' END
INTO #DTR_S_CheckConstraintDefs
FROM sys.check_constraints AS c
ORDER BY schema_name, object_name;

EXEC #AppendLine '## Constraint definitions - CHECK';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.check_constraints';
EXEC #AppendLine 'Why: Definition of CHECK constraint expressions.';
EXEC #AppendLine '```';
EXEC #AppendSql @TableName = '#DTR_S_CheckConstraintDefs', @BodyColumn = 'definition',
				@TypeCol = 'object_type', @SchemaCol = 'schema_name', @NameCol = 'object_name';
EXEC #AppendLine '';

-- Default constraint definitions
IF OBJECT_ID('tempdb..#DTR_S_DefaultConstraintDefs') IS NOT NULL DROP TABLE #DTR_S_DefaultConstraintDefs;
SELECT
IDENTITY(int) AS RowNumber,
object_type = 'DEFAULT CONSTRAINT',
schema_name = OBJECT_SCHEMA_NAME(d.parent_object_id),
object_name = d.name,
definition = CASE WHEN @SafeMode = 0 THEN d.definition ELSE '-- [Redacted]' END
INTO #DTR_S_DefaultConstraintDefs
FROM sys.default_constraints AS d
ORDER BY schema_name, object_name;

EXEC #AppendLine '## Constraint definitions - DEFAULT';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.default_constraints';
EXEC #AppendLine 'Why: Definition of DEFAULT constraint expressions.';
EXEC #AppendLine '```';
EXEC #AppendSql @TableName = '#DTR_S_DefaultConstraintDefs', @BodyColumn = 'definition',
				@TypeCol = 'object_type', @SchemaCol = 'schema_name', @NameCol = 'object_name';
EXEC #AppendLine '';

-- Computed column definitions
IF OBJECT_ID('tempdb..#DTR_S_ComputedColumnDefs') IS NOT NULL DROP TABLE #DTR_S_ComputedColumnDefs;
SELECT
IDENTITY(int) AS RowNumber,
object_type = 'COLUMN COMPUTED',
schema_name = s.name,
object_name = t.name,
name_path = t.name + '].[' + c.name,
definition = CASE WHEN @SafeMode = 0 THEN cc.definition ELSE '-- [Redacted]' END
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
EXEC #AppendSql @TableName = '#DTR_S_ComputedColumnDefs', @BodyColumn = 'definition',
				@TypeCol = 'object_type', @SchemaCol = 'schema_name', @NameCol = 'name_path';
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
WHERE i.index_id > 0 AND t.is_ms_shipped = 0
ORDER BY s.name, t.name, i.name;

EXEC #AppendLine '## Indexes';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.indexes; sys.tables; sys.schemas';
EXEC #AppendLine 'Why: Indexes on user tables with type, uniqueness, and filter (if any).';
EXEC #AppendLine '```';
EXEC #AppendCsv '#DTR_S_Indexes';
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
WHERE i.index_id > 0 AND t.is_ms_shipped = 0
ORDER BY s.name, t.name, i.name,
		CASE WHEN ic.is_included_column = 1 THEN 1 ELSE 0 END,
		ic.key_ordinal, c.name;

EXEC #AppendLine '## Index Columns';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.index_columns; sys.columns; sys.indexes; sys.tables; sys.schemas';
EXEC #AppendLine 'Why: Index key columns and included columns with order and sort direction.';
EXEC #AppendLine '```';
EXEC #AppendCsv '#DTR_S_IndexColumns';
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
column_names =
	STUFF((
		SELECT
		', ' + c_p.name
		FROM sys.foreign_key_columns AS fkc
		JOIN sys.columns AS c_p ON c_p.object_id = fkc.parent_object_id AND c_p.column_id = fkc.parent_column_id
		WHERE fkc.constraint_object_id = fk.object_id
		ORDER BY fkc.constraint_column_id
		FOR XML PATH(''), TYPE
	).value('(text())[1]', 'nvarchar(max)'), 1, 2, ''),
referenced_column_names =
	STUFF((
		SELECT
		', ' + c_r.name
		FROM sys.foreign_key_columns AS fkc
		JOIN sys.columns AS c_r ON c_r.object_id = fkc.referenced_object_id AND c_r.column_id = fkc.referenced_column_id
		WHERE fkc.constraint_object_id = fk.object_id
		ORDER BY fkc.constraint_column_id
		FOR XML PATH(''), TYPE
	).value('(text())[1]', 'nvarchar(max)'), 1, 2, '')
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
EXEC #AppendCsv '#DTR_S_ForeignKeys';
EXEC #AppendLine '';

-- Sequences
IF OBJECT_ID('tempdb..#DTR_S_Sequences') IS NOT NULL DROP TABLE #DTR_S_Sequences;

CREATE TABLE #DTR_S_Sequences (
	RowNumber int IDENTITY(1, 1) NOT NULL,
	schema_name sysname NOT NULL,
	sequence_name sysname NOT NULL,
	data_type sysname NULL,
	start_value sql_variant NULL,
	increment sql_variant NULL,
	min_value sql_variant NULL,
	max_value sql_variant NULL,
	is_cycling bit NULL,
	cache_size sql_variant NULL
);

IF OBJECT_ID('sys.sequences') IS NOT NULL
BEGIN
	DECLARE @SqlSequences nvarchar(max) = N'INSERT INTO #DTR_S_Sequences (
		schema_name,
		sequence_name,
		data_type,
		start_value,
		increment,
		min_value,
		max_value,
		is_cycling,
		cache_size
	)
	SELECT
	s.name,
	seq.name,
	TYPE_NAME(seq.user_type_id),
	seq.start_value,
	seq.increment,
	seq.minimum_value,
	seq.maximum_value,
	seq.is_cycling,
	seq.cache_size
	FROM sys.sequences AS seq
	JOIN sys.schemas AS s ON s.schema_id = seq.schema_id
	WHERE seq.is_ms_shipped = 0
	ORDER BY s.name, seq.name;';

	EXEC sys.sp_executesql @SqlSequences;
END

EXEC #AppendLine '## Sequences';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.sequences; sys.schemas';
EXEC #AppendLine 'Why: User-defined sequences and their parameters (data type, start, increment, min/max, cycle, cache).';
EXEC #AppendLine '```';
EXEC #AppendCsv '#DTR_S_Sequences';
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
EXEC #AppendCsv '#DTR_S_Synonyms';
EXEC #AppendLine '';

-- Partition functions (with boundaries)
IF OBJECT_ID('tempdb..#DTR_S_PartitionFunctions') IS NOT NULL DROP TABLE #DTR_S_PartitionFunctions;
SELECT
IDENTITY(int) AS RowNumber,
function_name = pf.name,
boundary_type = CASE WHEN pf.boundary_value_on_right = 1 THEN 'RIGHT' ELSE 'LEFT' END,
data_type = TYPE_NAME(pp.user_type_id),
max_length = pp.max_length,
precision = pp.precision,
scale = pp.scale,
boundary_values =
	STUFF((
		SELECT
		', ' + CONVERT(nvarchar(4000), prv.value)
		FROM sys.partition_range_values AS prv
		WHERE prv.function_id = pf.function_id
		ORDER BY prv.boundary_id
		FOR XML PATH(''), TYPE
	).value('(text())[1]', 'nvarchar(max)'), 1, 2, ''),
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
EXEC #AppendCsv '#DTR_S_PartitionFunctions';
EXEC #AppendLine '';

-- Partition schemes
IF OBJECT_ID('tempdb..#DTR_S_PartitionSchemes') IS NOT NULL DROP TABLE #DTR_S_PartitionSchemes;
SELECT
IDENTITY(int) AS RowNumber,
scheme_name = ps.name,
function_name = pf.name,
filegroups =
	STUFF((
		SELECT
		', ' + fg.name
		FROM sys.destination_data_spaces AS dds
		JOIN sys.filegroups AS fg ON fg.data_space_id = dds.data_space_id
		WHERE dds.partition_scheme_id = ps.data_space_id
		ORDER BY dds.destination_id
		FOR XML PATH(''), TYPE
	).value('(text())[1]', 'nvarchar(max)'), 1, 2, '')
INTO #DTR_S_PartitionSchemes
FROM sys.partition_schemes AS ps
JOIN sys.partition_functions AS pf ON pf.function_id = ps.function_id
ORDER BY ps.name;

EXEC #AppendLine '## Partition Schemes';
EXEC #AppendLine '```text';
EXEC #AppendLine 'Source: sys.partition_schemes; sys.partition_functions; sys.destination_data_spaces; sys.filegroups';
EXEC #AppendLine 'Why: Mapping of partition schemes to partition functions and filegroups.';
EXEC #AppendLine '```';
EXEC #AppendCsv '#DTR_S_PartitionSchemes';
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
	WHERE ic2.object_id = i.object_id AND ic2.index_id = i.index_id AND ic2.partition_ordinal = 1
),
partition_count = (
	SELECT
	COUNT(*)
	FROM sys.partitions AS p
	WHERE p.object_id = i.object_id AND p.index_id = i.index_id
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
EXEC #AppendCsv '#DTR_S_TablePartitions';
EXEC #AppendLine '';

-- Full-Text Indexes
IF OBJECT_ID('tempdb..#DTR_S_FullText') IS NOT NULL DROP TABLE #DTR_S_FullText;
SELECT
IDENTITY(int) AS RowNumber,
schema_name = s.name,
table_name = t.name,
fulltext_catalog_name = ftc.name,
unique_index_name = ui.name,
change_tracking_desc =
	CASE fti.change_tracking_state
		WHEN 0 THEN 'OFF'
		WHEN 1 THEN 'MANUAL'
		WHEN 2 THEN 'AUTO'
	END,
column_names =
	STUFF((
		SELECT
		', ' + c.name
		FROM sys.fulltext_index_columns AS fic
		JOIN sys.columns AS c ON c.object_id = fic.object_id AND c.column_id = fic.column_id
		WHERE fic.object_id = t.object_id
		ORDER BY c.name
		FOR XML PATH(''), TYPE
	).value('(text())[1]', 'nvarchar(max)'), 1, 2, '')
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
EXEC #AppendCsv '#DTR_S_FullText';
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
EXEC #AppendCsv '#DTR_S_XmlSchemaCollections';
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
EXEC #AppendCsv '#DTR_S_Assemblies';
EXEC #AppendLine '';

-- Extended Properties
IF OBJECT_ID('tempdb..#DTR_S_ExtendedProperties') IS NOT NULL DROP TABLE #DTR_S_ExtendedProperties;
SELECT
IDENTITY(int) AS RowNumber,
class_desc = ep.class_desc,
schema_name = COALESCE(sc_obj.name, sc_schema.name, sc_type.name, NULL),
object_name =
	CASE
		WHEN ep.class_desc = 'DATABASE' THEN DB_NAME()
		WHEN ep.class_desc = 'SCHEMA' THEN sc_schema.name
		WHEN ep.class_desc = 'DATABASE_PRINCIPAL' THEN dp.name
		WHEN ep.class_desc = 'TYPE' THEN typ.name
		ELSE o.name
	END,
subobject_name =
	CASE
		WHEN ep.class_desc = 'OBJECT_OR_COLUMN' AND ep.minor_id <> 0 THEN col.name
		WHEN ep.class_desc = 'PARAMETER' THEN '@' + par.name
		WHEN ep.class_desc = 'INDEX' THEN ix.name
		ELSE NULL
	END,
property_name = ep.name,
property_value = CASE WHEN @SafeMode = 0 THEN CONVERT(nvarchar(max), ep.value) ELSE '[Redacted]' END
INTO #DTR_S_ExtendedProperties
FROM sys.extended_properties AS ep
LEFT JOIN sys.objects AS o ON ep.class IN (1, 2, 7) AND ep.major_id = o.object_id
LEFT JOIN sys.columns AS col ON ep.class = 1 AND ep.minor_id <> 0 AND col.object_id = o.object_id AND col.column_id = ep.minor_id
LEFT JOIN sys.parameters AS par ON ep.class = 2 AND par.object_id = o.object_id AND par.parameter_id = ep.minor_id
LEFT JOIN sys.indexes AS ix ON ep.class = 7 AND ix.object_id = o.object_id AND ix.index_id = ep.minor_id
LEFT JOIN sys.schemas AS sc_obj ON ep.class IN (1, 2, 7) AND o.schema_id = sc_obj.schema_id
LEFT JOIN sys.schemas AS sc_schema ON ep.class = 3 AND ep.major_id = sc_schema.schema_id
LEFT JOIN sys.database_principals AS dp ON ep.class = 4 AND ep.major_id = dp.principal_id
LEFT JOIN sys.types AS typ ON ep.class = 6 AND ep.major_id = typ.user_type_id
LEFT JOIN sys.schemas AS sc_type ON ep.class = 6 AND typ.schema_id = sc_type.schema_id
WHERE (
ep.class_desc NOT IN ('DATABASE', 'DATABASE_PRINCIPAL', 'TYPE', 'SCHEMA')
OR (ep.class_desc IN ('DATABASE', 'DATABASE_PRINCIPAL', 'TYPE', 'SCHEMA') AND ep.major_id IS NOT NULL)
)
ORDER BY
CASE WHEN ep.class IN (2, 7) THEN 1 ELSE ep.class END,
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
EXEC #AppendCsv '#DTR_S_ExtendedProperties';
EXEC #AppendLine '';

GO
:OUT STDOUT

------------------------------------------------------------
-- Complete
------------------------------------------------------------
PRINT 'Database Tuner Schema Complete'
GO

PRINT ''
PRINT 'Markdown Saved: $(OutputDir)\dt_schema_report ($(TargetDB) - $(Version)).md'
GO
