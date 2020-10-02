CREATE PROC [dbo].[sp_ccd_event] AS
BEGIN

	CREATE TABLE #Temp
	(
		[extraction_date] [varchar](2000) NULL,
		[ce_case_data_id] [varchar](2000) NULL,
		[cd_created_date] [varchar](2000) NULL,
		[cd_last_modified] [varchar](2000) NULL,
		[cd_jurisdiction] [varchar](2000) NULL,
		[cd_latest_state] [varchar](2000) NULL,
		[cd_reference] [varchar](2000) NULL,
		[cd_security_classification] [varchar](2000) NULL,
		[cd_version] [varchar](2000) NULL,
		[cd_last_state_modified_date] [varchar](2000) NULL,
		[ce_id] [varchar](2000) NULL,
		[ce_created_date] [varchar](2000) NULL,
		[ce_event_id] [varchar](2000) NULL,
		[ce_summary] [varchar](2000) NULL,
		[ce_description] [varchar](2000) NULL,
		[ce_user_id] [varchar](2000) NULL,
		[ce_case_type_id] [varchar](2000) NULL,
		[ce_case_type_version] [varchar](2000) NULL,
		[ce_state_id] [varchar](2000) NULL,
		[ce_data] [nvarchar](MAX) NULL
	)
	WITH
	(
		HEAP
	);

	INSERT INTO #Temp
	SELECT REPLACE(REPLACE(JSON_VALUE(data, '$.extraction_date'), 'T', ' '), 'Z', '') AS extraction_date
		, JSON_VALUE(data, '$.ce_case_data_id') AS ce_case_data_id
		, REPLACE(REPLACE(JSON_VALUE(data, '$.cd_created_date'), 'T', ' '), 'Z', '') AS cd_created_date
		, REPLACE(REPLACE(JSON_VALUE(data, '$.cd_last_modified'), 'T', ' '), 'Z', '') AS cd_last_modified
		, JSON_VALUE(data, '$.cd_jurisdiction') AS cd_jurisdiction
		, JSON_VALUE(data, '$.cd_latest_state') AS cd_latest_state
		, JSON_VALUE(data, '$.cd_reference') AS cd_reference
		, JSON_VALUE(data, '$.cd_security_classification') AS cd_security_classification
		, JSON_VALUE(data, '$.cd_version') AS cd_version
		, REPLACE(REPLACE(JSON_VALUE(data, '$.cd_last_state_modified_date'), 'T', ' '), 'Z', '') AS cd_last_state_modified_date
		, JSON_VALUE(data, '$.ce_id') AS ce_id
		, REPLACE(REPLACE(JSON_VALUE(data, '$.ce_created_date'), 'T', ' '), 'Z', '') AS ce_created_date
		, JSON_VALUE(data, '$.ce_event_id') AS ce_event_id
		, JSON_VALUE(data, '$.ce_summary') AS ce_summary
		, JSON_VALUE(data, '$.ce_description') AS ce_description
		, JSON_VALUE(data, '$.ce_user_id') AS ce_user_id
		, JSON_VALUE(data, '$.ce_case_type_id') AS ce_case_type_id
		, JSON_VALUE(data, '$.ce_case_type_version') AS ce_case_type_version
		, JSON_VALUE(data, '$.ce_state_id') AS ce_state_id
		, CAST(JSON_QUERY(data, '$.ce_data') AS NVARCHAR(MAX)) AS ce_data
	FROM etb_ccd_json
	WHERE ISDATE(JSON_VALUE(data, '$.cd_created_date')) = 1;

	--DELETE FROM stg_ccd_event
	/*
	SELECT REPLACE(REPLACE(JSON_VALUE(data, '$.extraction_date'), 'T', ' '), 'Z', '') AS extraction_date
		, JSON_VALUE(data, '$.ce_case_data_id') AS ce_case_data_id
		, REPLACE(REPLACE(JSON_VALUE(data, '$.cd_created_date'), 'T', ' '), 'Z', '') AS cd_created_date
		, REPLACE(REPLACE(JSON_VALUE(data, '$.cd_last_modified'), 'T', ' '), 'Z', '') AS cd_last_modified
		, JSON_VALUE(data, '$.cd_jurisdiction') AS cd_jurisdiction
		, JSON_VALUE(data, '$.cd_latest_state') AS cd_latest_state
		, JSON_VALUE(data, '$.cd_reference') AS cd_reference
		, JSON_VALUE(data, '$.cd_security_classification') AS cd_security_classification
		, JSON_VALUE(data, '$.cd_version') AS cd_version
		, REPLACE(REPLACE(JSON_VALUE(data, '$.cd_last_state_modified_date'), 'T', ' '), 'Z', '') AS cd_last_state_modified_date
		, JSON_VALUE(data, '$.ce_id') AS ce_id
		, REPLACE(REPLACE(JSON_VALUE(data, '$.ce_created_date'), 'T', ' '), 'Z', '') AS ce_created_date
		, JSON_VALUE(data, '$.ce_event_id') AS ce_event_id
		, JSON_VALUE(data, '$.ce_summary') AS ce_summary
		, JSON_VALUE(data, '$.ce_description') AS ce_description
		, JSON_VALUE(data, '$.ce_user_id') AS ce_user_id
		, JSON_VALUE(data, '$.ce_case_type_id') AS ce_case_type_id
		, JSON_VALUE(data, '$.ce_case_type_version') AS ce_case_type_version
		, JSON_VALUE(data, '$.ce_state_id') AS ce_state_id
		, CAST(JSON_QUERY(data, '$.ce_data') AS NVARCHAR(MAX)) AS ce_data
	INTO #Temp
	FROM etb_ccd_json
	WHERE ISDATE(JSON_VALUE(data, '$.cd_created_date')) = 1;
	*/

	UPDATE stg_ccd_event
	SET extraction_date = src.extraction_date
		, ce_case_data_id = src.ce_case_data_id
		, cd_created_date = src.cd_created_date
		, cd_last_modified = src.cd_last_modified
		, cd_jurisdiction = src.cd_jurisdiction
		, cd_latest_state = src.cd_latest_state
		, cd_reference = src.cd_reference
		, cd_security_classification = src.cd_security_classification
		, cd_version = src.cd_version
		, cd_last_state_modified_date = src.cd_last_state_modified_date
		, ce_id = src.ce_id
		, ce_created_date = src.ce_created_date
		, ce_event_id = src.ce_event_id
		, ce_summary = src.ce_summary
		, ce_description = src.ce_description
		, ce_user_id = src.ce_user_id
		, ce_case_type_id = src.ce_case_type_id
		, ce_case_type_version = src.ce_case_type_version
		, ce_state_id = src.ce_state_id
		, ce_data = src.ce_data
	FROM #Temp src
	WHERE src.ce_id = stg_ccd_event.ce_id;

	INSERT INTO stg_ccd_event
	(
		extraction_date
		, ce_case_data_id
		, cd_created_date
		, cd_last_modified
		, cd_jurisdiction
		, cd_latest_state
		, cd_reference
		, cd_security_classification
		, cd_version
		, cd_last_state_modified_date
		, ce_id
		, ce_created_date
		, ce_event_id
		, ce_summary
		, ce_description
		, ce_user_id
		, ce_case_type_id
		, ce_case_type_version
		, ce_state_id
		, ce_data
	)
	SELECT extraction_date
		, ce_case_data_id
		, cd_created_date
		, cd_last_modified
		, cd_jurisdiction
		, cd_latest_state
		, cd_reference
		, cd_security_classification
		, cd_version
		, cd_last_state_modified_date
		, ce_id
		, ce_created_date
		, ce_event_id
		, ce_summary
		, ce_description
		, ce_user_id
		, ce_case_type_id
		, ce_case_type_version
		, ce_state_id
		, ce_data
	FROM #Temp src
	WHERE src.ce_id NOT IN
	(
		SELECT ce_id FROM stg_ccd_event
	);

	DROP TABLE #Temp;
END
