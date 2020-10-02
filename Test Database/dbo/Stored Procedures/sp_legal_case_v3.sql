CREATE PROC [dbo].[sp_legal_case_v3] AS
BEGIN

    WITH
    draft_case_events
    AS (
		SELECT source_event_type_cid
		FROM   v_all_case_event_grps
		WHERE  event_type_grp_key = 35.000028
    ),
    latest_case_event
    AS
    (
        SELECT  cm.ce_case_data_id,
                cm.ce_case_type_id,
                cm.ce_case_type_version,
                cm.ce_created_date,           
                cm.cd_last_modified,
                cm.cd_jurisdiction,
                cm.cd_reference,
                row_number() OVER (PARTITION BY cm.ce_case_data_id ORDER BY cm.ce_id DESC) AS row_num,
                MIN(cm.ce_created_date) OVER (PARTITION BY cm.ce_case_data_id) AS case_created_date,
                cm.ce_event_id
        FROM stg_ccd_event cm
		WHERE ISDATE(cm.ce_created_date) = 1
    )
    SELECT  m.ce_case_data_id,
            m.ce_case_type_id,
            m.ce_case_type_version,
            m.case_created_date,
            m.ce_created_date,           
            m.cd_last_modified,
            m.cd_jurisdiction,
            m.cd_reference
	INTO #Temp
    FROM latest_case_event m
    WHERE row_num = 1
    AND m.ce_event_id NOT IN (SELECT source_event_type_cid FROM draft_case_events);

	UPDATE tbl_legal_case
	SET legal_case_type_code_vrsn_nbr = src.ce_case_type_version ,
		last_modified_datetime        = src.ce_created_date ,
		jurisdiction_code             = src.cd_jurisdiction ,
		legal_case_reference_id       = src.cd_reference ,
		bis_last_modified_datetime    = getdate()
	FROM #Temp src
	WHERE src.ce_case_data_id = tbl_legal_case.legal_case_id 
	AND src.ce_case_type_id = tbl_legal_case.legal_case_type_name;

	INSERT INTO tbl_legal_case
	(
		legal_case_id ,
		legal_case_type_name ,
		legal_case_type_code_vrsn_nbr ,
		legal_case_created_timestamp ,
		last_modified_datetime ,
		jurisdiction_code ,
		legal_case_reference_id ,
		bis_first_created_datetime ,
		bis_inserted_by_process_name ,
		source_system_type_code
	)
	SELECT src.ce_case_data_id ,
		src.ce_case_type_id ,
		src.ce_case_type_version ,
		CAST(src.case_created_date AS DATETIME2(3)),
		CAST(src.ce_created_date AS DATETIME2(3)),
		src.cd_jurisdiction ,
		src.cd_reference ,
		CAST(getdate() AS DATETIME),
		'sp_legal_case',
		'CCD'
	FROM #Temp src
	WHERE CAST(src.ce_case_data_id AS VARCHAR(255)) + ' ' + src.ce_case_type_id NOT IN(
		SELECT CAST(legal_case_id AS VARCHAR(255)) + ' ' + legal_case_type_name FROM tbl_legal_case
	);

	DROP TABLE #Temp;
END
