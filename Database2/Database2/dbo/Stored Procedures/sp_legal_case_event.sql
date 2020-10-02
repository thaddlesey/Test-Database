CREATE PROC [sp_legal_case_event] AS
BEGIN
    WITH draft_case_events
    AS (
      SELECT event_type_key
      FROM   v_all_case_event_grps
      WHERE  event_type_grp_key = 35.000028
    )
    SELECT  lc.legal_case_id,
            md.ce_event_id,
            md.ce_state_id,
            md.ce_id,
            REPLACE(REPLACE(md.ce_created_date, 'T', ' '), 'Z', '') AS ce_created_date,
            md.ce_case_type_id,
            md.ce_case_type_version,
            md.ce_user_id,
            mle.legal_case_event_type_key,
            ml.legal_case_state_type_key,
            mj.jursdctn_type_key AS jurisdiction_key,
            md.cd_jurisdiction AS jurisdiction_name,
            cd_reference AS legal_case_reference_id,
            REPLACE(REPLACE(cd_created_date, 'T', ' '), 'Z', '') AS legal_case_created_timestamp
	INTO #Temp
    FROM tbl_legal_case lc
    JOIN stg_ccd_event md 
	ON md.ce_case_data_id = lc.legal_case_id                                             
	AND md.ce_case_type_id = lc.legal_case_type_name
    LEFT JOIN mdm_jursdctn_type_lu mj 
	ON mj.jursdctn_type_name = md.cd_jurisdiction
    LEFT JOIN mdm_legal_case_state_type_lu ml 
	ON ml.legal_case_state_type_cid = md.ce_state_id
    AND ML.CCD_CASE_TYPE_NAME = MD.CE_CASE_TYPE_ID
    LEFT JOIN MDM_CCD_CASE_TYPE_LU mct        
	ON mct.ccd_src_case_type_name = md.ce_case_type_id
    join mdm_legal_case_event_type_lu mle     
	on mle.legal_case_event_type_cid = md.ce_event_id
    and mle.ccd_case_type_name = case when md.ce_case_type_id = 'DIVORCE' then 'DIVORCE'
                                      when md.ce_case_type_id = ISNULL(mct.ccd_src_case_type_name,'-') then mct.ccd_case_type_name
                                      else MD.CE_CASE_TYPE_ID
                                      end
	WHERE ISDATE(md.ce_created_date) = 1
	AND ISDATE(md.cd_created_date) = 1
    AND mle.legal_case_event_type_key NOT IN (
		SELECT event_type_key 
		FROM draft_case_events
	);

    UPDATE tbl_legal_case_event 
	SET legal_case_state_type_code = src.ce_state_id,
        legal_case_event_type_cid  = src.ce_event_id,
        event_created_timestamp    = src.ce_created_date,
        legal_case_type_vrsn_nbr   = src.ce_case_type_version,
        legal_case_event_type_name = src.ce_state_id,
        legal_case_state_type_name = src.ce_event_id,
        bis_last_modified_datetime = GETDATE(),
        modified_by_process_name   = 'sp_legal_case_event',
        source_system_type_code    = 'CCD',
        user_cid                   = src.ce_user_id,
        user_forename              = 'AD',
        user_surname               = 'MIN',
        legal_case_event_type_key  = src.legal_case_event_type_key,
        legal_case_state_type_key  = src.legal_case_state_type_key,
        jurisdiction_key           = src.jurisdiction_key,
        jurisdiction_name          = src.jurisdiction_name,
        legal_case_reference_id    = src.legal_case_reference_id,
        legal_case_created_timestamp = src.legal_case_created_timestamp
	FROM #Temp src
	WHERE src.legal_case_id = tbl_legal_case_event.legal_case_id 
	AND src.ce_id = tbl_legal_case_event.legal_case_event_id;

    INSERT tbl_legal_case_event
	(	   
		   legal_case_id,
           legal_case_state_type_code,
           legal_case_event_id,
           legal_case_event_type_cid,
           event_created_timestamp,
           legal_case_type_name,
           legal_case_type_vrsn_nbr,
           legal_case_event_type_name,
           legal_case_state_type_name,
           bis_first_created_datetime,
           inserted_datetime,
           inserted_by_process_name,
           source_system_type_code,
           user_cid,
           user_forename,
           user_surname,
           legal_case_event_type_key,
           legal_case_state_type_key,
           jurisdiction_key,
           jurisdiction_name, 
           legal_case_reference_id, 
           legal_case_created_timestamp
     )
    SELECT src.legal_case_id,
           src.ce_state_id,
           src.ce_id,
           src.ce_event_id,
           src.ce_created_date,
           src.ce_case_type_id,
           src.ce_case_type_version,
           src.ce_event_id,
           src.ce_state_id,
           CAST(GETDATE() AS DATETIME),
           CAST(GETDATE() AS DATETIME),
           'sp_legal_case_event',
           'CCD',
           src.ce_user_id,
           'AD',
           'MIN',
           src.legal_case_event_type_key,
           src.legal_case_state_type_key,
           src.jurisdiction_key, 
           src.jurisdiction_name, 
           src.legal_case_reference_id, 
           src.legal_case_created_timestamp
	FROM #Temp src
	WHERE CAST(src.legal_case_id AS VARCHAR(255)) + ' ' + src.ce_id NOT IN(
		SELECT CAST(legal_case_id AS VARCHAR(255)) + ' ' + ce_id FROM tbl_legal_case_event
	);
	
	DROP TABLE #Temp;
END;
