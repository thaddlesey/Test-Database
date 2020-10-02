CREATE PROC [dbo].[sp_probate_legal_case_attr_v2] AS
BEGIN
	--Grant
	WITH sorted_grant_event
	AS
	(
		SELECT ce_case_data_id
		, ce_id
		, cd_reference
		, ce_data
		, row_number() OVER (PARTITION BY ce_case_data_id ORDER BY ce_created_date DESC) AS rownumber
		FROM stg_ccd_event
		WHERE cd_jurisdiction = 'PROBATE'
		AND ce_case_type_id = 'GrantOfRepresentation'
	), 
	latest_grant_event
	AS
	(
		SELECT *
		FROM sorted_grant_event
		WHERE rownumber = 1
	), 
	all_char_cols
	AS
	(
		SELECT ce_case_data_id AS legal_case_id
		, ce_id AS case_metadata_event_id
		, cd_reference AS legal_case_ref_cid
		, JSON_VALUE(ce_data, '$.applicationType') AS ce_app_type
		, JSON_VALUE(ce_data, '$.registryLocation') AS ce_reg_location
		, JSON_VALUE(ce_data, '$.deceasedDateOfDeath') AS ce_deceased_dod
		, JSON_VALUE(ce_data, '$.applicationSubmittedDate') AS ce_app_sub_date
		, JSON_VALUE(ce_data, '$.willExists') AS ce_will_exists
		, JSON_VALUE(ce_data, '$.deceasedAnyOtherNames') AS ce_deceased_other_names
		, JSON_VALUE(ce_data, '$.boCaseStopReasonList') AS ce_case_stop_reason
		, JSON_VALUE(ce_data, '$.caseType') AS ce_gor_case_type
		, JSON_VALUE(ce_data, '$.grantIssuedDate') AS ce_grantissued_date
		, JSON_VALUE(ce_data, '$.recordId') AS ce_leg_record_id
		, JSON_VALUE(ce_data, '$.paperForm') AS ce_paperform
		, JSON_VALUE(ce_data, '$.ihtNetValue') AS ce_iht_net_value
		, JSON_VALUE(ce_data, '$.ihtGrossValue') AS ce_iht_gross_value
		, JSON_VALUE(ce_data, '$.latestGrantReissueDate') AS ce_latest_grant_reissue_date
		, JSON_VALUE(ce_data, '$.reissueReasonNotation') AS ce_reissue_reason
		, JSON_VALUE(ce_data, '$.languagePreferenceWelsh') AS ce_welsh_lang_pref
		FROM latest_grant_event
	), 
	grant_case_attributes
	AS
	(
		SELECT  *
		FROM( 
			SELECT CAST(legal_case_id AS VARCHAR) AS legal_case_id
			, CAST(case_metadata_event_id AS VARCHAR) AS case_metadata_event_id
			, CAST(legal_case_ref_cid AS VARCHAR) AS legal_case_ref_cid
			, CAST(ce_app_type AS VARCHAR) AS ce_app_type
			, CAST(ce_reg_location AS VARCHAR) AS ce_reg_location
			, CAST(ce_deceased_dod AS VARCHAR) AS ce_deceased_dod
			, CAST(ce_app_sub_date AS VARCHAR) AS ce_app_sub_date
			, CAST(ce_will_exists AS VARCHAR) AS ce_will_exists
			, CAST(ce_deceased_other_names AS VARCHAR) AS ce_deceased_other_names
			, CAST(ce_case_stop_reason AS VARCHAR) AS ce_case_stop_reason
			, CAST(ce_gor_case_type AS VARCHAR) AS ce_gor_case_type
			, CAST(ce_grantissued_date AS VARCHAR) AS ce_grantissued_date
			, CAST(ce_leg_record_id AS VARCHAR) AS ce_leg_record_id
			, CAST(ce_paperform AS VARCHAR) AS ce_paperform
			, CAST(ce_iht_net_value AS VARCHAR) AS ce_iht_net_value
			, CAST(ce_iht_gross_value AS VARCHAR) AS ce_iht_gross_value
			, CAST(ce_latest_grant_reissue_date AS VARCHAR) AS ce_latest_grant_reissue_date
			, CAST(ce_reissue_reason AS VARCHAR) AS ce_reissue_reason
			, CAST(ce_welsh_lang_pref AS VARCHAR) AS ce_welsh_lang_pref
			FROM all_char_cols
		) p
		UNPIVOT
		( 
		column_val FOR original_column_name IN( 
			ce_app_type
			, ce_reg_location
			, ce_deceased_dod
			, ce_app_sub_date
			, ce_will_exists
			, ce_deceased_other_names
			, ce_case_stop_reason
			, ce_gor_case_type
			, ce_grantissued_date
			, ce_leg_record_id
			, ce_paperform
			, ce_iht_net_value
			, ce_iht_gross_value
			, ce_latest_grant_reissue_date
			, ce_reissue_reason 
			, ce_welsh_lang_pref 
			)                                     
		) AS pvt
	)  
	SELECT sc.legal_case_id,
			ak.attr_key,
			sc.legal_case_ref_cid,
			CAST(column_val AS VARCHAR(255)) AS column_val
	INTO #Temp
	FROM grant_case_attributes sc 
	JOIN mdm_attr_type ak 
	ON lower(sc.original_column_name) = ak.attr_original_name
	WHERE ak.jurisdiction_code IN ('PROBATE', 'ALL');

	UPDATE tbl_legal_case_attr 
	SET attr_value_text = src.column_val,
		bis_last_modified_datetime = GETDATE(),
		modified_by_process_name = 'sp_legal_grant_case_attr',
		legal_case_ref_cid = src.legal_case_ref_cid    
	FROM #Temp src
	WHERE src.legal_case_id = tbl_legal_case_attr.legal_case_id 
	AND src.attr_key = tbl_legal_case_attr.attr_key;

	INSERT INTO tbl_legal_case_attr
	(
		legal_case_id,
		legal_case_type_name,
		attr_key,
		attr_value_text,
		bis_first_created_datetime,
		inserted_by_process_name,
		source_system_type_code,
		legal_case_ref_cid
	)
	SELECT SRC.LEGAL_CASE_ID,
		'GrantOfRepresentation',
		src.attr_key,
		src.column_val,
		getdate(),
		'sp_legal_grant_case_attr',
		'CCD',
		src.legal_case_ref_cid
	FROM #Temp src
	WHERE CAST(src.legal_case_id AS VARCHAR(255))  + ' ' + CAST(src.attr_key AS VARCHAR(255)) NOT IN(
		SELECT CAST(legal_case_id AS VARCHAR(255)) + ' ' + CAST(attr_key AS VARCHAR(255)) FROM tbl_legal_case_attr
	);

	DROP TABLE #Temp;

/*
	--Caveat
	WITH sorted_caveat_event
	AS
	(
		SELECT legal_case_id AS ce_case_data_id,
				legal_case_event_id AS ce_id,
				legal_case_reference_id AS cd_reference,
				ROW_NUMBER() OVER (PARTITION BY legal_case_id ORDER BY legal_case_event_id DESC) rownumber
		FROM   tbl_legal_case_event
		WHERE  legal_case_type_name = 'Caveat'
	), 
	latest_caveat_event
	AS
	(
		SELECT *
		FROM sorted_caveat_event
		WHERE rownumber = 1
	), 
	all_char_cols
	AS
	(
		SELECT 
			lg.ce_case_data_id AS legal_case_id,
			convert(varchar, case_metadata_event_id) as case_metadata_event_id,
			convert(varchar, ce_app_type) as ce_app_type,
			convert(varchar, ce_reg_location) as ce_reg_location,
			ce_deceased_dod AS ce_deceased_dod,
			convert(varchar, ce_paperform) AS ce_paperform,
			ce_expiry_date AS ce_expiry_date,
			convert(varchar, ce_leg_record_id) AS ce_leg_record_id,
			ce_app_sub_date AS ce_app_sub_date,
			convert(varchar, cd_reference) as legal_case_ref_cid
		from stg_ccd_probatecaveat pc 
		join latest_caveat_event lg 
		on pc.case_metadata_event_id = lg.ce_id 
	), 
	caveat_case_attributes
	AS
	(
		SELECT  *
		FROM( 
			SELECT legal_case_id
			, case_metadata_event_id
			, ce_app_type
			, ce_reg_location
			, CAST(ce_deceased_dod AS VARCHAR) AS ce_deceased_dod
			, ce_paperform
			, CAST(ce_expiry_date AS VARCHAR) AS ce_expiry_date
			, ce_leg_record_id
			, CAST(ce_app_sub_date AS VARCHAR) AS ce_app_sub_date
			, legal_case_ref_cid
			FROM all_char_cols
		) p
		UNPIVOT
		( 
		column_val FOR original_column_name IN( 
			case_metadata_event_id
			, ce_app_type
			, ce_reg_location
			, ce_deceased_dod
			, ce_paperform
			, ce_expiry_date
			, ce_leg_record_id
			, ce_app_sub_date 
			)        
		) AS pvt
	)  
	SELECT sc.legal_case_id,
			ak.attr_key,
			sc.legal_case_ref_cid,
			column_val
	INTO #Temp
	FROM caveat_case_attributes sc 
	JOIN mdm_attr_type ak 
	ON  lower(sc.original_column_name) = ak.attr_original_name    
	WHERE ak.jurisdiction_code = 'PROBATE';

	UPDATE tbl_legal_case_attr
	SET attr_value_text = src.column_val,
		bis_last_modified_datetime = GETDATE(),
		modified_by_process_name = 'sp_legal_probatesrch_case_attr',
		legal_case_ref_cid = src.legal_case_ref_cid
	FROM #Temp src
	WHERE src.legal_case_id = tbl_legal_case_attr.legal_case_id 
	AND src.attr_key = tbl_legal_case_attr.attr_key;

	INSERT INTO tbl_legal_case_attr
	(
		legal_case_id,
		legal_case_type_name,
		attr_key,
		attr_value_text,
		bis_first_created_datetime,
		inserted_by_process_name,
		source_system_type_code,
		legal_case_ref_cid
	)
	SELECT SRC.LEGAL_CASE_ID,
		'Caveat',
		src.attr_key,
		src.column_val,
		getdate(),
		'sp_legal_caveat_case_attr',
		'CCD',
		src.legal_case_ref_cid
	FROM #Temp src
	WHERE CAST(src.legal_case_id AS VARCHAR(255))  + ' ' + src.attr_key NOT IN(
		SELECT CAST(legal_case_id AS VARCHAR(255))  + ' ' + attr_key FROM tbl_legal_case_attr
	);

	DROP TABLE #Temp;


	--Search
	WITH sorted_standing_search_event
	AS
	(
		SELECT legal_case_id AS ce_case_data_id,
				legal_case_event_id AS ce_id,
				legal_case_reference_id AS cd_reference,
				ROW_NUMBER() OVER (PARTITION BY legal_case_id ORDER BY legal_case_event_id DESC) rownumber
		FROM   tbl_legal_case_event
		WHERE  legal_case_type_name = 'StandingSearch'
	), 
	latest_standing_search_event
	AS
	(
		SELECT *
		FROM sorted_standing_search_event
		WHERE rownumber = 1
	), 
	all_char_cols
	AS
	(
		SELECT 
			lwl.ce_case_data_id AS legal_case_id,
			convert(varchar, ce_app_type) as ce_app_type,
			convert(varchar, ce_reg_location) AS ce_reg_location,
			ce_expiry_date AS ce_expiry_date,
			convert(varchar, loaded_datetime) as loaded_datetime,
			convert(varchar, ce_leg_record_id) AS ce_leg_record_id,
			ce_app_sub_date AS ce_app_sub_date,
			convert(varchar, cd_reference) as legal_case_ref_cid
		from stg_ccd_probatesrch pwl 
		join latest_standing_search_event lwl 
		on pwl.case_metadata_event_id = lwl.ce_id 
	), 
	standingsearch_case_attributes
	AS
	(
		SELECT  *
		FROM(
			SELECT legal_case_id
			, ce_app_type
			, ce_reg_location
			, CAST(ce_expiry_date AS VARCHAR) AS ce_expiry_date
			, loaded_datetime
			, ce_leg_record_id
			, CAST(ce_app_sub_date AS VARCHAR) AS ce_app_sub_date
			, legal_case_ref_cid
			FROM all_char_cols
		) p
		UNPIVOT
		( 
		column_val FOR original_column_name IN(
			ce_app_type
			, ce_reg_location
			, ce_expiry_date
			, loaded_datetime
			, ce_leg_record_id
			, ce_app_sub_date 
			) 
		) AS pvt
	)
	SELECT sc.legal_case_id,
			ak.attr_key,
			sc.legal_case_ref_cid,
			column_val
	INTO #Temp
	FROM standingsearch_case_attributes sc 
	JOIN mdm_attr_type ak 
	ON  lower(sc.original_column_name) = ak.attr_original_name    
	WHERE ak.jurisdiction_code = 'PROBATE';

	UPDATE tbl_legal_case_attr 
	SET attr_value_text = src.column_val,
		bis_last_modified_datetime = GETDATE(),
		modified_by_process_name = 'sp_legal_probatesrch_case_attr',
		legal_case_ref_cid = src.legal_case_ref_cid      
	FROM #Temp src
	WHERE src.legal_case_id = tbl_legal_case_attr.legal_case_id 
	AND src.attr_key = tbl_legal_case_attr.attr_key;

	INSERT INTO tbl_legal_case_attr
	(
		legal_case_id,
		legal_case_type_name,
		attr_key,
		attr_value_text,
		bis_first_created_datetime,
		inserted_by_process_name,
		source_system_type_code,
		legal_case_ref_cid
	)
	SELECT SRC.LEGAL_CASE_ID,
		'StandingSearch',
		src.attr_key,
		src.column_val,
		getdate(),
		'sp_legal_probatesrch_case_attr',
		'CCD',
		src.legal_case_ref_cid     
	FROM #Temp src
	WHERE CAST(src.legal_case_id AS VARCHAR(255))  + ' ' + src.attr_key NOT IN(
		SELECT CAST(legal_case_id AS VARCHAR(255)) + ' ' + attr_key FROM tbl_legal_case_attr
	);

	DROP TABLE #Temp;


	--Will Lodgement
	WITH sorted_will_lodgment_event
	AS
	(
		SELECT legal_case_id AS ce_case_data_id,
				legal_case_event_id AS ce_id,
				legal_case_reference_id AS cd_reference,
				ROW_NUMBER() OVER (PARTITION BY legal_case_id ORDER BY legal_case_event_id DESC) rownumber
		FROM   tbl_legal_case_event
		WHERE  legal_case_type_name = 'WillLodgement'
	), latest_will_lodgment_event
	AS
	(
		SELECT *
		FROM sorted_will_lodgment_event
		WHERE rownumber = 1
	), all_char_cols
	AS
	(
		SELECT 
			lwl.ce_case_data_id AS legal_case_id,
			convert(varchar, ce_leg_record_id) as ce_leg_record_id,
			convert(varchar, md_insert_time) as md_insert_time,
			convert(varchar, case_metadata_event_id) as case_metadata_event_id,
			convert(varchar, ce_app_type) as ce_app_type,
			convert(varchar, ce_reg_location) as ce_reg_location,
			convert(varchar, ce_lodgement_type) as ce_lodgement_type,
			ce_lodgement_date AS ce_lodgement_date,
			convert(varchar, ce_withdrawal_reason) AS ce_withdrawal_reason,
			convert(varchar, cd_reference) as legal_case_ref_cid
		FROM stg_ccd_probatewilllodgement pwl 
		JOIN latest_will_lodgment_event lwl 
		ON pwl.case_metadata_event_id = lwl.ce_id 
	), willlodgment_case_atttributes
	AS
	(
		SELECT *
		FROM(
			SELECT legal_case_id
			, ce_leg_record_id
			, md_insert_time
			, case_metadata_event_id
			, ce_app_type
			, ce_reg_location
			, ce_lodgement_type
			, CAST(ce_lodgement_date AS VARCHAR) AS ce_lodgement_date
			, ce_withdrawal_reason
			, legal_case_ref_cid
			FROM all_char_cols
		) p
		UNPIVOT
		( 
		column_val FOR original_column_name IN(
			md_insert_time
			, case_metadata_event_id
			, ce_app_type
			, ce_reg_location
			, ce_lodgement_type
			, ce_lodgement_date
			, ce_withdrawal_reason
			, ce_leg_record_id 
			) 
		) AS pvt
	)
	SELECT wc.legal_case_id,
			ak.attr_key,
			wc.legal_case_ref_cid,
			column_val
	INTO #Temp
	FROM willlodgment_case_atttributes wc 
	JOIN mdm_attr_type ak 
	ON lower(wc.original_column_name) = ak.attr_original_name
	WHERE ak.jurisdiction_code = 'PROBATE';

	UPDATE tbl_legal_case_attr 
	SET attr_value_text = src.column_val,
		bis_last_modified_datetime = GETDATE(),
		modified_by_process_name = 'sp_legal_willlodgement_case_attr',
		legal_case_ref_cid = src.legal_case_ref_cid
	FROM #Temp src
	WHERE src.legal_case_id = tbl_legal_case_attr.legal_case_id 
	AND src.attr_key = tbl_legal_case_attr.attr_key;

	INSERT INTO tbl_legal_case_attr
	(
		legal_case_id,
		legal_case_type_name,
		attr_key,
		attr_value_text,
		bis_first_created_datetime,
		inserted_by_process_name,
		source_system_type_code,
		legal_case_ref_cid
	)
	SELECT src.legal_case_id,
		'WillLodgement',
		src.attr_key,
		src.column_val,
		GETDATE(),
		'sp_legal_willlodgement_case_attr',
		'CCD',
		src.legal_case_ref_cid
	FROM #Temp src
	WHERE CAST(src.legal_case_id AS VARCHAR(255))  + ' ' + src.attr_key NOT IN(
		SELECT CAST(legal_case_id AS VARCHAR(255)) + ' ' + attr_key FROM tbl_legal_case_attr
	);

	DROP TABLE #Temp;
*/
END
