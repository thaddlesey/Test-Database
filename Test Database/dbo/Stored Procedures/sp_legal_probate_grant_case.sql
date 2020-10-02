CREATE PROC [sp_legal_probate_grant_case] AS
  BEGIN

    WITH 
    case_types 
    AS
    (
	SELECT lpgc.legal_case_id, 
	case_type_key,
	3.001511 AS default_case_type_key,
	lpgc.bis_first_created_datetime,
	lpgc.bis_last_modified_datetime,
	lpgc.applctn_submtd_role_type_name,
	lpgc.registry_location_name,
	lpgc.deceased_death_date,
	lpgc.applctn_paperform_ind,
	lpgc.applctn_submtd_date,
	lpgc.will_exists_ind,
	lpgc.estate_net_value_amount,
	lpgc.estate_gross_value_amount,
	lpgc.assets_held_int_othr_name_ind,
	lpgc.stop_reason_event_type_name,
	gor_case_type_name,
	lpgc.grant_issued_date,
	lpgc.legacy_case_record_id,
	lpgc.latest_reissue_date,
	lpgc.reissue_reason_type_name,
	CASE 
		WHEN lpgc.applctn_paperform_ind = 'No' AND lpgc.will_exists_ind = 'Yes' THEN 'Yes'
		ELSE 'No'
	END AS digal_will_ind,
	lpgc.legal_case_ref_cid AS ccd_case_ref_cid,
	CASE 
		WHEN UPPER(ISNULL(welsh_lang_pref_ind,'NO')) = 'YES' THEN 'Yes'
		ELSE 'No' 
	END AS welsh_lang_cind,
	CASE 
		WHEN UPPER(ISNULL(welsh_lang_pref_ind,'NO')) = 'YES' THEN 59.000002 --59.000002 -- Welsh / wel
		ELSE 59.000001 --59.000001 -- English (or not specified) / eng
	END AS prefrd_lang_type_key            
    FROM v_legal_probate_grant_case lpgc
    LEFT OUTER JOIN mdm_probate_case_type_lu mpct ON mpct.ce_gor_case_type = ISNULL(lpgc.gor_case_type_name, 'Not Specified')
    ),
    -- Pick rownumber = 1 record which is earliest event created for case
    earliest_cases
    AS
    (
	SELECT m.legal_case_id AS ce_case_data_id,
    MIN(m.event_created_timestamp) AS case_created_date
    FROM v_legal_probategrant_case_evt m 
    WHERE  m.legal_case_event_type_cid NOT IN (
		SELECT source_event_type_cid 
		FROM v_prbtgrant_case_event_grps 
		WHERE event_type_grp_key = 35.000028
	)
    GROUP BY m.legal_case_id
    ),
    event_dates
    AS
    (
	SELECT m.legal_case_id AS ce_case_data_id ,
    MIN(CASE WHEN m.legal_case_state_type_code = 'BOReadyForExamination' AND eg.event_type_grp_key = 35.000003 THEN m.event_created_timestamp END) AS case_examined_first_date,
    MIN(CASE WHEN m.legal_case_state_type_code IN ('CaseCreated', 'BOCaseImported', 'PAAppCreated') AND eg.event_type_grp_key = 35.000004 THEN m.event_created_timestamp END) AS case_submitted_event_date,
    MIN(CASE WHEN eg.event_type_grp_key = 35.000002 THEN m.event_created_timestamp END) AS case_withdrawn_first_date,
    MIN(CASE WHEN m.legal_case_state_type_code = 'BOCaseStopped' AND eg.event_type_grp_key = 35.000005 THEN m.event_created_timestamp END) AS case_stopped_first_date,
    MIN(CASE WHEN eg.event_type_grp_key = 35.000025 THEN m.event_created_timestamp END) AS doc_recvd_date
    FROM v_legal_probategrant_case_evt m
    JOIN v_prbtgrant_case_event_grps eg 
	ON m.legal_case_event_type_cid = eg.source_event_type_cid
    WHERE eg.case_type_key = 3.001511
    GROUP BY m.legal_case_id
    ),
    submitted_overall
    AS
    (
    --- 1. Use the submission date from events where the case attribute submission date is blank
    --- 2. Always use the case attribute submission date, if stated, for digital cases
    --- 3. For paper assumed paper cases, if the case attribute submission date is before the date of death, use the submission date from the events if possible, otherwise use the case creation date
    --- 4. For paper assumed paper cases, if the application submission date is after the case creation date, use the submission date from the events if possible, otherwise use the case creation date
    SELECT m.legal_case_id,
    CASE 
		WHEN m.applctn_submtd_date IS NULL THEN se.case_submitted_event_date
		WHEN m.applctn_paperform_ind = 'No' THEN ISNULL(m.applctn_submtd_date, se.case_submitted_event_date)
		WHEN (m.applctn_paperform_ind IS NULL OR m.applctn_paperform_ind = 'Yes')
			AND m.deceased_death_date IS NOT NULL 
			AND m.applctn_submtd_date < m.deceased_death_date
			AND se.case_submitted_event_date >= m.deceased_death_date THEN se.case_submitted_event_date
		WHEN (m.applctn_paperform_ind IS NULL OR m.applctn_paperform_ind = 'Yes') 
			AND m.deceased_death_date IS NOT NULL 
			AND m.applctn_submtd_date < m.deceased_death_date
			AND (se.case_submitted_event_date IS NULL OR se.case_submitted_event_date < m.deceased_death_date) THEN ec.case_created_date
		WHEN (m.applctn_paperform_ind IS NULL OR m.applctn_paperform_ind = 'Yes') 
			AND m.applctn_submtd_date > ec.case_created_date
			AND se.case_submitted_event_date IS NOT NULL AND se.case_submitted_event_date <= ec.case_created_date THEN se.case_submitted_event_date
		WHEN (m.applctn_paperform_ind IS NULL OR m.applctn_paperform_ind = 'Yes') 
			AND  m.applctn_submtd_date > ec.case_created_date
			AND (se.case_submitted_event_date IS NULL OR se.case_submitted_event_date > ec.case_created_date) THEN ec.case_created_date
		ELSE ISNULL(m.applctn_submtd_date, se.case_submitted_event_date)
    END case_submitted_overall_date
    FROM case_types m
    LEFT JOIN earliest_cases ec  
	ON m.legal_case_id = ec.ce_case_data_id
	LEFT JOIN event_dates se 
	ON se.ce_case_data_id = m.legal_case_id AND se.case_submitted_event_date IS NOT NULL
    ), 
    -- Get number of stops for each case.
    stopped_cases
    AS 
    (
    -- v1.1 - SG - Use the link table to get the matching Events for the given Event Group and Case Type
    SELECT m.legal_case_id AS ce_case_data_id
	, count(*) AS stops_count
    FROM  v_legal_probate_case_event m 
    INNER JOIN v_prbtgrant_case_event_grps eg 
	ON m.legal_case_event_type_cid = eg.source_event_type_cid 
    INNER JOIN case_types pgcs 
	ON m.legal_case_id = pgcs.legal_case_id 
	AND  pgcs.default_case_type_key = eg.case_type_key
    WHERE eg.event_type_grp_key = 35.000005  -- 35.000005 -- Case Stopped
    AND m.legal_case_type_name = 'GrantOfRepresentation'
    GROUP BY m.legal_case_id
    ),
    --Derive registrar escalation attributes from v_legal_probategrant_state, for the GrantOfRepresentation case type where state = BORegistrarEscalation
    --Note that the duration of the escalation is taken from the day on which the transformation is run where the state end date is null (i.e. case was still with the Registrar when the latest data was received)
    registrar_escalation
    AS
    (
    SELECT lcs.legal_case_id, 
    CASE 
		WHEN COUNT(*) > 0 THEN 1 
		ELSE 0 
	END AS registrar_escltn_ind,
    COUNT(*) AS registrar_escltn_count,      
	SUM(isnull(DATEDIFF(dd ,lcs.state_start_timestamp, isnull(lcs.legal_case_state_end_date, GETDATE())),0)) AS registrar_escltn_dur_day_count
    FROM v_legal_probategrant_state lcs
    WHERE lcs.legal_case_state_type_code = 'BORegistrarEscalation' 
    GROUP BY lcs.legal_case_id
    ), 
	submitted_issued_days   --- v1.2
    AS
    ( 
	SELECT pg.legal_case_id,
    CASE 
		WHEN so.case_submitted_overall_date IS NULL OR so.case_submitted_overall_date > pg.grant_issued_date THEN (
			SELECT count(*) 
			FROM dim_date d 
			WHERE d.working_day_flag = 'Y' 
			AND d.date_key BETWEEN (DATEADD(DAY, 1, ec.case_created_date)) AND pg.grant_issued_date
		)
		ELSE (
			SELECT count(*) 
			FROM dim_date d
			WHERE d.working_day_flag = 'Y' 
			AND d.date_key BETWEEN (DATEADD(DAY, 1, so.case_submitted_overall_date)) AND pg.grant_issued_date
		)
    END AS submtd_to_issued_wrkgdy_count,
    CASE 
		WHEN so.case_submitted_overall_date IS NULL OR so.case_submitted_overall_date > pg.grant_issued_date THEN DATEDIFF(dd, ec.case_created_date, pg.grant_issued_date)
        ELSE DATEDIFF(dd , so.case_submitted_overall_date, pg.grant_issued_date)
    END AS submitted_to_issued_days_count


    FROM case_types pg
    JOIN earliest_cases ec 
	ON pg.legal_case_id = ec.ce_case_data_id
    LEFT OUTER JOIN submitted_overall so 
	ON pg.legal_case_id = so.legal_case_id
    WHERE pg.grant_issued_date is not null
    ), 
	case_doc_date  -- v1.2
    AS   
    (   
	SELECT pgc.legal_case_id ce_case_data_id,
    --derive the doc date for digital applications with a will only
    CASE 
		WHEN pgc.digal_will_ind = 'Yes' THEN dc.doc_recvd_date
        ELSE NULL
    END AS case_doc_recvd_date,
    CASE 
        --for digital applications with a will use submission to issue if the grant has been issued and the doc received date is out of sequence:
        WHEN pgc.grant_issued_date IS NOT NULL AND pgc.digal_will_ind = 'Yes'
			AND so.case_submitted_overall_date IS NOT NULL AND dc.doc_recvd_date IS NOT NULL AND dc.doc_recvd_date > pgc.grant_issued_date
			THEN DATEDIFF(dd, so.case_submitted_overall_date, pgc.grant_issued_date)
        --for digital applications with a will use the created date if the grant has been issued and the submitted date is out of sequence:
        WHEN pgc.grant_issued_date IS NOT NULL AND pgc.digal_will_ind = 'Yes' AND dc.doc_recvd_date IS NOT NULL 
			AND ( so.case_submitted_overall_date IS NULL OR so.case_submitted_overall_date > pgc.grant_issued_date ) 
			THEN DATEDIFF(dd, ec.case_created_date, dc.doc_recvd_date)
        --for digital applications with a will set to 0 if the dates are out of sequence:
        WHEN pgc.digal_will_ind = 'Yes' 
			AND so.case_submitted_overall_date IS NOT NULL AND dc.doc_recvd_date IS NOT NULL 
			AND dc.doc_recvd_date <= so.case_submitted_overall_date
			THEN 0  
        --for digital applications with a will calculate the difference if both dates are stated:                 
        WHEN pgc.digal_will_ind = 'Yes' 
			AND so.case_submitted_overall_date IS NOT NULL AND dc.doc_recvd_date IS NOT NULL 
			THEN DATEDIFF(dd, so.case_submitted_overall_date, dc.doc_recvd_date)
		--in all other cases where the grant has issued, set the timeliness to 0:
		WHEN pgc.grant_issued_date IS NOT NULL 
			THEN 0
		--for paper cases or digital intestacy with a submission date, set the timeliness to 0:
		WHEN so.case_submitted_overall_date IS NOT NULL 
			AND ( pgc.applctn_paperform_ind = 'Yes' OR pgc.applctn_paperform_ind IS NULL OR pgc.will_exists_ind = 'No' OR pgc.will_exists_ind IS NULL ) 
			THEN 0
	END AS submsn_to_doc_recpt_days_count,
    CASE 
		--for digital applications with a will use submission to issue if the grant has been issued and the doc received date is out of sequence:
		WHEN pgc.grant_issued_date IS NOT NULL AND pgc.digal_will_ind = 'Yes'
			AND so.case_submitted_overall_date IS NOT NULL 
			AND dc.doc_recvd_date IS NOT NULL 
			AND dc.doc_recvd_date > pgc.grant_issued_date 
			THEN (
				SELECT count(*) 
				FROM dim_date d 
				WHERE d.working_day_flag = 'Y' 
				AND d.date_key BETWEEN DATEADD(DAY, 1, so.case_submitted_overall_date) AND pgc.grant_issued_date
			)
		--for digital applications with a will use the created date if the grant has been issued and the submitted date is out of sequence:
		WHEN pgc.grant_issued_date IS NOT NULL AND pgc.digal_will_ind = 'Yes' 
			AND dc.doc_recvd_date IS NOT NULL 
			AND ( so.case_submitted_overall_date IS NULL OR so.case_submitted_overall_date > pgc.grant_issued_date )
			THEN (
				SELECT count(*) 
				FROM dim_date d 
				WHERE d.working_day_flag = 'Y' 
				AND d.date_key BETWEEN DATEADD(DAY, 1, ec.case_created_date) AND dc.doc_recvd_date
			)
		--for digital applications with a will set to 0 if the dates are out of sequence:
		WHEN pgc.digal_will_ind = 'Yes' 
			AND so.case_submitted_overall_date IS NOT NULL 
			AND dc.doc_recvd_date IS NOT NULL 
			AND dc.doc_recvd_date <= so.case_submitted_overall_date
			THEN 0  
		--for digital applications with a will calculate the difference if both dates are stated:                 
		WHEN pgc.digal_will_ind = 'Yes'
			AND so.case_submitted_overall_date IS NOT NULL AND dc.doc_recvd_date IS NOT NULL
			THEN (
				SELECT count(*) 
				FROM dim_date d 
				WHERE d.working_day_flag = 'Y' 
				AND d.date_key BETWEEN DATEADD(DAY, 1, so.case_submitted_overall_date) AND dc.doc_recvd_date
			)
		--in all other cases where the grant has issued, set the timeliness to 0:
		WHEN pgc.grant_issued_date IS NOT NULL
			THEN 0
		--for paper cases or digital intestacy with a submission date, set the timeliness to 0:
		WHEN so.case_submitted_overall_date IS NOT NULL 
			AND ( pgc.applctn_paperform_ind = 'Yes' OR pgc.applctn_paperform_ind IS NULL OR pgc.will_exists_ind = 'No' OR pgc.will_exists_ind IS NULL )
			THEN 0
    END AS submsn_to_doc_recpt_wkdy_count,
    CASE  
		--for digital applications with a will set to 0 if the grant has been issued and the doc received date is out of sequence:
		WHEN pgc.grant_issued_date IS NOT NULL AND pgc.digal_will_ind = 'Yes'
			AND so.case_submitted_overall_date IS NOT NULL AND dc.doc_recvd_date IS NOT NULL AND dc.doc_recvd_date > pgc.grant_issued_date
			THEN 0
		--for digital applications with a will calculate the difference from submission if both dates are stated but doc received is before submission:
		WHEN pgc.grant_issued_date IS NOT NULL AND dc.doc_recvd_date IS NOT NULL 
			AND pgc.digal_will_ind = 'Yes' AND dc.doc_recvd_date < so.case_submitted_overall_date
			THEN DATEDIFF(dd, so.case_submitted_overall_date, pgc.grant_issued_date)
		--for digital applications with a will calculate the difference if both dates are stated:
		WHEN pgc.grant_issued_date IS NOT NULL AND dc.doc_recvd_date IS NOT NULL 
			AND pgc.digal_will_ind = 'Yes'
			THEN DATEDIFF(dd, dc.doc_recvd_date, pgc.grant_issued_date)
		--use the case created date if the submission date is null or out of sequence:
		WHEN pgc.grant_issued_date IS NOT NULL 
			AND ( so.case_submitted_overall_date IS NULL OR so.case_submitted_overall_date > pgc.grant_issued_date )
			THEN DATEDIFF(dd, ec.case_created_date, pgc.grant_issued_date)
		--normal calculation:
		WHEN pgc.grant_issued_date IS NOT NULL
			THEN DATEDIFF(dd, so.case_submitted_overall_date, pgc.grant_issued_date)
    END AS doc_recpt_to_issue_days_count,
    CASE
		--for digital applications with a will set to 0 if the grant has been issued and the doc received date is out of sequence:
		WHEN pgc.grant_issued_date IS NOT NULL AND pgc.digal_will_ind = 'Yes'
			AND so.case_submitted_overall_date IS NOT NULL AND dc.doc_recvd_date IS NOT NULL AND dc.doc_recvd_date > pgc.grant_issued_date
			THEN 0
			--for digital applications with a will calculate the difference from submission if both dates are stated but doc received is before submission:
		WHEN pgc.grant_issued_date IS NOT NULL AND dc.doc_recvd_date IS NOT NULL 
			AND pgc.digal_will_ind = 'Yes' AND dc.doc_recvd_date < so.case_submitted_overall_date
			THEN (
				SELECT count(*) 
				FROM dim_date d 
				WHERE d.working_day_flag = 'Y' 
				AND d.date_key BETWEEN DATEADD(DAY, 1, so.case_submitted_overall_date) AND pgc.grant_issued_date
			)
			--for digital applications with a will calculate the difference if both dates are stated:
		WHEN pgc.grant_issued_date IS NOT NULL AND dc.doc_recvd_date IS NOT NULL 
			AND pgc.digal_will_ind = 'Yes'
			THEN (
				SELECT count(*) 
				FROM dim_date d 
				WHERE d.working_day_flag = 'Y' 
				AND d.date_key BETWEEN DATEADD(DAY, 1, dc.doc_recvd_date) AND pgc.grant_issued_date
			)
			--use the case created date if the submission date is null or out of sequence:
		WHEN pgc.grant_issued_date IS NOT NULL 
			AND ( so.case_submitted_overall_date IS NULL OR so.case_submitted_overall_date > pgc.grant_issued_date )
			THEN (
				SELECT count(*) 
				FROM dim_date d 
				WHERE d.working_day_flag = 'Y' 
				AND d.date_key BETWEEN DATEADD(DAY, 1, ec.case_created_date) AND pgc.grant_issued_date
			)
			--normal calculation:
		WHEN pgc.grant_issued_date IS NOT NULL
			THEN (
				SELECT count(*) 
				FROM dim_date d 
				WHERE d.working_day_flag = 'Y' 
				AND d.date_key BETWEEN DATEADD(DAY, 1, so.case_submitted_overall_date) AND pgc.grant_issued_date
			)
    END AS doc_recpt_to_issue_wkdy_count
    FROM case_types pgc
    JOIN earliest_cases ec  
	ON pgc.legal_case_id = ec.ce_case_data_id
    LEFT JOIN event_dates dc 
	ON pgc.legal_case_id = dc.ce_case_data_id 
	AND dc.doc_recvd_date IS NOT NULL /* APD-6894 */
    LEFT JOIN submitted_overall so 
	ON pgc.legal_case_id = so.legal_case_id
    ) ,  
	case_restart --- v1.6                   
    AS
    (
		SELECT m.legal_case_id AS ce_case_data_id ,
		min(m.event_created_timestamp) as wrk_restrt_from_stop_date
		FROM v_legal_probategrant_case_evt m
		INNER JOIN v_prbtgrant_case_event_grps eg 
		ON m.legal_case_event_type_cid = eg.source_event_type_cid 
		INNER JOIN case_types pgcs 
		ON m.legal_case_id = pgcs.legal_case_id 
		and pgcs.default_case_type_key = eg.case_type_key
		INNER JOIN event_dates st 
		ON st.ce_case_data_id = m.legal_case_id
		WHERE eg.event_type_grp_key = 35.000036 --  Case Work Restart After Stop
		AND st.case_stopped_first_date < m.event_created_timestamp       -- After stop date to pick restart date
		GROUP BY m.legal_case_id
    ),  
	start_doc_recvd --- v1.6                    
    AS
    (
		SELECT m.legal_case_id AS ce_case_data_id ,
		min(m.event_created_timestamp) as wrk_strtd_from_doc_recvd_date,
		min (cdd.case_doc_recvd_date) case_doc_recvd_date
		FROM v_legal_probategrant_case_evt m
		INNER JOIN v_prbtgrant_case_event_grps eg 
		ON m.legal_case_event_type_cid = eg.source_event_type_cid 
		INNER JOIN case_types pgcs 
		ON m.legal_case_id = pgcs.legal_case_id 
		and pgcs.default_case_type_key = eg.case_type_key
		INNER JOIN case_doc_date cdd 
		ON m.legal_case_id = cdd.ce_case_data_id
		WHERE eg.event_type_grp_key =  35.000037 --35.000037 --  Case Work Start After Document Upload
		AND cdd.case_doc_recvd_date < m.event_created_timestamp             -- it must be after case_doc_recvd_date
		GROUP BY m.legal_case_id
    ), 
	recvd_restart_wdays -- v1.6
    AS
    ( 
    SELECT ce_case_data_id ,
    wrk_strtd_from_doc_recvd_date,
    case_doc_recvd_date,
    (
		SELECT count(*) 
		FROM dim_date d 
		WHERE d.working_day_flag = 'Y' 
		AND d.date_key BETWEEN DATEADD(DAY, 1, case_doc_recvd_date) AND wrk_strtd_from_doc_recvd_date
	) doc_recvd_to_strtd_wrkdy_durtn
    FROM start_doc_recvd      
    )
    select  case 
		when pgc.legacy_case_record_id is not null then 9.000019 
		else 9.000016 
	end as source_key,
	pgc.case_type_key,
	pl.location_key,
	pat.probate_applctn_type_key,
	pgc.applctn_paperform_ind,
	pgc.applctn_submtd_date,
	pgc.applctn_submtd_role_type_name,
	pgc.assets_held_int_othr_name_ind,
	pgc.deceased_death_date,
	pgc.estate_gross_value_amount,
	pgc.estate_net_value_amount,
	pgc.gor_case_type_name,
	pgc.grant_issued_date ,
	pgc.legacy_case_record_id,
	pgc.legal_case_id,
	pgc.registry_location_name,
	CASE 
		WHEN (s.case_stopped_first_date IS NOT NULL AND ((pgc.stop_reason_event_type_name IS NULL) OR (pgc.stop_reason_event_type_name = '[]'))) THEN 'Other' 
		ELSE pgc.stop_reason_event_type_name 
	END AS stop_reason_event_type_name, --1.5 DJ
    pgc.will_exists_ind,
    isnull(sc.stops_count,0) AS stops_count,
    pgc.latest_reissue_date , 
    pgc.reissue_reason_type_name, 
    so.case_submitted_overall_date AS case_submitted_date,  
    -- Application submitted date is being used to 
    -- set submitted indicator 
    -- If application submitted date is not available it will use
    -- case submitted date
    CASE WHEN so.case_submitted_overall_date IS NOT NULL THEN 1 ELSE 0 END AS applctn_submitted_ind,
    s.case_examined_first_date,
    CASE WHEN s.case_examined_first_date IS NOT NULL THEN 1 ELSE 0 END AS applctn_examined_ind,
    s.case_stopped_first_date,
    CASE WHEN s.case_stopped_first_date IS NOT NULL THEN 1 ELSE 0 END AS applctn_stopped_ind,
    case when pgc.grant_issued_date is not null then 1 else 0 end applctn_issued_ind,
    CASE WHEN pgc.latest_reissue_date IS NOT NULL THEN 1 ELSE 0 END reissued_ind,
    -- Probate Counts the day of Receipt as Day 1
    CASE 
		WHEN (pgc.grant_issued_date IS NOT NULL AND so.case_submitted_overall_date IS NOT NULL) AND (DATEDIFF(dd, so.case_submitted_overall_date, pgc.grant_issued_date)) + 1 <= 20 THEN '1' 
		ELSE '0' 
	END AS issued_in_20days_ind,
    siw.submitted_to_issued_days_count,  
    ec.case_created_date,
    re.registrar_escltn_ind,
    re.registrar_escltn_count,
    re.registrar_escltn_dur_day_count,
    pgc.bis_first_created_datetime,
    pgc.bis_last_modified_datetime,
    CASE 
		WHEN pgc.grant_issued_date IS NULL THEN NULL  
		ELSE siw.submtd_to_issued_wrkgdy_count
    END AS submtd_to_issued_wrkgdy_count,
    CASE 
		WHEN pgc.grant_issued_date IS NULL THEN NULL 
        WHEN siw.submtd_to_issued_wrkgdy_count <= 2 THEN 1
        else 0
    END AS issued_wthn_2wrkgdy_ind, 
    CASE 
		WHEN pgc.grant_issued_date IS NULL THEN NULL 
		WHEN siw.submtd_to_issued_wrkgdy_count <= 7 THEN 1 
		ELSE 0 
	END AS issued_in_7wdays_ind,
    CASE 
    when (CONVERT(BIGINT, pgc.estate_net_value_amount)/100) >= 5000 then 1
    else 0
    end as case_fee_payable_ind,
    s.case_withdrawn_first_date AS applctn_wthdrwn_date, 
    CASE 
		WHEN s.case_withdrawn_first_date IS NOT NULL THEN 1
        ELSE 0
    END AS applctn_wthdrwn_ind ,
    cdd.case_doc_recvd_date,
    cdd.submsn_to_doc_recpt_days_count,
    cdd.submsn_to_doc_recpt_wkdy_count,
    cdd.DOC_RECPT_TO_ISSUE_DAYS_COUNT,
    cdd.DOC_RECPT_TO_ISSUE_WKDY_COUNT,
    CASE 
		WHEN cdd.case_doc_recvd_date IS NOT NULL THEN 1
        ELSE 0
    end as case_doc_recvd_ind,
    CASE
        WHEN cdd.doc_recpt_to_issue_wkdy_count <= 7 THEN 1             
        WHEN cdd.doc_recpt_to_issue_wkdy_count > 7 THEN 0  
        WHEN cdd.doc_recpt_to_issue_wkdy_count IS NULL THEN NULL
    END AS doc_recvd_to_issue_7wrkdy_ind,
    cr.wrk_restrt_from_stop_date,
    CASE 
		WHEN cr.wrk_restrt_from_stop_date IS NOT NULL THEN 1 
		ELSE 0 
	END AS wrk_restrt_from_stop_ind,
    CASE
		WHEN s.case_stopped_first_date IS NOT NULL AND cr.wrk_restrt_from_stop_date IS NOT NULL THEN (
			SELECT count(*) 
			FROM dim_date d 
			WHERE d.working_day_flag = 'Y' 
			AND d.date_key BETWEEN DATEADD(DAY, 1, s.case_stopped_first_date) AND cr.wrk_restrt_from_stop_date
		)
        ELSE NULL 
	END AS frst_stop_restrt_wrkdy_durtn,
    sdr.wrk_strtd_from_doc_recvd_date,
    CASE 
		WHEN sdr.wrk_strtd_from_doc_recvd_date IS NOT NULL THEN 1 
		ELSE 0 
	END AS wrk_strtd_from_doc_recvd_ind,              
    CASE 
		WHEN cdd.case_doc_recvd_date IS NOT NULL AND sdr.wrk_strtd_from_doc_recvd_date IS NOT NULL THEN sdr.doc_recvd_to_strtd_wrkdy_durtn
        ELSE NULL 
	END AS doc_recvd_to_strtd_wrkdy_durtn,
    CASE 
		WHEN sdr.doc_recvd_to_strtd_wrkdy_durtn IS NULL THEN NULL
        WHEN cdd.case_doc_recvd_date IS NOT NULL AND sdr.wrk_strtd_from_doc_recvd_date IS NOT NULL AND sdr.doc_recvd_to_strtd_wrkdy_durtn  <= 2 THEN 1 
        ELSE 0 
	END AS doc_recvd_to_strtd_2wrkdy_ind,             
    case 
		WHEN pgc.grant_issued_date IS NOT NULL AND s.case_withdrawn_first_date  IS NOT NULL THEN CASE 
			WHEN pgc.grant_issued_date > s.case_withdrawn_first_date THEN pgc.grant_issued_date
			ELSE s.case_withdrawn_first_date
		END
		when pgc.grant_issued_date IS NOT NULL 
		AND s.case_withdrawn_first_date  IS NULL THEN pgc.grant_issued_date
		when pgc.grant_issued_date IS NULL AND s.case_withdrawn_first_date  IS NOT NULL THEN s.case_withdrawn_first_date
    else NULL
    end as case_closed_date,   --) 1.8 DJ
    pgc.ccd_case_ref_cid -- 2.0 DM
    ,pgc.welsh_lang_cind
    ,pgc.prefrd_lang_type_key
	INTO #Temp
	FROM case_types pgc 
	JOIN earliest_cases ec 
	ON pgc.legal_case_id = ec.ce_case_data_id
	LEFT OUTER JOIN mdm_probate_location_lu pl 
	ON pl.ce_reg_location = isnull(pgc.registry_location_name,'Not Specified')
	LEFT OUTER JOIN mdm_probate_applctn_type_lu pat 
	ON pat.applctn_submtd_role_type_name = ISNULL(pgc.applctn_submtd_role_type_name, 'Not Specified') 
	AND pat.applctn_paperform_ind = ISNULL(pgc.applctn_paperform_ind, 'Yes')
	LEFT OUTER JOIN stopped_cases sc 
	ON pgc.legal_case_id = sc.ce_case_data_id
	LEFT OUTER JOIN registrar_escalation re 
	ON pgc.legal_case_id = re.legal_case_id
	LEFT OUTER JOIN event_dates s
	ON s.ce_case_data_id = pgc.legal_case_id         
	AND (s.case_submitted_event_date IS NOT NULL)
	LEFT JOIN submitted_overall so 
	ON pgc.legal_case_id = so.legal_case_id
	LEFT OUTER JOIN submitted_issued_days siw 
	ON pgc.legal_case_id = siw.legal_case_id 
	LEFT OUTER JOIN case_doc_date cdd 
	ON pgc.legal_case_id = cdd.ce_case_data_id
	LEFT OUTER JOIN case_restart cr 
	ON pgc.legal_case_id = cr.ce_case_data_id 
	LEFT OUTER JOIN recvd_restart_wdays sdr 
	ON pgc.legal_case_id = sdr.ce_case_data_id

    UPDATE tbl_legal_probategrant_case 
	SET source_key = src.source_key,
    case_type_key = src.case_type_key,
    location_key = src.location_key,
    probate_applctn_type_key = src.probate_applctn_type_key,
    applctn_submtd_role_type_name = src.applctn_submtd_role_type_name,
    applctn_submtd_date = src.case_submitted_date, --v1.2 
    registry_location_name = src.registry_location_name,
    will_exists_ind =  src.will_exists_ind,
    estate_net_value_amount = CONVERT(BIGINT, src.estate_net_value_amount)/100,
    estate_gross_value_amount = CONVERT(BIGINT, src.estate_gross_value_amount)/100,
    --deceased_death_date =src.deceased_death_date,
    assets_held_int_othr_name_ind = src.assets_held_int_othr_name_ind,
    stop_reason_event_type_name = src.stop_reason_event_type_name,
    gor_case_type_name = src.gor_case_type_name,
    applctn_paperform_ind = src.applctn_paperform_ind,
    grant_issued_date = src.grant_issued_date,
    legacy_case_record_id = src.legacy_case_record_id,
    stops_count = src.stops_count,
    latest_reissue_date = src.latest_reissue_date,
    reissue_reason_type_name  = src.reissue_reason_type_name,
    case_submitted_date = src.case_submitted_date,
    applctn_submitted_ind = src.applctn_submitted_ind,
    case_examined_first_date = src.case_examined_first_date,
    applctn_examined_ind = src.applctn_examined_ind,    
    case_stopped_first_date = src.case_stopped_first_date,
    applctn_stopped_ind = src.applctn_stopped_ind,    
    applctn_issued_ind = src.applctn_issued_ind, 
    issued_in_7wdays_ind = src.issued_in_7wdays_ind,
    issued_in_20days_ind = src.issued_in_20days_ind,
    submitted_to_issued_days_count = src.submitted_to_issued_days_count,
    case_created_date = src.case_created_date,
    reissued_ind = src.reissued_ind,
    registrar_escltn_ind = src.registrar_escltn_ind,
    registrar_escltn_count = src.registrar_escltn_count,
    registrar_escltn_dur_day_count = src.registrar_escltn_dur_day_count,
    submtd_to_issued_wrkgdy_count  = src.submtd_to_issued_wrkgdy_count, 
    issued_wthn_2wrkgdy_ind       = src.issued_wthn_2wrkgdy_ind, 
    bis_last_modified_datetime = src.bis_last_modified_datetime,
    modified_by_process_name = 'sp_legal_probate_grant_case',
    case_fee_payable_ind =  src.case_fee_payable_ind,
    applctn_wthdrwn_date = src.applctn_wthdrwn_date,                      
    applctn_wthdrwn_ind  = src.applctn_wthdrwn_ind ,
    case_doc_recvd_date  = src.case_doc_recvd_date, 
    submsn_to_doc_recpt_days_count = src.submsn_to_doc_recpt_days_count,
    doc_recpt_to_issue_days_count  = src.doc_recpt_to_issue_days_count,
    submsn_to_doc_recpt_wkdy_count = src.submsn_to_doc_recpt_wkdy_count,
    doc_recpt_to_issue_wkdy_count  = src.doc_recpt_to_issue_wkdy_count,
    case_doc_recvd_ind             = src.case_doc_recvd_ind,           
    wrk_restrt_from_stop_date      = src.wrk_restrt_from_stop_date,
    wrk_restrt_from_stop_ind       = src.wrk_restrt_from_stop_ind,
    frst_stop_restrt_wrkdy_durtn   = src.frst_stop_restrt_wrkdy_durtn,
    wrk_strtd_from_doc_recvd_date  = src.wrk_strtd_from_doc_recvd_date,
    wrk_strtd_from_doc_recvd_ind   = src.wrk_strtd_from_doc_recvd_ind,
    doc_recvd_to_strtd_wrkdy_durtn = src.doc_recvd_to_strtd_wrkdy_durtn,
    doc_recvd_to_strtd_2wrkdy_ind  = src.doc_recvd_to_strtd_2wrkdy_ind,
    case_closed_date               = src.case_closed_date,  
    ccd_case_ref_cid               = src.ccd_case_ref_cid, 
    doc_recvd_to_issue_7wrkdy_ind  = src.doc_recvd_to_issue_7wrkdy_ind,    
    welsh_lang_cind = src.welsh_lang_cind,
    prefrd_lang_type_key = src.prefrd_lang_type_key
	FROM #Temp src
	WHERE src.legal_case_id = tbl_legal_probategrant_case.case_data_id;

    INSERT INTO tbl_legal_probategrant_case 
    (
		source_key,
		case_type_key,
		location_key,
		probate_applctn_type_key,
		case_data_id,
		applctn_submtd_role_type_name,
		applctn_submtd_date,
		registry_location_name,
		will_exists_ind,
		estate_net_value_amount,
		estate_net_value_curr_code,
		estate_gross_value_amount,
		estate_gross_value_curr_code,
		--deceased_death_date,
		assets_held_int_othr_name_ind,
		stop_reason_event_type_name,
		gor_case_type_name,
		applctn_paperform_ind,
		grant_issued_date,
		legacy_case_record_id,
		stops_count,
		latest_reissue_date,  
		reissue_reason_type_name,
		case_submitted_date,
		applctn_submitted_ind,
		case_examined_first_date,
		applctn_examined_ind,
		case_stopped_first_date,
		applctn_stopped_ind,
		applctn_issued_ind,
		issued_in_7wdays_ind,
		issued_in_20days_ind,
		submitted_to_issued_days_count,
		case_created_date,
		reissued_ind,
		registrar_escltn_ind,
		registrar_escltn_count,
		registrar_escltn_dur_day_count,
		submtd_to_issued_wrkgdy_count, 
		issued_wthn_2wrkgdy_ind,
		bis_first_created_datetime,
		bis_last_modified_datetime, 
		inserted_datetime,
		inserted_by_process_name,
		source_system_type_code,
		case_fee_payable_ind,
		applctn_wthdrwn_date,                       
		applctn_wthdrwn_ind,
		case_doc_recvd_date, 
		submsn_to_doc_recpt_days_count,
		doc_recpt_to_issue_days_count,
		submsn_to_doc_recpt_wkdy_count,
		doc_recpt_to_issue_wkdy_count,
		case_doc_recvd_ind,
		wrk_restrt_from_stop_date,
		wrk_restrt_from_stop_ind,
		frst_stop_restrt_wrkdy_durtn,
		wrk_strtd_from_doc_recvd_date,
		wrk_strtd_from_doc_recvd_ind,
		doc_recvd_to_strtd_wrkdy_durtn,
		doc_recvd_to_strtd_2wrkdy_ind,
		case_closed_date,  
		ccd_case_ref_cid, 
		doc_recvd_to_issue_7wrkdy_ind,
		welsh_lang_cind,
		prefrd_lang_type_key     
    )
    SELECT src.source_key,
    src.case_type_key,
    src.location_key,
    src.probate_applctn_type_key,
    src.legal_case_id,
    src.applctn_submtd_role_type_name,
    src.case_submitted_date,    -- v1.2
    src.registry_location_name,
    src.will_exists_ind,
    CONVERT(BIGINT, src.estate_net_value_amount)/100,
    'GBP',
    CONVERT(BIGINT, src.estate_gross_value_amount)/100,
    'GBP',
    --src.deceased_death_date,  ---Can't get to work
    src.assets_held_int_othr_name_ind,
    src.stop_reason_event_type_name,
    src.gor_case_type_name,
    src.applctn_paperform_ind,
    src.grant_issued_date,
    src.legacy_case_record_id,
    src.stops_count,
    src.latest_reissue_date,
    src.reissue_reason_type_name, 
    src.case_submitted_date,
    src.applctn_submitted_ind,
    src.case_examined_first_date,
    src.applctn_examined_ind,          
    src.case_stopped_first_date,
    src.applctn_stopped_ind,
    src.applctn_issued_ind,
    src.issued_in_7wdays_ind,
    src.issued_in_20days_ind,
    src.submitted_to_issued_days_count,
    src.case_created_date,
    src.reissued_ind,         
    src.registrar_escltn_ind,
    src.registrar_escltn_count,
    src.registrar_escltn_dur_day_count,
    src.submtd_to_issued_wrkgdy_count, 
    src.issued_wthn_2wrkgdy_ind,     
    src.bis_first_created_datetime,
    src.bis_last_modified_datetime,
    GETDATE(),
    'sp_legal_probate_grant_case',
    'CCD',
    case_fee_payable_ind,
    src.applctn_wthdrwn_date,                   
    src.applctn_wthdrwn_ind,
    src.case_doc_recvd_date,        
    src.submsn_to_doc_recpt_days_count,
    src.doc_recpt_to_issue_days_count,
    src.submsn_to_doc_recpt_wkdy_count,
    src.doc_recpt_to_issue_wkdy_count,
    src.case_doc_recvd_ind, --APD-4013                     
    src.wrk_restrt_from_stop_date,
    src.wrk_restrt_from_stop_ind,
    src.frst_stop_restrt_wrkdy_durtn,
    src.wrk_strtd_from_doc_recvd_date,
    src.wrk_strtd_from_doc_recvd_ind,
    src.doc_recvd_to_strtd_wrkdy_durtn,
    src.doc_recvd_to_strtd_2wrkdy_ind,
    src.case_closed_date,  
    src.ccd_case_ref_cid, 
    src.doc_recvd_to_issue_7wrkdy_ind,           
    src.welsh_lang_cind,
    src.prefrd_lang_type_key  
	FROM #Temp src
	WHERE legal_case_id NOT IN(
		SELECT case_data_id FROM tbl_legal_probategrant_case
	);

	DROP TABLE #Temp
  END;
