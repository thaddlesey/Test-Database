CREATE PROC [sp_probate_summary] AS
BEGIN
	WITH submitted
	AS
	(
		-- Query Submitted Data along with Case Type, Source Key and Registry Location -
		SELECT case1.case_submitted_date AS DATE_KEY,
		case1.case_type_key                   AS CASE_TYPE_KEY,
		case1.source_key                      AS source_key,
		case1.location_key                    as location_key,
		case1.probate_applctn_type_key        AS probate_applctn_type_key,
		case1.prefrd_lang_type_key            AS prefrd_lang_type_key,       
		sum(case1.applctn_submitted_ind)      as submitted,
		sum(case_fee_payable_ind)             as case_fee_payable_count,
		0                                     AS examined,
		0                                     AS stopped,
		0                                     AS ISSUED,
		0                                     AS DAYS_SUBMITTED_TO_ISSUED,
		0                                     AS ISSUED_IN_7WDAYS,
		0                                     as issued_in_20days,
		0                                     as total_case_stops_count,
		0                                     AS registrar_escltn_case_count,
		0                                     AS registrar_escltn_total_count,    
		0                                     AS registrar_escltn_days_count,
		0                                     as caveat_active_count,
		0                                     as wlgmt_count,
		0                                     as standing_search_count,
		0                                     AS caveat_count,
		0                                     as grant_issue_rgstr_escltn_count,   --- vidhya - EPF-2396
		0									  as reissued_total_count,              --- vidhya - EPF-2437
		0									  as reissued_duplct_count,             --- vidhya - EPF-2437
		0									  as reissued_rgstr_dirctn_count,       --- vidhya - EPF-2437
		0									  as reissued_rgstr_order_count,         --- vidhya - EPF-2437
		0									  as reissue_pending_count,  -- Chandra  -- EPF 2511
		0                                     as wthdrwn_total_count, -- SD EPF-2414
		0                                     as wthdrwn_for_proving_count, -- SD EPF-2414
		0                                     as wthdrwn_testator_count, -- SD EPF-2414
		0                                     as wthdrwn_cancelled_count, -- SD EPF-2414
		0                                     as case_srch_matched_count, -- SD EPF-2374
		0                                     as issued_wthn_2wrkgdy_count,           --- vidhya- EPF-2454
		0                                     as exprd_total_count, -- SD EPF-2326
		0                                     as applctn_wthdrwn_count,
		0                                     AS submsn_to_doc_recpt_days_count,
		0                                     AS submsn_to_doc_recpt_wkdy_count,
		0                                     AS doc_recpt_to_issue_days_count,
		0                                     AS doc_recpt_to_issue_wkdy_count,
		0                                     AS submsn_to_issue_wkdy_count,
		0                                     AS doc_recvd_count, -- APD-4013  
		0                                     AS wrk_restrt_from_stop_count,
		0                                     AS frst_stop_restrt_wrkdy_durtn,
		0                                     AS wrk_strtd_count,
		0                                     AS doc_recvd_to_strtd_wrkdy_durtn,
		0                                     AS doc_recvd_strtd_2wrkdy_count,
		0                                     AS grant_applctn_outstndng_count, -- 1.8 DJ
		0                                     AS doc_recvd_issue_7wrkdy_count   -- v2.1 sp
		FROM tbl_legal_probategrant_case case1
		WHERE case1.case_submitted_date IS NOT NULL
		GROUP BY case1.case_submitted_date,
		case1.case_type_key,
		case1.source_key,
		case1.location_key,
		case1.probate_applctn_type_key   
		/* WJB APD-5735 START */
		,case1.prefrd_lang_type_key         
		/* WJB APD-5735 END */        
	), examined
	AS
	(
		SELECT case2.case_examined_first_date AS DATE_KEY,
		case2.case_type_key                  as case_type_key,
		case2.source_key                     AS source_key,
		case2.location_key                   AS location_key,
		case2.probate_applctn_type_key       AS probate_applctn_type_key,
		case2.prefrd_lang_type_key           AS prefrd_lang_type_key,
		0                                    AS submitted,
		0                                    as case_fee_payable_count,
		SUM(case2.applctn_examined_ind)      AS examined,
		0                                    AS stopped,
		0                                    AS issued,
		0                                    AS DAYS_SUBMITTED_TO_ISSUED,
		0                                    AS ISSUED_IN_7WDAYS,
		0                                    as issued_in_20days,
		0                                    as total_case_stops_count,
		0                                    AS registrar_escltn_case_count,
		0                                    AS registrar_escltn_total_count,    
		0                                    AS registrar_escltn_days_count,
		0                                    as caveat_active_count,
		0                                    as wlgmt_count,
		0                                    as standing_search_count,
		0                                     AS caveat_count,
		0                                     as grant_issue_rgstr_escltn_count,   --- vidhya - EPF-2396
		0                                 as reissued_total_count,              --- vidhya - EPF-2437
		0                                 as reissued_duplct_count,             --- vidhya - EPF-2437
		0                                 as reissued_rgstr_dirctn_count,       --- vidhya - EPF-2437
		0                                 as reissued_rgstr_order_count,         --- vidhya - EPF-2437
		0                                 as reissue_pending_count,  -- Chandra  -- EPF 2511
		0                                     as wthdrwn_total_count, -- SD EPF-2414
		0                                     as wthdrwn_for_proving_count, -- SD EPF-2414
		0                                     as wthdrwn_testator_count, -- SD EPF-2414
		0                                     as wthdrwn_cancelled_count, -- SD EPF-2414
		0                                     as case_srch_matched_count, -- SD EPF-2374
		0                                     as issued_wthn_2wrkgdy_count,           --- vidhya- EPF-2454
		0                                     as exprd_total_count, -- SD EPF-2326
		0                                     as applctn_wthdrwn_count,
		0                                     AS submsn_to_doc_recpt_days_count,
		0                                     AS submsn_to_doc_recpt_wkdy_count,
		0                                     AS doc_recpt_to_issue_days_count,
		0                                     AS doc_recpt_to_issue_wkdy_count,
		0                                     AS submsn_to_issue_wkdy_count,
		0                                     as doc_recvd_count, -- APD-4013  
		--v1.6
		0                                     AS wrk_restrt_from_stop_count,
		0                                     AS frst_stop_restrt_wrkdy_durtn,
		0                                     AS wrk_strtd_count,
		0                                     AS doc_recvd_to_strtd_wrkdy_durtn,
		0                                     AS doc_recvd_strtd_2wrkdy_count,
		0                                     AS grant_applctn_outstndng_count, -- 1.8 DJ
		0                                     AS doc_recvd_issue_7wrkdy_count   -- v2.1 sp
		FROM tbl_legal_probategrant_case case2
		WHERE case2.case_examined_first_date IS NOT NULL
		GROUP BY case2.case_examined_first_date,
		case2.case_type_key,
		case2.source_key,
		case2.location_key,
		case2.probate_applctn_type_key 
		/* WJB APD-5735 START */
		,case2.prefrd_lang_type_key         
		/* WJB APD-5735 END */          
	), stopped
	AS
	(
		SELECT case3.case_stopped_first_date AS date_key,
		case3.case_type_key                 AS case_type_key,
		case3.source_key                    AS source_key,
		case3.location_key                  AS location_key,
		case3.probate_applctn_type_key      AS probate_applctn_type_key,
		/* WJB APD-5735 START */
		case3.prefrd_lang_type_key          AS prefrd_lang_type_key,
		/* WJB APD-5735 END */            
		0                                   as submitted,
		0                                   as case_fee_payable_count,
		0                                   AS examined,
		SUM(case3.applctn_stopped_ind)      AS stopped,
		0                                   AS issued,
		0                                   AS days_submitted_to_issued,
		0                                   AS ISSUED_IN_7WDAYS,
		0                                   as issued_in_20days,
		0                                   as total_case_stops_count,
		0                                   AS registrar_escltn_case_count,
		0                                   AS registrar_escltn_total_count,    
		0                                   as registrar_escltn_days_count,
		0                                   as caveat_active_count,
		0                                   as wlgmt_count,
		0                                   as standing_search_count,
		0                                     AS caveat_count,
		0                                     as grant_issue_rgstr_escltn_count,   --- vidhya - EPF-2396
		0                                 as reissued_total_count,              --- vidhya - EPF-2437
		0                                 as reissued_duplct_count,             --- vidhya - EPF-2437
		0                                 as reissued_rgstr_dirctn_count,       --- vidhya - EPF-2437
		0                                 as reissued_rgstr_order_count,         --- vidhya - EPF-2437
		0                                 as reissue_pending_count,  -- Chandra  -- EPF 2511
		0                                     as wthdrwn_total_count, -- SD EPF-2414
		0                                     as wthdrwn_for_proving_count, -- SD EPF-2414
		0                                     as wthdrwn_testator_count, -- SD EPF-2414
		0                                     as wthdrwn_cancelled_count, -- SD EPF-2414
		0                                     as case_srch_matched_count, -- SD EPF-2374
		0                                     as issued_wthn_2wrkgdy_count,           --- vidhya- EPF-2454
		0                                     as exprd_total_count, -- SD EPF-2326
		0                                     as applctn_wthdrwn_count,
		0                                     AS submsn_to_doc_recpt_days_count,
		0                                     AS submsn_to_doc_recpt_wkdy_count,
		0                                     AS doc_recpt_to_issue_days_count,
		0                                     AS doc_recpt_to_issue_wkdy_count,
		0                                     AS submsn_to_issue_wkdy_count,
		0                                     as doc_recvd_count, -- APD-4013  
		--v1.6
		0                                     AS wrk_restrt_from_stop_count,
		0                                     AS frst_stop_restrt_wrkdy_durtn,
		0                                     AS wrk_strtd_count,
		0                                     AS doc_recvd_to_strtd_wrkdy_durtn,
		0                                     AS doc_recvd_strtd_2wrkdy_count,
		0                                     AS grant_applctn_outstndng_count, -- 1.8 DJ
		0                                     AS doc_recvd_issue_7wrkdy_count   -- v2.1 sp
		FROM tbl_legal_probategrant_case case3
		WHERE case3.case_stopped_first_date IS NOT NULL
		GROUP BY case3.case_stopped_first_date,
		case3.case_type_key,
		case3.source_key,
		case3.location_key,
		case3.probate_applctn_type_key
		/* WJB APD-5735 START */
		,case3.prefrd_lang_type_key         
		/* WJB APD-5735 END */          
	), issued_escalations_stops 
	AS
	(
		SELECT case4.grant_issued_date          AS DATE_KEY,
		case4.case_type_key                         AS case_type_key,
		case4.source_key                            AS source_key,
		case4.location_key                          AS location_key,
		case4.probate_applctn_type_key              AS probate_applctn_type_key,
		/* WJB APD-5735 START */
		case4.prefrd_lang_type_key                  AS prefrd_lang_type_key,
		/* WJB APD-5735 END */          
		0                                           as submitted,
		0                                           as case_fee_payable_count,
		0                                           AS examined,
		0                                           AS stopped,
		SUM(case4.applctn_issued_ind)               AS issued,
		SUM(ISNULL(case4.submitted_to_issued_days_count, 0)) AS days_submitted_to_issued,
		SUM(ISNULL(case4.issued_in_7wdays_ind, 0))     AS issued_in_7wdays,
		SUM(ISNULL(case4.issued_in_20days_ind, 0))     AS issued_in_20days,
		SUM(stops_count)                            AS total_case_stops_count,
		-- Registrar escalations metrics are calculated using grant issued date
		SUM(case4.registrar_escltn_ind)           as registrar_escltn_case_count,
		SUM(case4.registrar_escltn_count)         AS registrar_escltn_total_count,    
		SUM(case4.registrar_escltn_dur_day_count) as registrar_escltn_days_count,
		0                                           as caveat_active_count,
		0                                           as wlgmt_count,
		0                                           as standing_search_count,
		0                                           AS caveat_count,
		SUM (CASE WHEN registrar_escltn_ind = 1 AND applctn_issued_ind = 1 
					THEN 1 ELSE 0 END)            as grant_issue_rgstr_escltn_count,    --- vidhya - EPF-2396
		0                                 as reissued_total_count,              --- vidhya - EPF-2437
		0                                 as reissued_duplct_count,             --- vidhya - EPF-2437
		0                                 as reissued_rgstr_dirctn_count,       --- vidhya - EPF-2437
		0                                 as reissued_rgstr_order_count ,         --- vidhya - EPF-2437
		0                                 as reissue_pending_count,  -- Chandra  -- EPF 2511
		0                                     as wthdrwn_total_count, -- SD EPF-2414
		0                                     as wthdrwn_for_proving_count, -- SD EPF-2414
		0                                     as wthdrwn_testator_count, -- SD EPF-2414
		0                                     as wthdrwn_cancelled_count, -- SD EPF-2414
		0                                     as case_srch_matched_count, -- SD EPF-2374
		sum(issued_wthn_2wrkgdy_ind)          as issued_wthn_2wrkgdy_count,           --- vidhya- EPF-2454
		0                                     as exprd_total_count, -- SD EPF-2326
		0                                     as applctn_wthdrwn_count,
		0                                     AS submsn_to_doc_recpt_days_count,
		0                                     AS submsn_to_doc_recpt_wkdy_count,
		SUM(doc_recpt_to_issue_days_count)    AS doc_recpt_to_issue_days_count,
		SUM(doc_recpt_to_issue_wkdy_count)    AS doc_recpt_to_issue_wkdy_count,
		SUM(SUBMTD_TO_ISSUED_WRKGDY_COUNT)    AS submsn_to_issue_wkdy_count,
		0                                     AS doc_recvd_count, -- APD-4013  
		--v1.6
		0                                     AS wrk_restrt_from_stop_count,
		0                                     AS frst_stop_restrt_wrkdy_durtn,
		0                                     AS wrk_strtd_count,
		0                                     AS doc_recvd_to_strtd_wrkdy_durtn,
		0                                     AS doc_recvd_strtd_2wrkdy_count,
		0                                     AS grant_applctn_outstndng_count, -- 1.8 DJ 
		sum(doc_recvd_to_issue_7wrkdy_ind)    AS doc_recvd_issue_7wrkdy_count   -- v2.1 sp
		FROM tbl_legal_probategrant_case case4
		WHERE case4.grant_issued_date IS NOT NULL
		GROUP BY case4.grant_issued_date,
		case4.case_type_key,
		case4.source_key,
		case4.location_key,
		case4.probate_applctn_type_key   
		/* WJB APD-5735 START */
		,case4.prefrd_lang_type_key         
		/* WJB APD-5735 END */        
	), 
	/*caveat_activecount
	as
	(   select pc.case_receipt_date  as date_key,
		pc.case_type_key                    as case_type_key,
		pc.source_key                       as source_key,
		pc.location_key                     as location_key,
		pc.probate_applctn_type_key         as probate_applctn_type_key,
		/* WJB APD-5735 START */
		-- assume 'English (or not specified)
		-- 59.000001                           
		gc_lang_eng_key                     AS prefrd_lang_type_key,
		/* WJB APD-5735 END */        
		0                                   as submitted,
		0                                   as case_fee_payable_count,
		0                                   as examined,
		0                                   as stopped,
		0                                   AS issued,
		0                                   AS DAYS_SUBMITTED_TO_ISSUED,
		0                                   as issued_in_7wdays,
		0                                   as issued_in_20days,
		0                                   as total_case_stops_count,
		0                                   as registrar_escltn_case_count,
		0                                   AS registrar_escltn_total_count,    
		0                                   as registrar_escltn_days_count,
		0                                   as caveat_active_count,
		0                                   as wlgmt_count,
		0                                   as standing_search_count,
		SUM(received_ind)                   AS caveat_count ,
		0                                as grant_issue_rgstr_escltn_count,   --- vidhya - EPF-2396
		0                                 as reissued_total_count,              --- vidhya - EPF-2437
		0                                 as reissued_duplct_count,             --- vidhya - EPF-2437
		0                                 as reissued_rgstr_dirctn_count,       --- vidhya - EPF-2437
		0                                 as reissued_rgstr_order_count ,         --- vidhya - EPF-2437
		0                                 as reissue_pending_count,  -- Chandra  -- EPF 2511
		0                                     as wthdrwn_total_count, -- SD EPF-2414
		0                                     as wthdrwn_for_proving_count, -- SD EPF-2414
		0                                     as wthdrwn_testator_count, -- SD EPF-2414
		0                                     as wthdrwn_cancelled_count, -- SD EPF-2414
		0                                     as case_srch_matched_count, -- SD EPF-2374
		0                                     as issued_wthn_2wrkgdy_count,           --- vidhya- EPF-2454
		0                                     as exprd_total_count, -- SD EPF-2326
		0                                     as applctn_wthdrwn_count,
		0                                     AS submsn_to_doc_recpt_days_count,
		0                                     AS submsn_to_doc_recpt_wkdy_count,
		0                                     AS doc_recpt_to_issue_days_count,
		0                                     AS doc_recpt_to_issue_wkdy_count,
		0                                     AS submsn_to_issue_wkdy_count,
		0                                     AS doc_recvd_count, -- APD-4013  
		--v1.6
		0                                     AS wrk_restrt_from_stop_count,
		0                                     AS frst_stop_restrt_wrkdy_durtn,
		0                                     AS wrk_strtd_count,
		0                                     AS doc_recvd_to_strtd_wrkdy_durtn,
		0                                     AS doc_recvd_strtd_2wrkdy_count,
		0                                     AS grant_applctn_outstndng_count, -- 1.8 DJ
		0                                     AS doc_recvd_issue_7wrkdy_count   -- v2.1 sp
		FROM tbl_legal_probatecaveat_case pc
		GROUP BY pc.case_receipt_date, 
		pc.source_key, 
		pc.location_key, 
		pc.case_type_key, 
		pc.probate_applctn_type_key
		)
	, wlgmt
	AS
	(      
		select coalesce(pw.wlgmt_date, pw.receipt_date, pw.case_created_datetime) as date_key,
		pw.case_type_key                  as case_type_key,
		pw.source_key                     as source_key,
		pw.location_key                   AS location_key,
		pw.probate_applctn_type_key       AS probate_applctn_type_key,
		/* WJB APD-5735 START */
		-- assume 'English (or not specified)
		--59.000001                         
		gc_lang_eng_key                   AS prefrd_lang_type_key,
		/* WJB APD-5735 END */           
		0                                 as submitted,
		0                                 as case_fee_payable_count,
		0                                 AS examined,
		0                                 AS stopped,
		0                                 AS issued,
		0                                 AS DAYS_SUBMITTED_TO_ISSUED,
		0                                 AS ISSUED_IN_7WDAYS,
		0                                 as issued_in_20days,
		0                                 as total_case_stops_count,
		0                                 AS registrar_escltn_case_count,
		0                                 AS registrar_escltn_total_count,    
		0                                 as registrar_escltn_days_count,
		0                                 as caveat_active_count,
		SUM(pw.received_ind)              as wlgmt_count,
		0                                 as standing_search_count,
		0                                 AS caveat_count  ,
		0                                as grant_issue_rgstr_escltn_count,   --- vidhya - EPF-2396
		0                                 as reissued_total_count,              --- vidhya - EPF-2437
		0                                 as reissued_duplct_count,             --- vidhya - EPF-2437
		0                                 as reissued_rgstr_dirctn_count,       --- vidhya - EPF-2437
		0                                 as reissued_rgstr_order_count ,         --- vidhya - EPF-2437
		0                                 as reissue_pending_count,  -- Chandra  -- EPF 2511
		0                                     as wthdrwn_total_count, -- SD EPF-2414
		0                                     as wthdrwn_for_proving_count, -- SD EPF-2414
		0                                     as wthdrwn_testator_count, -- SD EPF-2414
		0                                     as wthdrwn_cancelled_count, -- SD EPF-2414
		0                                     as case_srch_matched_count, -- SD EPF-2374
		0                                     as issued_wthn_2wrkgdy_count,           --- vidhya- EPF-2454
		0                                     as exprd_total_count, -- SD EPF-2326
		0                                     as applctn_wthdrwn_count,
		0                                     AS submsn_to_doc_recpt_days_count,
		0                                     AS submsn_to_doc_recpt_wkdy_count,
		0                                     AS doc_recpt_to_issue_days_count,
		0                                     AS doc_recpt_to_issue_wkdy_count,
		0                                     AS submsn_to_issue_wkdy_count,
		0                                     AS doc_recvd_count, -- APD-4013  
		--v1.6
		0                                     AS wrk_restrt_from_stop_count,
		0                                     AS frst_stop_restrt_wrkdy_durtn,
		0                                     AS wrk_strtd_count,
		0                                     AS doc_recvd_to_strtd_wrkdy_durtn,
		0                                     AS doc_recvd_strtd_2wrkdy_count,
		0                                     AS grant_applctn_outstndng_count, -- 1.8 DJ
		0                                     AS doc_recvd_issue_7wrkdy_count   -- v2.1 sp
		from tbl_legal_probatewlgmt_case pw
		GROUP BY coalesce(pw.wlgmt_date, pw.receipt_date, pw.case_created_datetime), 
		pw.source_key, 
		pw.location_key, 
		pw.case_type_key, 
		pw.probate_applctn_type_key
	)
	, search
	AS
	(       
		SELECT psc.srch_received_date AS date_key,    -- VIDHYA 2712
		psc.case_type_key                 AS case_type_key,
		psc.source_key                    as source_key,
		psc.location_key                  AS location_key,
		psc.probate_applctn_type_key      AS probate_applctn_type_key,
		/* WJB APD-5735 START */
		-- assume 'English (or not specified)
		--59.000001                         
		gc_lang_eng_key                     AS prefrd_lang_type_key,
		/* WJB APD-5735 END */          
		0                                   as submitted,
		0                                   as case_fee_payable_count,
		0                                   AS examined,
		0                                   AS stopped,
		0                                   AS issued,
		0                                   AS DAYS_SUBMITTED_TO_ISSUED,
		0                                   AS ISSUED_IN_7WDAYS,
		0                                   as issued_in_20days,
		0                                   as total_case_stops_count,
		0                                 AS registrar_escltn_case_count,
		0                                 AS registrar_escltn_total_count,    
		0                                 as registrar_escltn_days_count,
		0                                 as caveat_active_count,
		0                                 as wlgmt_count,
		SUM(psc.received_ind)             as standing_search_count,
		0                                 AS caveat_count   ,
		0                                as grant_issue_rgstr_escltn_count,   --- vidhya - EPF-2396
		0                                 as reissued_total_count,              --- vidhya - EPF-2437
		0                                 as reissued_duplct_count,             --- vidhya - EPF-2437
		0                                 as reissued_rgstr_dirctn_count,       --- vidhya - EPF-2437
		0                                 as reissued_rgstr_order_count ,         --- vidhya - EPF-2437
		0                                 as reissue_pending_count,  -- Chandra  -- EPF 2511
		0                                     as wthdrwn_total_count, -- SD EPF-2414
		0                                     as wthdrwn_for_proving_count, -- SD EPF-2414
		0                                     as wthdrwn_testator_count, -- SD EPF-2414
		0                                     as wthdrwn_cancelled_count, -- SD EPF-2414
		0                                     as case_srch_matched_count, -- SD EPF-2374
		0                                     as issued_wthn_2wrkgdy_count,           --- vidhya- EPF-2454
		0                                     as exprd_total_count, -- SD EPF-2326
		0                                     as applctn_wthdrwn_count,
		0                                     AS submsn_to_doc_recpt_days_count,
		0                                     AS submsn_to_doc_recpt_wkdy_count,
		0                                     AS doc_recpt_to_issue_days_count,
		0                                     AS doc_recpt_to_issue_wkdy_count,
		0                                     AS submsn_to_issue_wkdy_count,
		0                                     AS doc_recvd_count, -- APD-4013  
		--v1.6
		0                                     AS wrk_restrt_from_stop_count,
		0                                     AS frst_stop_restrt_wrkdy_durtn,
		0                                     AS wrk_strtd_count,
		0                                     AS doc_recvd_to_strtd_wrkdy_durtn,
		0                                     AS doc_recvd_strtd_2wrkdy_count,
		0                                     AS grant_applctn_outstndng_count, -- 1.8 DJ
		0                                     AS doc_recvd_issue_7wrkdy_count   -- v2.1 sp
		from TBL_LEGAL_PROBATESRCH_CASE psc
		GROUP BY psc.srch_received_date,    -- VIDHYA 2712
		psc.source_key, 
		psc.location_key, 
		psc.case_type_key, 
		psc.probate_applctn_type_key
	), st_date 
		AS
	( select date_key
		from dim_date
		where date_key > ( select min(caveat_raised_start_date) from tbl_legal_probatecaveat_case)
			and date_key < current_date
	), caveat
	AS
	(   SELECT date_key,
		pc.case_type_key                 AS case_type_key,
		pc.source_key                    AS source_key,
		pc.location_key                  AS location_key,
		pc.probate_applctn_type_key      AS probate_applctn_type_key,
		/* WJB APD-5735 START */
		-- assume 'English (or not specified)
		--59.000001                        
		gc_lang_eng_key                  AS prefrd_lang_type_key,
		/* WJB APD-5735 END */            
		0                                   as submitted,
		0                                   as case_fee_payable_count,
		0                                   AS examined,
		0                                   AS stopped,
		0                                   AS issued,
		0                                   AS DAYS_SUBMITTED_TO_ISSUED,
		0                                   AS ISSUED_IN_7WDAYS,
		0                                   AS ISSUED_IN_20DAYS,
		0                                   as total_case_stops_count,
		0                                 AS registrar_escltn_case_count,
		0                                 AS registrar_escltn_total_count,    
		0                                 AS registrar_escltn_days_count,
		sum(caveat_raised_ind)            as caveat_active_count,
		0                                 as wlgmt_count,
		0                                 as standing_search_count,
		0                                 AS caveat_count  ,
		0                                as grant_issue_rgstr_escltn_count,   --- vidhya - EPF-2396
		0                                 as reissued_total_count,              --- vidhya - EPF-2437
		0                                 as reissued_duplct_count,             --- vidhya - EPF-2437
		0                                 as reissued_rgstr_dirctn_count,       --- vidhya - EPF-2437
		0                                 as reissued_rgstr_order_count ,         --- vidhya - EPF-2437
		0                                 as reissue_pending_count,  -- Chandra  -- EPF 2511
		0                                     as wthdrwn_total_count, -- SD EPF-2414
		0                                     as wthdrwn_for_proving_count, -- SD EPF-2414
		0                                     as wthdrwn_testator_count, -- SD EPF-2414
		0                                     as wthdrwn_cancelled_count, -- SD EPF-2414
		0                                     as case_srch_matched_count, -- SD EPF-2374
		0                                     as issued_wthn_2wrkgdy_count,           --- vidhya- EPF-2454
		0                                     as exprd_total_count, -- SD EPF-2326
		0                                     as applctn_wthdrwn_count,
		0                                     AS submsn_to_doc_recpt_days_count,
		0                                     AS submsn_to_doc_recpt_wkdy_count,
		0                                     AS doc_recpt_to_issue_days_count,
		0                                     AS doc_recpt_to_issue_wkdy_count,
		0                                     AS submsn_to_issue_wkdy_count,
		0                                     AS doc_recvd_count , -- APD-4013  
		--v1.6
		0                                     AS wrk_restrt_from_stop_count,
		0                                     AS frst_stop_restrt_wrkdy_durtn,
		0                                     AS wrk_strtd_count,
		0                                     AS doc_recvd_to_strtd_wrkdy_durtn,
		0                                     AS doc_recvd_strtd_2wrkdy_count,
		0                                     AS grant_applctn_outstndng_count, -- 1.8 DJ
		0                                     AS doc_recvd_issue_7wrkdy_count   -- v2.1 sp
		FROM tbl_legal_probatecaveat_case pc
		cross join st_date
		WHERE ( caveat_raised_start_date <= date_key
		AND ( caveat_raised_end_date IS NULL OR caveat_raised_end_date > date_key))
		GROUP BY date_key,      
				pc.source_key,       
				pc.location_key,       
				pc.case_type_key,       
				pc.probate_applctn_type_key
		)    /* vidhya EPF 2437 START */
	, */
	reissued
	AS
	(       
		SELECT ris.latest_reissue_date AS date_key,
		ris.case_type_key                 AS case_type_key,
		ris.source_key                    as source_key,
		ris.location_key                  AS location_key,
		ris.probate_applctn_type_key      AS probate_applctn_type_key,
		/* WJB APD-5735 START */
		ris.prefrd_lang_type_key          AS prefrd_lang_type_key,
		/* WJB APD-5735 END */            
		0                                   as submitted,
		0                                   as case_fee_payable_count,
		0                                   AS examined,
		0                                   AS stopped,
		0                                   AS issued,
		0                                   AS DAYS_SUBMITTED_TO_ISSUED,
		0                                   AS ISSUED_IN_7WDAYS,
		0                                   as issued_in_20days,
		0                                   as total_case_stops_count,
		0                                 AS registrar_escltn_case_count,
		0                                 AS registrar_escltn_total_count,    
		0                                 as registrar_escltn_days_count,
		0                                 as caveat_active_count,
		0                                 as wlgmt_count,
		0                                 as standing_search_count,
		0                                 as caveat_count,
		0                                 as grant_issue_rgstr_escltn_count,
		SUM(reissued_ind)                 as reissued_total_count,
		SUM(CASE WHEN reissued_ind = 1 AND reissue_reason_type_name = 'duplicate' THEN 1 ELSE 0 END)  as reissued_duplct_count,
		SUM(CASE WHEN reissued_ind = 1 AND reissue_reason_type_name = 'registrarsDirection' THEN 1 ELSE 0 END) as reissued_rgstr_dirctn_count,
		sum(case when reissued_ind = 1 and reissue_reason_type_name = 'registrarsOrder' then 1 else 0 end) as reissued_rgstr_order_count,
		0                                 as reissue_pending_count,  -- Chandra  -- EPF 2511
		0                                     as wthdrwn_total_count, -- SD EPF-2414
		0                                     as wthdrwn_for_proving_count, -- SD EPF-2414
		0                                     as wthdrwn_testator_count, -- SD EPF-2414
		0                                     as wthdrwn_cancelled_count, -- SD EPF-2414
		0                                     as case_srch_matched_count, -- SD EPF-2374
		0                                     as issued_wthn_2wrkgdy_count,           --- vidhya- EPF-2454
		0                                     as exprd_total_count, -- SD EPF-2326
		0                                     as applctn_wthdrwn_count,
		0                                     AS submsn_to_doc_recpt_days_count,
		0                                     AS submsn_to_doc_recpt_wkdy_count,
		0                                     AS doc_recpt_to_issue_days_count,
		0                                     AS doc_recpt_to_issue_wkdy_count,
		0                                     AS submsn_to_issue_wkdy_count,
		0                                     AS doc_recvd_count, -- APD-4013  
		--v1.6
		0                                     AS wrk_restrt_from_stop_count,
		0                                     AS frst_stop_restrt_wrkdy_durtn,
		0                                     AS wrk_strtd_count,
		0                                     AS doc_recvd_to_strtd_wrkdy_durtn,
		0                                     AS doc_recvd_strtd_2wrkdy_count,
		0                                     AS grant_applctn_outstndng_count, -- 1.8 DJ
		0                                     AS doc_recvd_issue_7wrkdy_count   -- v2.1 sp
		from TBL_LEGAL_PROBATEGRANT_CASE ris
		GROUP BY ris.latest_reissue_date, 
		ris.source_key, 
		ris.location_key, 
		ris.case_type_key, 
		ris.probate_applctn_type_key
		/* WJB APD-5735 START */
		,ris.prefrd_lang_type_key      
		/* WJB APD-5735 START */      
	)
	/* Vidhya EPF 2437 END */
	/* Chandra EPF 2511 union*/
	,
	all_dates
	as 
	(SELECT  dd.date_key 
					FROM  dim_date dd
				where dd.date_key BETWEEN '01-Jan-2017' AND CONVERT(DATETIME, CONVERT(DATE,GETDATE()))
		)         
	, reissue_pending 
	as
	(
		Select d.date_key ,
	ri.case_type_key ,
	ri.source_key ,
	ri.location_key ,
	ri.probate_applctn_type_key,
	/* WJB APD-5735 START */
	ri.prefrd_lang_type_key AS prefrd_lang_type_key,
	/* WJB APD-5735 END */    
		0                                   as submitted,
		0                                   as case_fee_payable_count,
		0                                   AS examined,
		0                                   AS stopped,
		0                                   AS issued,
		0                                   AS DAYS_SUBMITTED_TO_ISSUED,
		0                                   AS ISSUED_IN_7WDAYS,
		0                                   as issued_in_20days,
		0                                   as total_case_stops_count,
		0                                 AS registrar_escltn_case_count,
		0                                 AS registrar_escltn_total_count,    
		0                                 as registrar_escltn_days_count,
		0                                 as caveat_active_count,
		0                                 as wlgmt_count,
		0                                 as standing_search_count,
		0                                 AS caveat_count  ,
		0                                 as grant_issue_rgstr_escltn_count,   --- vidhya - EPF-2396
		0                                 as reissued_total_count,              --- vidhya - EPF-2437
		0                                 as reissued_duplct_count,             --- vidhya - EPF-2437
		0                                 as reissued_rgstr_dirctn_count,       --- vidhya - EPF-2437
		0                                 as reissued_rgstr_order_count ,         --- vidhya - EPF-2437
		count(*)                          as reissue_pending_count,
		0                                     as wthdrwn_total_count, -- SD EPF-2414
		0                                     as wthdrwn_for_proving_count, -- SD EPF-2414
		0                                     as wthdrwn_testator_count, -- SD EPF-2414
		0                                     as wthdrwn_cancelled_count, -- SD EPF-2414
		0                                     as case_srch_matched_count, -- SD EPF-2374
		0                                     as issued_wthn_2wrkgdy_count,           --- vidhya- EPF-2454
		0                                     as exprd_total_count, -- SD EPF-2326
		0                                     as applctn_wthdrwn_count,
		0                                     AS submsn_to_doc_recpt_days_count,
		0                                     AS submsn_to_doc_recpt_wkdy_count,
		0                                     AS doc_recpt_to_issue_days_count,
		0                                     AS doc_recpt_to_issue_wkdy_count,
		0                                     AS submsn_to_issue_wkdy_count,
		0                                     AS doc_recvd_count , -- APD-4013  
		--v1.6
		0                                     AS wrk_restrt_from_stop_count,
		0                                     AS frst_stop_restrt_wrkdy_durtn,
		0                                     AS wrk_strtd_count,
		0                                     AS doc_recvd_to_strtd_wrkdy_durtn,
		0                                     AS doc_recvd_strtd_2wrkdy_count,
		0                                     AS grant_applctn_outstndng_count, -- 1.8 DJ
		0                                     AS doc_recvd_issue_7wrkdy_count   -- v2.1 sp
		from  
		all_dates d, 
			(SELECT c.case_data_id,
				c.case_type_key ,
				c.source_key ,
				c.location_key ,
				c.probate_applctn_type_key,
				/* WJB APD-5735 START */
				c.prefrd_lang_type_key,  
				/* WJB APD-5735 END */               
				s.state_start_timestamp, 
				s.legal_case_state_end_date
		FROM v_legal_probategrant_state s
		INNER JOIN tbl_legal_probategrant_case c
		ON c.case_data_id = s.legal_case_id
		WHERE s.legal_case_state_type_code IN('BOExaminingReissue','BOCaseStoppedReissue','AwaitingReissue','BOCaseMatchingReissue')
		) ri   
			where ri.state_start_timestamp <= d.date_key 
			and (ri.legal_case_state_end_date > d.date_key OR ri.legal_case_state_end_date IS NULL) 
			group by d.date_key ,
			ri.case_type_key ,
			ri.source_key ,
			ri.location_key ,
			ri.probate_applctn_type_key
			/* WJB APD-5735 START */
			,ri.prefrd_lang_type_key            
			/* WJB APD-5735 END */            
			--ORDER BY d.date_key DESC        
	)
	/* Chandra EPF 2511 union ends*/
	/* SD EPF-2414 union start */
	, 
	/*wlgmt_wthdrwn
	AS      
	(   
		SELECT psc.wthdrwn_date        AS date_key,
		psc.case_type_key                 AS case_type_key,
		psc.source_key                    as source_key,
		psc.location_key                  as location_key,
		psc.probate_applctn_type_key      AS probate_applctn_type_key,
		/* WJB APD-5735 START */
		-- assume 'English (or not specified)
		--59.000001                         
		gc_lang_eng_key                     AS prefrd_lang_type_key,
		/* WJB APD-5735 END */               
		0                                   as submitted,
		0                                   as case_fee_payable_count,
		0                                   AS examined,
		0                                   AS stopped,
		0                                   AS issued,
		0                                   AS DAYS_SUBMITTED_TO_ISSUED,
		0                                   AS ISSUED_IN_7WDAYS,
		0                                   as issued_in_20days,
		0                                   as total_case_stops_count,
		0                                 AS registrar_escltn_case_count,
		0                                 AS registrar_escltn_total_count,    
		0                                 as registrar_escltn_days_count,
		0                                 as caveat_active_count,
		0                                 as wlgmt_count,
		0                                 as standing_search_count,
		0                                 as caveat_count,
		0                                 as grant_issue_rgstr_escltn_count,
		0                                 as reissued_total_count,
		0                                 as reissued_duplct_count,
		0                                 as reissued_rgstr_dirctn_count,
		0                                 as reissued_rgstr_order_count , 
		0                                 as reissue_pending_count,
		sum(wthdrwn_ind)                  as wthdrwn_total_count,
		sum(wthdrwn_for_proving_ind)      as wthdrwn_for_proving_count,
		sum(wthdrwn_for_testator_ind)     as wthdrwn_testator_count,
		sum(wthdrwn_for_cancelled_ind)    as wthdrwn_cancelled_count,
		0                                 as case_srch_matched_count,
		0                                     as issued_wthn_2wrkgdy_count,           --- vidhya- EPF-2454
		0                                     as exprd_total_count, -- SD EPF-2326
		0                                     as applctn_wthdrwn_count,
		0                                     AS submsn_to_doc_recpt_days_count,
		0                                     AS submsn_to_doc_recpt_wkdy_count,
		0                                     AS doc_recpt_to_issue_days_count,
		0                                     AS doc_recpt_to_issue_wkdy_count,
		0                                     AS submsn_to_issue_wkdy_count,
		0                                     AS doc_recvd_count, -- APD-4013  
		--v1.6
		0                                     AS wrk_restrt_from_stop_count,
		0                                     AS frst_stop_restrt_wrkdy_durtn,
		0                                     AS wrk_strtd_count,
		0                                     AS doc_recvd_to_strtd_wrkdy_durtn,
		0                                     AS doc_recvd_strtd_2wrkdy_count,
		0                                     AS grant_applctn_outstndng_count, -- 1.8 DJ
		0                                     AS doc_recvd_issue_7wrkdy_count   -- v2.1 sp
		from tbl_legal_probatewlgmt_case psc
		GROUP BY psc.wthdrwn_date, 
		psc.source_key, 
		psc.location_key, 
		psc.case_type_key, 
		psc.probate_applctn_type_key
	)
	/* SD EPF-2414 union end */
	/* SD EPF-2374 union start */
	, srch_cases_matched
	AS
	(       
		SELECT psc.case_matched_date AS date_key,
		psc.case_type_key                 AS case_type_key,
		psc.source_key                    as source_key,
		psc.location_key                  as location_key,
		psc.probate_applctn_type_key      AS probate_applctn_type_key,
		/* WJB APD-5735 START */
		-- assume 'English (or not specified)
		--59.000001                         
		gc_lang_eng_key                   AS prefrd_lang_type_key,
		/* WJB APD-5735 END */         
		0                                 as submitted,
		0                                 as case_fee_payable_count,
		0                                 AS examined,
		0                                 AS stopped,
		0                                 AS issued,
		0                                 AS DAYS_SUBMITTED_TO_ISSUED,
		0                                 AS ISSUED_IN_7WDAYS,
		0                                 as issued_in_20days,
		0                                 as total_case_stops_count,
		0                                 AS registrar_escltn_case_count,
		0                                 AS registrar_escltn_total_count,    
		0                                 as registrar_escltn_days_count,
		0                                 as caveat_active_count,
		0                                 as wlgmt_count,
		0                                 as standing_search_count,
		0                                 AS caveat_count,
		0                                 as grant_issue_rgstr_escltn_count,
		0                                 as reissued_total_count,
		0                                 as reissued_duplct_count,
		0                                 as reissued_rgstr_dirctn_count,
		0                                 as reissued_rgstr_order_count , 
		0                                 as reissue_pending_count,
		0                                 as wthdrwn_total_count,
		0                                 as wthdrwn_for_proving_count,
		0                                 as wthdrwn_testator_count,
		0                                 as wthdrwn_cancelled_count,
		sum(srch_matched_ind)             as case_srch_matched_count,
		0                                 as issued_wthn_2wrkgdy_count,           --- vidhya- EPF-2454
		0                                 as exprd_total_count, -- SD EPF-2326
		0                                 as applctn_wthdrwn_count,
		0                                     AS submsn_to_doc_recpt_days_count,
		0                                     AS submsn_to_doc_recpt_wkdy_count,
		0                                     AS doc_recpt_to_issue_days_count,
		0                                     AS doc_recpt_to_issue_wkdy_count,
		0                                     AS submsn_to_issue_wkdy_count,
		0                                     AS doc_recvd_count, -- APD-4013  
		--v1.6
		0                                     AS wrk_restrt_from_stop_count,
		0                                     AS frst_stop_restrt_wrkdy_durtn,
		0                                     AS wrk_strtd_count,
		0                                     AS doc_recvd_to_strtd_wrkdy_durtn,
		0                                     AS doc_recvd_strtd_2wrkdy_count,
		0                                     AS grant_applctn_outstndng_count, -- 1.8 DJ
		0                                     AS doc_recvd_issue_7wrkdy_count   -- v2.1 sp        
		from tbl_legal_probatesrch_case psc
		where case_matched_date is not null
		GROUP BY psc.case_matched_date, 
		psc.source_key, 
		psc.location_key, 
		psc.case_type_key, 
		psc.probate_applctn_type_key
	)
	/* SD EPF-2374 union end */
	/* SD EPF-2326 union start */
	, caveat_expired --1.7 DJ
	as
	(
		select pc.case_expiry_date  as date_key,
		pc.case_type_key                    as case_type_key,
		pc.source_key                       as source_key,
		pc.location_key                     as location_key,
		pc.probate_applctn_type_key         as probate_applctn_type_key,
		/* WJB APD-5735 START */
		-- assume 'English (or not specified)
		--59.000001                           
		gc_lang_eng_key                     AS prefrd_lang_type_key,
		/* WJB APD-5735 END */            
		0                                   as submitted,
		0                                   as case_fee_payable_count,
		0                                   as examined,
		0                                   as stopped,
		0                                   AS issued,
		0                                   AS DAYS_SUBMITTED_TO_ISSUED,
		0                                   as issued_in_7wdays,
		0                                   as issued_in_20days,
		0                                   as total_case_stops_count,
		0                                   as registrar_escltn_case_count,
		0                                   AS registrar_escltn_total_count,    
		0                                   as registrar_escltn_days_count,
		0                                   as caveat_active_count,
		0                                   as wlgmt_count,
		0                                   as standing_search_count,
		0                                   AS caveat_count ,
		0                                as grant_issue_rgstr_escltn_count,   --- vidhya - EPF-2396
		0                                 as reissued_total_count,              --- vidhya - EPF-2437
		0                                 as reissued_duplct_count,             --- vidhya - EPF-2437
		0                                 as reissued_rgstr_dirctn_count,       --- vidhya - EPF-2437
		0                                 as reissued_rgstr_order_count ,         --- vidhya - EPF-2437
		0                                 as reissue_pending_count,  -- Chandra  -- EPF 2511
		0                                     as wthdrwn_total_count, -- SD EPF-2414
		0                                     as wthdrwn_for_proving_count, -- SD EPF-2414
		0                                     as wthdrwn_testator_count, -- SD EPF-2414
		0                                     as wthdrwn_cancelled_count, -- SD EPF-2414
		0                                     as case_srch_matched_count, -- SD EPF-2374
		0                                     as issued_wthn_2wrkgdy_count,           --- vidhya- EPF-2454
		sum(caveat_exprd_ind)                 as exprd_total_count,
		0                                     as applctn_wthdrwn_count,
		0                                     AS submsn_to_doc_recpt_days_count,
		0                                     AS submsn_to_doc_recpt_wkdy_count,
		0                                     AS doc_recpt_to_issue_days_count,
		0                                     AS doc_recpt_to_issue_wkdy_count,
		0                                     AS submsn_to_issue_wkdy_count,
		0                                     AS doc_recvd_count, -- APD-4013  
		--v1.6
		0                                     AS wrk_restrt_from_stop_count,
		0                                     AS frst_stop_restrt_wrkdy_durtn,
		0                                     AS wrk_strtd_count,
		0                                     AS doc_recvd_to_strtd_wrkdy_durtn,
		0                                     AS doc_recvd_strtd_2wrkdy_count,
		0                                     AS grant_applctn_outstndng_count, -- 1.8 DJ
		0                                     AS doc_recvd_issue_7wrkdy_count   -- v2.1 sp
		FROM tbl_legal_probatecaveat_case pc
		WHERE pc.case_expiry_date < CONVERT(DATETIME, CONVERT(DATE,GETDATE())) -- 1.7 DJ new where condition
		GROUP BY pc.case_expiry_date, 
		pc.source_key, 
		pc.location_key, 
		pc.case_type_key, 
		pc.probate_applctn_type_key
	), */
	grant_withdrawn
	AS
	(      
		SELECT ris.applctn_wthdrwn_date AS date_key,
		ris.case_type_key                 AS case_type_key,
		ris.source_key                    as source_key,
		ris.location_key                  AS location_key,
		ris.probate_applctn_type_key      AS probate_applctn_type_key,
		/* WJB APD-5735 START */
		ris.prefrd_lang_type_key          AS prefrd_lang_type_key,
		/* WJB APD-5735 END */          
		0                                   as submitted,
		0                                   as case_fee_payable_count,
		0                                   AS examined,
		0                                   AS stopped,
		0                                   AS issued,
		0                                   AS DAYS_SUBMITTED_TO_ISSUED,
		0                                   AS ISSUED_IN_7WDAYS,
		0                                   as issued_in_20days,
		0                                   as total_case_stops_count,
		0                                   AS registrar_escltn_case_count,
		0                                   AS registrar_escltn_total_count,    
		0                                   as registrar_escltn_days_count,
		0                                   as caveat_active_count,
		0                                   as wlgmt_count,
		0                                   as standing_search_count,
		0                                   as caveat_count,
		0                                   as grant_issue_rgstr_escltn_count,
		0                                   as reissued_total_count,
		0                                   as reissued_duplct_count,
		0                                   as reissued_rgstr_dirctn_count,
		0                                   as reissued_rgstr_order_count,
		0                                     as reissue_pending_count,  
		0                                     as wthdrwn_total_count, 
		0                                     as wthdrwn_for_proving_count, 
		0                                     as wthdrwn_testator_count, 
		0                                     as wthdrwn_cancelled_count, 
		0                                     as case_srch_matched_count, 
		0                                     as issued_wthn_2wrkgdy_count,           
		0                                     as exprd_total_count, 
		SUM(applctn_wthdrwn_ind)              as applctn_wthdrwn_count,
		0                                     AS submsn_to_doc_recpt_days_count,
		0                                     AS submsn_to_doc_recpt_wkdy_count,
		0                                     AS doc_recpt_to_issue_days_count,
		0                                     AS doc_recpt_to_issue_wkdy_count,
		0                                     AS submsn_to_issue_wkdy_count,
		0                                     AS doc_recvd_count, -- APD-4013  
		--v1.6
		0                                     AS wrk_restrt_from_stop_count,
		0                                     AS frst_stop_restrt_wrkdy_durtn,
		0                                     AS wrk_strtd_count,
		0                                     AS doc_recvd_to_strtd_wrkdy_durtn,
		0                                     AS doc_recvd_strtd_2wrkdy_count,
		0                                     AS grant_applctn_outstndng_count, -- 1.8 DJ
		0                                     AS doc_recvd_issue_7wrkdy_count   -- v2.1 sp
		from tbl_legal_probategrant_case ris
		Where ris.applctn_wthdrwn_date is not null
		GROUP BY ris.applctn_wthdrwn_date, 
		ris.source_key, 
		ris.location_key, 
		ris.case_type_key, 
		ris.probate_applctn_type_key
		/* WJB APD-5735 START */
		,ris.prefrd_lang_type_key
		/* WJB APD-5735 END */       
		)
	/* SD EPF-2326 union end */
	, doccument_recipt                --v1.2
	AS
	(   SELECT ris.case_doc_recvd_date AS date_key,
		ris.case_type_key                 AS case_type_key,
		ris.source_key                    as source_key,
		ris.location_key                  AS location_key,
		ris.probate_applctn_type_key      AS probate_applctn_type_key,
		/* WJB APD-5735 START */
		ris.prefrd_lang_type_key          AS prefrd_lang_type_key,
		/* WJB APD-5735 END */           
		0                                   as submitted,
		0                                   as case_fee_payable_count,
		0                                   AS examined,
		0                                   AS stopped,
		0                                   AS issued,
		0                                   AS DAYS_SUBMITTED_TO_ISSUED,
		0                                   AS ISSUED_IN_7WDAYS,
		0                                   as issued_in_20days,
		0                                   as total_case_stops_count,
		0                                   AS registrar_escltn_case_count,
		0                                   AS registrar_escltn_total_count,    
		0                                   as registrar_escltn_days_count,
		0                                   as caveat_active_count,
		0                                   as wlgmt_count,
		0                                   as standing_search_count,
		0                                   as caveat_count,
		0                                   as grant_issue_rgstr_escltn_count,
		0                                   as reissued_total_count,
		0                                   as reissued_duplct_count,
		0                                   as reissued_rgstr_dirctn_count,
		0                                   as reissued_rgstr_order_count,
		0                                     as reissue_pending_count,  
		0                                     as wthdrwn_total_count, 
		0                                     as wthdrwn_for_proving_count, 
		0                                     as wthdrwn_testator_count, 
		0                                     as wthdrwn_cancelled_count, 
		0                                     as case_srch_matched_count, 
		0                                     as issued_wthn_2wrkgdy_count,           
		0                                     as exprd_total_count, 
		0                                     as applctn_wthdrwn_count,
		SUM(submsn_to_doc_recpt_days_count)   AS submsn_to_doc_recpt_days_count,
		SUM(submsn_to_doc_recpt_wkdy_count)   AS submsn_to_doc_recpt_wkdy_count,
		0                                     AS doc_recpt_to_issue_days_count,
		0                                     AS doc_recpt_to_issue_wkdy_count,
		0                                     AS submsn_to_issue_wkdy_count,
		SUM(case_doc_recvd_ind )              AS doc_recvd_count, -- APD-4013  
		--v1.6
		0                                     AS wrk_restrt_from_stop_count,
		0                                     AS frst_stop_restrt_wrkdy_durtn,
		0                                     AS wrk_strtd_count,
		0                                     AS doc_recvd_to_strtd_wrkdy_durtn,
		0                                     AS doc_recvd_strtd_2wrkdy_count,
		0                                     AS grant_applctn_outstndng_count, -- 1.8 DJ
		0                                     AS doc_recvd_issue_7wrkdy_count   -- v2.1 sp
		from tbl_legal_probategrant_case ris
		Where ris.case_doc_recvd_date is not null
		GROUP BY ris.case_doc_recvd_date, 
		ris.source_key, 
		ris.location_key, 
		ris.case_type_key, 
		ris.probate_applctn_type_key
		/* WJB APD-5735 START */
		,ris.prefrd_lang_type_key      
		/* WJB APD-5735 END */      
		)
	, wrk_restart        --v1.6
	AS
	(   SELECT wrst.wrk_restrt_from_stop_date AS date_key,
		wrst.case_type_key                 AS case_type_key,
		wrst.source_key                    as source_key,
		wrst.location_key                  AS location_key,
		wrst.probate_applctn_type_key      AS probate_applctn_type_key,
		/* WJB APD-5735 START */
		wrst.prefrd_lang_type_key          AS prefrd_lang_type_key,
		/* WJB APD-5735 END */          
		0                                   as submitted,
		0                                   as case_fee_payable_count,
		0                                   AS examined,
		0                                   AS stopped,
		0                                   AS issued,
		0                                   AS DAYS_SUBMITTED_TO_ISSUED,
		0                                   AS ISSUED_IN_7WDAYS,
		0                                   as issued_in_20days,
		0                                   as total_case_stops_count,
		0                                   AS registrar_escltn_case_count,
		0                                   AS registrar_escltn_total_count,    
		0                                   as registrar_escltn_days_count,
		0                                   as caveat_active_count,
		0                                   as wlgmt_count,
		0                                   as standing_search_count,
		0                                   as caveat_count,
		0                                   as grant_issue_rgstr_escltn_count,
		0                                   as reissued_total_count,
		0                                   as reissued_duplct_count,
		0                                   as reissued_rgstr_dirctn_count,
		0                                   as reissued_rgstr_order_count,
		0                                     as reissue_pending_count,  
		0                                     as wthdrwn_total_count, 
		0                                     as wthdrwn_for_proving_count, 
		0                                     as wthdrwn_testator_count, 
		0                                     as wthdrwn_cancelled_count, 
		0                                     as case_srch_matched_count, 
		0                                     as issued_wthn_2wrkgdy_count,           
		0                                     as exprd_total_count, 
		0                                     as applctn_wthdrwn_count,
		0                                     AS submsn_to_doc_recpt_days_count,
		0                                     AS submsn_to_doc_recpt_wkdy_count,
		0                                     AS doc_recpt_to_issue_days_count,
		0                                     AS doc_recpt_to_issue_wkdy_count,
		0                                     AS submsn_to_issue_wkdy_count,
		0                                     AS doc_recvd_count ,
		SUM(wrst.wrk_restrt_from_stop_ind)         AS wrk_restrt_from_stop_count,
		SUM(wrst.frst_stop_restrt_wrkdy_durtn)     AS frst_stop_restrt_wrkdy_durtn,
		0                                     AS wrk_strtd_count,
		0                                     AS doc_recvd_to_strtd_wrkdy_durtn,
		0                                     AS doc_recvd_strtd_2wrkdy_count,
		0                                     AS grant_applctn_outstndng_count, -- 1.8 DJ
		0                                     AS doc_recvd_issue_7wrkdy_count   -- v2.1 sp
		from tbl_legal_probategrant_case wrst
		Where wrst.wrk_restrt_from_stop_date is not null
		GROUP BY wrst.wrk_restrt_from_stop_date, 
		wrst.source_key, 
		wrst.location_key, 
		wrst.case_type_key, 
		wrst.probate_applctn_type_key
		/* WJB APD-5735 START */
		,wrst.prefrd_lang_type_key
		/* WJB APD-5735 END */        
		)
	, wrk_st_doc_recv      --v1.6
	AS
	(   SELECT wsdr.wrk_strtd_from_doc_recvd_date AS date_key,
		wsdr.case_type_key                 AS case_type_key,
		wsdr.source_key                    as source_key,
		wsdr.location_key                  AS location_key,
		wsdr.probate_applctn_type_key      AS probate_applctn_type_key,
		/* WJB APD-5735 START */
		wsdr.prefrd_lang_type_key          AS prefrd_lang_type_key,
		/* WJB APD-5735 END */         
		0                                   as submitted,
		0                                   as case_fee_payable_count,
		0                                   AS examined,
		0                                   AS stopped,
		0                                   AS issued,
		0                                   AS DAYS_SUBMITTED_TO_ISSUED,
		0                                   AS ISSUED_IN_7WDAYS,
		0                                   as issued_in_20days,
		0                                   as total_case_stops_count,
		0                                   AS registrar_escltn_case_count,
		0                                   AS registrar_escltn_total_count,    
		0                                   as registrar_escltn_days_count,
		0                                   as caveat_active_count,
		0                                   as wlgmt_count,
		0                                   as standing_search_count,
		0                                   as caveat_count,
		0                                   as grant_issue_rgstr_escltn_count,
		0                                   as reissued_total_count,
		0                                   as reissued_duplct_count,
		0                                   as reissued_rgstr_dirctn_count,
		0                                   as reissued_rgstr_order_count,
		0                                     as reissue_pending_count,  
		0                                     as wthdrwn_total_count, 
		0                                     as wthdrwn_for_proving_count, 
		0                                     as wthdrwn_testator_count, 
		0                                     as wthdrwn_cancelled_count, 
		0                                     as case_srch_matched_count, 
		0                                     as issued_wthn_2wrkgdy_count,           
		0                                     as exprd_total_count, 
		0                                     as applctn_wthdrwn_count,
		0                                     AS submsn_to_doc_recpt_days_count,
		0                                     AS submsn_to_doc_recpt_wkdy_count,
		0                                     AS doc_recpt_to_issue_days_count,
		0                                     AS doc_recpt_to_issue_wkdy_count,
		0                                     AS submsn_to_issue_wkdy_count,
		0                                     AS doc_recvd_count ,
		0                                     AS wrk_restrt_from_stop_count,
		0                                     AS frst_stop_restrt_wrkdy_durtn,
		SUM(wrk_strtd_from_doc_recvd_ind)     AS wrk_strtd_count,
		SUM(doc_recvd_to_strtd_wrkdy_durtn)   AS doc_recvd_to_strtd_wrkdy_durtn,
		SUM(doc_recvd_to_strtd_2wrkdy_ind)    AS doc_recvd_strtd_2wrkdy_count,
		0                                     AS grant_applctn_outstndng_count, -- 1.8 DJ
		0                                     AS doc_recvd_issue_7wrkdy_count   -- v2.1 sp
		from tbl_legal_probategrant_case wsdr
		Where wsdr.wrk_strtd_from_doc_recvd_date is not null
		GROUP BY wsdr.wrk_strtd_from_doc_recvd_date, 
		wsdr.source_key, 
		wsdr.location_key, 
		wsdr.case_type_key, 
		wsdr.probate_applctn_type_key
		/* WJB APD-5735 START */
		,wsdr.prefrd_lang_type_key
		/* WJB APD-5735 END */       
		)
	, 
		sumbt_date -- 1.8 added new CTE
		AS
	( select date_key
		from dim_date
		where date_key > ( select min(case_submitted_date) from tbl_legal_probategrant_case )
		and date_key < GETDATE() --current_date
	)                                          
		, outstanding -- 1.8 added new CTE
	AS
	(   SELECT sud.date_key AS date_key,
		pc.case_type_key                 AS case_type_key,
		pc.source_key                    AS source_key,
		pc.location_key                  AS location_key,
		pc.probate_applctn_type_key      AS probate_applctn_type_key,
		/* WJB APD-5735 START */
		pc.prefrd_lang_type_key          AS prefrd_lang_type_key,
		/* WJB APD-5735 END */        
		0                                   as submitted,
		0                                   as case_fee_payable_count,
		0                                   AS examined,
		0                                   AS stopped,
		0                                   AS issued,
		0                                   AS DAYS_SUBMITTED_TO_ISSUED,
		0                                   AS ISSUED_IN_7WDAYS,
		0                                   AS ISSUED_IN_20DAYS,
		0                                   as total_case_stops_count,
		0                                 AS registrar_escltn_case_count,
		0                                 AS registrar_escltn_total_count,    
		0                                 AS registrar_escltn_days_count,
		0                                                                                                         as caveat_active_count,
		0                                 as wlgmt_count,
		0                                 as standing_search_count,
		0                                 AS caveat_count  ,
		0                                as grant_issue_rgstr_escltn_count,   -- vidhya - EPF-2396
		0                                 as reissued_total_count,              -- vidhya - EPF-2437
		0                                 as reissued_duplct_count,             -- vidhya - EPF-2437
		0                                 as reissued_rgstr_dirctn_count,       -- vidhya - EPF-2437
		0                                 as reissued_rgstr_order_count ,         -- vidhya - EPF-2437
		0                                 as reissue_pending_count,   --Chandra   EPF 2511
		0                                     as wthdrwn_total_count,  --SD EPF-2414
		0                                     as wthdrwn_for_proving_count,  --SD EPF-2414
		0                                     as wthdrwn_testator_count,  --SD EPF-2414
		0                                     as wthdrwn_cancelled_count,  --SD EPF-2414
		0                                     as case_srch_matched_count,  --SD EPF-2374
		0                                     as issued_wthn_2wrkgdy_count,  -- vidhya- EPF-2454
		0                                     as exprd_total_count,  --SD EPF-2326
		0                                     as applctn_wthdrwn_count,
		0                                     AS submsn_to_doc_recpt_days_count,
		0                                     AS submsn_to_doc_recpt_wkdy_count,
		0                                     AS doc_recpt_to_issue_days_count,
		0                                     AS doc_recpt_to_issue_wkdy_count,
		0                                     AS submsn_to_issue_wkdy_count,
		0                                     AS doc_recvd_count ,  --APD-4013  
		0                                     AS wrk_restrt_from_stop_count,
		0                                     AS frst_stop_restrt_wrkdy_durtn,
		0                                     AS wrk_strtd_count,
		0                                     AS doc_recvd_to_strtd_wrkdy_durtn,
		0                                     AS doc_recvd_strtd_2wrkdy_count, 
		count(1)                                                                                                                      AS grant_applctn_outstndng_count,
		0                                     AS doc_recvd_issue_7wrkdy_count   -- v2.1 sp
		FROM TBL_LEGAL_PROBATEGRANT_CASE pc
		cross join sumbt_date sud
		WHERE  pc.CASE_SUBMITTED_DATE  <= sud.date_key
		AND ( pc.CASE_CLOSED_DATE IS NULL OR pc.CASE_CLOSED_DATE > sud.date_key )
		/* WJB APD-6436 START */
		AND (sud.date_key - pc.case_submitted_date) <= 183
		AND pc.case_submitted_date >= '20-Mar-2019' 
		/* WJB APD-6436 END */        
		GROUP BY sud.date_key,      
				pc.source_key,       
				pc.location_key,       
				pc.case_type_key,       
				pc.probate_applctn_type_key
				/* WJB APD-5735 START */
				,pc.prefrd_lang_type_key              
				/* WJB APD-5735 END */
				)      
	, 
	all_measures
	AS
	(
		SELECT * FROM submitted
		UNION ALL
		SELECT * FROM examined
		UNION ALL
		SELECT * FROM stopped
		UNION ALL
		SELECT * FROM issued_escalations_stops
		UNION ALL
		--SELECT * FROM caveat_activecount
		--UNION ALL
		--SELECT * FROM wlgmt
		--UNION ALL
		--SELECT * FROM search
		--UNION ALL                
		--SELECT * FROM caveat    
		--UNION ALL                      -- vidhya EPF 2437
		SELECT * FROM reissued     -- vidhya EPF 2437
		UNION ALL
		select * from reissue_pending  -- Chandra EPF 2511
		--SD--
		UNION ALL
		--SELECT * FROM wlgmt_wthdrwn
		--UNION ALL
		--select * from srch_cases_matched
		--SD--
		--UNION ALL
		--SELECT * FROM caveat_expired
		--UNION ALL
		SELECT * FROM grant_withdrawn
		UNION ALL
		SELECT * FROM doccument_recipt -- v1.2
		UNION ALL             --v1.6
		SELECT * FROM wrk_st_doc_recv --v1.6
		UNION ALL --v1.6
		SELECT * FROM wrk_restart   --v1.6
		UNION ALL
		SELECT * FROM outstanding -- 1.8 DJ
	)
	SELECT date_key,
		case_type_key,
		source_key,
		location_key,
		probate_applctn_type_key,
		/* WJB APD-5735 START */
		prefrd_lang_type_key,
		/* WJB APD-5735 END */          
		sum(submitted)                                              as submitted,
		SUM(case_fee_payable_count)                                 as case_fee_payable_count,
		SUM(examined)                                               as examined,
		SUM(stopped)                                                as stopped,
		SUM(issued)                                                 as issued,
		SUM(submitted) + SUM(examined) + SUM(stopped) + SUM(issued) as total_cases,
		SUM(days_submitted_to_issued)                               as days_submitted_to_issued,
		SUM(issued_in_7wdays)                                       as issued_in_7wdays,
		SUM(issued_in_20days)                                       as issued_in_20days,
		SUM(total_case_stops_count)                                 as total_case_stops_count,
		SUM(registrar_escltn_case_count)                            AS registrar_escltn_case_count,
		SUM(registrar_escltn_total_count)                           AS registrar_escltn_total_count,
		SUM(registrar_escltn_days_count)                            AS registrar_escltn_days_count,    
		SUM(caveat_active_count)                                    as caveat_active_count,
		SUM(wlgmt_count)                                            as wlgmt_count,
		SUM(standing_search_count)                                  as standing_search_count,
		sum(caveat_count)                                           AS caveat_count,
		sum(grant_issue_rgstr_escltn_count)                         as grant_issue_rgstr_escltn_count,  --- vidhya - EPF-2396
		sum(reissued_total_count)                                   as reissued_total_count,     --- vidhya - EPF-2437
		sum(reissued_duplct_count)                                  as reissued_duplct_count,    --- vidhya - EPF-2437
		sum(reissued_rgstr_dirctn_count)                            as reissued_rgstr_dirctn_count,    --- vidhya - EPF-2437
		sum(reissued_rgstr_order_count)                             as reissued_rgstr_order_count,      --- vidhya - EPF-2437
		sum(reissue_pending_count)                                  as reissue_pending_count,
		sum(wthdrwn_total_count)                                    as wthdrwn_total_count,
		sum(wthdrwn_for_proving_count)                              as wthdrwn_for_proving_count,
		sum(wthdrwn_testator_count)                                 as wthdrwn_testator_count,
		sum(wthdrwn_cancelled_count)                                as wthdrwn_cancelled_count,
		sum(case_srch_matched_count)                                as case_srch_matched_count,
		sum(issued_wthn_2wrkgdy_count)                              as issued_wthn_2wrkgdy_count,           --- vidhya- EPF-2454
		sum(exprd_total_count)                                      as exprd_total_count,
		sum(applctn_wthdrwn_count)                                  AS grant_applctn_wthdrwn_count,
		sum(submsn_to_doc_recpt_days_count)                         AS submsn_to_doc_recpt_days_count,
		sum(submsn_to_doc_recpt_wkdy_count)                         AS submsn_to_doc_recpt_wkdy_count,
		sum(doc_recpt_to_issue_days_count)                          AS doc_recpt_to_issue_days_count,
		sum(doc_recpt_to_issue_wkdy_count)                          AS doc_recpt_to_issue_wkdy_count,
		sum(submsn_to_issue_wkdy_count)                             AS submsn_to_issue_wkdy_count,
		sum(doc_recvd_count)                                        as doc_recvd_count, -- APD-4013
		--v1.6
		SUM(wrk_restrt_from_stop_count)                             AS wrk_restrt_from_stop_count,
		SUM(frst_stop_restrt_wrkdy_durtn)                           AS frst_stop_restrt_wrkdy_durtn,
		SUM(wrk_strtd_count)                                        AS wrk_strtd_count,
		SUM(doc_recvd_to_strtd_wrkdy_durtn)                         AS doc_recvd_to_strtd_wrkdy_durtn,
		SUM(doc_recvd_strtd_2wrkdy_count)                           AS doc_recvd_strtd_2wrkdy_count,
		SUM(grant_applctn_outstndng_count)                          AS grant_applctn_outstndng_count,  -- 1.8 DJ
		SUM(doc_recvd_issue_7wrkdy_count)                           AS doc_recvd_issue_7wrkdy_count    -- v2.1 sp
	INTO #Temp
	FROM all_measures
	where isnull(submitted, 0) > 0
	OR ISNULL(case_fee_payable_count, 0) > 0
	OR ISNULL(examined, 0)     > 0
	OR ISNULL(stopped, 0)      > 0
	OR isnull(issued, 0)       > 0
	OR ISNULL(registrar_escltn_total_count,0) > 0
	OR isnull(caveat_count,0) > 0
	OR isnull(caveat_active_count,0) > 0
	OR isnull(wlgmt_count,0) > 0
	OR isnull(standing_search_count,0) > 0
	OR isnull(total_case_stops_count,0) > 0
	OR isnull(grant_issue_rgstr_escltn_count, 0) > 0  --- vidhya - EPF-2396
	OR isnull(reissued_total_count, 0) > 0            --- vidhya - EPF-2396
	OR isnull(reissue_pending_count,0) > 0            --- Chandra - EPF 2511
	OR isnull(wthdrwn_total_count, 0) > 0
	OR isnull(case_srch_matched_count, 0) > 0
	OR isnull(issued_wthn_2wrkgdy_count, 0) > 0       ---- vidhya - EPF 2454
	OR isnull(exprd_total_count, 0) > 0
	OR isnull(applctn_wthdrwn_count, 0) > 0
	OR isnull(submsn_to_doc_recpt_days_count, 0) > 0
	OR isnull(submsn_to_doc_recpt_wkdy_count, 0) > 0
	OR isnull(doc_recpt_to_issue_days_count, 0) > 0
	OR isnull(doc_recpt_to_issue_wkdy_count, 0) > 0
	OR isnull(submsn_to_issue_wkdy_count, 0) > 0
	OR isnull(doc_recvd_count, 0) > 0 -- APD-4013
	--v1.6
	OR isnull(wrk_restrt_from_stop_count, 0) > 0 
	OR isnull(frst_stop_restrt_wrkdy_durtn, 0) > 0 
	OR isnull(wrk_strtd_count, 0) > 0 
	OR isnull(doc_recvd_to_strtd_wrkdy_durtn, 0) > 0 
	OR isnull(doc_recvd_strtd_2wrkdy_count, 0) > 0
	OR isnull(grant_applctn_outstndng_count, 0) > 0 -- 1.8 DJ
	OR isnull(doc_recvd_issue_7wrkdy_count, 0) > 0  -- v2.1 sp
	GROUP BY 
	date_key,
	case_type_key,
	source_key,
	location_key,
	probate_applctn_type_key,
	prefrd_lang_type_key;

	INSERT INTO fct_probate_summary
	(
		date_key,
		case_type_key,
		source_key,
		location_key,
		probate_applctn_type_key,
		/* WJB APD-5735 START */
		prefrd_lang_type_key, 
		/* WJB APD-5735 END */           
		submitted,
		case_fee_payable_count,
		examined,
		stopped,
		issued,
		total_cases,
		days_submitted_to_issued,
		issued_in_7wdays,
		issued_in_20days,
		total_case_stops_count,
		rgstr_escltn_case_count,
		rgstr_escltn_total_count,
		rgstr_escltn_days_count,
		caveat_active_count,
		wlgmt_count,
		standing_search_count,
		caveat_count ,
		grant_issue_rgstr_escltn_count,  --- vidhya - epf-2396
		reissued_total_count,     --- vidhya - epf-2437
		reissued_duplct_count,    --- vidhya - epf-2437
		reissued_rgstr_dirctn_count,    --- vidhya - epf-2437
		reissued_rgstr_order_count,      --- vidhya - EPF-2437
		reissue_pending_count,   -- Chandra  -- EPF 2511
		--SD--
		wthdrwn_total_count,
		wthdrwn_for_proving_count,
		wthdrwn_testator_count,
		wthdrwn_cancelled_count,
		case_srch_matched_count,
		issued_wthn_2wrkgdy_count,       ---- vidhya - EPF-2454
		--SD--
		exprd_total_count,
		grant_applctn_wthdrwn_count,
		submsn_to_doc_recpt_days_count,       --v1.2
		submsn_to_doc_recpt_wkdy_count,
		doc_recpt_to_issue_days_count,
		doc_recpt_to_issue_wkdy_count,
		submsn_to_issue_wkdy_count,
		doc_recvd_count, -- APD-4013
		--v1.6
		wrk_restrt_from_stop_count,
		frst_stop_restrt_wrkdy_durtn,
		wrk_strtd_count,
		doc_recvd_to_strtd_wrkdy_durtn,
		doc_recvd_strtd_2wrkdy_count,
		grant_applctn_outstndng_count, -- 1.8 DJ
		doc_recvd_issue_7wrkdy_count   -- v2.1 sp
	)
	SELECT date_key,
		case_type_key,
		source_key,
		location_key,
		probate_applctn_type_key,
		/* WJB APD-5735 START */
		prefrd_lang_type_key, 
		/* WJB APD-5735 END */           
		submitted,
		case_fee_payable_count,
		examined,
		stopped,
		issued,
		total_cases,
		days_submitted_to_issued,
		issued_in_7wdays,
		issued_in_20days,
		total_case_stops_count,
		registrar_escltn_case_count,
		registrar_escltn_total_count,
		registrar_escltn_days_count,
		caveat_active_count,
		wlgmt_count,
		standing_search_count,
		caveat_count ,
		grant_issue_rgstr_escltn_count,  --- vidhya - epf-2396
		reissued_total_count,     --- vidhya - epf-2437
		reissued_duplct_count,    --- vidhya - epf-2437
		reissued_rgstr_dirctn_count,    --- vidhya - epf-2437
		reissued_rgstr_order_count,      --- vidhya - EPF-2437
		reissue_pending_count,   -- Chandra  -- EPF 2511
		--SD--
		wthdrwn_total_count,
		wthdrwn_for_proving_count,
		wthdrwn_testator_count,
		wthdrwn_cancelled_count,
		case_srch_matched_count,
		issued_wthn_2wrkgdy_count,       ---- vidhya - EPF-2454
		--SD--
		exprd_total_count,
		grant_applctn_wthdrwn_count,
		submsn_to_doc_recpt_days_count,       --v1.2
		submsn_to_doc_recpt_wkdy_count,
		doc_recpt_to_issue_days_count,
		doc_recpt_to_issue_wkdy_count,
		submsn_to_issue_wkdy_count,
		doc_recvd_count, -- APD-4013
		--v1.6
		wrk_restrt_from_stop_count,
		frst_stop_restrt_wrkdy_durtn,
		wrk_strtd_count,
		doc_recvd_to_strtd_wrkdy_durtn,
		doc_recvd_strtd_2wrkdy_count,
		grant_applctn_outstndng_count, -- 1.8 DJ
		doc_recvd_issue_7wrkdy_count   -- v2.1 sp
	FROM #Temp;

	DROP TABLE #Temp
END
