CREATE VIEW [dbo].[V_LEGAL_PROBATE_GRANT_CASE]
AS WITH legal_probate_grant_case AS
  (SELECT legal_case_id,
    legal_case_ref_cid,
    bis_first_created_datetime,
    bis_last_modified_datetime,
    "19.000009"            AS applctn_submtd_role_type_name,
    "19.00001"             AS registry_location_name,
    "19.000011"            AS deceased_death_date,
    "19.000012"            AS applctn_paperform_ind,
    "19.000013"            AS applctn_submtd_date,
    "19.000014"            AS will_exists_ind,
    "19.000015"			   AS estate_net_value_amount,
    "19.000016"			   AS estate_gross_value_amount,
    "19.000017"            AS assets_held_int_othr_name_ind,
    "19.000018"            AS stop_reason_event_type_name,
    "19.000019"            AS gor_case_type_name,
    "19.00002"             AS grant_issued_date,
    "19.000021"            AS legacy_case_record_id,
    "19.000026"            AS latest_reissue_date,
    "19.000027"            AS reissue_reason_type_name,
    "19.000122"            AS welsh_lang_pref_ind
  FROM
    (SELECT pca.legal_case_id,
      pca.legal_case_ref_cid,
      pca.attr_key,
      at.attr_data_type_name,
      --pca.attr_value_date,
      pca.attr_value_text,
      --pca.attr_value_number,
      MIN(pca.bis_first_created_datetime) OVER(PARTITION BY pca.legal_case_id) AS bis_first_created_datetime,
      MAX(pca.bis_last_modified_datetime) OVER(PARTITION BY pca.legal_case_id) AS bis_last_modified_datetime
    FROM v_legal_probategrant_case_attr pca
    JOIN mdm_attr_type at
    ON at.attr_key = pca.attr_key
    ) p
	PIVOT 
	( 
		MAX( attr_value_text ) 
		FOR attr_key IN( [19.000009], [19.00001], [19.000011], [19.000012], [19.000013], [19.000014], [19.000015], [19.000016], [19.000017], [19.000018], [19.000019], [19.00002], [19.000021], [19.000026], [19.000027], [19.000122] ) 
	) AS pvt
   )
  SELECT legal_case_id,
  legal_case_ref_cid,
  bis_first_created_datetime,
  bis_last_modified_datetime,
  applctn_submtd_role_type_name,
  registry_location_name,
  deceased_death_date,
  applctn_paperform_ind,
  applctn_submtd_date,
  will_exists_ind,
  estate_net_value_amount,
  estate_gross_value_amount,
  assets_held_int_othr_name_ind,
  stop_reason_event_type_name,
  gor_case_type_name,
  grant_issued_date,
  legacy_case_record_id,
  latest_reissue_date,
  reissue_reason_type_name,
  welsh_lang_pref_ind
FROM legal_probate_grant_case;