CREATE VIEW [v_legal_probategrant_case_attr]
AS SELECT legal_case_id
, legal_case_ref_cid
, attr_key
, attr_value_text
, attr_value_date
, attr_value_number
, bis_first_created_datetime
, bis_last_modified_datetime
, modified_by_process_name
, inserted_by_process_name
, source_system_type_code
FROM tbl_legal_case_attr
WHERE legal_case_type_name = 'GrantOfRepresentation';