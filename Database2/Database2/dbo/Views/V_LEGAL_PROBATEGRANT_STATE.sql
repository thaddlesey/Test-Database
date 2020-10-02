CREATE VIEW [V_LEGAL_PROBATEGRANT_STATE]
AS SELECT legal_case_id,

    legal_case_state_type_code,

    legal_case_state_event_id,

    legal_case_state_type_name,

    state_start_timestamp,

    legal_case_state_end_date,

    bis_first_created_datetime,

    bis_last_modified_datetime,

    modified_by_process_name,

    inserted_by_process_name,

    source_system_type_code,

    legal_case_state_type_key,

    durtn_days

  FROM tbl_legal_case_state

  WHERE legal_case_type_name = 'GrantOfRepresentation';