CREATE VIEW [V_PRBTGRANT_CASE_EVENT_GRPS]
AS SELECT c.case_type_key,

    c.case_type_descriptor      AS legal_case_type_name,

    e.src_system_case_type_name AS source_case_type_name,

    eg.event_type_grp_key,

    eg.event_type_grp_name,

    e.event_type_key,

    e.src_event_type AS source_event_type_cid

  FROM mdl_event_case_type_in_grp g

  JOIN dim_case_type c

  ON g.case_type_key = c.case_type_key

  JOIN mdm_event_type_grp eg

  ON eg.event_type_grp_key = g.event_type_grp_key

  JOIN dim_event_type e

  ON g.event_type_key      = e.event_type_key

  WHERE c.jurisdiction_key = 15.000047

  AND c.case_type_code    IN ( 'intestacy', 'admonWill', 'gop', 'edgeCase');