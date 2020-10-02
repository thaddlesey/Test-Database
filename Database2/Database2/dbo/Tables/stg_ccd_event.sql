CREATE TABLE [dbo].[stg_ccd_event] (
    [extraction_date]             VARCHAR (2000) NULL,
    [ce_case_data_id]             VARCHAR (2000) NULL,
    [cd_created_date]             VARCHAR (2000) NULL,
    [cd_last_modified]            VARCHAR (2000) NULL,
    [cd_jurisdiction]             VARCHAR (2000) NULL,
    [cd_latest_state]             VARCHAR (2000) NULL,
    [cd_reference]                VARCHAR (2000) NULL,
    [cd_security_classification]  VARCHAR (2000) NULL,
    [cd_version]                  VARCHAR (2000) NULL,
    [cd_last_state_modified_date] VARCHAR (2000) NULL,
    [ce_id]                       VARCHAR (2000) NULL,
    [ce_created_date]             VARCHAR (2000) NULL,
    [ce_event_id]                 VARCHAR (2000) NULL,
    [ce_summary]                  VARCHAR (2000) NULL,
    [ce_description]              VARCHAR (2000) NULL,
    [ce_user_id]                  VARCHAR (2000) NULL,
    [ce_case_type_id]             VARCHAR (2000) NULL,
    [ce_case_type_version]        VARCHAR (2000) NULL,
    [ce_state_id]                 VARCHAR (2000) NULL,
    [ce_data]                     NVARCHAR (MAX) NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

