﻿CREATE TABLE [dbo].[TBL_LEGAL_CASE_ATTR] (
    [LEGAL_CASE_ID]              DECIMAL (38)    NOT NULL,
    [LEGAL_CASE_TYPE_NAME]       VARCHAR (50)    NOT NULL,
    [ATTR_KEY]                   DECIMAL (10, 6) NOT NULL,
    [ATTR_VALUE_TEXT]            VARCHAR (4000)  NULL,
    [ATTR_VALUE_DATE]            DATETIME        NULL,
    [ATTR_VALUE_NUMBER]          DECIMAL (38)    NULL,
    [BIS_FIRST_CREATED_DATETIME] DATETIME        NULL,
    [BIS_LAST_MODIFIED_DATETIME] DATETIME        NULL,
    [MODIFIED_BY_PROCESS_NAME]   VARCHAR (50)    NULL,
    [INSERTED_BY_PROCESS_NAME]   VARCHAR (50)    NULL,
    [SOURCE_SYSTEM_TYPE_CODE]    VARCHAR (50)    NULL,
    [LEGAL_CASE_REF_CID]         VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

