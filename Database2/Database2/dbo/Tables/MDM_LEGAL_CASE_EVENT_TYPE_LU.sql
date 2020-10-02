﻿CREATE TABLE [dbo].[MDM_LEGAL_CASE_EVENT_TYPE_LU] (
    [LEGAL_CASE_EVENT_TYPE_NAME] VARCHAR (100)   NOT NULL,
    [LEGAL_CASE_EVENT_TYPE_KEY]  DECIMAL (10, 6) NULL,
    [LEGAL_CASE_EVENT_TYPE_CID]  VARCHAR (100)   NOT NULL,
    [CCD_CASE_TYPE_NAME]         VARCHAR (255)   NOT NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

