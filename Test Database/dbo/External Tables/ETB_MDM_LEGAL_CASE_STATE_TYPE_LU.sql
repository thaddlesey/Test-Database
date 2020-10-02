﻿CREATE EXTERNAL TABLE [dbo].[ETB_MDM_LEGAL_CASE_STATE_TYPE_LU] (
    [LEGAL_CASE_STATE_TYPE_CID] VARCHAR (8000) NULL,
    [LEGAL_CASE_STATE_TYPE_KEY] VARCHAR (8000) NULL,
    [LEGAL_CASE_STATE_TYPE_NAME] VARCHAR (8000) NULL,
    [SOURCE_SYSTEM_CASE_TYPE_NAME] VARCHAR (8000) NULL,
    [CCD_CASE_TYPE_NAME] VARCHAR (8000) NULL
)
    WITH (
    DATA_SOURCE = [EDS_DIM_TBL],
    LOCATION = N'/MDM_LEGAL_CASE_STATE_TYPE_LU.csv',
    FILE_FORMAT = [TextFileFormat],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
    );

