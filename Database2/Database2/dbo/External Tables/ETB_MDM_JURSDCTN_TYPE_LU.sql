﻿CREATE EXTERNAL TABLE [dbo].[ETB_MDM_JURSDCTN_TYPE_LU] (
    [JURSDCTN_TYPE_CODE] VARCHAR (8000) NULL,
    [JURSDCTN_TYPE_NAME] VARCHAR (8000) NULL,
    [JURSDCTN_TYPE_KEY] VARCHAR (8000) NULL
)
    WITH (
    DATA_SOURCE = [EDS_DIM_TBL],
    LOCATION = N'/MDM_ET_JURSDCTN_TYPE_LU.csv',
    FILE_FORMAT = [TextFileFormat],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
    );

