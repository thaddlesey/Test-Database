﻿CREATE EXTERNAL TABLE [dbo].[ETB_MDM_ATTR_TYPE] (
    [ATTR_KEY] VARCHAR (8000) NULL,
    [ATTR_LOGICAL_NAME] VARCHAR (8000) NULL,
    [ATTR_ORIGINAL_NAME] VARCHAR (8000) NULL,
    [ATTR_PHYSICAL_NAME] VARCHAR (8000) NULL,
    [ATTR_DOMAIN_TYPE_NAME] VARCHAR (8000) NULL,
    [ATTR_DATA_TYPE_NAME] VARCHAR (8000) NULL,
    [BIS_FIRST_CREATED_DATETIME] VARCHAR (8000) NULL,
    [BIS_LAST_CREATED_DATETIME] VARCHAR (8000) NULL,
    [BIS_MODIFIED_BY_PROCESS_NAME] VARCHAR (8000) NULL,
    [BIS_INSERTED_BY_PROCESS_NAME] VARCHAR (8000) NULL,
    [SOURCE_SYSTEM_TYPE_CODE] VARCHAR (8000) NULL,
    [JURISDICTION_CODE] VARCHAR (8000) NULL
)
    WITH (
    DATA_SOURCE = [EDS_DIM_TBL],
    LOCATION = N'/MDM_ATTR_TYPE.csv',
    FILE_FORMAT = [TextFileFormat],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
    );

