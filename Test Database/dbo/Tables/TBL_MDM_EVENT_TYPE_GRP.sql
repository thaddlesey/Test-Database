﻿CREATE TABLE [dbo].[TBL_MDM_EVENT_TYPE_GRP] (
    [EVENT_TYPE_GRP_KEY]         DECIMAL (10, 6) NOT NULL,
    [EVENT_TYPE_GRP_NAME]        VARCHAR (50)    NULL,
    [EVENT_TYPE_GRP_DESC]        VARCHAR (255)   NULL,
    [BIS_FIRST_CREATED_DATETIME] DATETIME        NULL,
    [BIS_LAST_MODIFIED_DATETIME] DATETIME        NULL,
    [MODIFIED_BY_PROCESS_NAME]   VARCHAR (50)    NULL,
    [INSERTED_BY_PROCESS_NAME]   VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

