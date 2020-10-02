﻿CREATE TABLE [dbo].[MDM_CCD_CASE_TYPE_LU] (
    [CCD_SRC_CASE_TYPE_NAME] VARCHAR (50) NOT NULL,
    [CCD_JURSDCTN_TYPE_NAME] VARCHAR (50) NULL,
    [CCD_CASE_TYPE_NAME]     VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

