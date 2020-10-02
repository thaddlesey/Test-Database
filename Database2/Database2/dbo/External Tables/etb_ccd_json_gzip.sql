﻿CREATE EXTERNAL TABLE [dbo].[etb_ccd_json_gzip] (
    [data] VARCHAR (MAX) NULL
)
    WITH (
    DATA_SOURCE = [eds_ccd_json],
    LOCATION = N'/',
    FILE_FORMAT = [eff_json_gzip],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
    );

