CREATE EXTERNAL DATA SOURCE [EDS_DIM_TBL]
    WITH (
    TYPE = HADOOP,
    LOCATION = N'wasbs://dimension-tables@cmdevdummydata.blob.core.windows.net',
    CREDENTIAL = [dbsc_cmdevdummydata]
    );

