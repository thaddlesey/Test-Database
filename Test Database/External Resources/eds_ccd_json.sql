CREATE EXTERNAL DATA SOURCE [eds_ccd_json]
    WITH (
    TYPE = HADOOP,
    LOCATION = N'wasbs://ccd@reformsftpmitest.blob.core.windows.net',
    CREDENTIAL = [dsc_ccd]
    );

