﻿CREATE EXTERNAL FILE FORMAT [eff_json_gzip]
    WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (FIELD_TERMINATOR = N'CHR(13)', FIRST_ROW = 2, ENCODING = N'UTF8'),
    DATA_COMPRESSION = N'org.apache.hadoop.io.compress.GzipCodec'
    );

