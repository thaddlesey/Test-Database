﻿CREATE TABLE branch_test2
(
    branch_test_id int NOT NULL
)
WITH
(
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
)
GO
