USE ${hiveconf:DB};

ANALYZE TABLE nation COMPUTE STATISTICS;
ANALYZE TABLE nation COMPUTE STATISTICS FOR COLUMNS;

ANALYZE TABLE region COMPUTE STATISTICS;
ANALYZE TABLE region COMPUTE STATISTICS FOR COLUMNS;

ANALYZE TABLE supplier COMPUTE STATISTICS;
ANALYZE TABLE supplier COMPUTE STATISTICS FOR COLUMNS;

ANALYZE TABLE part COMPUTE STATISTICS;
ANALYZE TABLE part COMPUTE STATISTICS FOR COLUMNS;

ANALYZE TABLE partsupp COMPUTE STATISTICS;
ANALYZE TABLE partsupp COMPUTE STATISTICS FOR COLUMNS;

ANALYZE TABLE customer COMPUTE STATISTICS;
ANALYZE TABLE customer COMPUTE STATISTICS FOR COLUMNS;

ANALYZE TABLE orders PARTITION(O_ORDERDATE) COMPUTE STATISTICS;
ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS;

ANALYZE TABLE lineitem PARTITION(L_SHIPDATE) COMPUTE STATISTICS;
ANALYZE TABLE lineitem COMPUTE STATISTICS FOR COLUMNS;
