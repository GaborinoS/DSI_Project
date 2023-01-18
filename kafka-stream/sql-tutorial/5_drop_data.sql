DROP TABLE <studentid>_products;
DROP PUBLICATION <studentid>_products_src;
SELECT pg_drop_replication_slot('<studentid>_regression_slot');