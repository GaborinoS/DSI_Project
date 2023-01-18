SHOW wal_level;

ALTER TABLE moh_products REPLICA IDENTITY FULL;

CREATE PUBLICATION moh_products_src FOR TABLE moh_products;

select * from moh_products