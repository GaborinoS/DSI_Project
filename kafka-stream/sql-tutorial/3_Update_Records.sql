-- Go ahead and update your records. Run some deletions/inserts or updates (but don't delete everything :-) )
SELECT * FROM moh_products;
UPDATE moh_products SET sku = 175 WHERE code = 'CHEES';
DELETE FROM moh_products WHERE code = 'ONION';

INSERT INTO moh_products VALUES('SPICE', 'spice', 'Mix of indian spices', 420, 2.99, 'Spreads', '80Z GOUR CRM CH');


UPDATE moh_products SET name = 'Cookie' where code = 'SUGAR';
