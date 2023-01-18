-- Transactional log information can now be read using pg_logical_slot_get_changes
-- Once changes are read, they're consumed and not emitted
-- in a subsequent call:
SELECT * FROM pg_logical_slot_get_changes('moh_regression_slot', NULL, NULL);

-- Run some changes again to verify :-) --> run query above again
Update moh_products SET sku = 100 where CODE = 'JALA';

-- You can also peek ahead in the change stream without consuming changes
SELECT * FROM pg_logical_slot_peek_changes('moh_regression_slot', NULL, NULL);

-- The next call to pg_logical_slot_peek_changes() returns the same changes again
SELECT * FROM pg_logical_slot_peek_changes('moh_regression_slot', NULL, NULL);

-- options can be passed to output plugin, to influence the formatting
SELECT * FROM pg_logical_slot_peek_changes('moh_regression_slot', NULL, NULL, 'include-timestamp', 'on');

-- Remember to destroy a slot you no longer need to stop it consuming
-- server resources:
SELECT pg_drop_replication_slot('moh_regression_slot');