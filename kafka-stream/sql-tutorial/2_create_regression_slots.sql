-- Create a logical replication slot to start tracking changes
-- replace with your student_id
SELECT * FROM pg_create_logical_replication_slot('moh_regression_slot', 'test_decoding');


-- Retrieve list of all existing replication_slots:
SELECT * FROM pg_replication_slots;