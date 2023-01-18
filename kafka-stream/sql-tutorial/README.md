# CDC with postgres
Change Data Capture (CDC) allows you to track and propagate changes in a Postgres database to downstream consumers based on its Write-Ahead Log (WAL). In this guide, we’ll cover how to setup such CDC and efficiently maintain real-time data in a downstream.

## Prepare some data and database
Before we get started with CDC, we are going to create an example use case, by creating a some data for our fictional product_store.

### Fictional food store
We have already prepared some queries for you inside our sql lab. Refer to the queries menu and you'll see a bunch of queries we are going to use through this tutorial.


Select **1_Create_data** and you'll see the query we're going to use. Take a look at the DDL to get an overview of the data.

**Important: **  Please replace the prefix `<studentid>` with your student id, e,g,. ds21m19_products.
So instead of 

``` CREATE TABLE <studentid>_products...``` 
use  
```CREATE TABLE ds21m019_products```.

Don't forget to the update the insert statement as well.

### Take a look at the data
After creating the data: Familiarize yourself with the dataset. Take a look at all products our store offers. Lookup some cheese products! 

## Configure database and table for CDC 

__Open Query: prepare_db_cdc__

Now that we created some data, we need to ensure that postgres database is configured to support [logical replication](https://www.postgresql.org/docs/10/logical-replication.html "External Link").

As a *superuser*:

1.     Check the [`wal_level` configuration](https://www.postgresql.org/docs/current/wal-configuration.html "External Link") setting:

     ```SHOW wal_level;```
    
     The default value is `replica`. For CDC, you'll need to set it to `logical` in the database configuration file (`postgresql.conf`). Keep in mind that changing the `wal_level` requires a restart of the Postgres instance and can affect database performance.

1.     Set the replica identity to `FULL` for the tables you want to replicate:

    ```ALTER TABLE repl_table REPLICA IDENTITY FULL;```

    This setting determines the amount of information that is written to the WAL in `UPDATE` and `DELETE` operations.

    As a heads-up, you should expect a performance hit in the database from increased CPU usage. For more information, see the [PostgreSQL documentation](https://www.postgresql.org/docs/current/logical-replication-publication.html "External Link").


1.  Create a [publication](https://www.postgresql.org/docs/current/logical-replication-publication.html "External Link") with the tables you want to replicate:

    *For specific tables:*

    ```
    CREATE PUBLICATION <studentid>_products_src FOR TABLE <studentid>_products;
    ```

    *FYI: If you want to publish changes of all tables you could use:*

    ```
    CREATE PUBLICATION .... FOR ALL TABLES;
    ```

    The `<studentid>_prod_src ` publication will contain the set of change events generated from the specified tables, and will later be used to ingest the replication stream. It is advised that you limit publications only to the tables you need.


## Review transactional logs
The transaction log (WAL) doesn't contain the SQL statements that ran, it contains the binary changes to the data files. In order to track changes, you'll need to create a replication slot. 

Replication slots provide an automated way to ensure that the database does not remove WAL segments until they have been received by all standbys, and that the master does not remove rows which could cause a recovery conflict even when the standby is disconnected.

PostgreSQL provides infrastructure to stream the modifications performed via SQL to external consumers. This functionality can be used for a variety of purposes, including replication solutions and auditing.

Changes are sent out in streams identified by logical replication slots. Each stream outputs each change exactly once.

In order to give this a try, we'll now manually create a slot '<studentid>_slot' using the output plugin 'test_decoding': 

__Open Query: 2_create_regression_slots__

```SELECT * FROM pg_create_logical_replication_slot('<studentid>_regression_slot', 'test_decoding');```

You can get an overview of the existing replication slots by querying
``` SELECT * FROM pg_replication_slots;```

### Time for some action

__Open Query: 3_Update_Records__

Now go ahead and update some data. For instance, you can insert some new records or update existing ones.

```UPDATE <student_id>_products SET sku = 175 WHERE code = 'CHEES';```

### Get log information 

__Open Query: 4_view_cdc_log__

Using  ```pg_logical_slot_get_changes()``` we can query the set of specified changes from the specified logical replication slot.

```SELECT * FROM pg_logical_slot_get_changes('<student_id>_regression_slot', NULL, NULL);```

If you run your query twice, you'll notice that the changes are gone, that's because once changes are read, **they're consumed** and **not** emitted again.


**Important:** The returned set of changes will begin from the point changes were previously consumed.

The options parameter enables provision of options as defined by the output plugin associated with the logical replication slot.
To retrieve changes without consuming them, use ```pg_logical_slot_peek_changes()```.

### Drop replication slot
If you're done experimenting with the replication_slot, you should destroy a slot you no longer need to stop it consuming server resources:

```SELECT pg_drop_replication_slot('<student_id>_regression_slot');```
