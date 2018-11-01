# database-replicator

Profile-based database table replication and sync tool

## Running the application

python3 database-replicator.py [-h] [-r] profile

`-h` Help

`-r` Force a retroactive sync

`profile` The string value specified in the config.xml (see Configuration)

## Dependencies

requirements.txt includes all dependencies

1. SQLAlchemy
2. PyLint and dependencies (included in requirements.txt but only necessary for development)
3. Pandas
4. PyODBC, PyMySQL, or any other required database connector (optional)

## Configuration

Copy or rename the config-sample.xml to config.xml for production.

`cp config-sample.xml config.xml`

The repo includes a sample config.xml. The predefined tags in this XML dictate the configuration of a given profile. Note that the `profile` tags can be duplicated within the `config` tags. Each duplication should have a unique `name` attribute value. The following is an explanation of each tag:

Tag | Data Type | Nullable | Notes
--- | --- | --- | ---
`profile` | string | NO | Must have a name attribute with a string value. For use as an argument when executing application.
`table` | string | NO | The targeted table name
`retroactive` | binary | NO | A value of 1 forces a retroactive sync for every run, regardless of other configurations
`primary_key` | string | NO | The primary key of the source table. Supports only single-column primary keys.
`selective_fields` | string | YES | * or blank to replicate all fields or comma seperated list of column headers to be selective
`incremental_field` | string | YES | The column holding a date/datetime/timestamp field. Used to determine what records have been updated for incremental replication. Required if using incremental replication.
`source` | none | NO | Tags to hold source connection config
`destination` | none | NO | Tags to hold destination connection config
`connection` | string | NO | Any valid SQLAlchemy connection string (<https://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls>)
`ssl` | none | YES | Tags to hold ssl config
`required` | binary | YES | A value of 1 forces the application to read the remaining SSL config options
`ca` | string | YES | File path of the certificate authority
`key` | string | YES | File path of the client key
`cert` | string | YES | File path of the client certificate
`offset` | none | NO | Tags to hold hours and minutes of offset
`hours` | int | NO | How many hours to offset a retroactive sync (A value of 1 would subtract 1 hour from the `incremental_field`
`minutes` | int | NO | How many minutes to offset a retroactive sync (A value of 30 would subtract 30 minutes from the `incremental_field`

## Limitations

1. Only supports single-column primary keys for incremental replication.
2. PyODBC for MySQL has limitations due to the connector. This is a known restriction, documented in the SQLAlchemy documentation.
3. The application does not perform a full comparison of all data during incremental replications. Should a data point change without updating the `incremental_field` datetime, it is possible the replication will not capture those changes.
4. The application performs only one-way replications, not syncing. Should the destination table be updated, those changes will not be replicated to the source, and could result in inconsistent data between each database table.
5. Updating is done by deleting from the destination table and appending a Pandas dataframe in its place. Deleting is not supported by Pandas (to the best of my knowledge). As such, raw SQL execution is used. Be wary of SQL injection. Since this should be used by administrators without user input, the risk of injection is mitigated, but other applications would want to consider these implications.

