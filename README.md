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
4. Numpy
5. PyODBC (optional)

## Configuration:

The repo includes a sample config.xml. The predefined tags in this XML dictate the configuration of a given profile. Note that the `profile` tags can be duplicated within the `config` tags. Each duplication should have a unique `name` attribute value. The following is an explanation of each tag:

Tag | Data Type | Requirements
--- | --- | ---
`profile` | string | Must have a name attribute with a string value. For use as an argument when executing application.
`table` | string | The targeted table name
`retroactive` | binary | A value of 1 forces a retroactive sync for every run, regardless of other configurations
`selective_fields` | string | * or blank to replicate all fields or comma seperated list of column headers to be selective
`incremental_field` | string | The column holding a date/datetime/timestamp field. Used to determine what records have been updated for incremental sync
`source` | none | Tags to hold source connection config
`destination` | none | Tags to hold destination connection config
`connection` | string | Any valid SQLAlchemy connection string (see Limitations)
`ssl` | none | Tags to hold ssl config
`required` | binary | A value of 1 forces the application to read the remaining SSL config options
`ca` | string | File path of the certificate authority
`key` | string | File path of the client key
`cert` | string | File path of the client certificate
`offset` | none | Tags to hold hours and minutes of offset
`hours` | int | How many hours to offset a retroactive sync (A value of 1 would subtract 1 hour from the `incremental_field`
`minutes` | int | How many minutes to offset a retroactive sync (A value of 30 would subtract 30 minutes from the `incremental_field`

## Limitations

1. Only supports single-column primary keys for incremental replication.
2. PyODBC for MySQL has limitations due to the connector. This is a known restriction, documented in the SQLAlchemy documentation.
3. The application does not perform a full comparison of all data during incremental replications. Should a data point change without updating the `incremental_field` datetime, it is possible the replication will not capture those changes.
4. The application performs only one-way replications, not syncing. Should the destination table be updated, those changes will not be replicated to the source, and could result in inconsistent data between each database table.

