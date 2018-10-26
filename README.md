# database-replicator

Profile-based database table replication and sync tool

## Running the application

python3 database-replicator.py [-h] [-r] profile

`-h` Help

`-r` Force a retroactive sync

`profile` The string value specified in the config.xml (see Configuration)

## Prerequisites

1. SQLAlchemy
2. PyLint and dependencies (included in requirements.txt but only necessary for development)

## Configuration:

The repo includes a sample config.xml. The predefined tags in this XML dictate the configuration of a given profile. Note that the `profile` tags can be duplicated as many times as needed to support as many profiles as are desired. The following is an explanation of each tag:

Tag | Data Type | Requirements
--- | --- | ---
`profile` | string | Must have a name attribute with a string value. For use as an argument when executing application.
`table` | string | The targeted table name
`retroactive` | binary | A value of 1 forces a retroactive sync for every run, regardless of other configurations
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

1. Only supports single-column primary keys for incremental sync
2. PyODBC for MySQL has limitations due to the connector. This is a known restriction in SQLAlchemy, and is not supported in most configurations of this application