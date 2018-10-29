# pylint: disable=invalid-name
""" Database replication utility """
import os, logging, datetime, argparse, pandas as pd, numpy as np
from pandas.io import sql
from app.Config import Config
from sqlalchemy import create_engine

now = datetime.datetime.now()
now = now.strftime('%Y-%m-%d %H:%M:%S')

parser = argparse.ArgumentParser()
parser.add_argument(help="Profile name for reference in XML configuration file.", action="store", dest="profile")
parser.add_argument("-r", "--retroactive", help="Flag to initiate a retroactive sync", action="store_true", dest="retro")
args = parser.parse_args()

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s: %(lineno)d (%(levelname)s) %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='%s.log' % (args.profile),
                    filemode='w')

console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter('(%(levelname)s) %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

# establish initial retroactive value and modify per arguments
retroactive = False
if args.retro:
    retroactive = True
    logging.info("Retroactive replication required per supplied argument.")

logging.info("Let's take a look at the configuration for the %s profile.", args.profile)

# pull variables from xml configuration
config = Config('config.xml', args.profile)

if config.retro == 1:
    retroactive = True

# connect to database and begin replication process
logging.info("We're starting this thing off right for the %s table.", config.table)

engine_source = create_engine(config.conn_source, echo=False, connect_args=config.get_ssl_src())
engine_destination = create_engine(config.conn_destination, echo=False, connect_args=config.get_ssl_dest())

# build dataframe from source database engine
data_source = pd.read_sql_table(config.table, engine_source)

# if destination table exists, consider difference in columns to determine retroactive replication
if engine_destination.has_table(config.table):
    logging.info("The destination database contains a %s table. Determining if tables are the same.", config.table)

    # build dataframe from destination database engine
    data_destination = pd.read_sql_table(config.table, engine_destination)

    # establish difference lengths between each dataframes column headers
    len_column_difference = len(set(data_source.columns).symmetric_difference(set(data_destination.columns)))

    # if difference in column headers exists, require retroactive replication
    if len_column_difference > 0:
        logging.info("Source and destination tables do not match. Requiring retroactive replication.")
        retroactive = True
    else:
        logging.info("Source and destination tables match. Preceding with preferred replication method.")

# if destination table does not exist, force retroactive replication
if not engine_destination.has_table(config.table):
    logging.info("The destination database does not contain a %s table. Requiring retroactive replication.", config.table)
    retroactive = True

# run retroactive replication
if retroactive:
    logging.info("Performing retroactive replication.")
    data_source.to_sql(config.table, engine_destination, index=False, if_exists='replace')

# run incremental replication
if not retroactive:
    logging.info("Performing incremental replication.")

    records_to_insert = set(data_source[config.primary_key]).difference(data_destination[config.primary_key])
    records_to_insert = data_source[data_source[config.primary_key].isin(records_to_insert)]
    records_to_insert.to_sql(config.table, engine_destination, index=False, if_exists='append')
    logging.info("Inserted %s records.", len(records_to_insert)) 

    records_to_delete = set(data_destination[config.primary_key]).difference(data_source[config.primary_key])
    records_to_delete = data_destination[data_destination[config.primary_key].isin(records_to_delete)]
    for record in records_to_delete[config.primary_key]:
        sql.execute('DELETE FROM {0} WHERE {1} = ?'.format(config.table, config.primary_key), engine_destination, params=[record])
        logging.debug("Deleted record %s as part of a DELETE action.", record)
    logging.info("Deleted %s records.", len(records_to_delete))

    # determine last modified date based on incremental_field and adjust for any provided offset
    incremental_threshold = data_destination[config.incremental_field].max()
    offset = datetime.timedelta(hours=config.offset_hours,minutes=config.offset_minutes)
    incremental_threshold = str(incremental_threshold - offset)

    records_to_update = data_source[(data_source[config.incremental_field] > incremental_threshold)]
    for record in records_to_update[config.primary_key]:
        sql.execute('DELETE FROM {0} WHERE {1} = ?'.format(config.table, config.primary_key), engine_destination, params=[record])
        logging.debug("Deleted record %s as part of an UPDATE action.", record)
    records_to_update.to_sql(config.table, engine_destination, index=False, if_exists='append')
    logging.info("Updated %s records.", len(records_to_update))

logging.info("Replication complete for the %s table.", config.table)