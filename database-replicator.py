# pylint: disable=invalid-name
""" Database replication utility """
import os, logging, datetime, pickle, argparse, xml.etree.ElementTree as ET
from sqlalchemy.inspection import inspect
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker, Session, load_only
from sqlalchemy.schema import CreateTable
from sqlalchemy.ext.automap import automap_base

now = datetime.datetime.now()
now = now.strftime('%Y-%m-%d %H:%M:%S')

parser = argparse.ArgumentParser()
parser.add_argument(help="Profile name for reference in XML configuration file.", action="store", dest="profile")
parser.add_argument("-r", "--retroactive", help="Flag to initiate a retroactive sync", action="store_true", dest="retro")
args = parser.parse_args()

# establish argument variables
profile = args.profile
retro = args.retro

# set retroactive based on arguments
if retro:
    retroactive = True
else:
    retroactive = False

try:
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s: %(lineno)d (%(levelname)s) %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename='log/%s.log' % (profile),
                        filemode='w')

    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(lineno)d (%(levelname)s) %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

    status_file = 'bin/{}.bin'.format(profile)

    if os.path.isfile(status_file):
        with open(status_file, 'rb') as f:
            status = pickle.load(f)
    else:
        status = {'LastAttempt': None, 'LastStatus': None, 'LastStatusDetails': None, 'LastSuccessfulAttempt': None, 'LastSuccessDestRowCount': None, 'LastSuccessPKs': None}
        retroactive = True

    logging.info("Last Attempt: %s", (status['LastAttempt']))
    logging.info("Last Status: %s", (status['LastStatus']))
    logging.info("Last Status Details: %s", (status['LastStatusDetails']))
    logging.info("Last Sucessful Attempt: %s", (status['LastSuccessfulAttempt']))
    logging.info("Last Successful Row Count: %s", (status['LastSuccessDestRowCount']))

    logging.info("Let's take a look at the configuration for the %s profile.", profile)

    # pull variables from xml configuration
    tree = ET.parse('config.xml')
    root = tree.getroot()
    for p in root.findall('profile'):
        if p.get('name') == profile:
            table = str(p.find('table').text)
            retro = int(p.find('retroactive').text)
            incremental_field = str(p.find('incremental_field').text)

            if retro == 1:
                retroactive = True

            conn_src = str(p.find('source').find('connection').text)
            conn_dest = str(p.find('destination').find('connection').text)

            ssl_req_src = int(p.find('source').find('ssl').find('required').text)
            ssl_req_dest = int(p.find('destination').find('ssl').find('required').text)

            ssl_ca_src = str(p.find('source').find('ssl').find('ca').text)
            ssl_key_src = str(p.find('source').find('ssl').find('key').text)
            ssl_cert_src = str(p.find('source').find('ssl').find('cert').text)

            ssl_ca_dest = str(p.find('destination').find('ssl').find('ca').text)
            ssl_key_dest = str(p.find('destination').find('ssl').find('key').text)
            ssl_cert_dest = str(p.find('destination').find('ssl').find('cert').text)

            if ssl_req_src == 1:
                ssl_src = {'ssl': {'cert':ssl_cert_src,'key':ssl_key_src,'ca':ssl_ca_src}}
            else:
                ssl_src = dict()

            if ssl_req_dest == 1:
                ssl_dest = {'ssl': {'cert':ssl_cert_dest,'key':ssl_key_dest,'ca':ssl_ca_dest}}
            else:
                ssl_dest = dict()

            offset_hours = int(p.find('offset').find('hours').text)
            offset_minutes = int(p.find('offset').find('minutes').text)

    logging.info("We're starting this thing off right for the %s table.", table)
    
    eng_src = create_engine(conn_src, echo=False, connect_args=ssl_src)
    eng_dest = create_engine(conn_dest, echo=False, connect_args=ssl_dest)
    collation_mssql = 'SQL_Latin1_General_CP1_CI_AS'
    collation_mysql = 'utf8_general_ci'

    base_src = automap_base()
    base_src.prepare(eng_src, reflect=True)
    tbl_src = getattr(base_src.classes, table)
    meta_src = base_src.metadata.tables["{}".format(table)]

    base_dest = automap_base()
    base_dest.prepare(eng_dest, reflect=True)
    tbls_dest = base_dest.classes.keys()

    for column in meta_src.columns:
        if hasattr(column.type, 'collation'):
            if 'mssql' in str(eng_src) and 'mysql' in str(eng_dest):
                if column.type.collation == collation_mssql:
                    column.type.collation = '{}'.format(collation_mysql)
            elif 'mysql' in str(eng_src) and 'mssql' in str(eng_dest):
                if column.type.collation == collation_mysql:
                    column.type.collation = '{}'.format(collation_mssql)

    # table confirmation/creation
    if table not in tbls_dest:
        logging.info('Table does not exist in destination. Creating table.')
        meta_src.create(eng_dest)
        logging.info('Table created in destination.')
        # force retroactive sync
        retroactive = True
    else:
        logging.info('Table exists in destination.')
        # confirm that tables are the same
        base_dest = automap_base()
        base_dest.prepare(eng_dest, reflect=True)
        meta_dest = base_dest.metadata.tables["{}".format(table)]
        
        # compare column lists of source and destination and recreate table if no match
        if str(meta_src.columns) != str(meta_dest.columns):
            logging.info('Table columns do not match. Dropping and recreating destination table.')
            meta_dest.drop(eng_dest)
            logging.info('Table dropped in destination.')
            meta_src.create(eng_dest)
            logging.info('Table recreated in destination.')
            # force retroactive sync
            retroactive = True

    base_dest = automap_base()
    base_dest.prepare(eng_dest, reflect=True)
    tbl_dest = getattr(base_dest.classes, table)

    Source = sessionmaker(bind=eng_src)
    Source = Source()
    logging.debug('Created source session.')

    Destination = sessionmaker(bind=eng_dest)
    Destination = Destination()
    logging.debug('Created destination session.')

    # establish primary key variables
    str_pk_src = inspect(tbl_src).primary_key[0].name
    obj_pk_src = getattr(tbl_src, str_pk_src)
    str_pk_dest = inspect(tbl_dest).primary_key[0].name
    obj_pk_dest = getattr(tbl_dest, str_pk_dest)
    logging.debug('Established primary key variables')

    # determine destination rows for comparison if retroactive is not already true
    if not retroactive:
        current_dest_pks = Destination.query(tbl_dest).options(load_only(str_pk_dest)).all()

        current_dest_pks_list = list()
        for c in current_dest_pks:
            current_dest_pks_list.append(getattr(c, str_pk_dest))

        current_dest_pks_set = set(current_dest_pks_list)
        prev_dest_pks_set = set(status['LastSuccessPKs'])

        pk_diff = current_dest_pks_set.symmetric_difference(prev_dest_pks_set)

        if len(pk_diff) != 0:
            retroactive = True
            logging.info('Current destination table and previous destination table do not match requiring retroactive sync.')

    # perform a full or retroactive sync else perform incremental
    if retroactive:
        logging.info('Requiring full or retroactive sync.')
        
        # determine if destination has any data and empty if yes
        count = Destination.query(tbl_dest).count()
        if count != 0:
            logging.info('Destination table is not empty. %s rows found.', count)
            Destination.query(tbl_dest).delete()
            logging.info('Destination table emptied.')

        # pull every row in source database and execute query from dictionary copy of row
        all_src = Source.query(tbl_src).all()
        logging.info('%s records have been identified for sync.', len(all_src))
        
        if all_src:
            for obj in all_src:
                mapper = inspect(obj)
                imap = tbl_dest()
                for column in mapper.attrs:
                    setattr(imap, column.key, column.value)
                src_pk_val = getattr(obj, str_pk_src)
                logging.debug("Inserted: %s", src_pk_val)
                Destination.add(imap)

    #perform an incremental sync
    else:
        logging.info('Performing an incremental sync.')

        #determine which records need to be deleted
        current_src_pks = Source.query(tbl_src).options(load_only(str_pk_src)).all()

        current_src_pks_list = list()
        for c in current_src_pks:
            current_src_pks_list.append(getattr(c, str_pk_src))

        current_src_pks_set = set(current_src_pks_list)

        pk_diff = prev_dest_pks_set.difference(current_src_pks_set)
        logging.info('%s records identified for deletion.', len(pk_diff))

        for pk in pk_diff:
            Destination.query(tbl_dest).filter(obj_pk_dest == pk).delete()
            logging.debug("Deleted: %s", pk)

        logging.info('%s records have been deleted.', len(pk_diff))

        #determine which records need to be added
        pk_diff = current_src_pks_set.difference(prev_dest_pks_set)
        logging.info('%s records identified for insert.', len(pk_diff))

        all_src = Source.query(tbl_src).filter(obj_pk_src.in_(pk_diff)).all()
        if all_src:
            for obj in all_src:
                src_pk_val = getattr(obj, str_pk_src)
                mapper = inspect(obj)
                imap = tbl_dest()
                for column in mapper.attrs:
                    setattr(imap, column.key, column.value)
                Destination.add(imap)
                logging.debug("Inserted: %s", src_pk_val)

        logging.info('%s records have been inserted.', len(pk_diff))

        # offset last successful timestamp to account for timezone differences and/or syncing latency
        success = status['LastSuccessfulAttempt']
        offset = datetime.timedelta(hours=offset_hours,minutes=offset_minutes)
        adjusted = str(success - offset)

        # determine which records need to be updated
        logging.info('Looking back to records modified since %s using field %s' % (adjusted, incremental_field))
        all_src = Source.query(tbl_src).filter(getattr(tbl_src, incremental_field) > adjusted).all()
        logging.info('%s records have been identified for update.', len(all_src))
        if all_src:
            for obj in all_src:
                src_pk_val = getattr(obj, str_pk_src)
                Destination.query(tbl_dest).filter(obj_pk_dest == src_pk_val).delete()
                logging.debug("Deleted: %s", src_pk_val)
                mapper = inspect(obj)
                imap = tbl_dest()
                for column in mapper.attrs:
                    setattr(imap, column.key, column.value)
                Destination.add(imap)
                logging.debug("Inserted: %s", src_pk_val)
        logging.info('%s records have been updated.', len(all_src))

    Destination.commit()
    logging.info('Transactions committed to destination table.')

    # determine count of finalized destination rows for serialization
    final_dest_pks = Destination.query(tbl_dest).options(load_only(str_pk_dest)).all()
    cdr = len(final_dest_pks)
    logging.info('%s rows found in destination.', cdr)

    final_dest_pks_list = list()
    for f in final_dest_pks:
        final_dest_pks_list.append(getattr(f, str_pk_dest))

    logging.info('Saved destination pk values for comparison on next sync.')

    Source.close_all()
    logging.debug('Closed source session.')
    
    if not retroactive:
        last_successful = Destination.query(tbl_dest).options(load_only(incremental_field)).order_by(desc(getattr(tbl_dest, incremental_field))).first()
        last_successful = getattr(last_successful, incremental_field)
        logging.info('Recording last successful incremental sync as: %s' % last_successful)
    else:
        last_successful = datetime.datetime.strptime(now, '%Y-%m-%d %H:%M:%S')
        logging.info('Incremental field not set, setting last success as: %s' % last_successful)

    Destination.close_all()
    logging.debug('Closed destination session.')

    # update status and serialize data
    status = {'LastAttempt': now, 'LastStatus': 'SUCCESS', 'LastStatusDetails': 'The database replication was successful.', 'LastSuccessfulAttempt': last_successful, 'LastSuccessDestRowCount': cdr, 'LastSuccessPKs': final_dest_pks_list}
    with open(status_file, 'wb') as f:
        pickle.dump(status, f)

except Exception as error:
    logging.error(error)
    # update status and serialize data
    status['LastAttempt'] = now
    status['LastStatus'] = 'EXCEPTION'
    status['LastStatusDetails'] = error
    with open(status_file, 'wb') as f:
        pickle.dump(status, f)
except:
    error = "Encountered an undefined exception application terminated"
    logging.error(error)
    # update status and serialize data
    status['LastAttempt'] = now
    status['LastStatus'] = 'ERROR'
    status['LastStatusDetails'] = error
    with open(status_file, 'wb') as f:
        pickle.dump(status, f)
