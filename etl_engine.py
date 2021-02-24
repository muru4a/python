import argparse
import configparser
import csv
import json
import logging
import os
import os.path
import pyodbc
import re
import shutil
import subprocess
import sys
from datetime import date
from datetime import datetime
from datetime import timedelta
from multiprocessing.pool import ThreadPool

import boto3
import botocore
import psycopg2
import psycopg2.extras

"""This is data engineering's etl engine module.
    5 operations are currently supported:
    1. e & l from sql server into rs
    2. e & l from s3 to rs
    3. e & l from rs to s3
    4. t in rs
    5. l scd in rs
"""
__all__ = ['execute']
__version__ = '1.1'

# constants
MSSQL_SOURCES = ('DB_MSSQL_ADV', 'DB_MSSQL_ADVRTDB', 'DB_MSSQL_AUXLIVE', 'DB_MSSQL_AUXSTG')
SQL_BASED_LOADERS = ('SQL_RS', 'SQL_MSSQL_ADV')
S3_ARGS = {'ServerSideEncryption': "AES256"}

# global variables
# TODO: enhance global variables to make them module specific with use of __all__
# currentdate = datetime.strftime((datetime.now() - timedelta(days=1)), '%Y%m%d') TODO : FYI - replaced with new
currentdate = str(date.today() - timedelta(days=1)).replace('-', '')
cfg = configparser.ConfigParser()

def load_config(oride_etl_cfg=None):
    """
    Loads the etl config file.

    Reads file defined by ETL_CFG_FILE env variable in os.environ,
    else uses default location /mnt/etl/config/etl.cfg.
    Exists system if none of the above is defined.
    Returns: None
    """

    if "ETL_CFG_FILE" in os.environ:
        if os.path.exists(os.environ['ETL_CFG_FILE']):  # TODO: do we need this?
            etl_cfg_file = os.environ['ETL_CFG_FILE']
        else:
            print("Config File {} doesn't exist".format(os.environ['ETL_CFG_FILE']))
            sys.exit(1)
    else:
        if os.path.exists('/mnt/etl/config/etl.cfg'):
            etl_cfg_file = '/mnt/etl/config/etl.cfg'
        else:
            print("Config File /mnt/etl/config/etl.cfg doesn't exist")
            sys.exit(1)

    cfg.read(etl_cfg_file)

    # If override etl config is passed update those variables
    if oride_etl_cfg:
        for cfg_key, cfg_value in oride_etl_cfg.items():
            cfg_key_info = cfg_key.split('.')
            cfg[cfg_key_info[0]][cfg_key_info[1]] = cfg_value

    return cfg


def set_global_variables(rscursor, loaderinfo):
    """
    Set global variables
    Args:
        rscursor: cursor object to redshift for max datenum lookup
        loaderinfo: loaderinfo object
        **kwargs (object): params to override, passed etl_engine

    Returns:
        0
    """
    global gvardict
    gvardict = {}

    # rscursor = rscnx.cursor(cursor_factory=psycopg2.extras.DictCursor)  # TODO : changed to pass rcursor instead
    # First set the search path  # TODO : is there a reason to put sql below over multiple lines?
    setpathsql = '''
                    SET SEARCH_PATH TO core, ops, stage, semantic, reporting
                '''
    try:
        rscursor.execute(setpathsql)
    except psycopg2.Error as error:
        log_info("Error during execution of SQL: {} ".format(setpathsql))
        log_error(error.args[0])

    # Add $gmaxdatenum Global variable  # TODO : is there a reason to put sql below over multiple lines?
    sql = '''
            SELECT
                INT4(TO_CHAR(CURRENT_DATE - 1, 'YYYYMMDD')) AS datenum,
                CAST(EXTRACT(YEAR FROM CURRENT_DATE - 1) AS CHAR(4)) AS year,
                CASE 
                    WHEN EXTRACT(MONTH FROM CURRENT_DATE - 1) BETWEEN 1 AND 9 THEN '0'||CAST(EXTRACT(MONTH FROM CURRENT_DATE - 1) AS CHAR(1)) 
                    ELSE CAST(EXTRACT(MONTH FROM CURRENT_DATE - 1) AS CHAR(2))
                END AS month, 
                CASE 
                    WHEN EXTRACT(DAY FROM CURRENT_DATE - 1) BETWEEN 1 AND 9 THEN '0'||CAST(EXTRACT(DAY FROM CURRENT_DATE - 1) AS CHAR(1)) 
                    ELSE CAST(EXTRACT(DAY FROM CURRENT_DATE - 1) AS CHAR(2))
                END AS day,
                CASE 
                    WHEN EXTRACT(DAY FROM CURRENT_DATE - 2) BETWEEN 1 AND 9 
                    THEN '0'||CAST(EXTRACT( DAY FROM CURRENT_DATE - 2) AS CHAR(1)) 
                    ELSE CAST(EXTRACT( DAY FROM CURRENT_DATE - 2) AS CHAR(2))
                END AS yesterdate
        '''

    try:
        rscursor.execute(sql)
    except psycopg2.Error as error:
        log_error(error.args[0])

    row = rscursor.fetchone()
    gvardict['$gmaxdatenum'] = row['datenum']  # TODO : this is the only reason for this function, reconsider

    gvardict['$yyyy'] = row['year']
    gvardict['$mm'] = row['month']
    gvardict['$dd'] = row['day']
    gvardict['$yyyymmdd'] = row['datenum']

    return 0  # TODO: remove this


def get_loader_info(rscursor, loadercode):
    """
    Returns loader metadata
    Args:
        rscursor: cursor to redshift for metadata store
        loadercode: param passed to etl_engine

    Returns:
        loaderinfo metadata object

    """
    # Parse the Loader metadata and get Loader attributes
    sql = '''
            SELECT
                *
            FROM ops.etl_catalog WHERE UPPER(loader_code) = UPPER('{}')
        '''

    try:
        rscursor.execute(sql.format(loadercode))
    except psycopg2.Error as error:
        log_error(error.args[0])

    if rscursor.rowcount == 0:
        log_error("Loader {} not defined metadata table etl_catalog".format(loadercode))

    # Create dictionary form Loader metadata
    meta_column_names = [col[0] for col in rscursor.description]
    meta = rscursor.fetchone()
    loaderinfo = dict(zip(meta_column_names, meta))  # TODO : consider making loaderinfo a meta class instead

    # Validate Loader
    log_info("Validation for Loader {} started ..".format(loaderinfo.get('loader_code')))
    validate_loader(loaderinfo)  # TODO : modified to take in loadercode and push code down
    log_info("Validation for Loader {} completed ..".format(loaderinfo.get('loader_code')))

    return loaderinfo


def get_logger():  # TODO: make this private
    """
    Configure logger with logging format
    Returns:
            logger object
    """

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(stream=None)
    handler.setLevel(logging.INFO)
    # Reset the logger.handlers if it already exists.
    if logger.handlers:
        logger.handlers = []

    # create a logging format
    if cfg['MISC']['TEST_RUN'] == 'Yes':  # TODO: Rename to more intuitive Airflow etc.
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    else:
        formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def log_info(msg):
    """
    Logs passed message as info
    Args:
        msg: message to be logged

    Returns:
        0
    """
    logger = get_logger()
    logger.info(msg)
    return 0


def log_error(msg):
    """
    Logs passed message as error
    Args:
        msg: message to be logged

    Returns:
        0
    """
    logger = get_logger()
    logger.error(msg)
    sys.exit(1)
    return 0


def log_warning(msg):
    """
    Logs passed message as warning
    Args:
        msg: message to be logged

    Returns:
        0
    """
    logger = get_logger()
    logger.warning(msg)
    return 0


def get_mssql_connection(loaderinfo):
    """
    Get connection object for ms sql server db
    Args:
        loaderinfo:

    Returns:
        pyodbc connection object
    """

    mssqldriver = cfg['MSSQL']['MSSQL_DRIVER']

    if loaderinfo.get('src') == 'DB_MSSQL_AUXLIVE':
        mssqlhost = cfg['MSSQL']['MSSQL_AUX_HOST']
        mssqldatabase = cfg['MSSQL']['MSSQL_AUX_DBNAME']
    elif loaderinfo.get('src') == 'DB_MSSQL_ADVRTDB':
        mssqlhost = cfg['MSSQL']['MSSQL_ADVRTDB_HOST']
        mssqldatabase = cfg['MSSQL']['MSSQL_ADVRTDB_DBNAME']
    elif loaderinfo.get('src') == 'DB_MSSQL_ADV' or loaderinfo.get('src') == 'SQL_MSSQL_ADV':
        mssqlhost = cfg['MSSQL']['MSSQL_ADV_HOST']
        mssqldatabase = cfg['MSSQL']['MSSQL_ADV_DBNAME']
    else:
        log_error('''Invalid source connection {} and it should be
                         one of in the list {}'''.format(loaderinfo.get('src'), MSSQL_SOURCES))

    mssqluser = cfg['MSSQL']['MSSQL_USER']
    mssqlpassword = cfg['MSSQL']['MSSQL_PASSWORD']

    mssqlcnxstr = 'DRIVER={};SERVER={};DATABASE={};UID={};PWD={};autocommit=True'.format(mssqldriver, mssqlhost,
                                                                                         mssqldatabase, mssqluser,
                                                                                         mssqlpassword)

    # Encrypt the connection
    if cfg['MSSQL']['ENCRYPT_CONN'] == 'Yes':  # TODO: make this mandatory by default, move it up in format
        mssqlcnxstr = mssqlcnxstr + ';Encrypt=Yes;TrustServerCertificate=Yes'

    try:
        sqlcon = pyodbc.connect(mssqlcnxstr)
    except pyodbc.Error as error:
        log_error(error.args[1])

    log_info("Connected to MSSQLServer Database {} with user {} ".format(mssqldatabase, mssqluser))

    return sqlcon


def get_rs_connection():
    """
    Get the redshift Connection
    Returns:
        psycopg2 connection object
    """

    rs_host = cfg['Redshift']['RS_HOST']
    rs_database = cfg['Redshift']['RS_DATABASE']
    rs_user = cfg['Redshift']['RS_USER']
    rs_password = cfg['Redshift']['RS_PASSWORD']

    try:
        rscon = psycopg2.connect(host=rs_host, user=rs_user, password=rs_password, dbname=rs_database, port='5439')
    except psycopg2.Error as error:
        log_error(error.args[0])

    log_info("Connected to Redshift Database {} with user {} on Host {}".format(rs_database, rs_user, rs_host))

    return rscon


def validate_loader(loaderinfo):  # TODO : extract this out to a test that runs in CI to catch misconfigurations
    """
    Validates loader aka. meta entry
    Args:
        loaderinfo:

    Returns:

    """
    # If SQL file name not defined in metadata log an error and exit
    if loaderinfo.get('src') in SQL_BASED_LOADERS and loaderinfo.get('load_target') != 'S3':
        if loaderinfo.get('sql_file_name') is None:
            log_error("SQL File name in which Code has Not defined in sc_loader Metadata for Loader : {}".format(
                loaderinfo.get('loader_code')))
        else:
            sqlfile = os.path.join(cfg['ETL_ENGINE_PATHS']['ETL_XFORMS_LOC'],
                                   loaderinfo.get('sql_file_name'))
            if not os.path.isfile(sqlfile):
                log_error("SQL File {} doesn't exist".format(sqlfile))
    # if Loader is of type extracting from MSSQLServer, src_table, tgt_table, src_data_set_type should be defined
    elif loaderinfo.get('src') in MSSQL_SOURCES and loaderinfo.get('load_target') != 'S3':
        # Check tgt_table is defined
        if loaderinfo.get('tgt_table') is None or loaderinfo.get('src_data_set_type') is None:
            log_error("Target table/src_data_set_type(FULL/INC) Not defined in sc_loader Metadata for Loader : {}"
                      .format(loaderinfo.get('loader_code')))
    elif loaderinfo.get('load_target') == 'S3':
        if loaderinfo.get('src_sql_override') is None:
            if (loaderinfo.get('src_table_schema') is None
                or loaderinfo.get('src_table') is None
                or loaderinfo.get('src_data_set_type') is None):
                log_error("Loader should have defined src_data_set_type(FULL/INC), src_sql_override or \
                              (src_table_schema and src_table)")
            elif loaderinfo.get('data_file_location') is None:
                log_error("Loader should have defined with S3 location to unload/upload")
        elif loaderinfo.get('data_file_location') is None or loaderinfo.get('src_data_set_type') is None:
            print(loaderinfo.get('data_file_location'))
            print(loaderinfo.get('src_data_set_type'))
            log_error("Loader should have defined with src_data_set_type(FULL/INC) and S3 location to unload/upload")

    return 0


# **********************************************
# Run FE ETL Loader POST SQL
# **********************************************
def run_fe_loader_pre_or_post_sql(rscnx, sql):

    # Replace the variables with corresponding values
    for key, value in gvardict.items():
        sql = sql.replace(str(key), str(value))

    # Open Connection
    rscursor = rscnx.cursor()

    # Execute SQL
    log_info("PRE/post SQL is: {} ".format(sql))
    try:
        rscursor.execute(sql)
        log_info("Rows affected:{}".format(rscursor.rowcount))
        rscnx.commit()
    except psycopg2.Error as error:
        log_info("Error during execution of SQL: {} ".format(sql))
        log_error(error.args[0])

    #rscnx.commit()


# **********************************************
# Run FE ETL SQLBased Loader
# **********************************************
def run_fe_sql_based_loader(rscnx, loaderinfo):
    # Get the SQL File
    filename = os.path.join(cfg['ETL_ENGINE_PATHS']['ETL_XFORMS_LOC'], loaderinfo.get('sql_file_name'))

    # Execute SQL Process
    log_info("SQLBased Loader {} started".format(loaderinfo.get('loader_code')))

    # Open and read the file as a single buffer
    infile = open(filename, 'r')
    tmpfilename = os.path.join(cfg['ETL_ENGINE_PATHS']['ETL_WORK_DIR'], os.path.basename(filename))
    tmpfile = open(tmpfilename, 'w')

    for line in infile:
        if line.lstrip().startswith('--'):
            continue
        else:
            tmpfile.write(line)

    infile.close()
    tmpfile.close()

    with open(tmpfilename, 'r') as infile:
        newstring = re.sub(r'/\*.*?\*/', ' ', infile.read(), flags=re.S)
    with open(tmpfilename, 'w') as infile:
        infile.write(newstring)

    infile = open(tmpfilename, 'r')
    sqlfile = infile.read()
    infile.close()

    # Replace the variables with corresponding values
    for key, value in gvardict.items():
        sqlfile = sqlfile.replace(str(key), str(value))

    # all SQL commands (split on ';')
    sqlcommands = sqlfile.split(';')

    if loaderinfo.get('src') == 'SQL_RS':

        # Open Connection
        rscursor = rscnx.cursor()
        sqlcnt = 0

        # Execute every command from the input file
        for sql in sqlcommands:
            sql = sql.strip()
            if sql != '':
                try:
                    sqlcnt = sqlcnt + 1
                    log_info("******* SQL : {}".format(sqlcnt))
                    sqlstarttime = datetime.now()
                    log_info(sql)
                    rscursor.execute(sql)
                    if sql.lower().startswith('insert') or sql.lower().startswith('update') or sql.lower().startswith('delete'):
                        log_info("Rows affected:{}".format(rscursor.rowcount))
                    sqlendtime = datetime.now()
                    delta = sqlendtime - sqlstarttime
                    log_info("SQL {} duration is : {}".format(sqlcnt, round(delta.seconds / 60.0, 2)))
                except psycopg2.Error as error:
                    # log_info("Error during execution of SQL: {} ".format(sql))
                    log_error(error.args[0])
        rscnx.commit()
    else:
        # Get the connection
        cnx = get_mssql_connection(loaderinfo)
        # open Connection
        mssqlcursor = cnx.cursor()

        # Execute every command from the input file
        for sql in sqlcommands:
            sql = sql.strip()
            log_info(sql)
            if sql != '':
                try:
                    mssqlcursor.execute(sql)
                except pyodbc.Error as error:
                    log_info("Error during execution of SQL: {} ".format(sql))
                    log_error(error.args[1])
        cnx.commit()

    log_info("SQLBased Loader {} completed".format(loaderinfo.get('loader_code')))

    # Remove the Temp File
    os.remove(tmpfilename)

    return 0


# ***************************************************************
# generate_sql_for_mssql_extract
# ***************************************************************
def generate_sql_for_mssql_extract(loaderinfo):
    # Generate Select statement
    log_info("SQLGeneration to be used during Extract for Loader {} started..".format(
        loaderinfo.get('loader_code')))

    if loaderinfo.get('src_sql_override') is not None:
        mssqlselectstmt = loaderinfo.get('src_sql_override')

        # Replace the variables with values
        for key, value in gvardict.items():
            mssqlselectstmt = mssqlselectstmt.replace(str(key), str(value))
    else:
        mssqlselectstmt = 'SELECT \n\t'

        # Check any Row limit is specified
        if loaderinfo.get('src_record_limit') is not None:
            mssqlselectstmt = mssqlselectstmt + 'TOP ' + str(loaderinfo.get('src_record_limit')) + '\n\t'

        # Append the from and where clause expressions if any

        mssqlselectstmt = mssqlselectstmt + '\nFROM' + '\n\t' + loaderinfo.get('src_table')

        # If any filter clause is needed Generate it

        if loaderinfo.get('src_specific_filter_condition') is not None:
            mssqlselectstmt = mssqlselectstmt + ' WHERE ' + loaderinfo.get('src_specific_filter_condition')

    log_info("SQL to extract from Source database is \n {} \n".format(mssqlselectstmt))

    log_info("SQLGeneration to be used during Extract for Loader {} completed..".format(
        loaderinfo.get('loader_code')))

    return mssqlselectstmt


# ********************************************
# Iterate over a huge table records
# (DB memory optimization)
# ********************************************

def rows_iter(cursor, arraysize):
    while True:
        results = cursor.fetchmany(arraysize)
        if len(results) == 0:
            break
        for result in results:
            yield result


# ***************************************************************
# extract_from_mssql_to_csv -- using bcp command
# ***************************************************************
def extract_from_sqldb(loaderinfo):
    sql4extract = generate_sql_for_mssql_extract(loaderinfo)

    # Check weather source is adv or aux

    if loaderinfo.get('src').lower().find('adv') != -1:
        dbsrc = 'adv'
    elif loaderinfo.get('src').lower().find('auxlive') != -1:
        dbsrc = 'auxlive'

    if loaderinfo.get('data_file_location') is None:
        datadir = os.path.join(cfg['ETL_ENGINE_PATHS']['ETL_DATA_DIR'], 'raw', currentdate, dbsrc,
                               loaderinfo.get('tgt_table'))
    else:
        # In case of S3 load discard the Filename in data_file_location if any exists
        # Filename is generated by the engine
        if os.path.basename(loaderinfo.get('data_file_location')).find('.') == -1:
            datadir = os.path.join(cfg['ETL_ENGINE_PATHS']['ETL_DATA_DIR'], loaderinfo.get('data_file_location'))
        else:
            datadir = os.path.join(cfg['ETL_ENGINE_PATHS']['ETL_DATA_DIR'],
                                   os.path.dirname(loaderinfo.get('data_file_location')))

    # If directory exist delete the directory and its contents
    if os.path.exists(datadir):
        shutil.rmtree(datadir)

    # Create directory
    os.makedirs(datadir)

    if loaderinfo.get('file_delimiter') is None:
        filedelimiter = '|'
    else:
        filedelimiter = loaderinfo.get('file_delimiter')

    if loaderinfo.get('src_data_set_type') == 'INC':
        datafilename = os.path.join(datadir, loaderinfo.get('tgt_table') + '_' + currentdate + '.csv')
    else:
        datafilename = os.path.join(datadir, loaderinfo.get('tgt_table') + '.csv')

    # Extract data using bcp command

    if loaderinfo.get('src') == 'DB_MSSQL_AUXLIVE':
        mssqlhost = cfg['MSSQL']['MSSQL_AUX_HOST']
        mssqldatabase = cfg['MSSQL']['MSSQL_AUX_DBNAME']
    elif loaderinfo.get('src') == 'DB_MSSQL_ADVRTDB':
        mssqlhost = cfg['MSSQL']['MSSQL_ADVRTDB_HOST']
        mssqldatabase = cfg['MSSQL']['MSSQL_ADVRTDB_DBNAME']
    elif loaderinfo.get('src') == 'DB_MSSQL_ADV' or loaderinfo.get('src') == 'SQL_MSSQL_ADV':
        mssqlhost = cfg['MSSQL']['MSSQL_ADV_HOST']
        mssqldatabase = cfg['MSSQL']['MSSQL_ADV_DBNAME']
    else:
        log_error('''Invalid source connection {} and it should be
                         one of in the list {}'''.format(loaderinfo.get('src'), MSSQL_SOURCES))

    mssqluser = cfg['MSSQL']['MSSQL_USER']
    mssqlpassword = cfg['MSSQL']['MSSQL_PASSWORD']

    bcp_cmd = '''
                /opt/mssql-tools/bin/bcp "{}" queryout {} -S {} -U {} -P '{}' -d {} -c -t '{}'
            '''

    bcp_cmd = bcp_cmd.format(sql4extract, datafilename, mssqlhost, mssqluser,
                             mssqlpassword, mssqldatabase, filedelimiter)

    sub_process = subprocess.Popen(bcp_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #sub_process.wait()
    (std_out, std_err) = sub_process.communicate()
    retcode = sub_process.returncode

    if retcode > 0:
        log_error("Data extract exited with error {} {}".format(std_err, std_out))
    else:
        log_info("Data exported to file {}".format(datafilename))
        log_info("Data export stats are:")
        log_info(std_out)

    # Split the big Files

    if loaderinfo.get('src_record_split_count') is not None and loaderinfo.get('src_data_set_type') != 'INC':

        # Split the file to small size files
        log_info("Splitting of file {} started....".format(datafilename))

        # Split using Unix split command
        currentwd = os.getcwd()
        os.chdir(datadir)
        splitcmd = 'split -l ' + str(loaderinfo.get('src_record_split_count')) + ' ' + \
                   loaderinfo.get('tgt_table') + '.csv' + ' ' + loaderinfo.get('tgt_table')
        sub_process = subprocess.Popen(splitcmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        #sub_process.wait()
        (std_out, std_err) = sub_process.communicate()
        retcode = sub_process.returncode
        if retcode > 0:
            log_error("Split command {} failed".format(splitcmd))

        os.chdir(currentwd)
        os.remove(datafilename)

    return 0


# ***************************************************************
# extract_from_mssql_to_csv -- using Cursor
# ***************************************************************
def extract_from_mssql_to_csv(loaderinfo):
    # Get the SQL Server connection
    mssqlcnx = get_mssql_connection(loaderinfo)
    sql4extract = generate_sql_for_mssql_extract(loaderinfo)

    # Generate the CSV file based on SQL
    mssqlcur = mssqlcnx.cursor()
    try:
        mssqlcur.execute(sql4extract)
    except pyodbc.ProgrammingError as error:
        log_error(error)

    if mssqlcur.rowcount == 0:
        log_warning("ZERO rows extracted from Advisor. Skipping the remaining steps..")
        sys.exit(0)
    # Check weather source is adv or aux
    if loaderinfo.get('src').lower().find('adv') != -1:
        dbsrc = 'adv'
    elif loaderinfo.get('src').lower().find('auxlive') != -1:
        dbsrc = 'auxlive'

    if loaderinfo.get('data_file_location') is None:
        datadir = os.path.join(cfg['ETL_ENGINE_PATHS']['ETL_DATA_DIR'], 'raw', currentdate, dbsrc,
                               loaderinfo.get('tgt_table'))
    else:
        # In case of S3 load discard the Filename in data_file_location if any exists
        # Filename is generated by the engine
        if os.path.basename(loaderinfo.get('data_file_location')).find('.') == -1:
            datadir = os.path.join(cfg['ETL_ENGINE_PATHS']['ETL_DATA_DIR'], loaderinfo.get('data_file_location'))
        else:
            datadir = os.path.join(cfg['ETL_ENGINE_PATHS']['ETL_DATA_DIR'],
                                   os.path.dirname(loaderinfo.get('data_file_location')))

    # If directory exist delete the directory and its contents
    if os.path.exists(datadir):
        shutil.rmtree(datadir)

    # Create directory
    os.makedirs(datadir)

    if loaderinfo.get('file_delimiter') is None:  # TODO : move this to common place, code duplication
        filedelimiter = '|'
    else:
        filedelimiter = loaderinfo.get('file_delimiter')

    if loaderinfo.get('src_data_set_type') == 'INC':
        datafilename = os.path.join(datadir, loaderinfo.get('tgt_table') + '_' + currentdate + '.csv')
    else:
        datafilename = os.path.join(datadir, loaderinfo.get('tgt_table') + '.csv')

    datafile = open(datafilename, 'w')
    csvwriter = csv.writer(datafile, delimiter=filedelimiter, quoting=csv.QUOTE_MINIMAL, lineterminator='\n')

    for rows in rows_iter(mssqlcur, 100000):
        csvwriter.writerow(rows)
    datafile.close()

    log_info("Data exported to file {}".format(datafilename))

    # Split the big Files

    if loaderinfo.get('src_record_split_count') is not None and loaderinfo.get('src_data_set_type') != 'INC':

        # Split the file to small size files
        log_info("Splitting of file {} started....".format(datafilename))

        # Split using Unix split command
        currentwd = os.getcwd()
        os.chdir(datadir)
        splitcmd = 'split -l ' + str(loaderinfo.get('src_record_split_count')) + ' ' + \
                   loaderinfo.get('tgt_table') + '.csv' + ' ' + loaderinfo.get('tgt_table')
        sub_process = subprocess.Popen(splitcmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        #sub_process.wait()
        (std_out, std_err) = sub_process.communicate()
        retcode = sub_process.returncode
        if retcode > 0:
            log_error("Split command {} failed".format(splitcmd))

        os.chdir(currentwd)
        os.remove(datafilename)

    return 0


# ***************************************************************
# Copy the file to S3
# ***************************************************************
def copy_file_to_s3(filename):
    # Transfer File to S3 bucket
    srcfile = os.path.join(cfg['ETL_ENGINE_PATHS']['ETL_DATA_DIR'], filename)
    s3bucket = cfg['S3']['S3_BUCKET']
    s3client = boto3.client('s3', cfg['S3']['S3_REGION'])
    transfer = boto3.s3.transfer.S3Transfer(s3client)
    s3location = "s3://{}/{}".format(s3bucket, filename)

    try:
        transfer.upload_file(srcfile, s3bucket, filename, extra_args=S3_ARGS)
        log_info("File {} transferred to S3 location {}".format(srcfile, s3location))
    except botocore.exceptions.ClientError as e:
        log_error("S3 transfer for File {} Failed with Error {}: {}".format(srcfile, e.response['Error']['Code'],
                                                                            e.response['Error']['Message']))

    return 0


def copy_from_ec2_to_s3(loaderinfo):
    """
    Copy the Splitted Loader files to S3 using parallel process  # TODO : what do we mean by splitted?
    Args:
        loaderinfo:

    Returns:

    """

    # Source Folder for copy to S3
    # Check weather source is adv or aux
    if loaderinfo.get('src').lower().find(
            'adv') != -1:  # TODO : move this check to metadata validation so we could loaderinfo.get('src')
        dbsrc = 'adv'
    elif loaderinfo.get('src').lower().find('auxlive') != -1:
        dbsrc = 'auxlive'

    if loaderinfo.get('load_target') == 'S3':
        # In case of S3 load discard the Filename in data_file_location if any exists
        # Filename is generated by the engine
        if os.path.basename(loaderinfo.get('data_file_location')).find('.') == -1:
            bucket = loaderinfo.get('data_file_location')
        else:
            bucket = os.path.dirname(loaderinfo.get('data_file_location'))

        srcfolder = os.path.join(cfg['ETL_ENGINE_PATHS']['ETL_DATA_DIR'], bucket)
    else:
        bucket = 'raw/' + currentdate + '/' + dbsrc + '/' + loaderinfo.get('tgt_table')
        srcfolder = os.path.join(cfg['ETL_ENGINE_PATHS']['ETL_DATA_DIR'], 'raw', currentdate, dbsrc,
                                 loaderinfo.get('tgt_table'))

    # Check Source file(s) exist or not
    if (not os.path.exists(srcfolder)) | len(os.listdir(srcfolder)) == 0:
        log_error("There are no files to upload to S3")

    # Get FileNames
    filelist1 = filter(lambda x: x.startswith(loaderinfo.get('tgt_table')), os.listdir(srcfolder))
    filelist2 = []

    for filename in filelist1:
        filelist2.append(bucket + '/' + filename)

    # Check If bucket exists or not
    s3bucket = cfg['S3']['S3_BUCKET']
    try:
        s3resource = boto3.resource('s3')
    except botocore.exceptions.ClientError as e:
        log_error("Error : {} - {}".format(e.response['Error']['Code'], e.response['Error']['Message']))

    try:
        s3resource.meta.client.head_bucket(Bucket=s3bucket)
    except botocore.exceptions.ClientError as e:
        log_error("Error : {} - {}".format(e.response['Error']['Code'], e.response['Error']['Message']))

    # In case of src_data_set_type is FULL with S3 load target  or load target is not S3 clean existing objects in S3
    # target location
    if not (loaderinfo.get('src_data_set_type') == 'INC' and loaderinfo.get('load_target') == 'S3'):
        objects_to_delete = []

        # Get the File Names in Table Bucket Folder
        for obj in s3resource.Bucket(s3bucket).objects.filter(Prefix=bucket):
            objects_to_delete.append({'Key': obj.key})

        try:
            s3resource.Bucket(s3bucket).delete_objects(Delete={'Objects': objects_to_delete})
            log_info("Folder {} in bucket {} deleted".format(bucket, s3bucket))
        except botocore.exceptions.ClientError as e:
            log_info("Folder {} not exists in S3 Bucket {} and proceeding copy".format(bucket, s3bucket))

    # Upload the Files in parallel
    pool = ThreadPool(processes=10)
    pool.map(copy_file_to_s3, filelist2)  # TODO : removed results
    pool.close()
    pool.join()

    return 0


# ***************************************************************
# Copy the Files from S3 to redshift
# ***************************************************************
def copy_from_s3_to_redshift(rscnx, loaderinfo):
    # Set the redshift Schema
    rscursor = rscnx.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    #Due to recent Redshift version upgrade SSL error occurring for long running jobs.
    #Apply temp fix that will reconnect when connection lost
    
        # Execute the redshift Copy command
    try:
        rscursor.execute('SELECT CURRENT_DATE')
        log_info("No issue with Redshift connection")
    except psycopg2.Error as error:
        #Recreate the connection
        rscnx = get_rs_connection()  
        rscursor = rscnx.cursor(cursor_factory=psycopg2.extras.DictCursor)
        rscursor.execute('SET SEARCH_PATH TO core, ops, stage, semantic, reporting')
        log_info("Redshift connection Reestablished")    

    # Cleanup/Truncate the target table
    if loaderinfo.get('is_truncate_load'):
        cleanupsql = "TRUNCATE TABLE {}".format(loaderinfo.get('tgt_table'))
        log_info("Truncate SQL : {}".format(cleanupsql))

        try:
            rscursor.execute(cleanupsql)
        except psycopg2.Error as error:
            log_error(error.args[0])

    elif loaderinfo.get('cleanup_condition') is not None:
        cleanupsql = "DELETE FROM {} WHERE {}".format(loaderinfo.get('tgt_table'), loaderinfo.get('cleanup_condition'))

        # Iterate over Global variables in dictionary and replace variable with value
        for key, value in gvardict.items():
            cleanupsql = cleanupsql.replace(str(key), str(value))
        log_info("DELETE SQL : {}".format(cleanupsql))

        try:
            rscursor.execute(cleanupsql)
        except psycopg2.Error as error:
            log_error(error.args[0])

    # If any column names to use explicitly in copy command

    collist = ''

    if loaderinfo.get('copy_col_list') is not None:
        collist = collist + '(' + loaderinfo.get('copy_col_list') + ')'

    # Get the S3 location
    s3bucket = cfg['S3']['S3_BUCKET']

    if loaderinfo.get('src') == 'FILE_DELIM':
        s3location = "s3://{}/{}".format(s3bucket, loaderinfo.get('data_file_location'))
    else:
        # Check weather source is adv or aux
        if loaderinfo.get('src').lower().find('adv') != -1:
            dbsrc = 'adv'
        elif loaderinfo.get('src').lower().find('auxlive') != -1:
            dbsrc = 'auxlive'

        bucketcontent = 'raw/' + currentdate + '/' + dbsrc + '/' + loaderinfo.get('tgt_table')
        s3location = "s3://{}/{}/".format(s3bucket, bucketcontent)

    # File options
    if loaderinfo.get('file_format') is not None:
        fileformat = loaderinfo.get('file_format')
    else:
        fileformat = ''

    if loaderinfo.get('rs_copy_extra_args') is not None:
        copycmdextraargs = loaderinfo.get('rs_copy_extra_args')
    else:
        copycmdextraargs = ''

    if loaderinfo.get('file_delimiter') is not None:
        filedelimter = loaderinfo.get('file_delimiter')
    else:
        filedelimter = '|'

    # Get the AWS IAM role from environment varaible
    redshiftcopycredentials = cfg['Redshift']['AWS_IAM_ROLE_ARN']
    # If fileformat is JSON then don't include DELIMITER clause
    delim_expr = '''DELIMITER \'{}\''''.format(filedelimter)
    if 'JSON' in fileformat.upper():
        delim_expr = ''
    elif 'AVRO' in fileformat.upper():
        delim_expr = ''
        additional_args = ''
        if loaderinfo.get('rs_copy_extra_args') is not None:
            avro_params = loaderinfo.get('rs_copy_extra_args')
        else:
            avro_params = '''\'auto\''''
        fileformat = '''FORMAT AS AVRO {}'''.format(avro_params)
        copycmdextraargs = ''
    additional_args = '''EMPTYASNULL ACCEPTINVCHARS DATEFORMAT 'auto' TIMEFORMAT 'auto' IGNOREHEADER {} {}'''.format(loaderinfo.get('skiprows'), copycmdextraargs)
    # Generate COPY SQL
    rscopysql = '''
                    COPY {}{}
                    FROM '{}'
                    CREDENTIALS 'aws_iam_role={}'
                    {} {} {}
                '''

    rscopysql = rscopysql.format(loaderinfo.get('tgt_table'), collist, s3location,
                                 redshiftcopycredentials, delim_expr, fileformat,
                                 additional_args
                                 )
    # Iterate over Global variables in dictionary and replace variable with value
    for key, value in gvardict.items():
        rscopysql = rscopysql.replace(str(key), str(value))
        s3location = s3location.replace(str(key), str(value))

    log_info("COPY SQL : {}".format(rscopysql))

    # Check If bucket exists or not
    s3bucket = cfg['S3']['S3_BUCKET']
    try:
        s3resource = boto3.resource('s3')
    except botocore.exceptions.ClientError as e:
        log_error("Error : {} - {}".format(e.response['Error']['Code'], e.response['Error']['Message']))

    try:
        # if s3resource.Bucket(cfg['S3']['S3_BUCKET']) not in s3resource.buckets.all():
        #    log_error("S3 bucket {} doesn't exist".format(s3bucket))
        s3resource.meta.client.head_bucket(Bucket=s3bucket)
    except botocore.exceptions.ClientError as e:
        log_error("Error : {} - {}".format(e.response['Error']['Code'], e.response['Error']['Message']))

    # Check if S3 objct exist
    s3listcmd = 'aws s3 ls ' + s3location
    sub_process = subprocess.Popen(s3listcmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #sub_process.wait()
    (std_out, std_err) = sub_process.communicate()
    retcode = sub_process.returncode
    if retcode > 0:
        log_error("Failed to locate S3 object in location : {}".format(s3location))

    # Execute the redshift Copy command
    try:
        rscursor.execute(rscopysql)
        log_info("Copy to table {} from Location {} completed".format(loaderinfo.get('tgt_table'), s3location))
    except psycopg2.Error as error:
        log_error(error.args[0])

    rscnx.commit()
    rscursor.execute('SELECT pg_last_copy_count()')
    row = rscursor.fetchone()
    message = "Table {} loaded with {} rows".format(loaderinfo.get('tgt_table'), row['pg_last_copy_count'])
    log_info(message)

    # Cleanup the File on EC2
    # Check weather source is adv or aux
    dbsrc = ''
    if loaderinfo.get('src').lower().find('adv') != -1:
        dbsrc = 'adv'
    elif loaderinfo.get('src').lower().find('auxlive') != -1:
        dbsrc = 'auxlive'

    if dbsrc != '':
        srcfolder = os.path.join(cfg['ETL_ENGINE_PATHS']['ETL_DATA_DIR'], 'raw', currentdate, dbsrc,
                                 loaderinfo.get('tgt_table'))
        if os.path.exists(srcfolder):
            shutil.rmtree(srcfolder)

    return 0


def evaluate_sql_parameters(rscursor, sql_parameters):  # TODO : move this to meta class processing
    """
    Load the JSON format sql_parameters and update the global variables list
    Args:
        rscursor:
        sql_parameters:

    Returns:

    """
    sql_parameters = json.loads(sql_parameters)

    if 'sql_params' not in sql_parameters:
        raise ValueError('sql_parameters JSON format is invalid')

    for sqlparam in sql_parameters['sql_params']:
        if 'name' not in sqlparam:
            raise ValueError('Attribute name is not defined in JSON for sql_params')
        else:
            sqlparamname = sqlparam['name']
        if 'type' not in sqlparam:
            raise ValueError('Attribute type is not defined in JSON for sql_params')
        else:
            sqlparamtype = sqlparam['type']

        if sqlparamtype == 'const':
            if 'value' not in sqlparam:
                raise ValueError('value for constant is not defined in JSON for sql_params')
            else:
                sqlparamvalue = sqlparam['value']

                # Replace the variables with corresponding values
                for key, value in gvardict.items():
                    sqlparamvalue = sqlparamvalue.replace(str(key), str(value))

                # If the parameter type is const assign the value to param name
                gvardict.update({sqlparamname: sqlparamvalue})
        elif sqlparamtype == 'expr':

            if 'sql' in sqlparam:
                sql = sqlparam['sql']
            else:
                if 'schema' not in sqlparam or 'table' not in sqlparam:
                    sql = '''
                            SELECT {} AS retvalue
                          '''

                    sql = sql.format(sqlparam['value'])
                else:
                    sqlparamschema = sqlparam['schema']
                    sqlparamtable = sqlparam['table']

                    sqlparamvalue = sqlparam['value']

                    sql = '''
                            SELECT
                                {} AS retvalue
                            FROM
                                {}.{}
                          '''

                    sql = sql.format(sqlparamvalue, sqlparamschema, sqlparamtable)

                    if 'filter' in sqlparam:
                        sqlfilter = sqlparam['filter']

                        sql = sql + '''
                              WHERE 
                                {}
                        '''

                        sql = sql.format(sqlfilter)

            # Replace the variables with corresponding values
            for key, value in gvardict.items():
                sql = sql.replace(str(key), str(value))

            log_info("SQL for evaluating the param : {}".format(sql))

            # Execute the SQL to evaluate parameter
            try:
                rscursor.execute(sql)
            except psycopg2.Error as error:
                log_error(error.args[0])

            if rscursor.rowcount == 0:
                log_error("SQL expression is incorrect. Please fix the loader code..")
            else:
                retvalue = rscursor.fetchone()
                gvardict.update({sqlparamname: retvalue[0]})
    return 0


# ***************************************************************
# Copy the Required for Loader From FTP or
# shared location to EC2
# ***************************************************************
def copy_from_ftp_to_ec2(loaderinfo):
    log_info("Processing Source File for Loader {}".format(loaderinfo.get('loader_code')))
    # Replace variables in FileName with values in Global variables dictionary
    srcfile = loaderinfo.get('data_file_location')
    for key, value in gvardict.items():
        srcfile = srcfile.replace(str(key), str(value))

    # Check if Source File exists
    if not os.path.exists(srcfile):
        log_error("File {} missing. Aborting the loader".format(srcfile))

    # Create Directory on EC2 instance
    datadir = os.path.join(cfg['ETL_ENGINE_PATHS']['ETL_DATA_DIR'], currentdate, loaderinfo.get('tgt_table'))

    # If directory exist delete the directory and its contents
    if os.path.exists(datadir):
        shutil.rmtree(datadir)

    # Create directory
    os.makedirs(datadir)
    subprocess.call(['chmod', '775', os.path.join(cfg['ETL_ENGINE_PATHS']['ETL_DATA_DIR'], currentdate)])

    # Copy File from FTP Share to EC2
    shutil.copyfile(srcfile, os.path.join(datadir, loaderinfo.get('tgt_table') + '.txt'))

    # Remove the Header If exists
    currentwd = os.getcwd()
    os.chdir(datadir)

    if loaderinfo.get('skiprows') > 0:
        rmheadercmd = 'sed -i ' + str(loaderinfo.get('skiprows')) + 'd ' + str(loaderinfo.get('tgt_table')) + '.txt'
        sub_process = subprocess.Popen(rmheadercmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        #sub_process.wait()
        (std_out, std_err) = sub_process.communicate()
        retcode = sub_process.returncode
        if retcode > 0:
            log_error("Deleting header on File {} failed".format(os.path.basename(srcfile)))

    if loaderinfo.get('src_record_split_count') is not None:

        # Split using Unix split command
        splitcmd = 'split -l ' + loaderinfo.get('src_record_split_count') + ' ' + os.path.basename(
            srcfile) + ' ' + loaderinfo.get('tgt_table')
        sub_process = subprocess.Popen(splitcmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        #sub_process.wait()
        (std_out, std_err) = sub_process.communicate()
        retcode = sub_process.returncode
        if retcode > 0:
            log_error("Split command {} failed".format(splitcmd))

        os.remove(os.path.basename(srcfile))
    os.chdir(currentwd)

    return 0


# ************************************************************************************
# Generate the SCD Metadata to be used during SCD load
# Parameters:
#    1. scdTableName - Name of the SCD table for which metadata to be generated
#    2. stgTableName - stgTableName where the source data available
#    3. keyColumns - Key columns in source data seperated by comma(,)
#    4. isStgDataIncremental - Is source data is Fill data or incremental data(True,False)
# ************************************************************************************
def generate_scd_metadata(scd_table_name, stg_table_name, key_columns, is_stg_data_incremental):
    # Convert the params to lowercase
    scd_table_name = scd_table_name.lower()
    stg_table_name = stg_table_name.lower()
    key_columns = key_columns.lower()

    # Connect to redshift
    cnx = get_rs_connection()

    # Generate the row_key_hash expression by parsing the input parameter
    # key_columns and concatinate each column with Special char ~
    row_key_hash_expr = ''
    row_data_hash_expr = ''
    col_list_expr = ''
    for keycolumn in key_columns.split(','):
        expr1 = '''{}||''~''||\n'''
        row_key_hash_expr = row_key_hash_expr + expr1.format(keycolumn)

    # Strip the last occurance of ||'~'||\n
    row_key_hash_expr = row_key_hash_expr.strip('''||''~''||\n''')

    # Add the MD5 function to row_key_hash_expr string
    row_key_hash_expr = 'MD5(' + row_key_hash_expr + ') AS row_key_hash'

    # Generate the row_data_hash expression by parsing the columns
    # from information_schema.columns
    cursor = cnx.cursor(cursor_factory=psycopg2.extras.DictCursor)
    sql = '''
            SELECT
                column_name,
                CASE WHEN is_nullable = 'NO' AND data_type <> 'boolean' THEN column_name
                     ELSE
                        CASE WHEN data_type IN ('bigint', 'integer', 'real', 'smallint', 'double precision', 'numeric')
                                    THEN 'NVL('||column_name||', -999)'
                             WHEN data_type IN ('character', 'character varying')
                                    THEN 'NVL('||column_name||',\''\''\''\'')'
                             WHEN data_type = 'boolean'
                                    THEN 'CASE WHEN '||column_name||' IS NULL THEN \''\''\''\'' WHEN '||column_name||' THEN \''\''TRUE\''\'' ELSE \''\''FALSE\''\'' END'
                             WHEN data_type IN ('timestamp without time zone', 'date')
                                    THEN 'NVL('||column_name||',\''\''9999-12-31\''\'')'
                        END
                END AS col_expr
            FROM
                information_schema.columns
            WHERE
                table_name = '{}' 
                AND column_name NOT IN ( 'row_key_hash', 
                             'row_data_hash', 
                             'start_datenum', 
                             'end_datenum', 
                             'etl_created_job_id', 
                             'etl_updated_job_id', 
                             'etl_created_tms', 
                             'etl_updated_tms'
                            )           
            order by ordinal_position
            '''

    try:
        cursor.execute(sql.format(scd_table_name))
    except psycopg2.Error as error:
        log_error(error.args[0])

    expr1 = '''{}||''~''||\n'''
    expr2 = '{},\n'

    for row in cursor:
        if re.search('created', row['column_name']) is None and re.search('last_updated', row['column_name']) is None:
            row_data_hash_expr = row_data_hash_expr + expr1.format(row['col_expr'])
        col_list_expr = col_list_expr + expr2.format(row['column_name'])

    # Strip the last occurance of ||'~'||\n
    row_data_hash_expr = row_data_hash_expr.strip('''||''~''||\n''')

    # Add the MD5 function to row_key_hash_expr string
    row_data_hash_expr = 'MD5(' + row_data_hash_expr + ') AS row_data_hash'

    # Strip the last occurance of ||','||\n
    col_list_expr = col_list_expr.strip(',\n')

    # Generate the INSERT statement
    innsert_sql = '''
                  INSERT INTO scd_metadata
                  VALUES
                  (
                    '{}',
                    '{}',
                    '{}',
                     {},
                    '{}',
                    '{}',
                    '{}',
                    CURRENT_TIMESTAMP,
                    CURRENT_TIMESTAMP
                  );
                '''

    log_info(innsert_sql.format(scd_table_name, stg_table_name, key_columns, is_stg_data_incremental, row_key_hash_expr,
                                row_data_hash_expr, col_list_expr))
    cnx.close()
    return 0


# ************************************************************************************
# Load SCD table
# Parameters:
#    1. cnx - redshift connection object
#    2. scdTableName - Name of the SCD table for which metadata to be generated
# ************************************************************************************
def load_scd(cnx, scd_table_name, **kwargs):
    log_info("SCD load started for table : {}".format(scd_table_name))
    # Assign the variables
    scd_table_name = scd_table_name.lower()
    jobname = 'J_LOAD_' + scd_table_name
    loadts = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')

    # For history load pass additional arguments startDatenum, endDatenum
    startdatenum = kwargs.get('startDateNum', None)
    enddatenum = kwargs.get('endDateNum', None)

    cursor = cnx.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # get the job id to be used
    sql = '''
          SELECT MAX(job_id) AS job_id FROM job_info WHERE job_category = 'AnalyticsMart'
          '''
    try:
        cursor.execute(sql)
    except psycopg2.Error as error:
        log_info("Error during execution of SQL: {} ".format(sql))
        log_error(error.args[0])

    for row in cursor:
        jobid = row['job_id']

    # Get the meta data to Load SCD table
    sql = '''
          SELECT
                src_tbl_name, 
                is_stg_data_incremental, 
                row_key_hash_expr, 
                row_data_hash_expr, 
                col_list_expr
          FROM
              scd_metadata 
          WHERE 
              tbl_name = '{}'
    '''

    try:
        cursor.execute(sql.format(scd_table_name))
    except psycopg2.Error as error:
        log_info("Error during execution of SQL: {} ".format(sql.format(scd_table_name)))
        log_error(error.args[0])

    if cursor.rowcount == 0:
        log_error('Loader metadata not defined for Table {}'.format(scd_table_name))

    for row in cursor:
        srctablename = row['src_tbl_name']
        isstgdataincremental = row['is_stg_data_incremental']
        rowkeyhashexpr = row['row_key_hash_expr']
        rowdatahashexpr = row['row_data_hash_expr']
        collistexpr = row['col_list_expr']

    # Check previous day data is missing in target table

    if startdatenum is None:
        sql = '''
              SELECT
                    datenum
              FROM
                   {}
              LIMIT 1
              '''

        try:
            cursor.execute(sql.format(srctablename))
        except psycopg2.Error as error:
            log_info("Error during execution of SQL: {} ".format(sql.format(srctablename)))
            log_error(error.args[0])

        if cursor.rowcount == 0:
            log_error("No data available in table {}. Please load source data".format(srctablename))
        else:
            for row in cursor:
                startdatenum = row['datenum']
                enddatenum = row['datenum']

    sql = '''
              SELECT
                INT4(TO_CHAR((TO_DATE({}, 'YYYYMMDD') - 1), 'YYYYMMDD')) AS prevenddatenum
          '''
    try:
        cursor.execute(sql.format(startdatenum))
    except psycopg2.Error as error:
        log_info("Error during execution of SQL: {} ".format(sql.format(startdatenum)))
        log_error(error.args[0])

    for row in cursor:
        prevenddatenum = row['prevenddatenum']

    sql = '''
             SELECT
                   TO_DATE(COALESCE(max_datenum,19000101),'YYYYMMDD') AS curmaxdate
             FROM 
                tbl_max_datenum 
            WHERE 
                tbl_name = '{}'
          '''
    try:
        cursor.execute(sql.format(scd_table_name))
    except psycopg2.Error as error:
        log_info("Error during execution of SQL: {} ".format(sql.format(scd_table_name)))
        log_error(error.args[0])

    if cursor.rowcount == 0:
        currentmaxdate = datetime.strptime('01-01-1990', '%d-%m-%Y').date()

    for row in cursor:
        currentmaxdate = row['curmaxdate']

    delta = datetime.strptime(str(startdatenum), '%Y%m%d').date() - currentmaxdate
    if delta.days > 1 and (currentmaxdate != datetime.strptime('01-01-1990', '%d-%m-%Y').date()):
        expstartdatenum = currentmaxdate + timedelta(days=1)
        log_error('Source data is missing , The start_datenum should be {}'.format(expstartdatenum))
    # Populate the temp table with row_key_hash, row_data_hash, business columns, start_datenum, end_datenum
    # and ETL hosekeeping related feilds etl_created_job_id, etl_updated_job_id, etl_created_tms, etl_updated_tms

    sql = '''
            DROP TABLE IF EXISTS temp_{}
    '''

    try:
        cursor.execute(sql.format(scd_table_name))
    except psycopg2.Error as error:
        log_info("Error during execution of SQL: {} ".format(sql.format(scd_table_name)))
        log_error(error.args[0])

    sql = '''
              CREATE TEMP TABLE temp_{} AS SELECT * FROM {} WHERE 1=2
        '''

    try:
        cursor.execute(sql.format(scd_table_name, scd_table_name))
    except psycopg2.Error as error:
        log_info("Error during execution of SQL: {} ".format(sql.format(scd_table_name, scd_table_name)))
        log_error(error.args[0])

    sql = '''
            INSERT INTO temp_{}
            SELECT 
                {},
                {},
                {},
                datenum AS start_datenum,
                99991231 AS end_datenum,
                {} AS etl_created_job_id,
                {} AS etl_updated_job_id,
                '{}' AS etl_created_tms,
                '{}' AS etl_updated_tms
            FROM 
              {}
        '''

    try:
        cursor.execute(
            sql.format(scd_table_name, rowkeyhashexpr, rowdatahashexpr, collistexpr, jobid, jobid, loadts, loadts,
                       srctablename))
    except psycopg2.Error as error:
        log_info("Error during execution of SQL: {} ".format(
            sql.format(scd_table_name, rowkeyhashexpr, rowdatahashexpr, collistexpr, jobid, jobid, loadts, loadts,
                       srctablename)))
        log_error(error.args[0])

    logmsg = "{} Rows read from Source table {}".format(cursor.rowcount, srctablename)

    sql = '''
          INSERT INTO job_dtl_log
          (job_id, job_name, log_msg, logged_tms)
          VALUES ({}, '{}', '{}', '{}') 
          '''

    try:
        cursor.execute(sql.format(jobid, jobname, logmsg, loadts))
    except psycopg2.Error as error:
        log_info("Error during execution of SQL: {} ".format(sql.format(jobid, jobname, logmsg, loadts)))
        log_error(error.args[0])

    # use it for History load
    if kwargs.get('startDateNum', None) is not None:
        sql = '''
              DROP TABLE IF EXISTS temp_{}_hash_ranges
        '''

        try:
            cursor.execute(sql.format(scd_table_name))
        except psycopg2.Error as error:
            log_info("Error during execution of SQL: {} ".format(sql.format(scd_table_name)))
            log_error(error.args[0])

        sql = '''
              CREATE TEMP TABLE temp_{}_hash_ranges
              AS
              SELECT 	
                row_data_hash,
                MIN(start_datenum) AS start_datenum,
                MAX(start_datenum) AS end_datenum
              FROM
              (
                SELECT
                  row_data_hash,
                  start_datenum,
                  MAX(rng) OVER(PARTITION BY row_data_hash ORDER BY start_datenum 
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) rng
                FROM
                  (
                    SELECT
                      row_data_hash,
                      start_datenum,
                      CASE 
                        WHEN TO_DATE(
                                    NVL( 
                                        LAG(start_datenum) OVER(PARTITION BY row_data_hash ORDER BY start_datenum), 
                                        start_datenum
                                        ),
                                    'YYYYMMDD'
                                    ) <> ( TO_DATE(start_datenum, 'YYYYMMDD' ) - 1)
                        THEN start_datenum
                      END AS rng
                    FROM 
                      temp_{}
                  )temp1
              )temp2
              GROUP BY 
                row_data_hash,
                rng
        '''

        try:
            cursor.execute(sql.format(scd_table_name, scd_table_name))
        except psycopg2.Error as error:
            log_info("Error during execution of SQL: {} ".format(sql.format(scd_table_name, scd_table_name)))
            log_error(error.args[0])

        # By using ROW_DATA_HASH Ranges get the SCD row for given snapshot
        # data chunk( Converting Many rows to 1 Row for same ROW_DATA_HASH)

        sql = '''
              DROP TABLE IF EXISTS temp_{}_1
        '''

        try:
            cursor.execute(sql.format(scd_table_name))
        except psycopg2.Error as error:
            log_info("Error during execution of SQL: {} ".format(sql.format(scd_table_name)))
            log_error(error.args[0])

        sql = '''
              CREATE TEMP TABLE temp_{}_1
              AS
              SELECT
                row_key_hash,
                hr.row_data_hash,
                {},
                hr.start_datenum,
                CASE WHEN hr.end_datenum = {}
                        THEN 99991231 
                     ELSE hr.end_datenum
                END AS end_datenum,
                etl_created_job_id,
                etl_updated_job_id,
                etl_created_tms,
                etl_updated_tms
              FROM 
                temp_{} t 
              JOIN 
                temp_{}_hash_ranges hr 
              ON 
                t.row_data_hash = hr.row_data_hash 
                AND t.start_datenum = hr.start_datenum
        '''

        try:
            cursor.execute(sql.format(scd_table_name, collistexpr, enddatenum, scd_table_name, scd_table_name))
        except psycopg2.Error as error:
            log_info("Error during execution of SQL: {} ".format(
                sql.format(scd_table_name, collistexpr, enddatenum, scd_table_name, scd_table_name)))
            log_error(error.args[0])

        # Update the end_datenum of SCD table record(ROW_DATA_HASH)
        # if ROW_DATA_HASH not occurred in current chunk first Day

        sql = '''
              UPDATE 
                {} 
              SET
                end_datenum = {},
                etl_updated_job_id = {},
                etl_updated_tms = '{}'
              WHERE end_datenum = 99991231 
                    AND NOT EXISTS
                    (
                      SELECT 
                        1 
                      FROM 
                        temp_{}_1 t
                      WHERE 
                        {}.row_data_hash = t.row_data_hash 
                        AND t.start_datenum = {}
                    )
        '''

        try:
            cursor.execute(
                sql.format(scd_table_name, prevenddatenum, jobid, loadts, scd_table_name, scd_table_name, startdatenum))
        except psycopg2.Error as error:
            log_info("Error during execution of SQL: {} ".format(
                sql.format(scd_table_name, prevenddatenum, jobid, loadts, scd_table_name, scd_table_name,
                           startdatenum)))
            log_error(error.args[0])

        updrowcount = cursor.rowcount

        # Update the end_datenum of SCD table record(ROW_DATA_HASH)
        # if ROW_DATA_HASH expired in middle day of current chunk
        sql = '''
              UPDATE 
                {}
              SET 
                end_datenum =  t.end_datenum,
                etl_updated_job_id = {},
                etl_updated_tms = '{}' 
              FROM 
                temp_{}_1 t
              WHERE 
                {}.row_data_hash = t.row_data_hash 
                AND {}.end_datenum = 99991231 
                AND t.end_datenum <> 99991231 
                AND t.start_datenum = {}
        '''

        try:
            cursor.execute(
                sql.format(scd_table_name, jobid, loadts, scd_table_name, scd_table_name, scd_table_name, startdatenum))
        except psycopg2.Error as error:
            log_info("Error during execution of SQL: {} ".format(
                sql.format(scd_table_name, jobid, loadts, scd_table_name, scd_table_name, scd_table_name,
                           startdatenum)))
            log_error(error.args[0])

        updrowcount = updrowcount + cursor.rowcount

        logmsg = "{} Rows updated in table {}".format(updrowcount, scd_table_name)

        sql = '''
                INSERT INTO 
                job_dtl_log
                    (job_id, job_name, log_msg, logged_tms)
                VALUES 
                    ({}, '{}', '{}', '{}') 
        '''

        try:
            cursor.execute(sql.format(jobid, jobname, logmsg, loadts))
        except psycopg2.Error as error:
            log_info("Error during execution of SQL: {} ".format(sql.format(jobid, jobname, logmsg, loadts)))
            log_error(error.args[0])

        # Determine the MAX of start_datenum, end_datenum of ROW_DATA_HASH
        # in target table for which contains entry in current chunk

        sql = '''
            DROP TABLE IF EXISTS hash_existing_in_target
        '''

        try:
            cursor.execute(sql)
        except psycopg2.Error as error:
            log_info("Error during execution of SQL: {} ".format(sql))
            log_error(error.args[0])

        sql = '''
               CREATE TEMP TABLE 
               hash_existing_in_target
               AS
               SELECT  
                  row_data_hash,
                  MAX(start_datenum) start_datenum,
                  MAX(end_datenum) end_datenum
               FROM 
                  {} tgt
               WHERE EXISTS
               (
                  SELECT 
                    1 
                  FROM 
                    temp_{}_1 t
                  WHERE 
                    t.row_data_hash = tgt.row_data_hash
              )
              GROUP BY 
                  row_data_hash
         '''

        try:
            cursor.execute(sql.format(scd_table_name, scd_table_name))
        except psycopg2.Error as error:
            log_info("Error during execution of SQL: {} ".format(sql.format(scd_table_name, scd_table_name)))
            log_error(error.args[0])

        # Insert the records from current chunk to Target table
        # IF ROW_DATA_HASH exists and it already expired in target

        sql = '''
              INSERT INTO 
                {} 
              SELECT 
                * 
              FROM 
                temp_{}_1 t
              WHERE EXISTS
              (
                SELECT 
                  1 
                FROM 
                  hash_existing_in_target h 
                WHERE 
                  t.row_data_hash = h.row_data_hash 
                  AND t.start_datenum >= h.end_datenum 
                  AND t.end_datenum > h.end_datenum
              )
        '''

        try:
            cursor.execute(sql.format(scd_table_name, scd_table_name))
        except psycopg2.Error as error:
            log_info("Error during execution of SQL: {} ".format(sql.format(scd_table_name, scd_table_name)))
            log_error(error.args[0])

        insrowcount = cursor.rowcount

        # Insert the records from current chunk to Target table
        # IF ROW_DATA_HASH not exists in target

        sql = '''
              INSERT INTO 
                {} 
              SELECT 
                * 
              FROM 
                temp_{}_1 t1
              WHERE NOT EXISTS
              (
                SELECT 
                  1 
                FROM 
                  {} t2 
                WHERE 
                  t1.row_data_hash = t2.row_data_hash 
              )
        '''

        try:
            cursor.execute(sql.format(scd_table_name, scd_table_name, scd_table_name))
        except psycopg2.Error as error:
            log_info(
                "Error during execution of SQL: {} ".format(sql.format(scd_table_name, scd_table_name, scd_table_name)))
            log_error(error.args[0])

        insrowcount = insrowcount + cursor.rowcount

        logmsg = "{} Rows Inserted in table {}".format(insrowcount, scd_table_name)

        sql = '''
                INSERT INTO 
                job_dtl_log
                    (job_id, job_name, log_msg, logged_tms)
                VALUES 
                    ({}, '{}', '{}', '{}') 
                '''
        try:
            cursor.execute(sql.format(jobid, jobname, logmsg, loadts))
        except psycopg2.Error as error:
            log_info("Error during execution of SQL: {} ".format(sql.format(jobid, jobname, logmsg, loadts)))
            log_error(error.args[0])

    else:
        if isstgdataincremental:
            sql = '''
                UPDATE 
                    {}
                SET
                    end_datenum = '{}',
                    etl_updated_job_id = {},
                    etl_updated_tms = '{}'
                WHERE 
                    end_datenum = 99991231 
                    AND EXISTS
                    (
                      SELECT 
                        1 
                      FROM 
                        temp_{} t
                      WHERE 
                        {}.row_key_hash = t.row_key_hash
                        AND {}.row_data_hash <> t.row_data_hash
                    )
            '''

            try:
                cursor.execute(sql.format(scd_table_name, prevenddatenum, jobid, loadts, scd_table_name, scd_table_name,
                                          scd_table_name))
            except psycopg2.Error as error:
                log_info("Error during execution of SQL: {} ".format(
                    sql.format(scd_table_name, prevenddatenum, jobid, loadts, scd_table_name, scd_table_name,
                               scd_table_name)))
                log_error(error.args[0])
        else:
            sql = '''
                UPDATE 
                    {}
                SET
                    end_datenum = {},
                    etl_updated_job_id = {},
                    etl_updated_tms = '{}'
                WHERE 
                    end_datenum = 99991231 
                    AND NOT EXISTS
                    (
                      SELECT 
                        1 
                      FROM 
                        temp_{} t
                      WHERE 
                        {}.row_data_hash = t.row_data_hash
                    )
            '''

            try:
                cursor.execute(
                    sql.format(scd_table_name, prevenddatenum, jobid, loadts, scd_table_name, scd_table_name))
            except psycopg2.Error as error:
                log_info("Error during execution of SQL: {} ".format(
                    sql.format(scd_table_name, prevenddatenum, jobid, loadts, scd_table_name, scd_table_name)))
                log_error(error.args[0])

        logmsg = "{} Rows updated in table {}".format(cursor.rowcount, scd_table_name)

        sql = '''
            INSERT INTO 
            job_dtl_log
                (job_id, job_name, log_msg, logged_tms)
            VALUES 
                ({}, '{}', '{}', '{}') 
        '''

        try:
            cursor.execute(sql.format(jobid, jobname, logmsg, loadts))
        except psycopg2.Error as error:
            log_info("Error during execution of SQL: {} ".format(sql.format(jobid, jobname, logmsg, loadts)))
            log_error(error.args[0])

        # Insert the records from incremental data to target SCD table
        # if no matching ROW_DATA_HASH with active record exists in target table
        sql = '''
              INSERT INTO 
                {}
              SELECT *
              FROM
                temp_{} t1
              WHERE 
              NOT EXISTS
              (
                SELECT 
                    1 
                FROM
                    {} t2
                WHERE 
                    t1.row_data_hash = t2.row_data_hash
                    AND t2.end_datenum = 99991231
              )
        '''

        try:
            cursor.execute(sql.format(scd_table_name, scd_table_name, scd_table_name))
        except psycopg2.Error as error:
            log_info(
                "Error during execution of SQL: {} ".format(sql.format(scd_table_name, scd_table_name, scd_table_name)))
            log_error(error.args[0])

        logmsg = "{} Rows inserted into table {}".format(cursor.rowcount, scd_table_name)

        sql = '''
            INSERT INTO 
            job_dtl_log
                (job_id, job_name, log_msg, logged_tms)
            VALUES 
                ({}, '{}', '{}', '{}') 
        '''

        try:
            cursor.execute(sql.format(jobid, jobname, logmsg, loadts))
        except psycopg2.Error as error:
            log_info("Error during execution of SQL: {} ".format(sql.format(jobid, jobname, logmsg, loadts)))
            log_error(error.args[0])

    # Capture SCD run time stats
    delta = datetime.now() - datetime.strptime(loadts, '%Y-%m-%d %H:%M:%S')

    logmsg = "SCD load completed in {} seconds".format(delta.seconds)

    try:
        cursor.execute(sql.format(jobid, jobname, logmsg, loadts))
    except psycopg2.Error as error:
        log_info("Error during execution of SQL: {} ".format(sql.format(jobid, jobname, logmsg, loadts)))
        log_error(error.args[0])

    # Update max datenum reference for table in tbl_max_datenum

    sql = '''
        SELECT 
            * 
        FROM 
            tbl_max_datenum
        WHERE tbl_name = '{}' 
    '''

    try:
        cursor.execute(sql.format(scd_table_name))
    except psycopg2.Error as error:
        log_info("Error during execution of SQL: {} ".format(sql.format(scd_table_name)))
        log_error(error.args[0])

    if cursor.rowcount == 0:
        sql = '''
        INSERT INTO tbl_max_datenum (max_datenum, tbl_name) VALUES ( {}, '{}' )
        '''
    else:
        sql = '''
        UPDATE 
            tbl_max_datenum 
        SET 
            max_datenum = {} 
        WHERE
            tbl_name = '{}'
        '''

    try:
        cursor.execute(sql.format(enddatenum, scd_table_name))
    except psycopg2.Error as error:
        log_info("Error during execution of SQL: {} ".format(sql.format(enddatenum, scd_table_name)))
        log_error(error.args[0])

    sql = '''
        DROP TABLE IF EXISTS temp_{}
    '''

    try:
        cursor.execute(sql.format(scd_table_name))
    except psycopg2.Error as error:
        log_info("Error during execution of SQL: {} ".format(sql.format(scd_table_name)))
        log_error(error.args[0])

    cnx.commit()

    log_info("SCD load completed for table : {}".format(scd_table_name))
    return 0


# ***************************************************************
# Cleanup Extracted Files on EC2 instance
# ***************************************************************
def cleanup_extracts_from_ec2():
    # Get the dir name
    datadir = os.path.join(cfg['ETL_ENGINE_PATHS']['ETL_DATA_DIR'], currentdate)

    # If directory exist delete the directory and its contents
    if os.path.exists(datadir):
        shutil.rmtree(datadir)


def unload_from_rs_to_s3(rscursor, loaderinfo):
    """
    Unload the data from redshift table/view or SQL to S3
    Args:
        rscursor:
        loaderinfo:

    Returns:

    """
    s3bucket = cfg['S3']['S3_DATALAKE_ENRICHED_BUCKET']
    s3 = boto3.resource('s3')
    s3.meta.client.head_bucket(Bucket=s3bucket)  # TODO : remove this check?

    # S3 location , In case data_file_location contains Filename ignore the file name
    # only use dirname
    if os.path.basename(loaderinfo.get('data_file_location')).find('.') == -1:
        unload_loc = loaderinfo.get('data_file_location')
    else:
        unload_loc = os.path.dirname(loaderinfo.get('data_file_location'))

    # Replace the variables with corresponding values
    for key, value in gvardict.items():
        unload_loc = unload_loc.replace(str(key), str(value))

    if loaderinfo.get('src_table') is None:
        tbl_prefix = loaderinfo.get('loader_code').lower()
    else:
        tbl_prefix = loaderinfo.get('src_table').lower()

    # In case of src_data_set_type is INC use YYYYMMDD_tablename as prefix
    if loaderinfo.get('src_data_set_type').upper() == 'INC':
        unload_loc_with_prefix = unload_loc + '/' + currentdate + '_' + tbl_prefix + '_'
    else:
        unload_loc_with_prefix = unload_loc + '/' + tbl_prefix + '_'

    # Delete the files in S3 if already exists
    objects_to_delete = []

    # Delete all the contents bucket in case data set is FULL
    if loaderinfo.get('src_data_set_type').upper() == 'FULL':
        for obj in s3.Bucket(s3bucket).objects.filter(Prefix=unload_loc):
            objects_to_delete.append({'Key': obj.key})
        try:
            s3.Bucket(s3bucket).delete_objects(Delete={'Objects': objects_to_delete})
            log_info("Folder {} in bucket {} deleted".format(unload_loc, s3bucket))
        except botocore.exceptions.ClientError as e:
            log_info("Folder {} not exists in S3 Bucket {} and proceeding unload".format(unload_loc, s3bucket))


    # Generate the UNLOAD SQL , If src_sql_override is defined in loader catalog use that SQL
    # else use src_table_schema, src_table
    if loaderinfo.get('src_sql_override') is not None:
        sql = loaderinfo.get('src_sql_override')
    else:
        sql = 'SELECT * FROM {}.{}'.format(loaderinfo.get('src_table_schema'), loaderinfo.get('src_table'))

    redshiftcopycredentials = cfg['Redshift']['AWS_IAM_ROLE_ARN']

    if loaderinfo.get('file_delimiter') is None:
        filedelimiter = '|'
    else:
        filedelimiter = loaderinfo.get('file_delimiter')

    if loaderinfo.get('rs_copy_extra_args') != None:
        unloadcmdextraargs = loaderinfo.get('rs_copy_extra_args')
    else:
        unloadcmdextraargs = ''

    unload_sql = '''
                    UNLOAD ('{}')
                    TO 's3://{}/{}'
                    iam_role '{}'
                    DELIMITER AS '{}'
                    {}
                    ALLOWOVERWRITE
                '''
    unload_sql = unload_sql.format(sql, s3bucket, unload_loc_with_prefix, redshiftcopycredentials, filedelimiter,
                                   unloadcmdextraargs)

    # Replace the variables with corresponding values
    #for key, value in gvardict.items():
    #    unload_sql = unload_sql.replace(str(key), str(value))

    log_info("Unload SQL: {}".format(unload_sql))

    # Execute UNLOAD SQL
    try:
        rscursor.execute(unload_sql)
        log_info("Data Unloaded to S3 location {}".format(unload_loc))
    except psycopg2.Error as error:
        log_info("Error during execution of SQL: {} ".format(unload_sql))
        log_error(error.args[0])

    return 0


def run_etl(loadercode, **kwargs):
    """
    Executes requested etl task viz. loader.
    5 operations are currently supported:
    1. e & l from sql server into rs
    2. e & l from s3 to rs
    3. e & l from rs to s3
    4. t in rs
    5. l scd in rs
    Args:
        loadercode: requested etl task
        **kwargs: parameters to override, if any. By default, runs etl tasks for yesterday

    Returns:
        returns 0 - that needs to be changed
    """
    # load configuration along with override cfg
    load_config(kwargs.get('oride_etl_cfg'))

    rscnx = get_rs_connection()  # TODO : may be we push this code in parent where exceptions are handled?
    rscursor = rscnx.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    # get loader metadata, after validating it (should we do it everytime?)
    loaderinfo = get_loader_info(rscursor, loadercode)

    # set_global_variables(rscursor, loaderinfo, kwargs)  # TODO : changed to pass rscursor, **kwargs
    set_global_variables(rscursor, loaderinfo)

    # add/update etl_variables to gvardict if variables passed
    if kwargs.get('etl_var_dict') is not None:
        for key, value in kwargs.get('etl_var_dict').items():
            if value is not None:
                param = '$' + key.replace('$', '')
                gvardict[param] = value

    # Evaluate the sql parameters and update the global var  TODO : FYI - centralized logic for global variables
    if loaderinfo.get('sql_parameters') is not None:
        evaluate_sql_parameters(rscursor, loaderinfo.get('sql_parameters'))

    if loaderinfo.get('pre_sql') is not None:
        run_fe_loader_pre_or_post_sql(rscnx, loaderinfo.get('pre_sql'))

    # Run S3 Unload from redshift
    if loaderinfo.get('src') in SQL_BASED_LOADERS and loaderinfo.get('load_target') == 'S3':
        unload_from_rs_to_s3(rscursor, loaderinfo)
    # Run SQLBased Loader
    elif loaderinfo.get('src') in SQL_BASED_LOADERS:  # TODO : is this RS only? i.e. run transformations?
        run_fe_sql_based_loader(rscnx, loaderinfo)
    # If source SQL database perform Extract, Upload to S3, then Load into redshift
    elif loaderinfo.get('src') in MSSQL_SOURCES:
        extract_from_sqldb(loaderinfo)
        copy_from_ec2_to_s3(loaderinfo)
        if loaderinfo.get('load_target') != 'S3':  # TODO : do we really need this check?
            copy_from_s3_to_redshift(rscnx, loaderinfo)
    # TODO : what is this operation?
    elif loaderinfo.get('src') == 'FILE_DELIM':
        copy_from_s3_to_redshift(rscnx, loaderinfo)
    # load SCD table  TODO : can this be merged with copy_from_s3_to_redshift()
    elif loaderinfo.get('src') == 'SCD':
        load_scd(rscnx, loaderinfo.get('tgt_table'))
    else:  # TODO : will never reach due to upstream validations, remove? and replace with below instead
        log_error("Loader Source {} is not a Valid type".format(loaderinfo.get('src')))

    if loaderinfo.get('post_sql') is not None:
        run_fe_loader_pre_or_post_sql(rscnx, loaderinfo.get('post_sql'))

    rscnx.close()
    

    return 0  # TODO : remove this


def execute(loadercode, **kwargs):
    """
    Processes input params and runs ETL engine
    Args:
        loadercode: etl task to execute
        kwargs: Additional arguments

    Returns:

    """
    if args.etl_var_dict is not None:
        print(args.etl_var_dict[0])
        try:
            etl_var_dict = json.loads(args.etl_var_dict[0])
        except json.decoder.JSONDecodeError:
            print('The etl var info should be in dictionary format')
            sys.exit(-1)
    else:
        etl_var_dict = None

    if args.oride_etl_cfg is not None:
        try:
            oride_etl_cfg = json.loads(args.oride_etl_cfg[0])
        except json.decoder.JSONDecodeError:
            print('The etl config should be in dictionary format')
            sys.exit(-1)

        for cfg_key in oride_etl_cfg.keys():
            if len(cfg_key.split('.')) != 2:
                print("Config Key {} is invalid".format(cfg_key))
                sys.exit(-1)
    else:
        oride_etl_cfg = None

    try:
        run_etl(loadercode, etl_var_dict=etl_var_dict, oride_etl_cfg=oride_etl_cfg)
    except botocore.exceptions.ClientError as e:
        log_error("Error : {} - {}".format(e.response['Error']['Code'],
                                           e.response['Error']['Message']))  # TODO : log as exception
    except psycopg2.Error as error:
        log_error(error.args[0])
    except Exception as exc:
        log_error("Unexpected exception: {}".format(str(exc)))


if __name__ == "__main__":
    # execute command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--loadercode', nargs=1, help='Loader Code', required=True, type=str)

    # Provide the variables that need to run etl_engine in dictionary format
    parser.add_argument('--etl_var_dict', nargs=1, help='In case of to override the etl config ', required=False,
                        type=str)

    # Provide the cfg in dictionary format .  ex {'S3.S3_DATALAKE_ENRICHED_BUCKET':'com-fngn-prod-dataeng'}
    # Where dict key is cfg category , cfg variable concatinated by . dictionary can have multiple keys
    parser.add_argument('--oride_etl_cfg', nargs=1, help='In case of to override the etl config ', required=False,
                        type=str)
    args = parser.parse_args()

    execute(loadercode=args.loadercode, etl_var_dict=args.etl_var_dict, oride_etl_cfg=args.oride_etl_cfg)
