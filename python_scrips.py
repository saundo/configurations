import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

import datetime
import pickle
import pandas as pd
import argparse


def strffer(date, form='%Y-%m-%d'):
    return datetime.datetime.strftime(date, form)


def strpper(date, form='%Y-%m-%d'):
    return datetime.datetime.strptime(date, form)


def grab_creds(creds_path):
    with open(creds_path, 'rb') as f:
        return pickle.load(f)


class Instance:
    def __init__(self, instance_id, region_name="us-east-2", creds=None):
        self.instance_id = instance_id
        self.region_name = region_name

        if creds is None:
            self.resource = boto3.resource('ec2', region_name=self.region_name)

        else:
            self.resource = boto3.resource(
                'ec2', region_name=self.region_name,
                aws_access_key_id=creds['aws_access_key_id'],
                aws_secret_access_key=creds['aws_secret_access_key'])

        self.instance = self.resource.Instance(self.instance_id)

    @property
    def public_dns_name(self):
        return self.instance.public_dns_name

    def wake_up_instance(self):
        return self.instance.start()

    def shut_down_instance(self):
        return self.instance.stop()

import os
import pyarrow as pa
import pickle
import json
import psycopg2
import yaml
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.errors import DatatypeMismatch
# from common import *

Redshift_to_Pyarrow = {
    'INTEGER': pa.int32(),
    'BIGINT': pa.int64(),
    'REAL': pa.float32(),
    'DOUBLE': pa.float64(),
    'VARCHAR': pa.string(),
    'BOOLEAN': pa.bool_(),
    'TIMESTAMP': pa.timestamp('ms'),
}

Redshift_to_Pyarrow_lower = (
    {key.lower(): value for key, value in Redshift_to_Pyarrow.items()}
)


def local_creds_dir():
    if 'ubuntu' in os.getcwd():
        creds_dir = '/home/ubuntu/.credentials/'

    else:
        with open('/users/creds/creds.yaml') as f:
            creds = yaml.safe_load(f)

        creds_dir = creds['creds']

    return creds_dir


def load_conn(db='analytics',
              local_folder='/users/csaunders/Desktop/credentials/'):
    """
    Function for loading connection to the redshift database. Some annoying
    inconsistencies as newspicks credential is stored as a json on EC2 but as
    a pickle on local systems

    Returns: conn

    Parameters
    ----------

    db : {'analytics', 'newspicks'}, default 'analytics'
        The Redshift cluster to use

    local_folder : string
        Path to local folder where the Redshift credentials are stored;
        Default is Saundo's computer

    """

    try:
        os.listdir(local_folder)
    except FileNotFoundError:
        local_folder = local_creds_dir()

    assert db in ('analytics', 'newspicks'), 'unknown db'

    np_pkl = 'app_redshift_credentials.pkl'
    qz_pkl = 'analytics_redshift_credentials.pkl'

    if db == 'analytics':
        local_credential_path = os.path.join(local_folder, qz_pkl)
        EC2_credential_path = '/home/ubuntu/.credentials/analytics_redshift_credentials.pkl'
    else:
        local_credential_path = os.path.join(local_folder, np_pkl)
        EC2_credential_path = '/home/ubuntu/.credentials/redshift_credentials.json'

    if 'ubuntu' in os.getcwd():
        if db == 'analytics':
            with open(EC2_credential_path, 'rb') as cfg:
                config = pickle.load(cfg)

            host = config['host']
            user = config['user']
            password = config['password']
            dbname = config['dbname']

        else:
            with open(EC2_credential_path, 'r') as cfg:
                config = json.load(cfg)

            host = config['redshift_newspicks']['host']
            user = config['redshift_newspicks']['user']
            password = config['redshift_newspicks']['password']
            dbname = config['redshift_newspicks']['dbname']
    else:
        with open(local_credential_path, 'rb') as f:
            app_redshift_credentials = pickle.load(f)

        host = app_redshift_credentials['host']
        user = app_redshift_credentials['user']
        password = app_redshift_credentials['password']
        dbname = app_redshift_credentials['dbname']

    conn = psycopg2.connect(host=host,
                            port=5439,
                            user=user,
                            password=password,
                            dbname=dbname)
    return conn


def redshift_query_executor(conn, query, set_autocommit=False):
    """
    Execute SQL queries through redshift; used primarily for table creation,
    deletion, modififcation, etc

    If looking for SQL result as DataFrame use pd.read_sql_query

    Parameters
    ----------

    conn : psycopg2.connect method

    query : string
        SQL query f

    set_autocommit : boolean, default False
        if creating or altering External table, set this to True (buy why?)

    """

    if set_autocommit:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    cur.execute(query)
    cur.close()
    conn.commit()

    if conn is not None:
        conn.close()


def table_creation_query_builder(tablename, col_dtype_schema, folder,
                                 bucket='qz-analytics',
                                 dbschema='campaign_reporting'):
    """
    Template for a SQL string for creating a Table

    Returns: query for creating an external table in Redshift

    Parameters
    ----------

    tablename : string
        Name of the table to be created in Redshift

    col_dtype_schema : dict
        Dictionary where keys are the column names and values are the Redshift
        data types

        col_dtype_schema = {
            'state': 'VARCHAR(255)',
            'population': 'INTEGER',
            'democratic': 'BOOL',
            'date': 'TIMESTAMP'
        }

    folder : sting
        S3 folder where the data files are stored -> 'state-data/population'

    bucket : string, default 'qz-analytics'
        S3 bucket where the folder resides

    dbschema : string, default 'campaign_reporting'
        The Redshift external schema where the table will be created

    """

    top = "create external table {0}.{1}( \n".format(dbschema, tablename)

    definition = ''
    for key in col_dtype_schema.keys():
        definition += '"{0}" {1}, \n'.format(key, col_dtype_schema[key])
    definition = definition[:definition.rfind(',')]

    bottom = (
        """
    )
    row format delimited
    fields terminated by '\001'
    stored as parquet
    location 's3://{0}/{1}'
        """
    ).format(bucket, folder)

    query = top + definition + bottom

    return query


def unload_sql(sql, destination, iam_role, maxfilesize, db='analytics'):
    """
    Unnest redshift query into a S3 parquet file

    Returns:

    Parameters
    ----------

    sql : string
        sql query to unnest
        IMPORTANT: make sure individual quotes are duplciated per unnest
        requirements: sql.replace("'", "''")

    destination : string
        s3 destination for the unnest operation
        destination = 's3://qz-analytics/unload_testing/email/JAN_'

    iam_role : string
        iam role enabling the unnest operation
        iam_role = 'arn:aws:iam::612251176106:role/ad_impressions'

    maxfilesize : int
        the maximum size of parquet files in s3
        maxfilesize = 256

    db : string, default 'analytics'
        the redshift db to use

    """

    query = (
        f"""
        UNLOAD ('{sql}'
        )
        TO '{destination}'
        IAM_ROLE '{iam_role}'
        PARQUET
        maxfilesize {maxfilesize} mb
        """
    )

    redshift_query_executor(load_conn(db=db), query)


def alter_table(table, date, location, partition='date'):
    """
    date = 20190410
    location: 's3://qz-analytics/bigquery_ga/pageview/date=20190410'
    """

    alter_table_sql = (
        f"""
        ALTER TABLE campaign_reporting.{table} ADD
        PARTITION({partition}={date})
        LOCATION '{location}'
        """
    )

    print(alter_table_sql)

    conn = load_conn()
    try:
        redshift_query_executor(conn, alter_table_sql, set_autocommit=True)
    except DatatypeMismatch as e:
        print(e)


class Argbash:
    """
    argparse helper class for use in Airflow

    the only options it handles are 'execute' and 'backfill'
    the execute_function can only handle a single date argument that is in
    string format of '2019-06-01'
    """

    def __init__(self, execute_function):
        self.execute_function = execute_function

    def backfill(self):
        dates = pd.date_range(self.start, self.end)
        for date in dates:
            date = strffer(date)
            self.execute_function(date)

    def run(self):
        ap = argparse.ArgumentParser()

        ap.add_argument("-in", "--instructions", required=True,
                        help="execute instructions: 'execute', 'backfill'")

        ap.add_argument("-dt", "--date", required=False,
                        help=("which date to run, '2019-04-01', if "
                              "empty the instructions execute the current date"))

        ap.add_argument("-bf", "--backfill", required=False,
                        help=("which dates to backfill, used only with backfill, "
                              "'2019-04-01_2019-04-30'"))

        args = vars(ap.parse_args())

        assert args['instructions'] in ('execute', 'backfill'), (
            'instructions should be either execyte or backill')

        if args['instructions'] == 'backfill':
            if args['backfill'] is None:
                raise Exception("you need to use -bf shit head")
            dates = args['backfill']
            self.start = dates.split('_')[0]
            self.end = dates.split('_')[1]
            self.backfill()

        elif args['instructions'] == 'execute':
            if args['date'] is not None:
                date = args['date']
            else:
                date = strffer(datetime.datetime.today() - datetime.timedelta(1))

            self.execute_function(date)


# good video https://www.youtube.com/watch?v=7I2s81TsCnc&t=304s
# credentials need to be  SERVICE ACCOUNT ->
# need to share the google sheet with the email listed in this json


SCOPE = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']


def load_credentials(service_account_cred_path):
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        service_account_cred_path, SCOPE)

    return creds


def read_gsheet_to_df(service_account_cred_path, google_sheet, sheet_name):

    credentials = load_credentials(service_account_cred_path)

    gc = gspread.authorize(credentials)
    wks = gc.open(google_sheet)

    return pd.DataFrame(wks.worksheet(sheet_name).get_all_records())


def update_gsheet_with_df(service_account_cred_path, df, google_sheet,
                          sheet_name, clear=True):

    credentials = load_credentials(service_account_cred_path)

    gc = gspread.authorize(credentials)
    wks = gc.open(google_sheet).worksheet(sheet_name)

    if clear:
        wks.clear()

    wks.update([df.columns.values.tolist()] + df.values.tolist())


def print_sheet_names(service_account_cred_path, google_sheet):

    credentials = load_credentials(service_account_cred_path)
    gc = gspread.authorize(credentials)
    wks = gc.open(google_sheet)

    print(wks.worksheets())

# TODO -> writing to a google sheet
