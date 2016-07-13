# TODO Make this run faster (than 3000 rows / second)
# Does about 16 000 rows in 0.8 seconds,
# that means almost 10 hours for 100 000 000 rows.

import argparse
import decimal
from contextlib import contextmanager
import collections
import hashlib
import json
import datetime
import sqlite3
import time
import os
import pyodbc

parser = argparse.ArgumentParser(
        description="Audit database changes"
)
parser.add_argument("-l", action="store_true", help='List tables')
parser.add_argument("-a", action="store_true", help='Audit')

script_dir = os.path.join(
    os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__))
    )
)

config_path = os.path.join(
    script_dir, "config.json"
)
cf = open(config_path, "r")
config_file = json.loads(cf.read())
cf.close()

# Default config
config = {
    "target": {
        "type": "sqlite",
        "connection_string": os.path.join(script_dir, "Chinook_Sqlite.sqlite")
    },
    "audit": {
        "connection_string": os.path.join(script_dir, "audit.db")
    }
}

# Config file overrides defaults
if "target" in config_file:
    if "type" in config_file["target"]:
        config["target"]["type"] = config_file["target"]["type"]
    if "connection_string" in config_file["target"]:
        config["target"]["connection_string"] = \
            config_file["target"]["connection_string"]


# TODO Page over tables if more than config["page_size"] number of rows
row_hashes = {}
row_batch = []
first_run = False


def get_timestamp(ts=None, ts_format="%Y-%m-%d %H:%M:%S"):
    if ts is None:
        ts = time.time()
    return datetime.datetime.fromtimestamp(ts).strftime(ts_format)


current_timestamp = get_timestamp()


@contextmanager
def open_sqlite_conn(connection_string, commit=False):
    connection = sqlite3.connect(connection_string)
    cursor = connection.cursor()
    try:
        yield cursor
    except sqlite3.DatabaseError as err:
        cursor.execute("ROLLBACK")
        raise err
    else:
        if commit:
            try:
                print("commit")
                cursor.execute("COMMIT")
            except sqlite3.OperationalError as err:
                error, = err.args
                # Sometimes we specify commit=True but there might not be
                # any transactions that have to be committed, better way?
                if error != "cannot commit - no transaction is active":
                    raise err
    finally:
        connection.close()


@contextmanager
def open_odbc_conn(dsn, commit=False):
    connection = pyodbc.connect(dsn)
    cursor = connection.cursor()
    try:
        yield cursor
    except pyodbc.DatabaseError as err:
        cursor.execute("ROLLBACK")
        raise err
    else:
        if commit:
            cursor.execute("COMMIT")
    finally:
        connection.close()


def row_to_dict(cursor, row):
    # If an OrderedDict is not used the keys are dumped to json
    # in arbitrary order, and that messes up hashing for changes.
    # The order of the keys in the cursor will correspond to the
    # order that was specified in the SQL query.
    row_data = collections.OrderedDict()

    if type(cursor) == sqlite3.Cursor:
        for i, col in enumerate(cursor.description):
            row_data[col[0]] = row[i]
    else:
        if row is not None:
            index = 0
            for key in row.cursor_description:
                value = row[index]
                if type(value) is str:
                    value = value.replace("\u0000", "")

                elif type(value) is decimal.Decimal:
                    value = float(value)

                elif type(value) is datetime.date:
                    value = value.isoformat()

                elif type(value) is datetime.datetime:
                    value = value.isoformat()

                elif isinstance(value, bytearray):
                    value = int.from_bytes(value, byteorder='big')

                if type(key[0]) is str:
                    key = key[0].lower()
                else:
                    key = key[0]

                row_data[key] = value
                index += 1

    return row_data


def get_hash(s):
    hash_value = hashlib.md5()
    hash_value.update(str.encode(s))
    return hash_value.hexdigest()


def get_tables():
    tables = []

    if config["target"]["type"] == "sqlite":
        with open_sqlite_conn(config["target"]["connection_string"]) as cursor:
            table_name_index = 0
            cursor.execute("select name from sqlite_master where type='table'")
            rows = cursor.fetchall()
            for row in rows:
                tables.append(row[table_name_index])

    elif config["target"]["type"] == "odbc":
        with open_odbc_conn(config["target"]["connection_string"]) as cursor:
            cursor.tables()
            row = cursor.fetchone()
            while row is not None:
                row_data = row_to_dict(None, row)
                # Ignore SYSTEM TABLE
                if row_data["table_type"] == "TABLE":
                    tables.append(row_data["table_name"])
                row = cursor.fetchone()

    return tables


def fetch_one(table_name, cursor):
    try:
        row = cursor.fetchone()

    except pyodbc.DataError:
        # TODO Some rows might not be readable,
        # investigate causes of this exception and provide better error msg
        # TODO This should go in the error table
        print("Error reading from table_name {}".format(table_name))
        return None

    return row


def map_table_row(meta, cursor):
    table_names = get_tables()

    for table_name in table_names:
        meta["table_changes"] = 0
        cb_table_start(table_name, meta)
        sql = "select * from {}".format(table_name)
        cursor.execute(sql)
        row = fetch_one(table_name, cursor)

        while row is not None:
            row_data = row_to_dict(cursor, row)
            cb_row(table_name, row_data, meta)
            row = fetch_one(table_name, cursor)

        cb_table_finished(table_name, meta)
        print("===> {} {}".format(table_name, meta["table_changes"]))


def map_table_rows():
    meta = {
        "rows_processed": 0,
        "database_changes": 0,
        "table_changes": 0,
        "errors": {},
        "execution_time": 0
    }

    t1 = time.time()

    if config["target"]["type"] == "sqlite":
        with open_sqlite_conn(config["target"]["connection_string"]) as cursor:
            map_table_row(meta, cursor)

    elif config["target"]["type"] == "odbc":
        with open_odbc_conn(config["target"]["connection_string"]) as cursor:
            map_table_row(meta, cursor)

    t2 = time.time()
    meta["execution_time"] = t2 - t1
    cb_finished(meta)


def init_audit():
    global first_run
    results = []

    # Initialise new audit database
    with open_sqlite_conn(config["audit"]["connection_string"]) as cursor:
        if os.path.isfile(config["audit"]["connection_string"]):
            sql = "select * from sqlite_master where type='table'"
            cursor.execute(sql)
            results = cursor.fetchall()

    if len(results) == 0:
        first_run = True
        with open_sqlite_conn(
                config["audit"]["connection_string"], commit=True) as cursor:

            # TODO Add auto increment id, could be used for synchronisation
            cursor.execute(
                "create table audit (table_name, row_hash, row_data, modified)")
            cursor.execute(
                "create table row_count \
                    (table_name, database_changes, table_changes)")
            cursor.execute(
                "create table error (table_name, count)")

    else:
        with open_sqlite_conn(config["audit"]["connection_string"]) as cursor:
            sql = "select count(*) as audit_count from audit"
            cursor.execute(sql)
            row = cursor.fetchone()
            row_data = row_to_dict(cursor, row)
            if "audit_count" in row_data:
                if row_data["audit_count"] == 0:
                    first_run = True

    # TODO Clear temp tables
    # delete * from row_count
    # delete * from error


def cb_table_start(table_name, meta):
    # Create dict keyed on row hashes
    with open_sqlite_conn(config["audit"]["connection_string"]) as cursor:
        row_hash_index = 0
        # TODO Ability to specify primary key per table
        cursor.execute("""
            select row_hash from audit where table_name = ?
        """, (table_name,)
        )
        row = cursor.fetchone()
        while row is not None:
            row_hash = row[row_hash_index]
            row_hashes[row_hash] = True
            row = cursor.fetchone()


def cb_table_finished(table_name, meta):
    global row_batch

    with open_sqlite_conn(
            config["audit"]["connection_string"], commit=True) as cursor:

        for row_item in row_batch:
            row_dump = None
            if not first_run:
                row_dump = row_item["row_dump"]
            cursor.execute(
                "insert into audit values (?, ?, ?, ?)", ((
                    table_name,
                    row_item["row_hash"],
                    row_dump,
                    current_timestamp
                ))
            )
            # TODO Commit for every x rows?

    # Reset row batch
    row_batch = []


def cb_row(table_name, row_data, meta):
    meta["rows_processed"] += 1
    row_dump = json.dumps(row_data)
    row_hash = get_hash(row_dump)
    # row_hashes contains all existing hashes for this table,
    # if new hash in row_hashes then remove it.
    if row_hash in row_hashes:
        row_hashes.pop(row_hash, None)
    else:
        meta["database_changes"] += 1
        meta["table_changes"] += 1
        row_batch.append({
            "row_hash": row_hash,
            "row_dump": row_dump,
        })


def cb_finished(meta):
    # TODO This data should go in the row_count and error tables
    print("Database changes ---> {}".format(meta["database_changes"]))
    print("Rows processed ---> {}".format(meta["rows_processed"]))
    print("Errors ---> {}".format(meta["errors"]))
    print("Execution time ---> {}".format(meta["execution_time"]))


if __name__ == '__main__':
    args = parser.parse_args()

    # List tables
    if args.l:
        print(get_tables())

    # Audit
    elif args.a:
        init_audit()
        map_table_rows()

    else:
        parser.print_usage()

