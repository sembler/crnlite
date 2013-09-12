'''
Created on Aug 8, 2013

@author: Scott.Embler
'''
from uscrn.ftp import discover, pull, station_metadata, hourly02_files, daily01_files, \
    monthly01_files, subhourly01_files
import contextlib
import sqlite3

def _import_ddl(cursor):
    cursor.execute("DROP TABLE IF EXISTS imports")
    cursor.execute('''CREATE TABLE imports(
                        path text,
                        modified text
                   )''')


def _station_ddl(cursor):
    cursor.execute("DROP TABLE IF EXISTS stations")
    cursor.execute('''CREATE TABLE stations(
                        wban int,
                        country text,
                        state text,
                        location text,
                        vector text,
                        name text,
                        latitude real,
                        longitude real,
                        elevation real,
                        status text,
                        commissioning text,
                        closing text,
                        operation text,
                        pairing text,
                        network text
                   )''')
    cursor.execute("DROP INDEX IF EXISTS station_key_index")
    cursor.execute("CREATE UNIQUE INDEX station_key_index ON stations (wban)")

def _hourly02_ddl(cursor):
    cursor.execute("DROP TABLE IF EXISTS hourly02")
    cursor.execute('''CREATE TABLE hourly02(
                        wban int,
                        utc_hour int,
                        local_hour int,
                        crx_version text,
                        temp_calc real,
                        temp_avg real,
                        temp_max real,
                        temp_min real,
                        precip_sum real,
                        solar_avg real,
                        solar_avg_flag int,
                        solar_max real,
                        solar_max_flag int,
                        solar_min real,
                        solar_min_flag int,
                        surface_type text,
                        surface_avg real,
                        surface_avg_flag int,
                        surface_max real,
                        surface_max_flag int,
                        surface_min real,
                        surface_min_flag int,
                        rh_avg real,
                        rh_avg_flag int,
                        soil_moisture_5cm real,
                        soil_moisture_10cm real,
                        soil_moisture_20cm real,
                        soil_moisture_50cm real,
                        soil_moisture_100cm real,
                        soil_temp_5cm real,
                        soil_temp_10cm real,
                        soil_temp_20cm real,
                        soil_temp_50cm real,
                        soil_temp_100cm real
                    )''')
    cursor.execute("DROP INDEX IF EXISTS hourly02_key_index")
    cursor.execute("CREATE UNIQUE INDEX hourly02_key_index ON hourly02 (wban, utc_hour)")


def _daily01_ddl(cursor):
    cursor.execute("DROP TABLE IF EXISTS daily01")
    cursor.execute('''CREATE TABLE daily01(
                        wban int,
                        local_day int,
                        crx_version text,
                        temp_max real,
                        temp_min real,
                        temp_mean real,
                        temp_avg real,
                        precip_sum real,
                        solar_sum real,
                        surface_type text,
                        surface_max real,
                        surface_min real,
                        surface_avg real,
                        rh_max real,
                        rh_min real,
                        rh_avg real,
                        soil_moisture_5cm real,
                        soil_moisture_10cm real,
                        soil_moisture_20cm real,
                        soil_moisture_50cm real,
                        soil_moisture_100cm real,
                        soil_temp_5cm real,
                        soil_temp_10cm real,
                        soil_temp_20cm real,
                        soil_temp_50cm real,
                        soil_temp_100cm real
                    )''')
    cursor.execute("DROP INDEX IF EXISTS daily01_key_index")
    cursor.execute("CREATE UNIQUE INDEX daily01_key_index ON daily01 (wban, local_day)")


def _monthly01_ddl(cursor):
    cursor.execute("DROP TABLE IF EXISTS monthly01")
    cursor.execute('''CREATE TABLE monthly01(
                        wban int,
                        local_month int,
                        crx_version text,
                        temp_max real,
                        temp_min real,
                        temp_mean real,
                        temp_avg real,
                        precip_sum real,
                        solar_avg real,
                        surface_type text,
                        surface_max real,
                        surface_min real,
                        surface_avg real
                    )''')
    cursor.execute("DROP INDEX IF EXISTS monthly01_key_index")
    cursor.execute("CREATE UNIQUE INDEX monthly01_key_index ON monthly01 (wban, local_month)")


def _subhourly01_ddl(cursor):
    cursor.execute("DROP TABLE IF EXISTS subhourly01")
    cursor.execute('''CREATE TABLE subhourly01(
                        wban int,
                        utc_time int,
                        local_time int,
                        crx_version text,
                        temp_avg real,
                        precip_sum real,
                        solar_avg real,
                        solar_flag int,
                        surface_avg real,
                        surface_type text,
                        surface_flag int,
                        rh_avg real,
                        rh_flag int,
                        soil_moisture_5cm real,
                        soil_temp_5cm real,
                        wet real,
                        wet_flag int,
                        wind real,
                        wind_flag int
                    )''')
    cursor.execute("DROP INDEX IF EXISTS subhourly01_key_index")
    cursor.execute("CREATE UNIQUE INDEX subhourly01_key_index ON subhourly01 (wban, utc_time)")


def _record_import(cursor, path, modified):
    cursor.execute('DELETE FROM imports where path = ?', [path])
    cursor.execute('INSERT INTO imports values (?,?)', [path, modified])


def _merge_station_metadata(cursor, lines):
    for line in lines:
        record = line.strip().split('\t') # Tab-separated fields.
        if record[0].isdigit(): # This ignores the header and any unofficial stations.
            cursor.execute('DELETE FROM stations WHERE wban = ?', record[0:1])
            cursor.execute('INSERT INTO stations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', record)


def _merge_hourly02(cursor, lines):
    for line in lines:
        record = line.strip().split()
        # Alter the record fields so that they are more easily indexed and queried.
        record[1] = record[1] + record[2][0:2]  # Condense the UTC date and time into one field.
        record[3] = record[3] + record[4][0:2]  # Condense the local date and time into one field.
        # Remove the fields which are now redundant, plus lat. and lon.
        del record[7]
        del record[6]
        del record[4]
        del record[2]
        # Convert missing values into None so that they will not be treated as numbers in the database.
        record = [item if item not in ('-9999.0', '-9999.00', '-99.000') else None for item in record]

        # Replace any existing records, or insert a new record.
        cursor.execute('DELETE FROM hourly02 WHERE wban = ? and utc_hour = ?', record[0:2])
        cursor.execute('INSERT INTO hourly02 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', record)


def _merge_daily01(cursor, lines):
    for line in lines:
        record = line.strip().split()
        # Alter the record fields so that they are more easily indexed and queried.
        del record[4] # Remove lat.
        del record[3] # Remove lon.
        # Convert missing values into None so that they will not be treated as numbers in the database.
        record = [item if item not in ('-99999','-9999.0', '-9999.00', '-99.000') else None for item in record]
        # Replace any existing records, or insert a new record.
        cursor.execute('DELETE FROM daily01 WHERE wban = ? and local_day = ?', record[0:2])
        cursor.execute('INSERT INTO daily01 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', record)


def _merge_monthly01(cursor, lines):
    for line in lines:
        record = line.strip().split()
        # Alter the record fields so that they are more easily indexed and queried.
        del record[4] # Remove lat.
        del record[3] # Remove lon.
        # Convert missing values into None so that they will not be treated as numbers in the database.
        record = [item if item not in ('-99999','-9999.0') else None for item in record]
        # Replace any existing records, or insert a new record.
        cursor.execute('DELETE FROM monthly01 WHERE wban = ? and local_month = ?', record[0:2])
        cursor.execute('INSERT INTO monthly01 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)', record)


def _merge_subhourly01(cursor, lines):
    for line in lines:
        record = line.strip().split()
        # Alter the record fields so that they are more easily indexed and queried.
        record[1] = record[1] + record[2]  # Condense the UTC date and time into one field.
        record[3] = record[3] + record[4]  # Condense the local date and time into one field.
        # Remove the fields which are now redundant, plus lat. and lon.
        del record[7]
        del record[6]
        del record[4]
        del record[2]
        # Convert missing values into None so that they will not be treated as numbers in the database.
        record = [item if item not in ('-9999', '-99999', '-9999.0', '-9999.00', '-99.000') else None for item in record]

        # Replace any existing records, or insert a new record.
        cursor.execute('DELETE FROM subhourly01 WHERE wban = ? and utc_time = ?', record[0:2])
        cursor.execute('INSERT INTO subhourly01 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', record)

def _product_revisions(cursor, product_files):
    def is_modified(product_file):
        path, props = product_file
        modified = cursor.execute('SELECT modified FROM imports WHERE path = ?', [path]).fetchone()
        return modified == None or modified[0] != props['modify']

    return [product_file for product_file in product_files if is_modified(product_file)]


def _synchronize(db_connection, product_files, merge_function):
    with contextlib.closing(db_connection.cursor()) as cursor:
        discovered_files = discover(product_files)
        revisions = _product_revisions(cursor, discovered_files)
        for path, props, lines in pull(revisions):
            merge_function(cursor, lines)
            _record_import(cursor, path, props['modify'])
            db_connection.commit()


def define_schema(db_file):
    with sqlite3.connect(db_file) as db_connection:
        with contextlib.closing(db_connection.cursor()) as cursor:
            for ddl in (_import_ddl, _station_ddl, _hourly02_ddl, _daily01_ddl, _monthly01_ddl, _subhourly01_ddl):
                ddl(cursor)
                db_connection.commit()


def sync_station_metadata(db_file):
    with sqlite3.connect(db_file) as db_connection:
        _synchronize(db_connection, station_metadata, _merge_station_metadata)


def sync_hourly02(db_file):
    with sqlite3.connect(db_file) as db_connection:
        _synchronize(db_connection, hourly02_files, _merge_hourly02)


def sync_daily01(db_file):
    with sqlite3.connect(db_file) as db_connection:
        _synchronize(db_connection, daily01_files, _merge_daily01)


def sync_monthly01(db_file):
    with sqlite3.connect(db_file) as db_connection:
        _synchronize(db_connection, monthly01_files, _merge_monthly01)


def sync_subhourly01(db_file):
    with sqlite3.connect(db_file) as db_connection:
        _synchronize(db_connection, subhourly01_files, _merge_subhourly01)