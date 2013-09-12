'''
Created on Aug 7, 2013

@author: Scott.Embler
'''
import ftplib
import re

class _MLSD:
    '''A utility for retrieving and inspecting the contents of a directory on an FTP server.'''

    def __init__(self, ftp, path):
        self.directory = path
        self.listing = {}
        ftp.retrlines('MLSD ' + self.directory, self._append)

    def _append(self, line):
        props, name = str.split(line, ' ', 1)
        self.listing[name] = self._parse(props)

    def _parse(self, props):
        pairs = [str.split(pair, '=', 1) for pair in str.split(props, ';') if pair != '']
        return {key: value for (key, value) in pairs}

    def __str__(self):
        return str(self.listing)

    def match(self, regex):
        return {name: props for (name, props) in self.listing.items() if re.match(regex, name)}

    def dirs(self, regex='.+'):
        return {name: props for (name, props) in self.match(regex).items() if 'dir' in props['type']}

    def dir_paths(self, regex='.+'):
        return {self.directory + '/' + name: props for (name, props) in self.dirs(regex).items()}

    def files(self, regex='.+'):
        return {name: props for (name, props) in self.match(regex).items() if props['type'] == 'file'}

    def file_paths(self, regex='.+'):
        return {self.directory + '/' + name: props for (name, props) in self.files(regex).items()}


def _retrieve_lines(ftp_connection, path):
    """Returns the lines of a file that can be found using the given ftp_connection and path."""
    lines = []
    ftp_connection.retrlines('RETR ' + path , lines.append)
    return lines


def _scan_years(ftp_connection, directory, regex):
    """Scans each directory named by a year, and yields each file path that matches the regex."""
    for year_dir in sorted(_MLSD(ftp_connection, directory).dir_paths('\d{4}')):
        for file_data in _scan_files(ftp_connection, year_dir, regex):
            yield file_data


def _scan_files(ftp_connection, directory, regex):
    """Scans the directory and yields each file path that matches the regex"""
    for file_data in sorted(_MLSD(ftp_connection, directory).file_paths(regex).items(), key=lambda x: x[0]):
        yield file_data

def ncdc_ftp():
    return ftplib.FTP('ftp.ncdc.noaa.gov', user='anonymous', passwd='', timeout=100)


def discover(product_files):
    with ncdc_ftp() as ftp_connection:
        for product_file in product_files(ftp_connection):
            yield product_file


def pull(product_files):
    """Yield (file_path, file_props, lines) for ftp file paths returned by the given function.

    Connects to NCDC's ftp site and yields all lines from each of the file paths that
    are returned by the product_files function.  In order to identify where each lines
    comes from, a tuple is returned that contains the file path/properties, and the
    lines of text. When complete, or interrupted by an exception, the ftp connection
    will be closed.

    """
    with ncdc_ftp() as ftp_connection:
        for path, props in product_files:
            yield (path, props, _retrieve_lines(ftp_connection, path))


def stream(product_files):
    """Yield (file_path, line) for ftp file paths returned by the given function.

    Connects to NCDC's ftp site and yields each line from each of the file paths that
    are returned by the product_files function.  In order to identify where each line
    comes from, a tuple is returned that contains the file path, and the line of text.
    When complete, or interrupted by an exception, the ftp connection will be closed.

    """
    with ncdc_ftp() as ftp_connection:
        for path, props in product_files(ftp_connection):
            for line in _retrieve_lines(ftp_connection, path):
                yield (path, line)

def station_metadata(ftp_connection):
    '''Yields (file_path, file_props) for the station meta data file.'''
    return _scan_files(ftp_connection, '/pub/data/uscrn/products/', 'stations.tsv')


def hourly02_files(ftp_connection):
    '''Yields (file_path, file_props) for all Hourly02 v03 product files.'''
    return _scan_years(ftp_connection, '/pub/data/uscrn/products/hourly02', 'CRNH0203-.*')


def daily01_files(ftp_connection):
    '''Yields (file_path, file_props) for all Daily01 v03 product files.'''
    return _scan_years(ftp_connection, '/pub/data/uscrn/products/daily01', 'CRND0103-.*')


def monthly01_files(ftp_connection):
    '''Yields (file_path, file_props) for all Monthly01 v02 product files.'''
    return _scan_files(ftp_connection, '/pub/data/uscrn/products/monthly01', 'CRNM0102-.*')


def subhourly01_files(ftp_connection):
    '''Yields (file_path, file_props) for all Subhourly01 v01 product files.'''
    return _scan_years(ftp_connection, '/pub/data/uscrn/products/subhourly01', 'CRNS0101-.*')