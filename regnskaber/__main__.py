import argparse
from collections import OrderedDict
import configparser
import datetime
import os

from pathlib import Path

#from . import fetch
config_fields = ['host', 'port', 'user', 'pass', 'database']
config_path = Path(__file__).parent / 'config.ini'

def configure_connection():
    print('No configuration file found.')
    print('Please enter the database connection information below')
    config_values = OrderedDict(
        (f, input(f + ': ')) for f in config_fields
    )
    config_values = dict(Global=config_values)
    config = configparser.ConfigParser()
    config.read_dict(config_values)
    with open(config_path, 'w') as fp:
        config.write(fp)
    return

def ensure_config_exists():
    if not config_path.exists():
        configure_connection()

def read_config():
    ensure_config_exists()
    config = configparser.ConfigParser()
    with open(config_path) as fp:
        config.read_file(fp)
        actual_config_fields = config['Global'].keys()
        missing = set(config_fields) - actual_config_fields
        if missing:
            print('The configuration file (%s) is invalid. ' % config_path +
                  'Missing fields %s' % (', '.join(map(repr, missing))))
            raise Exception
        return config

class Commands:

    @staticmethod
    def fetch(from_date, processes, create_tables, **general_options):
        configurations = read_config()
        #fetch.producer_scan(processes, from_date)
        pass

    @staticmethod
    def transform(bar, **general_options):
        configurations = read_config()
        pass

def parse_date(datestr):
    try:
        return datetime.datetime.strptime(datestr, '%Y-%m-%dT%H:%M:%S')
    except ValueError:
        return datetime.datetime.strptime(datestr, '%Y-%m-%d')


parser = argparse.ArgumentParser()

subparsers = parser.add_subparsers(dest='command')

parser_fetch = subparsers.add_parser('fetch', help='fetch from erst')
parser_fetch.add_argument('-f', '--from-date',
                          dest='from_date',
                          help='From date on the form: YYYY-mm-dd[THH:MM:SS].',
                          type=parse_date,
                          required=True)

parser_fetch.add_argument('-p', '--processes',
                          dest='processes',
                          help=('The number of parallel jobs to start.'),
                          type=int,
                          default=1)

# TODO: if tables exist, don't create, otherwise create.
parser_fetch.add_argument('--no-create-tables',
                          dest='create_tables',
                          action='store_const',
                          const=False,
                          default=True)

parser_transform = subparsers.add_parser('transform', help='build useful tables from data fetched from erst')
parser_transform.add_argument('--bar', type=str, default='321')

if __name__ == "__main__":
    args = vars(parser.parse_args())
    getattr(Commands, args.pop('command'))(**args)
