import argparse
from collections import OrderedDict
import datetime
import os

from pathlib import Path

from . import read_config, interactive_ensure_config_exists

class Commands:
    @staticmethod
    def fetch(from_date, processes, create_tables, **general_options):
        interactive_ensure_config_exists()
        # setup engine and Session.
        #fetch.producer_scan(processes, from_date)
        pass

    @staticmethod
    def transform(bar, **general_options):
        interactive_ensure_config_exists()
        # setup engine and Session.
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
