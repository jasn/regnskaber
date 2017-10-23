import argparse
import datetime

from . import (interactive_ensure_config_exists, setup_database_connection,
               parse_date)

from . import fetch
from . import make_feature_table as transform


class Commands:
    @staticmethod
    def fetch(from_date, processes, **general_options):
        interactive_ensure_config_exists()
        # setup engine and Session.
        setup_database_connection()
        fetch.fetch_to_db(processes, from_date)

    @staticmethod
    def transform(table_definition_file, **general_options):
        interactive_ensure_config_exists()
        # setup engine and Session.
        setup_database_connection()
        transform.main(table_definition_file)


parser = argparse.ArgumentParser()

subparsers = parser.add_subparsers(dest='command')
subparsers.required = True

parser_fetch = subparsers.add_parser('fetch', help='fetch from erst')
parser_fetch.add_argument('-f', '--from-date',
                          dest='from_date',
                          help='From date on the form: YYYY-mm-dd[THH:MM:SS].',
                          type=parse_date,
                          default=datetime.datetime(2011, 1, 1),
                          required=False)

parser_fetch.add_argument('-p', '--processes',
                          dest='processes',
                          help=('The number of parallel jobs to start.'),
                          type=int,
                          default=1)

# TODO: if tables exist, don't create, otherwise create.

parser_transform = subparsers.add_parser('transform',
                                         help=('build useful tables from data '
                                               'fetched from erst'))
parser_transform.add_argument('table_definition_file', type=str,
                              help=('A file that specifies the table to be '
                                    'created.'))

if __name__ == "__main__":
    args = vars(parser.parse_args())
    getattr(Commands, args.pop('command'))(**args)
