import configparser
import datetime

from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from collections import OrderedDict


config_path = Path(__file__).parent / 'config.ini'

# mysql://user:pass@host/database

_engine = None
_session = None


class DefaultEngineProxy:
    def __getattr__(self, item):
        return getattr(_engine, item)

    def __setattr__(self, name, value):
        return setattr(_engine, name, value)

    def __delattr__(self, name):
        return delattr(_engine, name)

    def __eq__(self, other):
        return _engine == other

    def __hash__(self):
        return _engine.__hash__()

class DefaultSessionProxy:
    def __getattr__(self, item):
        return getattr(_session, item)

    def __setattr__(self, name, value):
        return setattr(_session, name, value)

    def __delattr__(self, name):
        return delattr(_session, name)

    def __eq__(self, other):
        return _session == other

    def __hash__(self):
        return _engine.__hash__()

    def __call__(self, *args, **kwargs):
        return _session(*args, **kwargs)


engine = DefaultEngineProxy()
Session = DefaultSessionProxy()

config_fields = ['host', 'port', 'user', 'pass', 'database', 'sql_type']

def configure_connection():
    print('No configuration file found.')
    print('Please enter the database connection information below.')
    print('sql_type is either mysql or postgresql')
    config_values = OrderedDict(
        (f, input(f + ': ')) for f in config_fields
    )
    config_values = dict(Global=config_values)
    config = configparser.ConfigParser()
    config.read_dict(config_values)
    with open(config_path, 'w') as fp:
        config.write(fp)
    return

def interactive_ensure_config_exists():
    if not config_path.exists():
        configure_connection()

def read_config():
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


def setup_database_connection():
    global _engine, _session

    config = read_config()
    connection_url = "{sql_type}://{user}:{pass}@{host}:{port}/{database}"
    connection_url = connection_url.format(**config['Global'])
    _engine = create_engine(connection_url, encoding='utf8')
    _session = sessionmaker(bind=engine)


def parse_date(datestr):
    try:
        return datetime.datetime.strptime(datestr, '%Y-%m-%dT%H:%M:%S')
    except ValueError:
        return datetime.datetime.strptime(datestr, '%Y-%m-%d')

