import configparser

from pathlib import Path

from sqlalchemy import create_engine, sessionmaker

config_path = Path(__file__).parent / 'config.ini'

# mysql://user:pass@host/database

_engine = None
_Session = None


class DefaultConnectionProxy:
    def __getattr__(self, item):
        return getattr(_engine, item)

    def __setattr__(self, name, value):
        return setattr(_engine, name, value)

    def __delattr__(self, name):
        return delattr(_engine, name)

    def __eq__(self, other):
        return _engine == other


engine = DefaultConnectionProxy()

config_fields = ['host', 'port', 'user', 'pass', 'database']

def configure_connection():
    print('No configuration file found.')
    print('Please enter the database connection information below.')
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
