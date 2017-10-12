"""This module is responsible for setting up and maintaining connections to the
database via sqlalchemy for the regnskab parsing script, as well as actually
inserting each 'regnskab'.
"""

import pathlib
import configparser
import os
import csv
#from .setup import get_engine, load_tables
from . import engine

_engine = None
_orm = None

def setup_db():
    global _engine, _orm
    if _engine is None:
        _engine = get_engine(None)
    if _orm is None:
        _orm = load_tables(_engine)
    return


def get_connection():
    setup_db()
    return _engine.connect()

def get_orm():
    setup_db()
    return _orm


def initialize_regnskab(regnskab):
    """Creates a row in the regnskaber_files table

    Arguments:
    regnskab -- Regnskab object representing the regnskab to create a row for
    """

    conn = _engine.connect()
    offentlig_dato = str(regnskab.offentliggoerelsesTidspunkt)[:19]
    result = conn.execute(_orm.regnskaber_files.insert(),
                          offentliggoerelsesTidspunkt=offentlig_dato,
                          cvrnummer=regnskab.cvrnummer,
                          regnskabsForm=regnskab.regnskabsForm,
                          erst_id=regnskab._erst_id,
                          indlaesningsTidspunkt=regnskab.indlaesningsTidspunkt)
    inserted_id = result.inserted_primary_key[0]
    conn.close()
    return inserted_id


def cvr_parse(regnskabsId, _input):
    input_washed = _input.strip().replace(' ', '')
    try:
        cvr = int(input_washed)
        return cvr
    except ValueError:
        if regnskabsId == cvr_parse.last_invalid_regnskabsId:
            return -1
        cvr_parse.last_invalid_regnskabsId = regnskabsId
        error_message = "cvr was not a number."
        conn = _engine.connect()
        statement = _orm.regnskaber_errors.insert()
        result = conn.execute(statement,
                              regnskabsId=regnskabsId,
                              reason=error_message,
                              encountered=_input)
        conn.close()
        return -1
cvr_parse.last_invalid_regnskabsId = None


def insert_regnskab(f, xml_unit_map, regnskab):
    regnskabsId = initialize_regnskab(regnskab)
    assert(regnskabsId != 0)

    rows = []
    csv_reader = iter(csv.reader(f))
    next(csv_reader)
    for line in csv_reader:
        line = [s.strip() for s in line]
        unit_id_xbrl = (xml_unit_map[line[3].strip()][0]
                            if line[3].strip() in xml_unit_map.keys() else '')
        unit_name_xbrl = (xml_unit_map[line[3].strip()][1]
                          if line[3].strip() in xml_unit_map.keys() else '')

        rows.append({
            'regnskabsId': regnskabsId,
            'fieldName': line[0],
            'fieldValue': line[1],
            'contextRef': line[2],
            'unitRef': line[3],
            'decimals': line[4],
            'precision': line[5],
            'cvrnummer': cvr_parse(regnskabsId, line[7]),
            'startDate': line[8] or line[9],
            'endDate': line[9],
            'dimensions': ', '.join(line[10:]),
            'unitIdXbrl': unit_id_xbrl,
            'unitNameXbrl': unit_name_xbrl
        })

    connection = _engine.connect()
    connection.execute(_orm.regnskaber.insert(), rows)
    connection.close()
    return


def drive_regnskab(regnskab):
    setup_db()
    filename = regnskab.xbrl_file.name + '.csv'
    with open(filename, encoding='utf-8') as csv_file, open(filename + '_units', encoding='utf-8') as unit_file:
        csv_reader = csv.reader(unit_file)
        unit_map = {row[0]: (row[1], row[2]) for row in csv_reader}
        insert_regnskab(csv_file, unit_map, regnskab)

