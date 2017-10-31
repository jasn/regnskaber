""" This module is responsible for inserting each 'regnskab'. """


import csv
import datetime

from . import Session
from .models import FinancialStatement, FinancialStatementEntry


def initialize_financial_statement(regnskab):
    financial_statement = FinancialStatement(
        offentliggoerelsesTidspunkt=regnskab.offentliggoerelsesTidspunkt,
        indlaesningsTidspunkt=regnskab.indlaesningsTidspunkt,
        cvrnummer=regnskab.cvrnummer,
        regnskabsForm=regnskab.regnskabsForm,
        erst_id=regnskab.erst_id
    )
    return financial_statement


def insert_regnskab(f, xml_unit_map, regnskab):
    session = Session()
    try:
        regnskaber_file = initialize_financial_statement(regnskab)
        session.add(regnskaber_file)
        csv_dict_reader = csv.DictReader(f)
        for row in csv_dict_reader:
            for key in row:
                if key and row[key]:
                    row[key] = row[key].strip()
                if key and not row[key]:
                    row[key] = ''

            # keys in row:
            # Name,Value,contextRef,unitRef,Dec,Prec,Lang,EntityIdentifier,Start,End/Instant,Dimensions
            row['Dimensions'] = ', '.join([row.pop('Dimensions', '')] +
                                          row.pop(None, []))
            assert row.keys() == set("Name,Value,contextRef,unitRef,Dec,Prec,"
                                     "Lang,EntityIdentifier,Start,End/Instant,"
                                     "Dimensions".split(','))

            unit_id_xbrl = (xml_unit_map[row['unitRef']]['id']
                            if row['unitRef'] in xml_unit_map.keys() else '')
            unit_name_xbrl = (xml_unit_map[row['unitRef']]['name']
                              if row['unitRef'] in xml_unit_map.keys() else '')

            try:
                row['EntityIdentifier'] = int(row['EntityIdentifier'])
            except ValueError:
                row['EntityIdentifier'] = int(regnskaber_file.cvrnummer)

            row['Start'] = datetime.datetime.strptime((row['Start'] or
                                                       row['End/Instant']),
                                                      '%Y-%m-%d')
            row['End/Instant'] = datetime.datetime.strptime(row['End/Instant'],
                                                            '%Y-%m-%d')

            regnskaber_file.financial_statement_entries.append(
                FinancialStatementEntry(
                    fieldName=row['Name'], fieldValue=row['Value'],
                    contextRef=row['contextRef'], unitRef=row['unitRef'],
                    decimals=row['Dec'], precision=row['Prec'],
                    cvrnummer=row['EntityIdentifier'],
                    startDate=row['Start'], endDate=row['End/Instant'],
                    dimensions=row['Dimensions'],
                    unitIdXbrl=unit_id_xbrl, unitNameXbrl=unit_name_xbrl
                )
            )

        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
    return


def drive_regnskab(regnskab):
    filename = regnskab.xbrl_file.name + '.csv'
    arelle_csv_encoding = 'utf-8-sig'
    with open(filename, encoding=arelle_csv_encoding) as csv_file,\
            open(filename + '_units', encoding='utf-8') as unit_file:
        csv_reader = csv.reader(unit_file)
        unit_map = {row[0]: {'id': row[1], 'name': row[2]}
                    for row in csv_reader}
        insert_regnskab(csv_file, unit_map, regnskab)
