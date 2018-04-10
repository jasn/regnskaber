""" This module is responsible for inserting each 'regnskab'. """
import datetime

import xbrl_ai
import xbrl_local.xbrl_ai_dk

from . import Session
from .models import FinancialStatement, FinancialStatementEntry


def initialize_financial_statement(regnskab):
    financial_statement = FinancialStatement(
        offentliggoerelsesTidspunkt=regnskab.offentliggoerelsesTidspunkt,
        indlaesningsTidspunkt=regnskab.indlaesningsTidspunkt,
        cvrnummer=regnskab.cvrnummer,
        erst_id=regnskab.erst_id
    )
    return financial_statement


def insert_regnskab(regnskab):
    session = Session()
    try:
        x = xbrl_ai.xbrlinstance_to_dict(regnskab.xbrl_file_contents)
        y = xbrl_local.xbrl_ai_dk.xbrldict_to_xbrl_dk_64(x)
        financial_statement = initialize_financial_statement(regnskab)
        session.add(financial_statement)
        for key, val in y.items():
            if key in ('{http://www.xbrl.org/2003/linkbase}schemaRef',
                       '@{http://www.w3.org/2001/XMLSchema-instance}schemaLocation'):
                continue
            fieldName, startDate, endDate = key[0], key[1], key[2]
            label_typed_id, koncern, xbrl_unit = key[3], key[4], key[5]
            fieldValue, unit, decimals, dimension_list = val

            if xbrl_unit is not None:
                xbrl_unit = str(xbrl_unit)
            if unit is not None:
                unit = str(unit)
            if fieldValue is not None:
                fieldValue = str(fieldValue)
            if decimals is not None:
                decimals = str(decimals)
            assert(xbrl_unit == unit)

            dimensions = label_typed_id
            # keys in row:
            # Name,Value,contextRef,unitRef,Dec,Prec,Lang,EntityIdentifier,Start,End/Instant,Dimensions
            cvrnummer = regnskab.cvrnummer

            financial_statement.financial_statement_entries.append(
                FinancialStatementEntry(
                    fieldName=fieldName, fieldValue=fieldValue,
                    decimals=decimals,
                    cvrnummer=cvrnummer,
                    startDate=startDate, endDate=endDate,
                    dimensions=dimensions,
                    unitIdXbrl=xbrl_unit,
                    koncern=koncern
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
    insert_regnskab(regnskab)
