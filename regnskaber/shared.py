from collections import namedtuple
from contextlib import closing

from .models import FinancialStatement
from . import Session

from sqlalchemy.sql.expression import func

fs_entry_row = namedtuple('fs_entry_row', ['fs_id', 'fieldname',
                                           'fieldValue', 'decimals',
                                           'precision', 'startDate', 'endDate',
                                           'unitIdXbrl', 'consolidated',
                                           'has_resultdistribution',
                                           'other_dimensions'])


def preprocess_fs_entry_rows(fs_entry_rows):
    """Removes ConsolidatedSoloDimension, ConsolidatedMember, and SoloMember
    dimensions and attaches a consolidated flag to the tuple instead.  Parses
    the value of fieldValue to a proper type.  Attaches to each tuple whether
    it is a resultdistribution dimension.

    --------
    returns a list of 'fs_entry_row' tuples (named tuple defined above).

    """
    fs_result = []
    for entry in fs_entry_rows:
        dimensions = list(map(str.strip, entry.dimensions.split(',')))
        if 'cmn:ConsolidatedMember' in dimensions:
            consolidated = True
        else:
            consolidated = False

        for r in ['cmn:ConsolidatedSoloDimension', 'cmn:ConsolidatedMember',
                  'cmn:SoloMember']:
            try:
                dimensions.remove(r)
            except:
                pass

        # remove empty dimensions.
        dimensions = [d for d in dimensions
                      if len(d.strip()) > 0 and d.strip() != 'None']

        if 'fsa:ResultDistributionDimension' in dimensions:
            has_resultdistribution = True
        else:
            has_resultdistribution = False

        result_row = fs_entry_row(
            fs_id=entry.financial_statement_id, fieldname=entry.fieldName,
            fieldValue=arelle_parse_value(entry.fieldValue),
            decimals=entry.decimals, precision=entry.precision,
            startDate=entry.startDate, endDate=entry.endDate,
            unitIdXbrl=entry.unitIdXbrl, consolidated=consolidated,
            has_resultdistribution=has_resultdistribution,
            other_dimensions=dimensions
        )
        fs_result.append(result_row)
    return fs_result


def fetch_regnskabsform_dict():
    """
    conn -- Connection to the sql server.
    returns a dict from regnskabsId to regnskabsForm.
    """
    with closing(Session()) as session:
        result = session.query(FinancialStatement.id,
                               FinancialStatement.regnskabsForm).all()
        return {r.id: r.regnskabsForm for r in result}


def arelle_parse_value(d):
    """Decodes an arelle string as a python type (float, int or str)"""
    if not isinstance(d, str):  # already decoded.
        return d
    try:
        return int(d.replace(',', ''))
    except ValueError:
        pass
    try:
        return float(d.replace(",", ""))
    except ValueError:
        pass
    return d


def partition_consolidated(regnskab_tuples):
    regnskab_tuples_cons = [r for r in regnskab_tuples
                                    if r.consolidated]
    regnskab_tuples_solo = [r for r in regnskab_tuples
                                    if not r.consolidated]

    return regnskab_tuples_cons, regnskab_tuples_solo


def get_number_of_rows(start_idx):
    with closing(Session()) as session:
        total_rows = session.query(FinancialStatement).filter(
            FinancialStatement.id >= start_idx).count()
        return total_rows


def financial_statement_iterator(start_idx=1, end_idx=None, length=None,
                                 buffer_size=500):
    """ Provide an iterator over regnskaber in order of regnskabsId

    Arguments:
    conn -- Connection to the sql server.

    Keyword arguments:
    start_idx -- The regnskabsId to start the iteration from
    end_idx -- One past the last regnskabsId to iterate over.
    length -- The number of regnskaber to iterate.
              Note only one of end_idx and length can be provided.
    buffer_size -- the internal buffer size to use for iterating.
                   The buffer size is measured in number of regnskaber.

    """

    if end_idx is not None and length is not None:
        raise ValueError("Cannot accept both end_idx and length.")

    if end_idx is None and length is None:
        try:
            session = Session()
            max_id = session.query(func.max(FinancialStatement.id)).scalar()
            end_idx = max_id + 1
        except (IndexError, ValueError):
            raise LookupError('Could not lookup maximum regnskabsid in '
                              'regnskaber_files.')
        finally:
            session.close()

    if end_idx is not None:
        assert(isinstance(end_idx, int))

    if length is not None:
        assert(isinstance(length, int))
        end_idx = start_idx + length

    total_rows = get_number_of_rows(start_idx)

    curr = start_idx
    session = Session()
    while curr < end_idx:
        q = session.query(FinancialStatement).filter(
            FinancialStatement.id >= curr,
            FinancialStatement.id <= min(curr+500, end_idx)
        ).enable_eagerloads(True).all()
        for i, fs in enumerate(q):
            entry_rows = preprocess_fs_entry_rows(fs.financial_statement_entries)
            yield i+curr, total_rows, fs.id, entry_rows
        curr += 500
    session.close()
    return
