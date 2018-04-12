import datetime

from contextlib import closing

from .models import FinancialStatement
from . import Session

from sqlalchemy.sql.expression import func


def get_reporting_period(fs_entries):
    date_format = '%Y-%m-%d'
    start_date = None
    end_date = None
    for entry in fs_entries:
        if entry.fieldName == 'gsd:ReportingPeriodStartDate':
            start_date = entry.fieldValue
        if entry.fieldName == 'gsd:ReportingPeriodEndDate':
            end_date = entry.fieldValue
    start_date = datetime.datetime.strptime(start_date[:10], date_format)
    end_date = datetime.datetime.strptime(end_date[:10], date_format)
    return start_date, end_date


def date_is_in_range(start_date, end_date, query_date):
    if start_date is None and end_date is None:
        return True

    if start_date is None:
        return query_date <= end_date

    if end_date is None:
        return query_date >= start_date

    return query_date >= start_date and query_date <= end_date


def date_is_instant(start_date, end_date):
    return start_date is None and end_date is not None


def filter_reporting_period(fs_entries):
    """
    returns a subset fs_entries where each entry is in the reporting period.

    """
    start_date, end_date = get_reporting_period(fs_entries)
    result = []
    for entry in fs_entries:
        if entry.startDate is None and entry.endDate is None:
            continue
        if date_is_instant(entry.startDate, entry.endDate):
            if date_is_in_range(start_date, end_date, entry.endDate):
                # append to result
                result.append(entry)
            continue

        if (not date_is_in_range(start_date, end_date, entry.startDate) or
                not date_is_in_range(start_date, end_date, entry.endDate)):
            continue
        result.append(entry)

    return result


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


def partition_consolidated(fs_entries):
    fs_tuples_cons = [r for r in fs_entries
                      if r.koncern]
    fs_tuples_solo = [r for r in fs_entries
                      if not r.koncern]

    return fs_tuples_cons, fs_tuples_solo


def get_number_of_rows():
    with closing(Session()) as session:
        total_rows = session.query(FinancialStatement).count()
        return total_rows


def financial_statement_iterator(end_idx=None, length=None, buffer_size=500):
    """Provide an iterator over financial_statements in order of id

    Keyword arguments:
    end_idx -- One past the last financial_statement_id to iterate over.
    length -- The number of financial statements to iterate.
              Note only one of end_idx and length can be provided.
    buffer_size -- the internal buffer size to use for iterating.  The buffer
                   size is measured in number of financial statements.

    """

    if end_idx is not None and length is not None:
        raise ValueError("Cannot accept both end_idx and length.")

    if end_idx is None and length is None:
        try:
            session = Session()
            max_id = session.query(func.max(FinancialStatement.id)).scalar()
            end_idx = max_id + 1
        except (IndexError, ValueError):
            raise LookupError('Could not lookup maximum financial_statement_id'
                              ' in financial_statement table.')
        finally:
            session.close()

    if end_idx is not None:
        assert(isinstance(end_idx, int))

    if length is not None:
        assert(isinstance(length, int))
        end_idx = length

    total_rows = get_number_of_rows()

    with closing(Session()) as session:
        curr = 1
        while curr < end_idx:
            q = session.query(FinancialStatement).filter(
                FinancialStatement.id >= curr,
                FinancialStatement.id < min(curr + buffer_size, end_idx)
            ).enable_eagerloads(True).all()
            for i, fs in enumerate(q):
                entries = filter_reporting_period(fs.financial_statement_entries)
                yield i+curr, total_rows, fs.id, entries
            curr += buffer_size
    return
