from collections import namedtuple
from itertools import groupby

regnskab_row = namedtuple('regnskab_row', ['regnskabs_id', 'fieldname',
                                           'fieldValue', 'decimals', 'precision',
                                           'startDate', 'endDate', 'unitIdXbrl',
                                           'consolidated',
                                           'has_resultdistribution',
                                           'other_dimensions'])


def preprocess_regnskab_tuples(regnskab_tuples):
    """Removes ConsolidatedSoloDimension, ConsolidatedMember, and SoloMember
    dimensions and attaches a consolidated flag to the tuple instead.  Parses
    the value of fieldValue to a proper type.  Attaches to each tuple whether
    it is a resultdistribution dimension.

    --------
    returns a list of 'regnskab_row' tuples (named tuple defined above).

    """
    regnskab_result = []
    for row in regnskab_tuples:
        dimensions = list(map(str.strip, row[8].split(',')))
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

        result_row = regnskab_row(regnskabs_id=row[0],
                                  fieldname=row[1],
                                  fieldValue=arelle_parse_value(row[2]),
                                  decimals=row[3],
                                  precision=row[4],
                                  startDate=row[5],
                                  endDate=row[6],
                                  unitIdXbrl=row[7],
                                  consolidated=consolidated,
                                  has_resultdistribution=has_resultdistribution,
                                  other_dimensions=dimensions)
        regnskab_result.append(result_row)
    return regnskab_result


def fetch_regnskabsform_dict(conn):
    """
    conn -- Connection to the sql server.
    returns a dict from regnskabsId to regnskabsForm.
    """
    sql = "select regnskabsId, regnskabsForm from regnskaber_files"
    cursor = conn.cursor()
    cursor.execute(sql)
    return dict(cursor.fetchall())


def arelle_parse_value(d):
    """Decodes an arelle string as a python type (float, int or str)"""
    if not isinstance(d, str):  # already decoded.
        return d
    try:
        return int(d.replace(',',''))
    except ValueError:
        pass
    try:
        return float(d.replace(",", ""))
    except ValueError:
        pass
    return d


def regnskab_iterator(conn, start_idx=1, end_idx=None, length=None, buffer_size=500):
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
            sql = "select max(regnskabsId) as m from regnskaber_files"
            cursor = conn.cursor()
            cursor.execute(sql)
            res = cursor.fetchall()
            end_idx = int(res[0][0]) + 1
            cursor.close()
        except (IndexError, ValueError):
            raise LookupError('Could not lookup maximum regnskabsid in regnskaber_files.')

    if end_idx is not None:
        assert(isinstance(end_idx, int))

    if length is not None:
        assert(isinstance(length, int))
        end_idx = start_idx + length


    sql = """
    SELECT regnskabsId, fieldname, fieldValue, decimals,
           regnskaber.precision, startDate, endDate,
           unitIdXbrl, dimensions
    FROM regnskaber WHERE regnskabsid >= {0} and regnskabsId < {1}
    ORDER BY regnskabsId, fieldname
    """

    with conn.cursor() as cursor:
        current = start_idx
        delta = buffer_size
        while current < end_idx:
            sql_query = sql.format(current, current + delta)
            print('Iterating %d <= regnskabsId < %d' % (current, current + delta))
            current += delta
            cursor.execute(sql_query)
            sql_result = cursor.fetchall()
            if len(sql_result) == 0:
                continue
            for regnskabs_id, regnskab in groupby(sql_result, lambda x: x[0]):
                regnskab_tuples = preprocess_regnskab_tuples(regnskab)
                yield regnskabs_id, regnskab_tuples



def partition_consolidated(regnskab_tuples):
    regnskab_tuples_cons = [r for r in regnskab_tuples
                                    if r.consolidated]
    regnskab_tuples_solo = [r for r in regnskab_tuples
                                    if not r.consolidated]

    return regnskab_tuples_cons, regnskab_tuples_solo
