import datetime
import json

from itertools import groupby
from pprint import pprint

from .shared import financial_statement_iterator, partition_consolidated

from sqlalchemy import Table, Column, ForeignKey, MetaData
from sqlalchemy import DateTime, String, Text
from sqlalchemy import Sequence, UniqueConstraint
from sqlalchemy import BigInteger, Boolean, Float, Integer


from .models import Base
from . import Session, engine

current_regnskabs_id = 0


def generic_number(regnskab_dict, fieldName, when_multiple=None,
                   dimensions=None):
    # The following dict is based on 'årsregnskabsloven', see
    # https://www.retsinformation.dk/forms/r0710.aspx?id=175792#id84310183-d8a6-4104-9f32-cee6d8214740
    # for more info.  Each list contains the mandatory labels (roman
    # and arab numerals), which are 0 if not specified in a regnskab.
    # Each number is the schema specified in the appendix from the above url.
    regnskabsform_defaults = {
        'fsa:Assets',
        'fsa:NoncurrentAssets',
        'fsa:IntangibleAssets',
        'fsa:PropertyPlantAndEquipment',
        'fsa:LongtermInvestmentsAndReceivables',
        'fsa:CurrentAssets',
        'fsa:Inventories',
        'fsa:ShorttermReceivables',
        'fsa:ShorttermInvestments',
        'fsa:CashAndCashEquivalents',
        'fsa:LiabilitiesAndEquity',
        'fsa:Equity',
        'fsa:ContributedCapital',
        'fsa:SharePremium',
        'fsa:RevaluationReserve',
        'fsa:OtherReserves',
        'fsa:Provisions',
        'fsa:LongtermLiabilitiesOtherThanProvisions',
        'fsa:ShorttermLiabilitiesOtherThanProvisions',
        'fsa:Assets',
        'fsa:NoncurrentAssets',
        'fsa:IntangibleAssets',
        'fsa:PropertyPlantAndEquipment',
        'fsa:LongtermInvestmentsAndReceivables',
        'fsa:CurrentAssets',
        'fsa:Inventories',
        'fsa:ShorttermReceivables',
        'fsa:ShorttermInvestments',
        'fsa:CashAndCashEquivalents',
        'fsa:LiabilitiesAndEquity',
        'fsa:Equity',
        'fsa:ContributedCapital',
        'fsa:SharePremium',
        'fsa:RevaluationReserve',
        'fsa:OtherReserves',
        'fsa:RetainedEarnings',
        'fsa:LongtermLiabilitiesOtherThanProvisions',
        'fsa:ShorttermLiabilitiesOtherThanProvisions',
    }
    try:
        values = [t for t in regnskab_dict[fieldName]]
        if dimensions is not None:
            values = [t for t in values if t.dimensions == dimensions]

        if len(values) == 0:
            raise ValueError('No tuples with fieldName %s' % fieldName)
        most_precise = get_most_precise(values)
        return most_precise
    except (ValueError, KeyError):
        pass
    regnskabs_id = find_regnskabs_id()
    if regnskabs_id == 0:
        pprint(regnskab_dict, indent=2)

    if fieldName in regnskabsform_defaults:
        return 0
    return None


def generic_text(regnskab_dict, fieldName, when_multiple='concatenate',
                 dimensions=None):
    values = regnskab_dict.get(fieldName, ())
    if dimensions is not None:
        values = [t for t in values if t.dimensions == dimensions]

    xs = set(t.fieldValue for t in values)
    if xs:
        try:
            x, = xs
            return str(x)
        except ValueError:
            if when_multiple == 'any':
                return str(next(iter(xs)))  # return any value
            elif when_multiple == 'none':
                return None
            elif when_multiple == 'concatenate':
                return ' '.join(str(x) for x in xs)
            else:
                print('Multiple values for %s: %s' % (fieldName, xs))
                raise
    return None


def generic_date(regnskab_dict, fieldName, when_multiple=None,
                 dimensions=None):
    try:
        values = regnskab_dict[fieldName]
        if dimensions is not None:
            values = [t for t in values if t.dimensions == dimensions]
    except KeyError:
        return None

    for v in values:
        try:
            return datetime.datetime.strptime(v.fieldValue, '%Y-%m-%d')
        except ValueError:
            pass
    return None


method_translation = {
        'generic_number': generic_number,
        'generic_text': generic_text,
        'generic_date': generic_date
}


class Header(Base):
    __tablename__ = 'Header'
    id = Column(Integer, Sequence('id_seq'), primary_key=True)
    financial_statement_id = Column(Integer())
    consolidated = Column(Boolean())
    currency = Column(String(5))
    language = Column(String(5))
    balancedato = Column(DateTime)
    gsd_IdentificationNumberCvrOfReportingEntity = Column(BigInteger)
    gsd_InformationOnTypeOfSubmittedReport = Column(Text)
    gsd_ReportingPeriodStartDate = Column(DateTime)
    fsa_ClassOfReportingEntity = Column(Text)
    cmn_TypeOfAuditorAssistance = Column(Text)

    __table_args__ = (
        UniqueConstraint('financial_statement_id', 'consolidated',
                         name='unique_financial_statement_id_consolidated'
                         ),
    )


def make_header(fs_dict, financial_statement_id, consolidated, session):
    instance = session.query(Header).filter(
        Header.financial_statement_id == financial_statement_id,
        Header.consolidated == consolidated
    ).first()

    if instance:
        return instance

    header_values = {
        'financial_statement_id': financial_statement_id,
        'language': find_language(fs_dict),
        'currency': find_currency(fs_dict),
        'balancedato': find_balancedato(fs_dict),
        'consolidated': consolidated
    }

    try:
        header_values['gsd_IdentificationNumberCvrOfReportingEntity'] = (
            generic_number(fs_dict,
                           'gsd:IdentificationNumberCvrOfReportingEntity')
        )
    except ValueError:
        header_values['gsd_IdentificationNumberCvrOfReportingEntity'] = None

    try:
        header_values['gsd_InformationOnTypeOfSubmittedReport'] = (
            generic_text(fs_dict,
                         'gsd:InformationOnTypeOfSubmittedReport')
        )
    except ValueError:
        header_values['gsd_InformationOnTypeOfSubmittedReport'] = None

    try:
        header_values['gsd_ReportingPeriodStartDate'] = (
            generic_date(fs_dict, 'gsd:ReportingPeriodStartDate')
        )
    except ValueError:
        header_values['gsd_ReportingPeriodStartDate'] = None

    try:
        header_values['fsa_ClassOfReportingEntity'] = (
            generic_text(fs_dict, 'fsa:ClassOfReportingEntity')
        )
    except ValueError:
        header_values['fsa_ClassOfReportingEntity'] = None

    try:
        header_values['cmn_TypeOfAuditorAssistance'] = (
            generic_text(fs_dict, 'cmn:TypeOfAuditorAssistance',
                         when_multiple='any')
        )
    except ValueError:
        header_values['cmn_TypeOfAuditorAssistance'] = None

    header = Header(**header_values)
    session.add(header)
    session.commit()
    return header


def create_table(table_description, drop_table=False):
    assert(isinstance(table_description, dict))

    def type_str_to_alchemy_type(s):
        if s == 'Double':
            return Float()
        if s[0:4] == 'Text':
            size = 0
            if len(s) > 4:
                try:
                    size = int(s[5:-1])
                except ValueError:
                    pass
            if size:
                return Text(size)
            else:
                return Text
        if s == 'Integer':
            return Integer()
        if s == 'BigInteger':
            return BigInteger()
        if s == 'Datetime':
            return DateTime()
        if s == 'Boolean':
            return Boolean()
        raise ValueError('%s did not match any known type' % s)

    metadata = MetaData(bind=engine)
    tablename = table_description['tablename']
    columns = [Column('headerId', Integer,
                      ForeignKey(Header.id),
                      primary_key=True)]
    for column_description in table_description['columns']:
        alchemy_type = type_str_to_alchemy_type(column_description['sqltype'])
        column = Column(column_description['name'],
                        alchemy_type)
        columns.append(column)

    t = Table(tablename, metadata, *columns, mysql_ROW_FORMAT='COMPRESSED')
    if drop_table:
        t.drop(engine, checkfirst=True)
        t.create(engine, checkfirst=False)
    return t


def populate_row(table_description, fs_entries, fs_id,
                 consolidated=False):
    """
    returns a dict with keys based on table_description and values
    read from regnskab_tuples based on the method in table_description
    """
    global current_regnskabs_id
    current_regnskabs_id = fs_id
    fs_dict = dict([(k, list(v))
                    for k, v in groupby(fs_entries,
                                        lambda k: k.fieldName)])
    session = Session()
    header = make_header(fs_dict, fs_id, consolidated, session)
    result = {'headerId': header.id}
    session.close()

    for column_description in table_description['columns']:
        methodname = column_description['method']['name']
        assert methodname in method_translation.keys()
        dimensions = column_description['dimensions']
        regnskabs_fieldname = column_description['regnskabs_fieldname']
        column_name = column_description['name']
        if 'when_multiple' in column_description['method'].keys():
            when_multiple = column_description['method']['when_multiple']
            result[column_name] = method_translation[methodname](
                fs_dict,
                regnskabs_fieldname,
                dimensions=dimensions,
                when_multiple=when_multiple
            )
        else:
            result[column_name] = method_translation[methodname](
                fs_dict,
                regnskabs_fieldname,
                dimensions=dimensions,
            )

    return result


def populate_table(table_description, table):
    assert(isinstance(table_description, dict))
    assert(isinstance(table, Table))
    print("Populating table %s" % table_description['tablename'])
    cache = []
    cache_sz = 2000
    fs_iterator = financial_statement_iterator()

    ERASE = '\r\x1B[K'
    progress_template = "Processing financial statements %s/%s"
    for i, end, fs_id, fs_entries in fs_iterator:
        print(ERASE, end='', flush=True)
        print(progress_template % (i, end), end='', flush=True)
        partition = partition_consolidated(fs_entries)
        fs_entries_cons, fs_entries_solo = partition
        if len(fs_entries_cons):
            row_values = populate_row(table_description, fs_entries_cons,
                                      fs_id, consolidated=True)
            if row_values:
                cache.append(row_values)
        if len(fs_entries_solo):
            row_values = populate_row(table_description, fs_entries_solo,
                                      fs_id, consolidated=False)
            if row_values:
                cache.append(row_values)
        if len(cache) >= cache_sz:
            engine.execute(table.insert(), cache)
            cache = []
    if len(cache):
        engine.execute(table.insert(), cache)
        cache = []
    print(flush=True)
    return


def find_regnskabs_id():
    return current_regnskabs_id


def find_currency(fs_dict):
    """Check balance statements of regnskab to find the currency used.  We assume
    the same currency is used for everything, though this is not a requirement.

    regnskab_dict -- dict of fieldname to corresponding tuples for regnskab.

    returns the ISO name for the currency used, and defaults to 'DKK'
    otherwise.

    """
    balance_keys = set(['fsa:Equity', 'fsa:Assets', 'fsa:LiabilitiesAndEquity',
                        'fsa:CurrentAssets', 'fsa:ContributedCapital',
                        'fsa:ProfitLoss', 'fsa:RetainedEarnings',
                        'fsa:LiabilitiesOtherThanProvisions',
                        'fsa:ShorttermLiabilitiesOtherThanProvisions'])

    for key, fs_entries in fs_dict.items():
        if key not in balance_keys:
            continue
        for t in fs_entries:
            if t.unitIdXbrl is not None and t.unitIdXbrl[0:8] == 'iso4217:':
                return t.unitIdXbrl[8:]
    return 'DKK'


def find_language(regnskab_dict):
    """ Detects language from dict of text fields """
    for key, regnskab_tuples in regnskab_dict.items():
        for t in regnskab_tuples:
            if t.unitIdXbrl is not None and t.unitIdXbrl[0:5] == 'lang:':
                return t.unitIdXbrl[5:]
    return 'da'


def find_balancedato(fs_dict):
    try:
        if 'gsd:ReportingPeriodEndDate' in fs_dict.keys():
            date_format = '%Y-%m-%d'
            end_date = fs_dict['gsd:ReportingPeriodEndDate'][0]
            return datetime.datetime.strptime(end_date, date_format)
    except Exception:
        pass
    try:
        flattened = [item for sublist in fs_dict.values()
                     for item in sublist]
        return max(t.endDate for t in flattened)
    except Exception:
        pass
    return None


def get_most_precise(regnskab_tuples):
    """ Returns the 'best' value for a given entry in a financial statement """

    def order_key(t):
        """
        Key for tuple of regnskabsid, fieldname, fieldvalue, decimals,
        precision, startDate, endDate, unitId.
        """
        dec = -1000
        if (t.decimals is not None and len(t.decimals) > 0 and
                t.decimals.lower() != 'inf'):
            dec = float(t.decimals)
        is_dec_inf = False
        if t.decimals is not None and t.decimals.lower() == 'inf':
            is_dec_inf = True

        return (t.startDate, t.endDate, is_dec_inf,
                dec, t.fieldValue)

    regnskab_tuples.sort(key=order_key, reverse=True)
    return regnskab_tuples[0].fieldValue


def fieldname_to_colname(fieldname):
    colname = fieldname.replace(':', '_')
    if len(colname) <= 64:
        return colname
    return colname[:40] + colname[-64+40:]


def register_method(name, func):
    global method_translation
    assert name not in ['generic_date', 'generic_number', 'generic_text']
    method_translation[name] = func


def main(table_descriptions_file):
    Base.metadata.create_all(engine)
    tables = dict()

    with open(table_descriptions_file) as fp:
        table_descriptions = json.load(fp)

    for t in table_descriptions:
        table = create_table(t, drop_table=True)
        populate_table(t, table)
        tables[t['tablename']] = table

    return
