import datetime
import json

from itertools import groupby
from pprint import pprint

from .shared import financial_statement_iterator, partition_consolidated
from .shared import fetch_regnskabsform_dict

from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException

from sqlalchemy import Table, Column, ForeignKey, MetaData
from sqlalchemy import DateTime, String, Text
from sqlalchemy import Sequence, UniqueConstraint
from sqlalchemy import BigInteger, Boolean, Float, Integer


from .models import Base
from . import Session, engine

current_regnskabs_id = 0

regnskabsform = {}  # initialized in __main__


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


def make_header(regnskab_dict, financial_statement_id, consolidated, session):
    instance = session.query(Header).filter(
        Header.financial_statement_id == financial_statement_id,
        Header.consolidated == consolidated
    ).first()

    if instance:
        return instance

    header_values = {
        'financial_statement_id': financial_statement_id,
        'language': find_language(regnskab_dict),
        'currency': find_currency(regnskab_dict),
        'balancedato': find_balancedato(regnskab_dict),
        'consolidated': consolidated
    }

    try:
        header_values['gsd_IdentificationNumberCvrOfReportingEntity'] = (
            generic_number(regnskab_dict,
                           'gsd:IdentificationNumberCvrOfReportingEntity')
        )
    except ValueError:
        header_values['gsd_IdentificationNumberCvrOfReportingEntity'] = None

    try:
        header_values['gsd_InformationOnTypeOfSubmittedReport'] = (
            generic_text(regnskab_dict,
                         'gsd:InformationOnTypeOfSubmittedReport')
        )
    except ValueError:
        header_values['gsd_InformationOnTypeOfSubmittedReport'] = None

    try:
        header_values['gsd_ReportingPeriodStartDate'] = (
            generic_date(regnskab_dict, 'gsd:ReportingPeriodStartDate')
        )
    except ValueError:
        header_values['gsd_ReportingPeriodStartDate'] = None

    try:
        header_values['fsa_ClassOfReportingEntity'] = (
            generic_text(regnskab_dict, 'fsa:ClassOfReportingEntity')
        )
    except ValueError:
        header_values['fsa_ClassOfReportingEntity'] = None

    try:
        header_values['cmn_TypeOfAuditorAssistance'] = (
            generic_text(regnskab_dict, 'cmn:TypeOfAuditorAssistance',
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


def populate_row(table_description, regnskab_tuples, regnskabs_id,
                 consolidated=False):
    """
    returns a dict with keys based on table_description and values
    read from regnskab_tuples based on the method in table_description
    """
    global current_regnskabs_id
    current_regnskabs_id = regnskabs_id
    regnskab_dict = dict([(k, list(v))
                          for k, v in groupby(regnskab_tuples,
                                              lambda k: k.fieldname)])
    session = Session()

    header = make_header(regnskab_dict, regnskabs_id, consolidated, session)
    result = {'headerId': header.id}

    method_translation = {
        'generic_number': generic_number,
        'generic_text': generic_text,
        'generic_date': generic_date
    }

    for column_description in table_description['columns']:
        methodname = column_description['method']['name']
        dimensions = column_description['dimensions']
        regnskabs_fieldname = column_description['regnskabs_fieldname']
        column_name = column_description['name']
        if 'when_multiple' in column_description['method'].keys():
            when_multiple = column_description['method']['when_multiple']
            result[column_name] = method_translation[methodname](
                regnskab_dict,
                regnskabs_fieldname,
                dimensions=dimensions,
                when_multiple=when_multiple
            )
        else:
            result[column_name] = method_translation[methodname](
                regnskab_dict,
                regnskabs_fieldname,
                dimensions=dimensions,
            )
    session.close()
    return result


def populate_table(table_description, table, start_idx=1):
    assert(isinstance(table_description, dict))
    assert(isinstance(table, Table))
    print("Populating table %s" % table_description['tablename'])
    cache = []
    cache_sz = 2000
    for i, end, fs_id, fs_entries in financial_statement_iterator(start_idx=start_idx):
        partition = partition_consolidated(fs_entries)
        fs_entries_cons, fs_entries_solo = partition
        if len(fs_entries_cons):
            row_values = populate_row(table_description, fs_entries_cons,
                                      fs_id, consolidated=True)
            cache.append(row_values)
        if len(fs_entries_solo):
            row_values = populate_row(table_description, fs_entries_solo,
                                      fs_id, consolidated=False)
            cache.append(row_values)
        if len(cache) >= cache_sz:
            engine.execute(table.insert(), cache)
            cache = []
    if len(cache):
        engine.execute(table.insert(), cache)
        cache = []
    return


def get_regnskabsform(regnskabs_id):
    """
    Every regnskab is reported according to some rules.
    This function determines under which rules it is reported.
    Concretely, this function returns the kind of regnskab
    based on the 'xsd' filename.
    """
    try:
        xsd_file = regnskabsform[regnskabs_id]
        if ('AccountFormIncomeStatementByNature' in xsd_file or
                'AccountFormIncomeStatementByFunction' in xsd_file):
            return 1  # type 1.
        elif 'AccountByCurrentAndLongTermFormIncomeStatement' in xsd_file:
            return 2
        elif 'ReportFormIncomeStatementByNature' in xsd_file:
            return 3
        elif 'ReportFormIncomeStatementByFunction' in xsd_file:
            return 4
    except KeyError:
        pass

    return None


def find_regnskabs_id():
    return current_regnskabs_id


def find_currency(regnskab_dict):
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

    for key, regnskab_tuples in regnskab_dict.items():
        if key not in balance_keys:
            continue
        for t in regnskab_tuples:
            if t.unitIdXbrl != '':
                return t.unitIdXbrl
    return 'DKK'


def find_language(regnskab_dict):
    """ Detects language from dict of text fields """
    str_to_use = ''
    for key, regnskab_tuples in regnskab_dict.items():
        for t in regnskab_tuples:
            if not isinstance(t.fieldValue, str):
                break
            if len(t.fieldValue) <= len(str_to_use):
                continue
            str_to_use = t.fieldValue
    if len(str_to_use) > 20:
        try:
            return detect(str_to_use)
        except LangDetectException:
            return 'da'
    return 'da'


def find_balancedato(regnskab_dict):
    try:
        flattened = [item for sublist in regnskab_dict.values()
                     for item in sublist]
        return max(t.endDate for t in flattened)
    except:
        pass
    return None


def get_most_precise(regnskab_tuples):
    """ Returns the 'best' value for a given entry in a financial statement """

    def order_key(t):
        """
        Key for tuple of regnskabsid, fieldname, fieldvalue, decimals,
        precision, startDate, endDate, unitId.
        """
        dec = -1000 if len(t.decimals) == 0 else float(t.decimals)
        return ((t.startDate, t.endDate, (t.decimals.lower() == 'inf')
                 or (t.precision.lower == 'inf'), dec, t.fieldValue))

    regnskab_tuples.sort(key=order_key, reverse=True)
    return regnskab_tuples[0].fieldValue


def generic_number(regnskab_dict, fieldName, dimensions=None):
    # The following dict is based on 'årsregnskabsloven', see
    # https://www.retsinformation.dk/forms/r0710.aspx?id=175792#id84310183-d8a6-4104-9f32-cee6d8214740
    # for more info.  Each list contains the mandatory labels (roman
    # and arab numerals), which are 0 if not specified in a regnskab.
    # Each number is the schema specified in the appendix from the above url.
    regnskabsform_defaults = {
        # 1. Skema for balance i kontoform (regnskabsklasse B, C og D)
        1: [
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
            'fsa:ShorttermLiabilitiesOtherThanProvisions', ],
        # 2. Skema for balance i kontoform – opdeling i lang- og
        # kortfristede aktiver og passiver (regnskabsklasse B, C og D)
        2: ['fsa:Assets',
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
            'fsa:ShorttermLiabilitiesOtherThanProvisions'],
        # 3. Skema for resultatopgørelse i beretningsform, artsopdelt
        # (regnskabsklasse B, C og D)
        3: [],
        # 4. Skema for resultatopgørelse i beretningsform, funktionsopdelt
        # (regnskabsklasse B, C og D)
        4: [],
    }
    try:
        if dimensions is None:
            values = [t for t in regnskab_dict[fieldName]
                      if len(t.other_dimensions) == 0]
        else:
            values = [t for t in regnskab_dict[fieldName]
                      if t.other_dimensions == dimensions]
        if len(values) == 0:
            raise ValueError('No tuples with fieldName %s' % fieldName)
        most_precise = get_most_precise(values)
        return most_precise
    except (ValueError, KeyError):
        pass
    regnskabs_id = find_regnskabs_id()
    if regnskabs_id == 0:
        pprint(regnskab_dict, indent=2)
    kind = get_regnskabsform(regnskabs_id)
    if kind is not None and fieldName in regnskabsform_defaults[kind]:
        return 0
    return None


def generic_text(regnskab_dict, fieldName, when_multiple='concatenate',
                 dimensions=None):
    values = regnskab_dict.get(fieldName, ())
    if dimensions is None:
        dimensions = []
    xs = set(t.fieldValue for t in values if t.other_dimensions == dimensions)
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


def generic_date(regnskab_dict, fieldName, dimensions=None):
    try:
        values = regnskab_dict[fieldName]
    except KeyError:
        return None

    for v in values:
        try:
            return datetime.datetime.strptime(v.fieldValue, '%Y-%m-%d')
        except ValueError:
            pass
    return None


def fieldname_to_colname(fieldname):
    colname = fieldname.replace(':', '_')
    if len(colname) <= 64:
        return colname
    return colname[:40] + colname[-64+40:]


def main(table_descriptions_file, start_idx=1):
    global regnskabsform
    regnskabsform = fetch_regnskabsform_dict()
    Base.metadata.create_all(engine)
    tables = dict()

    with open(table_descriptions_file) as fp:
        table_descriptions = json.load(fp)

    for t in table_descriptions:
        table = create_table(t, drop_table=(start_idx == 1))
        populate_table(t, table, start_idx=start_idx)
        tables[t['tablename']] = table

    return
