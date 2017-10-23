from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, BigInteger, UnicodeText, ForeignKey, Sequence
from sqlalchemy.orm import relationship


Base = declarative_base()

class FinancialStatement(Base):

    __tablename__ = 'financial_statement'

    id = Column(Integer, Sequence('id_sequence'), primary_key=True)
    offentliggoerelsesTidspunkt = Column(DateTime)
    indlaesningsTidspunkt = Column(DateTime)
    cvrnummer = Column(BigInteger)
    regnskabsForm = Column(String(length=200))
    erst_id = Column(String(length=100), index=True, unique=True)

    financial_statement_entries = relationship(
        'FinancialStatementEntry',
        back_populates='financial_statement',
        order_by='FinancialStatementEntry.id',
    )

    __table_args__ = {'mysql_row_format': 'COMPRESSED'}

class FinancialStatementEntry(Base):

    __tablename__ = 'financial_statement_entry'

    id = Column(Integer, Sequence('id_sequence'), primary_key=True)
    financial_statement_id = Column(Integer, ForeignKey('financial_statement.id'))
    fieldName = Column(String(length=1000))
    fieldValue = Column(String(length=2**32-1, convert_unicode=True))
    contextRef = Column(String(length=300))
    unitRef = Column(String(length=100))
    decimals = Column(String(length=20))
    precision = Column(String(length=20))
    cvrnummer = Column(BigInteger)
    startDate = Column(DateTime)
    endDate = Column(DateTime)
    dimensions = Column(String(length=10000))
    unitIdXbrl = Column(String(length=100))
    unitNameXbrl = Column(String(length=100))

    financial_statement = relationship(
        'FinancialStatement',
        back_populates='financial_statement_entries',
    )

    __table_args__ = {'mysql_row_format': 'COMPRESSED'}

