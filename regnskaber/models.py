from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, BigInteger, Text

Base = declarative_base()

class RegnskaberFiles(Base):

    __tablename__ = 'regnskaber_files'

    regnskabsId = Column(Integer, primary_key=True)
    filename = Column(String(length=1000))
    offentliggoerelsesTidspunkt = Column(DateTime)
    indlaesningsTidspunkt = Column(DateTime)
    cvrnummer = Column(BigInteger)
    regnskabsForm = Column(String(length=200))
    erst_id = Column(String(length=100))

    __table_args__ = {'mysql_row_format': 'COMPRESSED'}

class Regnskaber(Base):

    __tablename__ = 'regnskaber'

    regnskabspostId = Column(Integer, primary_key=True)
    regnskabsId = Column(Integer, ForeignKey('regnskaber_files.regnskabsId'))
    fieldName = Column(String(length=1000))
    fieldValue = Column(Text)
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

    __table_args__ = {'mysql_row_format': 'COMPRESSED'}

