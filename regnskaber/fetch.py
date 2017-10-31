import csv
import functools
import os
import re
import shutil
import sys
import tempfile
import time
import zipfile

from contextlib import redirect_stdout
from datetime import datetime
from io import BytesIO, StringIO
from multiprocessing import Process, Lock
from pathlib import Path

import elasticsearch1
import requests

from elasticsearch1 import Elasticsearch
from elasticsearch1_dsl import Search

from . import fix_ifrs_extensions as ifrs
from .ioqueue import IOQueueManager

from arelle import Cntlr, ModelManager, ViewFileFactList, FileSource
from .unitrefs import UnitHandler
from .regnskab_inserter import drive_regnskab

from . import Session, engine, parse_date
from .models import FinancialStatement, Base

ERASE = '\r\x1B[K'
ENCODING = 'UTF-8'

csv.field_size_limit(2**31-1)


def setup_tables():
    Base.metadata.create_all(engine)
    return


def arelle_generate_csv(regnskab):
    """
    Runs regnskab through Arelle and generates a csv file as if running
    Arelle on the command line.

    This method assumes that regnskab and its associated xsds are linking
    properly.
    """
    with StringIO() as arelle_log:
        fixed_filename = regnskab.xbrl_file.name
        cols = ["Name", "Value", "contextRef", "unitRef", "Dec", "Prec",
                "Lang", "EntityIdentifier", "Period", "Dimensions"]
        cntlr = Cntlr.Cntlr()
        with redirect_stdout(arelle_log):
            cntlr.startLogging(
                logFileName="logToPrint",
                logFormat="[%(messageCode)s] %(message)s - %(file)s",
                logLevel="DEBUG",
                logToBuffer=False
            )
            filesource = FileSource.FileSource(fixed_filename, cntlr)
            model_manager = ModelManager.initialize(cntlr)
            model_xbrl = model_manager.load(filesource, "loaded")
            outfilename = fixed_filename + '.csv'
            ViewFileFactList.viewFacts(model_xbrl, outfilename, cols=cols)


def fix_namespaces_in_csv(regnskab):
    """
    Fixes various nuissances such as wrong namespace in csv file.
    Fixes the filename in place.
    """
    filename = regnskab.xbrl_file.name + '.csv'
    output = StringIO()
    # assume that filename + '_namespaces' exists
    with open(filename + '_namespaces', encoding=ENCODING) as file_namespaces,\
            open(filename, encoding=ENCODING) as f:

        namespaces = {prefix: uri
                      for line in file_namespaces if line.find(':') != -1
                      for prefix, uri in [line.strip().split(':', maxsplit=1)]}

        # replace everything in 1st column and columns 11 through last
        csv_reader = csv.reader(f)
        csv_writer = csv.writer(output)

        replacements = {
            'http://xbrl.dcca.dk/arr': 'arr',
            'http://xbrl.dcca.dk/cmn': 'cmn',
            'http://xbrl.dcca.dk/dst': 'dst',
            'http://xbrl.dcca.dk/fsa': 'fsa',
            'http://xbrl.dcca.dk/gsd': 'gsd',
            'http://xbrl.dcca.dk/mrv': 'mrv',
            'http://xbrl.dcca.dk/sob': 'sob',
            'http://xbrl.dcca.dk/tax': 'tax'
        }

        for i, row in enumerate(csv_reader):
            if i == 0:
                csv_writer.writerow(row)
                continue

            columns = [0] + list(range(10, len(row)))  # first and dimensions.

            for c in columns:
                index = row[c].find(':')
                if index != -1:
                    prefix = row[c][:index]
                    suffix = row[c][index+1:]
                    replacement = prefix
                    if prefix in namespaces.keys():
                        if namespaces[prefix] in replacements:
                            replacement = replacements[namespaces[prefix]]
                    row[c] = replacement + ':' + suffix
            csv_writer.writerow(row)

    output.seek(0)
    with open(filename, 'w', encoding=ENCODING) as f:
        f.write(output.read())


class XSDCollection(object):

    def __init__(self):
        self.xsd_cache = dict()

    def find_xsds(self, base_directory):

        def find_xsds_rec(directory):
            for name in os.listdir(directory):
                absname = os.path.abspath(os.path.join(directory, name))
                if os.path.isdir(absname):
                    yield from find_xsds_rec(absname)
                elif name.endswith('.xsd'):
                    yield absname

        res = find_xsds_rec(base_directory)
        self.xsd_cache.update(
            {xsd.split('/')[-1]: xsd for xsd in res}
        )
        return

    def get_xsd(self, name):
        if name in self.xsd_cache:
            return self.xsd_cache[name]
        return None

    pass


class AArlCollection(XSDCollection):

    aarl_zip_location = Path(__file__).parent / 'resources' / 'aarl.zip'

    def __init__(self):
        super(AArlCollection, self).__init__()

        self.tmp_dir = tempfile.mkdtemp()
        self._unzip()

        self.find_xsds(self.tmp_dir)

    def _unzip(self):
        with open(str(AArlCollection.aarl_zip_location), 'rb') as aarl_zip:
            z = zipfile.ZipFile(aarl_zip)
            z.extractall(self.tmp_dir)
            return
        print("Fatal Error. Could not unzip aarl.zip. Exiting.",
              file=sys.stderr, flush=True)
        sys.exit(1)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if os.path.isdir(self.tmp_dir):
            shutil.rmtree(self.tmp_dir)
        return


class ExtensionCollection(XSDCollection):

    def __init__(self, extension_dir):
        super(ExtensionCollection, self).__init__()
        self.find_xsds(extension_dir)


class InputRegnskabError(Exception):
    """Exception raised for errors in the raw regnskabs data.
    """
    def __init__(self, erst_id, cvrnummer, offentliggoerelsesTidspunkt,
                 reason):
        self.erst_id = erst_id
        self.cvrnummer = cvrnummer
        self.reason = reason
        self.offentliggoerelsesTidspunkt = offentliggoerelsesTidspunkt

    def __str__(self):
        msg = ("[erst_id = %s] "
               "[cvrnummer = %s] "
               "[offentliggoerelsesTidspunkt: %s] "
               "%s"
               ) % (
                   self.erst_id,
                   self.cvrnummer,
                   self.offentliggoerelsesTidspunkt,
                   self.reason
               )
        return msg


class InputRegnskab(object):
    """Responsible for providing financial_statement data based on the xbrl_file
    and its possible extension.
    """

    def __init__(self, cvrnummer, offentliggoerelsesTidspunkt,
                 xbrl_file_url, xbrl_extension_url, erst_id,
                 indlaesningsTidspunkt):
        self.cvrnummer = cvrnummer
        self.erst_id = erst_id
        self.offentliggoerelsesTidspunkt = offentliggoerelsesTidspunkt
        self.indlaesningsTidspunkt = indlaesningsTidspunkt
        self.xbrl_file = self._download_file(xbrl_file_url)
        self._xbrl_extension = self._download_extension(xbrl_extension_url)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        files = [
            self.xbrl_file.name,
            self.xbrl_file.name + '.csv',
            self.xbrl_file.name + '.csv_namespaces',
            self.xbrl_file.name + '.csv_units',
        ]
        for fil in files:
            if os.path.isfile(fil):
                os.remove(fil)

        if (self._xbrl_extension is not None and
                os.path.isdir(self._xbrl_extension)):

            shutil.rmtree(self._xbrl_extension)

    def _download_extension(self, xbrl_extension_url):
        """
        returns directory where the extension was unpacked or None if there was
        no extension.
        """
        if xbrl_extension_url is None:
            return None

        response = requests.get(xbrl_extension_url)
        with zipfile.ZipFile(BytesIO(response.content)) as z:
            tmp_dir = tempfile.mkdtemp()
            z.extractall(tmp_dir)
            return tmp_dir

        error_msg = 'Error: could not download extension zip file: %s\n' % (
            xbrl_extension_url
        )
        raise InputRegnskabError(self.erst_id, self.cvrnummer,
                                 self.offentliggoerelsesTidspunkt, error_msg)

    def _download_file(self, xbrl_file_url):
        """
        returns full path of local file where the contens of xbrl_file_url
        has been written.
        """
        response = requests.get(xbrl_file_url)
        with tempfile.NamedTemporaryFile('w',
                                         delete=False,
                                         encoding='utf8') as tmp_file:
            try:
                tmp_file.write(response.text)
                return tmp_file
            except OSError as exc:
                error_msg = 'Error: could not save xbrl file: %s\n' % (
                    xbrl_file_url
                )
                raise InputRegnskabError(self.erst_id, self.cvrnummer,
                                         self.offentliggoerelsesTidspunkt,
                                         error_msg)

    def fix_extension(self, basedir):
        if self._xbrl_extension is None:
            return
        ifrs.xsd_basedir = basedir
        # assume extension has been downloaded and extracted.
        for file_path in ifrs.iter_files(self._xbrl_extension):
            with open(file_path, encoding=ENCODING) as input_file:
                processed_document = ifrs.replace_xsd_href(input_file.read())

            with open(file_path, 'w', encoding=ENCODING) as output_file:
                output_file.write(processed_document)
        return

    def fix_xbrl_file(self, aarlcollection):
        self.fix_xbrl_encoding()
        self.fix_xbrl_url_spaces()
        if self._xbrl_extension is not None:
            self.fix_xbrl_xsd_href(ExtensionCollection(self._xbrl_extension))
        else:
            self.fix_xbrl_xsd_href(aarlcollection)
        return

    def fix_xbrl_xsd_href(self, xsd_collection):
        def repl(match_object):
            filename = match_object.group('filename')
            replacement = xsd_collection.get_xsd(filename)
            if replacement is None:
                return match_object.group(0)
            else:
                self.regnskabsForm = replacement.split('/')[-1]
                return 'href="file://%s"' % replacement

        document = None
        with open(self.xbrl_file.name, encoding=ENCODING) as f:
            document = f.read()

        reg_href = re.compile(r'href="(?:[^"]*/)?(?P<filename>[^"]*\.xsd)"')
        self.regnskabsForm = None
        res = re.sub(reg_href, repl, document)
        if res == document:
            msg = "Error: XBRL extension missing."
            raise InputRegnskabError(self.erst_id, self.cvrnummer,
                                     self.offentliggoerelsesTidspunkt, msg)

        with open(self.xbrl_file.name, 'w', encoding=ENCODING) as f:
            f.write(res)

    def fix_xbrl_url_spaces(self):
        fixed_document = None
        with open(self.xbrl_file.name, 'r', encoding=ENCODING) as f:
            document = f.read()

            def repl(mo):
                return '="%s"' % mo.group(1).replace(' ', '%20')

            reg_uri = re.compile('="(http[^"]*)"')
            fixed_document = re.sub(reg_uri, repl, document)

        if fixed_document is None:
            msg = 'Error: could not fix url space in downloaded file.\n'
            raise InputRegnskabError(self.erst_id, self.cvrnummer,
                                     self.offentliggoerelsesTidspunkt, msg)

        with open(self.xbrl_file.name, 'w', encoding=ENCODING) as f:
            f.write(fixed_document)
        return

    def fix_xbrl_encoding(self):
        document = None
        with open(self.xbrl_file.name, 'rb') as xbrl_file:
            document_raw = xbrl_file.read().decode('utf-8')
            try:
                document = document_raw.encode("windows-1252").decode('utf-8')
            except (UnicodeDecodeError, UnicodeEncodeError):
                try:
                    document = document_raw.encode('latin1').decode('utf-8')
                except (UnicodeDecodeError, UnicodeEncodeError):
                    document = document_raw
        if document is None:
            msg = 'Error: could not fix encoding of the downloaded xbrl file'
            raise InputRegnskabError(self.erst_id, self.cvrnummer,
                                     self.offentliggoerelsesTidspunkt, msg)

        with open(self.xbrl_file.name, 'wb') as o:
            o.write(document.encode('utf-8'))

    def output_namespaces(self):
        """
        It turns out some xbrl files use strange prefixes for namespaces.
        This function makes sure to also output a file with
        'prefix:uri' pairs, such that we can later replace them with the
        recommended pairs instead.
        """
        filename = self.xbrl_file.name
        outfilename = self.xbrl_file.name + '.csv_namespaces'
        with open(filename, encoding=ENCODING) as f,\
                open(outfilename, 'w', encoding=ENCODING) as o:
            contents = f.read()
            end = contents.find('xbrl')
            end = contents.find('>', end)
            index = contents.find('xmlns', 0, end)
            while index != -1:
                if contents[index + len('xmlns')] == ':':
                    eq_sign = contents.find('=', index+len('xmlns'), end)
                    prefix = contents[index+len('xmlns')+1:eq_sign]
                    citation = contents.find('"', eq_sign+2, end)
                    namespace = contents[eq_sign+2:citation]
                    print('%s:%s' % (prefix, namespace.strip()), file=o)
                else:
                    citation = contents.find('"', index+len('xmlns="'), end)
                    namespace = contents[index + len('xmlns="'):citation]
                    print('default:%s' % namespace.strip(), file=o)
                index = contents.find('xmlns', index + len('xmlns'), end)

    def output_units(self, unit_handler):
        outfilename = self.xbrl_file.name + '.csv_units'

        with open(self.xbrl_file.name, encoding=ENCODING) as f,\
                open(outfilename, 'w', encoding=ENCODING) as o:
            contents = f.read()
            xml_units = unit_handler.translate_units(contents)
            for xml_id, (xbrl_id, xbrl_name) in xml_units.items():
                print('"%s","%s","%s"' % (xml_id,
                                          xbrl_id,
                                          xbrl_name),
                      file=o)


def query_by_erst_id(erst_id):
    url = 'http://distribution.virk.dk:80'
    client = Elasticsearch(url, timeout=300)
    index = 'offentliggoerelser'
    search = Search(using=client, index=index).query(
        'match', _id=erst_id
    )
    response = search.execute()
    hits = response.hits.hits
    return hits


# Algorithm:
# Input: URL of xbrl file, URL of zip file and other meta info.
# write to temporary files and folders.
# prepare for arelle pass
# write to temporary csv
# fix temporary csv
# insert csv into database.

def process(cvrnummer, offentliggoerelsesTidspunkt, xbrl_file, xbrl_extension,
            erst_id, indlaesningsTidspunkt,
            aarlcollection, unit_handler):
    if erst_id_present(erst_id):
        return
    try:
        with InputRegnskab(cvrnummer, offentliggoerelsesTidspunkt,
                           xbrl_file, xbrl_extension, erst_id,
                           indlaesningsTidspunkt) as regnskab:
            regnskab.output_namespaces()
            regnskab.output_units(unit_handler)
            regnskab.fix_extension(aarlcollection.tmp_dir)
            regnskab.fix_xbrl_file(aarlcollection)
            arelle_generate_csv(regnskab)
            fix_namespaces_in_csv(regnskab)
            drive_regnskab(regnskab)
    except InputRegnskabError as e:
        with open('erst_data_errors.txt', 'a') as f:
            print(e, file=f, flush=True)
    except Exception as e:
        import traceback
        etype, exc, tb = sys.exc_info()
        msg = '[erst_id = %s] Caught Exception.\n' % erst_id
        msg += ''.join(traceback.format_tb(tb))
        print(msg, file=sys.stderr, flush=True)
    return


def debug_by_erst_id(erst_id):
    # datetime_format = '%Y-%m-%dT%H:%M:%S.%f'
    aarl = AArlCollection()
    unit_handler = UnitHandler()
    res = query_by_erst_id(erst_id)
    hit = res[0]
    cvrnummer = hit['_source']['cvrNummer']
    offentliggoerelsesTidspunkt = hit['_source']['offentliggoerelsesTidspunkt']
    offentliggoerelsesTidspunkt = offentliggoerelsesTidspunkt[:19]
    offentliggoerelsesTidspunkt = parse_date(offentliggoerelsesTidspunkt)

    indlaesningsTidspunkt = hit['_source']['indlaesningsTidspunkt'][:19]
    indlaesningsTidspunkt = parse_date(indlaesningsTidspunkt)

    assert(erst_id == hit['_id'])

    dokumenter = hit['_source']['dokumenter']
    xbrl_file_url = None
    xbrl_extension_url = None
    import json
    print(json.dumps(hit, indent=2))
    print()
    for dokument in dokumenter:
        if (dokument['dokumentMimeType'].lower() == 'application/xml' and
                dokument['dokumentType'].lower() == 'aarsrapport'):
            xbrl_file_url = dokument['dokumentUrl']
        elif dokument['dokumentMimeType'].lower() == 'application/zip':
            xbrl_extension_url = dokument['dokumentUrl']

    if xbrl_file_url is not None:
        regnskab = InputRegnskab(cvrnummer, offentliggoerelsesTidspunkt,
                                 xbrl_file_url, xbrl_extension_url, erst_id,
                                 indlaesningsTidspunkt)
        regnskab.output_namespaces()
        regnskab.output_units(unit_handler)
        regnskab.fix_extension(aarl.tmp_dir)
        regnskab.fix_xbrl_file(aarl)
        arelle_generate_csv(regnskab)
        fix_namespaces_in_csv(regnskab)
        files = [
            regnskab.xbrl_file.name,
            regnskab.xbrl_file.name + '.csv',
            regnskab.xbrl_file.name + '.csv_namespaces',
            regnskab.xbrl_file.name + '.csv_units',
        ]
        print('\n'.join(files))
    return


def erst_id_present(erst_id):
    session = Session()
    try:
        erst_id_found = session.query(FinancialStatement.erst_id).filter(
            FinancialStatement.erst_id == erst_id
        ).first()
        return erst_id_found is not None
    finally:
        session.close()


def error_elastic_cvr_none(erst_id, offentliggoerelsesTidspunkt):
    msg = ("[erst_id = %s] [offentliggoerelsesTidspunkt: %s] "
           "Error: CVR-nummer returned by elasticsearch was None") % (
               erst_id, offentliggoerelsesTidspunkt
           )
    print(msg, file=sys.stderr, flush=True)
    return


def consumer_insert(queue, aarl=None, unit_handler=None, queue_lock=None):
    engine.dispose()  # for multiprocessing.
    if aarl is None:
        aarl = AArlCollection()
    if unit_handler is None:
        unit_handler = UnitHandler()
    while True:
        do_sleep = False
        try:
            queue_lock.acquire()
            if queue.size() == 0:
                do_sleep = True
                continue
            msg = queue.get()
            popped, pushed = queue.get_statistics()
            print(ERASE + 'Inserting into db: %s/%s' % (popped, pushed),
                  end='', flush=True)
        finally:
            queue_lock.release()
            if do_sleep:
                time.sleep(2)

        if isinstance(msg, str) and msg == 'DONE':
            break
        cvrnummer, offentliggoerelsesTidspunkt, xbrl_file_url = msg[:3]
        xbrl_extension_url, erst_id, indlaesningsTidspunkt = msg[3:]
        offentliggoerelsesTidspunkt = parse_date(offentliggoerelsesTidspunkt)
        indlaesningsTidspunkt = parse_date(indlaesningsTidspunkt)
        try:
            if cvrnummer is None:
                error_elastic_cvr_none(erst_id, offentliggoerelsesTidspunkt)
                continue
            process(cvrnummer, offentliggoerelsesTidspunkt, xbrl_file_url,
                    xbrl_extension_url, erst_id, indlaesningsTidspunkt,
                    aarl, unit_handler)
        except Exception as e:
            # already logged elsewhere.
            pass
    return


def producer_scan(search_result, queue, queue_lock=None):
    for document in search_result.scan():
        erst_id = document.meta.id
        cvrnummer = document['cvrNummer']
        # cvrnummer is possibly None, e.g. Greenland companies

        # date format: Y-m-dTH:M:s[Z+x]
        offentliggoerelsesTidspunkt = document['offentliggoerelsesTidspunkt']
        offentliggoerelsesTidspunkt = offentliggoerelsesTidspunkt[:19]
        indlaesningsTidspunkt = document['indlaesningsTidspunkt'][:19]

        dokumenter = document['dokumenter']
        xbrl_file_url = None
        xbrl_extension_url = None
        for dokument in dokumenter:
            mime_type = dokument['dokumentMimeType'].lower()
            xml_type = 'application/xml'
            dokument_type = dokument['dokumentType'].lower()
            if (mime_type == xml_type and dokument_type == 'aarsrapport'):
                xbrl_file_url = dokument['dokumentUrl']
            elif mime_type == 'application/zip':
                # TODO: dokument['dokumenType'].lower() == ?
                xbrl_extension_url = dokument['dokumentUrl']
        if xbrl_file_url is not None:
            msg = (cvrnummer, offentliggoerelsesTidspunkt, xbrl_file_url,
                   xbrl_extension_url, erst_id, indlaesningsTidspunkt)
            queue_lock.acquire()
            queue.put(msg)
            popped, pushed = queue.get_statistics()
            print(ERASE + 'Inserting into db: %s/%s' % (popped, pushed),
                  end='', flush=True)
            queue_lock.release()
    return


def get_virk_search(from_date):
    client = elasticsearch1.Elasticsearch('http://distribution.virk.dk:80',
                                          timeout=300)
    s = Search(using=client, index='offentliggoerelser')
    s = s.filter('range', offentliggoerelsesTidspunkt={'gte': from_date})
    s = s.sort('offentliggoerelsesTidspunkt')
    return s


def fetch_to_db(process_count=1, from_date=datetime(2011, 1, 1)):
    setup_tables()

    with AArlCollection() as aarl:

        unit_handler = UnitHandler()
        s = get_virk_search(from_date)
        try:
            tmp_file = tempfile.NamedTemporaryFile(delete=False)
            m = IOQueueManager()
            m.start()
            queue = m.IOQueue(tmp_file.name)
            queue_lock = Lock()
            consumer_partial = functools.partial(consumer_insert, aarl=aarl,
                                                 queue_lock=queue_lock,
                                                 unit_handler=unit_handler)

            processes = [Process(target=consumer_partial,
                                 args=(queue,),
                                 daemon=True) for _ in range(process_count)]
            for p in processes:
                p.start()
            engine.dispose()  # for multiprocessing.
            producer_scan(s, queue, queue_lock=queue_lock)

            queue_lock.acquire()
            for end in range(process_count):
                queue.put('DONE')
            queue_lock.release()

            for p in processes:
                p.join()

        finally:
            os.remove(tmp_file.name)
            pass
    return
