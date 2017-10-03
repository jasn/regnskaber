import argparse
import os
import re
import shutil

extensions_dir = None  # will be set by argument parser
output_dir = None  # will be set by argument parser

class UnknownXsdException(Exception):
    """
    Exception to raise when an unknown xsd is found while
    processing an xbrl file.
    """
    def __init__(self, name):
        super().__init__()
        self.name = name

def iter_files(directory):
    if directory is None:
        return
    for to_visit in os.scandir(directory):
        if to_visit.is_dir():
            yield from iter_files(to_visit.path)
        elif to_visit.is_file():
            yield to_visit.path


def iter_xsds(directory):
    for file_path in iter_files(directory):
        if os.path.isfile(file_path) and file_path.endswith('.xsd'):
            yield file_path


xsd_basedir = None
def get_xsd(xsd_name):
    global xsd_basedir
    if xsd_basedir is None:
        xsd_basedir = os.path.abspath('base/')

    if xsd_basedir not in get_xsd._cache:
        xsds = iter_xsds(os.path.abspath(xsd_basedir))
        xsds = {os.path.basename(xsd): xsd for xsd in xsds}
        get_xsd._cache[xsd_basedir] = xsds

    xsds = get_xsd._cache[xsd_basedir]
    for k, v in xsds.items():
        if v.endswith(xsd_name):
            return v

    raise UnknownXsdException(xsd_name)

get_xsd._cache = {}


def replace_xsd_href(document):
    """
    Function to change the path to the xsd document used.
    The document points somewhere on the web, but the file is local.
    """

    def repl(match_object):
        path = match_object.group(1)
        rest = match_object.group('rest')
        if rest == None:
            rest = ""
        try:
            local_file = get_xsd(path)
            replacement = '="file://%s"' % (local_file + rest)
        except UnknownXsdException:
            replacement = match_object.group(0)
        return replacement

    reg = re.compile(r'="http://archprod.service.eogs.dk/taxonomy/([^"]*.xsd)(?P<rest>#[^"]*)?"')
    res = re.sub(reg, repl, document)
    return res


def run():
    
    for file_path in iter_files(extensions_dir):
        # we know file_path is prefixed by extensions_dir
        destination = os.path.join(output_dir,
                                   file_path[len(extensions_dir):])
        destination_dir = os.path.dirname(destination)
        if not os.path.exists(destination_dir):
            os.makedirs(destination_dir)

        #if file_path.endswith('.xsd'):
            #with open(file_path) as input_file,\
                #open(destination, 'w') as output_file:
        input_file = open(file_path)
        output_file = open(destination, 'w')
        processed_document = replace_xsd_href(input_file.read())
        output_file.write(processed_document)
            

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--basedir", default="base/",
                        help=("path to directory containing "
                              "non-extension ifrs and arl files"))
    parser.add_argument("-e", "--extensionsdir", default="extensions/",
                        help=("path to directory containing extension files"))
    parser.add_argument("-o", "--outputdir", default="extensions_fixed/",
                        help=("path to output directory."
                              "The directory will be created if nonexistent"))
    args = parser.parse_args()

    global xsd_basedir
    global extensions_dir
    global output_dir
    xsd_basedir = args.basedir
    extensions_dir = args.extensionsdir
    output_dir = args.outputdir
    
    run()

if __name__ == "__main__":
    main()
