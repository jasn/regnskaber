import requests
import xml.etree.ElementTree as ET
from io import StringIO
from collections import defaultdict


def build_unit_map():

    # Download and parse the following xml file.
    # It contains many units.
    utr_url =  'https://www.xbrl.org/utr/utr.xml'

    r = requests.get(utr_url)

    if r.status_code == 200:
        pass

    tree = ET.parse(StringIO(r.text))
    utr = tree.getroot()
    units = utr.getchildren()[0]

    ns = '{http://www.xbrl.org/2009/utr}'

    unit_map = {unit_id.text: unit_name.text for unit_id, unit_name in (
                            zip(units.findall('./%sunit/%sunitId' % (ns, ns)),
                                units.findall('./%sunit/%sunitName' % (ns, ns))))}

    return unit_map


def translate_units(document, unit_map):
    """
    document is an xml document that defines units. 
    The unit_map argument is produced by build_unit_map().

    The return value is a dict with unitId to unitName where unitId comes from
    the definition in document and unitName comes from the definition in unit_map.
    """
    tree = ET.parse(StringIO(document))
    root = tree.getroot()
    ns = '{http://www.xbrl.org/2003/instance}'
    units = root.findall('./%sunit' % ns)
    result = dict()
    for unit in units:
        unit_id = unit.attrib['id']
        children = unit.getchildren()
        if len(children) != 1 or not children[0].tag.endswith('measure'):
            # error
            print('Something bad happened with unit_id %s' % unit.attrib['id'])
            continue

        measure = children[0].text
        measure_no_prefix = measure.split(':', maxsplit=1)[-1]

        search = [(k, len(k)) for k in unit_map.keys() if k in measure]
        if not len(search):
            continue
        ans = max(search, key=lambda x: x[1])

        best_length = ans[1]
        if abs(len(measure_no_prefix) - best_length) > 2:
            print('found unit was no good')
        else:
            result[unit_id] = (ans[0], unit_map[ans[0]])

    return result

class UnitHandler(object):

    def __init__(self):
        self.unit_map = build_unit_map()

    def translate_units(self, document):
        return translate_units(document, self.unit_map)

