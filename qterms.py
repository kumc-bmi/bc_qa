'''qterms -- Extract terms from i2b2 query_definition in CSV format.
'''

from xml.etree import ElementTree as ET
import csv
from collections import namedtuple


Term = namedtuple('Term',
                  'id,item_key,concept_path,name_char,name'.split(','))


def main(argv, stdout, open_arg):
    doc = ET.parse(open_arg(1))

    out = csv.writer(stdout)
    out.writerow(Term._fields)
    out.writerows(terms(doc))


def terms(doc):
    items = doc.findall('.//item')

    return [
        Term(id, item_key.text, concept_path,
             fixtext(name_char.text), fixtext(name))
        for (id, item) in enumerate(items)
        for item_key in item.findall('item_key')
        for concept_path in [key2path(item_key.text)]
        for name_char in item.findall('item_name')
        for name in [name_char.text.split('[', 1)[0]]]


def key2path(k):
    return '\\' + '\\'.join(k.split('\\')[3:])


def fixtext(t):
    return t.replace(u'\xbf', "'")


if __name__ == '__main__':
    def _privileged_main():
        from sys import argv, stdout

        main(argv, stdout,
             open_arg=lambda ix: open(argv[ix]))

    _privileged_main()
