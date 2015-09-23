'''combine_options - format codebook options in REDCap style
'''

import csv
import logging
from itertools import groupby

from redcap_upload import FieldDef

log = logging.getLogger(__name__)


def main(access):
    input_lines, open_out = access()
    records = csv.DictReader(input_lines)

    codebook = Codebook.convert(list(records))

    with open_out() as out:
        dest = csv.DictWriter(out, FieldDef._fields)
        dest.writerow(dict(zip(FieldDef._fields, FieldDef._fields)))
        dest.writerows([fdef._asdict() for fdef in codebook])


class Codebook(object):
    @classmethod
    def convert(cls, records):
        variables = groupby(records,
                            lambda r: r['variable_num'])
        return (cls.as_field(int(v_id), records)
                for (v_id, records) in variables)

    @classmethod
    def as_field(cls, v_id, records):
        records = list(records)
        record = records[0]

        ty, choices = (
            ('dropdown', FieldDef.encode_choices(pairs=[
                (r['Code values'], r['Label']) for r in records]))
            if len(records) > 1 and records[0]['Code values']
            else
            ('dropdown', FieldDef.encode_choices(labels=[
                r['Label'] for r in records]))
            if len(records) > 1
            else ('text', None))

        f = FieldDef._default()._replace(
            field_name='v%02d_%s' % (
                v_id,
                record['Variable Name'].replace(' ', '_').replace('/', 'X')),
            form_name=record['var_type'],
            field_type=ty,
            select_choices_or_calculations=choices
            )
        return f


if __name__ == '__main__':
    def _script():
        from __builtin__ import open as open_any
        from sys import argv

        def access():
            logging.basicConfig(level=logging.DEBUG if '--debug' in argv
                                else logging.INFO)
            input_filename, output_filename = argv[1:3]
            return (open_any(input_filename),
                    lambda: open_any(output_filename, 'wb'))

        main(access)

    _script()
