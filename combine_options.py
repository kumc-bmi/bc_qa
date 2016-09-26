'''combine_options - format codebook options in REDCap style
'''

import csv
import logging
from itertools import groupby
import re

from redcap_upload import FieldDef

log = logging.getLogger(__name__)


def main(argv, cwd):
    [input_filename, output_filename] = argv[1:3]
    records = csv.DictReader((cwd / input_filename).open('rb'))

    codebook = Codebook.convert(list(records))

    with (cwd / output_filename).open('wb') as out:
        dest = csv.DictWriter(out, FieldDef._fields)
        dest.writerow(dict(zip(FieldDef._fields, FieldDef._fields)))
        dest.writerows([fdef._asdict() for fdef in codebook])


class Codebook(object):
    @classmethod
    def convert(cls, records):
        variables = groupby(records,
                            lambda r: r['variable_num'])
        fields = (cls.as_field(int(v_id), records)
                  for (v_id, records) in variables)
        return sorted(fields,
                      key=lambda f: f.form_name)

    @classmethod
    def as_field(cls, v_id, records):
        records = list(records)

        # Use x__ in attempt to be sure we don't put
        # such fields before the study ID.
        form_required = lambda hint: hint or 'x__missing'

        # Variables/field names must consist of ONLY
        # lower-case letters, numbers, and underscores.
        mk_name = lambda id, hint: 'v%02d_%s' % (
            v_id,
            re.sub('__+', '_',
                   re.sub(r'[^a-z0-9_]', '',
                          hint.replace(' ', '_').lower())))

        note_source = lambda concept, source, note: (
            '%s source: %s %s' % (
                concept, source, note)).strip()

        # Multiple choice fields can only have coded values
        # (in the choices in column F) that are numeric or
        # alpha-numeric (lower case or upper case, with or
        # without underscores), thus they cannot have codings
        # that contain spaces or other characters. Please make
        # the following corrections:
        # "00*" (F51) - Suggestion: replace with "00"
        fix_code = lambda c: c.replace('*', '')

        item = records[0]

        ty, choices, validation = (
            ('text', None, 'date_ymd')
            if item['Code, width, format '] == 'date_ymd'
            else
            ('dropdown', FieldDef.encode_choices(pairs=[

                (fix_code(r['Code values']),
                 r['Label']) for r in records
                if r['Label']  # skip "headings"
            ]), None)
            if len(records) > 1 and item['Code values']
            else
            ('dropdown', FieldDef.encode_choices(labels=[
                r['Label'] for r in records]), None)
            if len(records) > 1
            else ('text', None, None))

        f = FieldDef._default()._replace(
            form_name=form_required(item['var_type']),
            field_name=mk_name(v_id, item['Variable Name']),
            field_label=item['Variable Name'],
            field_note=note_source(item['Concept'],
                                   item['source'], item['Notes']),
            field_type=ty,
            text_validation_type_or_show_slider_number=validation,
            select_choices_or_calculations=choices
            )
        return f


class Path(object):
    def __init__(self, path, ops):
        io_open, pathjoin = ops
        self.open = lambda mode='r': io_open(path, mode=mode)
        self.pathjoin = lambda other: Path(pathjoin(path, other), ops)

    def __div__(self, other):
        return self.pathjoin(other)


if __name__ == '__main__':
    def _script():
        from io import open as io_open
        from sys import argv
        from os.path import join as pathjoin

        logging.basicConfig(level=logging.DEBUG if '--debug' in argv
                            else logging.INFO)

        main(argv, cwd=Path('.', (io_open, pathjoin)))

    _script()
