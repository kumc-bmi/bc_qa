'''combine_options - format codebook options in REDCap style
'''

import csv

from redcap_upload import FieldDef


def main(access):
    input_lines, open_out = access()
    rows = csv.reader(input_lines)

    codebook = Codebook.convert(list(rows))

    with open_out() as out:
        dest = csv.DictWriter(out, FieldDef._fields)
        dest.writerow(zip(FieldDef._fields, FieldDef._fields))
        dest.writerows(codebook)


class Codebook(object):
    @classmethod
    def convert(cls, rows):
        raise NotImplementedError()


if __name__ == '__main__':
    def _script():
        from __builtin__ import open as open_any
        from sys import argv

        def access():
            input_filename, output_filename = argv[1:3]
            return (open_any(input_filename),
                    lambda: open_any(output_filename, 'wb'))

        main(access)

    _script()
