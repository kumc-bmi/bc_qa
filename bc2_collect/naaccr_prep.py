#!/usr/bin/env python
'''naaccr_prep -- prepare NAACCR data for submission to GPC BC datamart

Usage:
  naaccr_prep.py [options] <naaccr_file> <crosswalk> <output>

Options:
  --fields FILE  field metadata [default: naaccr_item_bc_field.csv]
  --debug        verbose logging
'''

import logging

from docopt import docopt
import pandas as pd


log = logging.getLogger()

PATIENT_ID_NUMBER = 20


def main(argv, cwd):
    opts = docopt(__doc__, argv=argv[1:])

    naaccr_fixed = (cwd / opts['<naaccr_file>']).open().readlines()
    log.info('found %d NAACCR records in %s',
             len(naaccr_fixed), opts['<naaccr_file>'])

    fields = pd.read_csv((cwd / opts['--fields']).open(), index_col=0)
    log.info('Using metadata on %d NAACCR fields from REDCap data dictionary',
             len(fields))

    data = fixed_items(naaccr_fixed, fields, key_ix=PATIENT_ID_NUMBER)

    log.info('Writing %d records to %s.',
             len(data), opts['<output>'])
    with (cwd / opts['<output>']).open('wb') as out:
        data.to_csv(out)


def fixed_items(lines, fields, key_ix):
    '''
    @param lines: an iterable of fixed-width lines
    @param fields: a DataFrame of field descriptions with
                   .columnstart, .columnend, .field_name, .validation
                   indexed by NAACCR item numbers
    @param key_ix: NAACCR item number of the patient identifier field
    '''
    key_field = fields.loc[key_ix]
    eav = pd.DataFrame(
        [fmt_field(line[f.columnstart - 1:f.columnend],
                   line[key_field.columnstart - 1:key_field.columnend],
                   item_num, f.field_name, f.validation)
         for line in lines
         for item_num, f in fields.iterrows()])
    return eav.pivot(index='key', columns='field_name', values='value')


def fmt_field(s, key, item_num, field_name, ty):
    '''Format one field value: None, date, or string.

    @param key: EAV entity key
    @param item_num: EAV attribute number
    @param field_name: REDCap field name
    @param ty: REDCap validation: date_ymd or other
    '''
    val = None if s.replace(' ', '') == '' else s
    val = ('%s-%s-%s' % (s[:4], s[4:6], s[6:])
           if val and ty == 'date_ymd'
           else val)
    return dict(key=key, item_num=item_num, value=val,
                field_name=field_name)


if __name__ == '__main__':
    def _script(format='%(levelname)s %(asctime)s %(message)s',
                datefmt='%H:%M:%S'):
        from sys import argv
        from pathlib import Path

        logging.basicConfig(level=logging.DEBUG if '--debug' in argv
                            else logging.INFO,
                            format=format,
                            datefmt=datefmt)
        main(argv, cwd=Path('.'))

    _script()
