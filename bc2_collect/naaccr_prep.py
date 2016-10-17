#!/usr/bin/env python
'''naaccr_prep -- prepare NAACCR data for submission to GPC BC datamart

Usage:
  naaccr_prep.py [options] <naaccr_file> <crosswalk> <output>

Options:
  <naaccr_file>  fixed width NAACCR data file
  <crosswalk>    CSV file with mrn, study_id columns
  <output>       write to write CSV data to be imported into REDCap
  --fields FILE  field metadata [default: naaccr_item_bc_field.csv]
  --mrn-item N   NAACCR Patient ID Number item to match against
                 mrn in crosswalk [default: 20]
  --debug        verbose logging

Example:

$ ./naaccr_prep.py site-data/T.DAT site-data/studyid_mrn.csv site-data/out.csv
INFO 17:13:29 found 88503 NAACCR records in site-data/T.DAT
INFO 17:13:29 found 88 patients in crosswalk site-data/studyid_mrn.csv
INFO 17:13:29 Using metadata on 116 NAACCR fields from REDCap data dictionary
INFO 17:13:30 found 92 fixed records from 88 subjects
INFO 17:13:31 Writing 92 tumor records to site-data/out.csv.

'''

import logging

from docopt import docopt
import pandas as pd


log = logging.getLogger()


def main(argv, cwd):
    opts = docopt(__doc__, argv=argv[1:])

    naaccr_fixed = (cwd / opts['<naaccr_file>']).open('rb').readlines()
    log.info('found %d NAACCR records in %s',
             len(naaccr_fixed), opts['<naaccr_file>'])

    crosswalk = pd.read_csv((cwd / opts['<crosswalk>']).open())[
        ['mrn', 'study_id']]
    log.info('found %d patients in crosswalk %s',
             len(crosswalk), opts['<crosswalk>'])

    fields = pd.read_csv((cwd / opts['--fields']).open(), index_col=0)
    log.info('Using metadata on %d NAACCR fields from REDCap data dictionary',
             len(fields))

    tumor = fixed_items(naaccr_fixed, fields, key_ix=int(opts['--mrn-item']),
                        subjects=crosswalk.mrn)
    tumor.mrn = tumor.mrn.astype('int64')

    study_id = pd.merge(tumor[['mrn']], crosswalk, how='left')
    del tumor['mrn']

    tumor.insert(0, 'v01_studyid', study_id.study_id.values)
    tumor.insert(0, 'v00_tumorid', [
        tumor_id(r) for _, r in tumor.iterrows()])

    log.info('Writing %d tumor records to %s.',
             len(tumor), opts['<output>'])
    with (cwd / opts['<output>']).open('wb') as out:
        tumor.to_csv(out, index=False)


def tumor_id(r):
    return (str(r.v01_studyid) + ':' +
            (r.v15_0380_sequence_numbercentral
             if r.v15_0380_sequence_numbercentral > ''
             else r.v16_0560_sequence_numberhospital
             if r.v16_0560_sequence_numberhospital > ''
             else 'XX'))


def fixed_items(lines, fields, key_ix, subjects):
    '''
    @param lines: an iterable of fixed-width lines
    @param fields: a DataFrame of field descriptions with
                   .columnstart, .columnend, .field_name, .validation
                   indexed by NAACCR item numbers
    @param key_ix: NAACCR item number of the patient identifier field
    '''
    key_field = fields.loc[key_ix]

    entities = [
        (ix, entity)
        for ix, line in enumerate(lines)
        for entity in [
                int(line[key_field.columnstart - 1:key_field.columnend])]
        if entity in subjects.values
    ]

    log.info('found %d fixed records from %d subjects',
             len(entities), len(subjects))

    eav = pd.DataFrame(
        [fmt_field(line[f.columnstart - 1:f.columnend],
                   ix,
                   item_num, f.field_name, f.validation)
         for (ix, entity) in entities
         for line in [lines[ix]]
         for item_num, f in fields.iterrows()])

    return eav.pivot(index='key', columns='field_name', values='value')


def fmt_field(s, key, item_num, field_name, ty):
    '''Format one field value: None, date, or string.

    @param s: fixed-with value
    @param key: EAV entity key
    @param item_num: EAV attribute number
    @param field_name: EAV attribute REDCap field name
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
