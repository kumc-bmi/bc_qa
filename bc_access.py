'''bc_access -- get data files from REDCap

Usage:
  bc_access [options] <api_key>
  bc_access [options] normalize <dbfile>...

Options
 <project_key>   environment variable name to find
                 REDCap API key for target project
 --data          export data in CSV format to stdout [default: False]
 --fetch         fetch data files [default: False]
 --url URL       REDCap API URL
                 [default: https://redcap.kumc.edu/api/]
 --verify-ssl    verify SSL certs [default: False]
 normalize       ensure sqlite3 format; unzip if necessary
 --debug         verbose logging [default: False]
 -h, --help      show this help message and exit
 --version       show version and exit

Project data is written in CSV to stdout.

'''

import csv
import logging
import re
from sqlite3 import DatabaseError

from docopt import docopt

from version import version

log = logging.getLogger(__name__)


def main(argv, stdout, saveFile, projectAccess, checkDB, unzip):
    usage = __doc__.split('\n..')[0]
    cli = docopt(usage, argv=argv[1:], version=version)
    log.debug('cli args: %s', cli)

    if cli['<api_key>']:
        project = projectAccess(cli)
        records = project.export_records()
        if cli['--export']:
            export_csv(records, stdout)
        if cli['--fetch']:
            get_files(project, records, dd, saveFile)
    elif cli['normalize']:
        normalize(checkDB, unzip, cli['<dbfile>'])


def choices(dd, field_name):
    descs = [f['select_choices_or_calculations'] for f in dd
          if f['field_name'] == field_name]
    if not descs:
        raise KeyError(field_name)
    lines = descs[0].replace('\\n', '\n').split('\n')
    return dict(line.strip().split(', ', 1) for line in lines)


def export_csv(records, outfp):
    if not records:
        return
    columns = records[0].keys()
    out = csv.DictWriter(outfp, columns)
    log.info('saving %d records with %d columns...',
             len(records), len(columns))
    out.writerow(dict(zip(columns, columns)))
    out.writerows(records)
    log.info('saved.')


class DevTeams(object):
    lines = '''
      1-Marshfield_Clinic (MCRF)
      2-Medical_College_of_Wisconsin (MCW)
      3-University_of_Iowa (UIOWA)
      4-University_of_Minnesota (UMN)
      5-University_of_Nebraska_Medical_Center (UNMC)
      6-University_of_Texas_Health_Sciences_Center_at_San_Antonio (UTHSCSA)
      7-University_of_Texas_Southwestern_Medical_Center (UTSW)
      8-University_of_Wisconsin (WISC)
      9-Kansas_University_Medical_School (KUMC)
    '''.strip().split('\n')

    pattern = re.compile(r'\s*(?P<num>\d+)-(?P<name>[^(]+)\((?P<abbr>[^)]+)')

    @classmethod
    def map(cls):
        '''Get a map of pulldown choices to DevTeams abbreviations.
        >>> DevTeams.map()['9']
        'KUMC'
        '''
        return dict((m.group('num'), m.group('abbr'))
                    for line in cls.lines
                    for m in [re.match(cls.pattern, line)])


def get_files(project, records, saveFile):
    current = [r for r in records if not r['obsolete']]
    log.info('%d current submissions; %d total',
             len(current), len(records))
    dt = DevTeams.map()
    for r in current:
        which = r['institution']
        category = '%s-%s' % (dt[which], r['record_id'])
        content, headers = project.export_file(
            record=r['record_id'], field='bc_file')
        saveFile(category, headers['name'], content)


def mkProjectAccess(mkProject, env):
    def projectAccess(cli):
        api_key = env[cli['<api_key>']]
        return mkProject(cli['--url'], api_key,
                         verify_ssl=cli['--verify-ssl'])
    return projectAccess


def normalize(checkDB, unzip, dbfiles):
    def dberr(f):
        try:
            qty = checkDB(f)
            log.info('%s has %d patients.', f, qty)
            return None
        except IOError as ex:
            log.error('cannot access %s', f, exc_info=ex)
            return ex
        except DatabaseError as ex:
            return ex

    for f in dbfiles:
        if dberr(f) is None:
            continue
        try:
            f = unzip(f)
        except IOError as ex:
            log.error('cannot unzip %s', f, exc_info=ex)
        ex = dberr(f)
        if ex is not None:
            log.error('cannot query %s', f, exc_info=ex)


def mkCheckDB(exists, connect,
              testq='select count(*) from patient_dimension'):
    def checkDB(filename):
        if not exists(filename):
            raise IOError
        conn = connect(filename)
        q = conn.cursor()
        q.execute(testq)
        return q.fetchone()[0]

    return checkDB


def mkUnzip(mkZipFileRd, splitext, path_join, rename, rmdir):

    def unzip(f):
        z = mkZipFileRd(f)
        names = z.namelist()
        if len(names) != 1:
            raise IOError('more than one item in zip file; which to use? %s' % names)
        member = names[0]
        # x.zip    -> x    -> x
        # x.db.zip -> x.db -> x
        destdir = splitext(splitext(f)[0])[0]
        dest = destdir + '.db'
        z.extract(member, destdir)
        rename(path_join(destdir, member), dest)
        rmdir(destdir)
        return dest

    return unzip


def mkSaveFile(open_wr):
    def saveFile(category, name, content):
        log.info('saveFile(%s, %s, [%d])',
                 category, name, len(content))

        path = '%s-%s' % (category, name)
        with open_wr(path) as out:
            out.write(content)
        log.info('saved %s', path)
    return saveFile


if __name__ == '__main__':
    def _configure_logging():
        from sys import argv

        logging.basicConfig(level=logging.WARN)  # for requests etc.
        level = logging.DEBUG if '--debug' in argv else logging.INFO
        log.setLevel(level)

    def _privileged_main():
        from __builtin__ import open
        from sys import argv, stdout
        from os import environ, rename, rmdir
        from os.path import exists, splitext, join
        from zipfile import ZipFile
        from sqlite3 import connect
        from redcap import Project

        open_wr = lambda n: open(n, 'w')
        main(argv, stdout,
             checkDB=mkCheckDB(exists, connect),
             unzip=mkUnzip(lambda f: ZipFile(f, 'r'), splitext, join, rename, rmdir),
             saveFile=mkSaveFile(open_wr),
             projectAccess=mkProjectAccess(Project, environ))

    _configure_logging()
    _privileged_main()
