'''bc_access -- get data files from REDCap

Usage:
  bc_access [options] export
  bc_access [options] fetch
  bc_access [options] normalize

Options
 export          export data in CSV format to stdout
 fetch           fetch data files; export file details to stdout
 --key=VAR       environment variable name to find
                 REDCap API key for target project [default: BCREAD]
 --url URL       REDCap API URL
                 [default: https://redcap.kumc.edu/api/]
 --verify-ssl    verify SSL certs [default: False]
 normalize       ensure sqlite3 format; unzip if necessary
                 get file details from stdin
 --debug         verbose logging [default: False]
 -h, --help      show this help message and exit
 --version       show version and exit

'''

from functools import partial
from posixpath import splitext, join as path_join
from sqlite3 import DatabaseError
import csv
import logging
import re

from docopt import docopt

from version import version

log = logging.getLogger(__name__)


def main(argv, stdin, stdout, mkTeam, projectAccess,
         checkDB, unzip, gunzip):
    usage = __doc__.split('\n..')[0]
    cli = docopt(usage, argv=argv[1:], version=version)
    log.debug('cli args: %s', cli)

    if cli['export']:
        project = projectAccess(cli)
        records = project.export_records(export_survey_fields=True)
        export_csv(records, stdout)
    elif cli['fetch']:
        project = projectAccess(cli)
        records = project.export_records()
        details = get_files(project, records, mkTeam)
        export_csv(details, stdout)
    elif cli['normalize']:
        details = list(csv.DictReader(stdin))
        normalize(checkDB, unzip, gunzip, details)
        export_csv(details, stdout)


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

    pattern = re.compile(r'\s*(?P<num>\d+)-(?P<name>[^(]+)\((?P<site>[^)]+)')

    @classmethod
    def map(cls):
        '''Get a map of pulldown choices to DevTeams site abbreviations.
        >>> DevTeams.map()['9']
        'KUMC'
        '''
        return dict((m.group('num'), m.group('site'))
                    for line in cls.lines
                    for m in [re.match(cls.pattern, line)])

    @classmethod
    def maker(cls, open_wr):
        return lambda site: cls(open_wr, site)

    def __init__(self, open_wr, site):
        self.saveFile = partial(self.saveFile, open_wr)
        self.site = site

    def saveFile(self, open_wr, record_id, filename, content):
        log.info('%s.saveFile(%s, %s, [%d])',
                 self.site, record_id, filename, len(content))

        path = '%s-%s-%s' % (self.site, record_id, filename)
        with open_wr(path) as out:
            out.write(content)
        log.info('saved %s', path)
        return dict(site=self.site,
                    record_id=record_id,
                    content_length=len(content),
                    bc_file=path)


def get_files(project, records, mkTeam):
    current = [r for r in records if not r['obsolete']]
    log.info('%d current submissions; %d total',
             len(current), len(records))
    to_site = DevTeams.map()
    details = []
    for r in current:
        team = mkTeam(to_site[r['institution']])
        content, headers = project.export_file(
            record=r['record_id'], field='bc_file')
        detail = team.saveFile(r['record_id'], headers['name'], content)
        details.append(detail)
    return details


def mkProjectAccess(mkProject, env):
    def projectAccess(cli):
        api_key = env[cli['--key']]
        return mkProject(cli['--url'], api_key,
                         verify_ssl=cli['--verify-ssl'])
    return projectAccess


def normalize(checkDB, unzip, gunzip, details):
    def dberr(f):
        try:
            qty = checkDB(f)
            log.info('%s has %d patients.', f, qty)
            return (qty, None)
        except IOError as ex:
            log.error('cannot access %s', f, exc_info=ex)
            return (None, ex)
        except DatabaseError as ex:
            return (None, ex)

    for detail in details:
        f = detail['bc_file']
        qty, ex = dberr(f)
        if ex is None:
            detail['bc_db'] = f
            detail['patient_qty'] = qty
            continue
        try:
            if f.endswith('.gz'):
                f = gunzip(f)
            else:
                f = unzip(f)
        except Exception as ex:
            log.error('cannot uncompress %s', f, exc_info=ex)
        qty, ex = dberr(f)
        if ex is None:
            detail['bc_db'] = f
            detail['patient_qty'] = qty
        else:
            log.error('cannot query %s', f, exc_info=ex)
    return details


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


def mkUnzip(mkZipFileRd, rename, rmdir):

    def unzip(f):
        z = mkZipFileRd(f)
        names = z.namelist()
        if len(names) != 1:
            raise IOError('more than one item in zip file; which to use? %s' % names)  # noqa
        member = names[0]
        log.info('extracting %s from %s', member, f)
        # x.zip    -> x    -> x
        # x.db.zip -> x.db -> x
        destdir = splitext(splitext(f)[0])[0]
        dest = destdir + '.db'
        z.extract(member, destdir)
        rename(path_join(destdir, member), dest)
        rmdir(destdir)
        return dest

    return unzip


def mkGunzip(mkGzipFile, openwr):
    def gunzip(f):
        dest = splitext(f)[0]
        log.info('extracting %s from %s', dest, f)
        gz = mkGzipFile(f)
        with openwr(dest) as out:
            out.write(gz.read())
        return dest
    return gunzip


if __name__ == '__main__':
    def _configure_logging():
        from sys import argv

        logging.basicConfig(level=logging.WARN)  # for requests etc.
        level = logging.DEBUG if '--debug' in argv else logging.INFO
        log.setLevel(level)

    def _privileged_main():
        from __builtin__ import open as openf
        from gzip import GzipFile
        from os import environ, rename, rmdir
        from os.path import exists
        from sqlite3 import connect
        from sys import argv, stdin, stdout
        from zipfile import ZipFile
        import warnings

        from redcap import Project

        mkTeam = DevTeams.maker(lambda n: openf(n, 'w'))
        with warnings.catch_warnings():
            warnings.filterwarnings("once")
            main(argv, stdin, stdout,
                 checkDB=mkCheckDB(exists, connect),
                 unzip=mkUnzip(lambda f: ZipFile(f, 'r'),
                               rename, rmdir),
                 gunzip=mkGunzip(GzipFile, lambda n: openf(n, 'wb')),
                 mkTeam=mkTeam,
                 projectAccess=mkProjectAccess(Project, environ))

    _configure_logging()
    _privileged_main()
