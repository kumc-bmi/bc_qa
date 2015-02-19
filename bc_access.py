'''bc_access -- get data files from REDCap

Usage:
  bc_access [options] <api_key>

Options
 <project_key>   environment variable name to find
                 REDCap API key for target project
 --fetch         fetch data files [default: False]
 --url URL       REDCap API URL
                 [default: https://redcap.kumc.edu/api/]
 --verify-ssl    verify SSL certs [default: False]
 --debug         verbose logging [default: False]
 -h, --help      show this help message and exit
 --version       show version and exit

Project data is written in CSV to stdout.

'''

import csv
import logging

from docopt import docopt

from version import version

log = logging.getLogger(__name__)


def main(argv, stdout, saveFile, projectAccess):
    usage = __doc__.split('..')[0]
    cli = docopt(usage, argv=argv[1:], version=version)
    log.debug('cli args: %s', cli)

    project = projectAccess(cli)
    records = project.export_records()
    export_csv(records, stdout)

    if cli['--fetch']:
        dd = project.export_metadata()
        get_files(project, records, dd, saveFile)


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


def get_files(project, records, dd, saveFile):
    current = [r for r in records if not r['obsolete']]
    log.info('%d current submissions; %d total',
             len(current), len(records))
    institutions = choices(dd, 'institution')
    for r in current:
        which = r['institution']
        category = '%s-%s' % (which, institutions[which].replace(' ', '_'))
        content, headers = project.export_file(
            record=r['record_id'], field='bc_file')
        saveFile(category, headers['name'], content)


def mkProjectAccess(mkProject, env):
    def projectAccess(cli):
        api_key = env[cli['<api_key>']]
        return mkProject(cli['--url'], api_key,
                         verify_ssl=cli['--verify-ssl'])
    return projectAccess


def mkSaveFile(open_wr, mkdir, path_join):
    def saveFile(category, name, content):
        log.info('saveFile(%s, %s, [%d])',
                 category, name, len(content))
        try:
            mkdir(category)
        except OSError:  # already exists
            pass

        path = path_join(category, name)
        with open_wr(path) as out:
            out.write(content)
        log.info('saved %s', path)
    return saveFile


if __name__ == '__main__':
    def _configure_logging():
        from sys import argv

        level = logging.DEBUG if '--debug' in argv else logging.INFO
        logging.basicConfig(level=level)

    def _privileged_main():
        from __builtin__ import open
        from sys import argv, stdout
        from os import environ, mkdir
        from os.path import join as path_join
        from redcap import Project

        open_wr = lambda n: open(n, 'w')
        main(argv, stdout,
             saveFile=mkSaveFile(open_wr, mkdir, path_join),
             projectAccess=mkProjectAccess(Project, environ))

    _configure_logging()
    _privileged_main()
