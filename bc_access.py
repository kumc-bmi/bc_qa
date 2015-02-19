'''bc_access -- get data files from REDCap

Usage:
  bc_access [options] <api_key>

Options
 <project_key>   environment variable name to find
                 REDCap API key for target project
 --url URL       REDCap API URL
                 [default: https://redcap.kumc.edu/api/]
 --verify-ssl    verify SSL certs [default: False]
 --debug         verbose logging [default: False]
 -h, --help      show this help message and exit
 --version       show version and exit

'''

import logging

from docopt import docopt

from version import version

log = logging.getLogger(__name__)


def main(argv, projectAccess):
    usage = __doc__.split('..')[0]
    cli = docopt(usage, argv=argv[1:], version=version)
    log.debug('cli args: %s', cli)
    project = projectAccess(cli)
    import pdb; pdb.set_trace()
    raise NotImplementedError


def mkProjectAccess(mkProject, env):
    def projectAccess(cli):
        api_key = env[cli['<api_key>']]
        return mkProject(cli['--url'], api_key,
                         verify_ssl=cli['--verify-ssl'])
    return projectAccess


if __name__ == '__main__':
    def _configure_logging():
        from sys import argv

        level = logging.DEBUG if '--debug' in argv else logging.INFO
        logging.basicConfig(level=level)

    def _privileged_main():
        from sys import argv
        from os import environ
        from redcap import Project
        main(argv,
             projectAccess=mkProjectAccess(Project, environ))

    _configure_logging()
    _privileged_main()
