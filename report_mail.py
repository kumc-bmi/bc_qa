'''report_mail -- send reports to GPC sites by email

Usage:
  report_mail [options] BODY [<site>]...

Options:
  --datasets=FILE    CSV file describing datasets (from bc_fetch.Rmd)
                     [default: dataset.csv]
  --datadir=DIR      directory where datasets are stored [default: data-files]
  --readme=FILE      first attachment to send [default: README.html]
  --terms-report=F   file explaining expected terms [default: bc_qa2.html]
  --sender=ADDR      SMTP sender of messages [default: %(LOGNAME)s@kumc.edu]
  --cc=ADDR...       addresses to copy
  --site-email=FILE  file of site to email address mappings
                     [default: bc_site_email.txt]
  --server=HOST      SMTP server [default: smtp.kumc.edu]
  --debug            debug logging
  --dry-run -n       Just log; don't send any mail.
'''

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE
from functools import partial
from posixpath import basename
import csv
import logging

from docopt import docopt

log = logging.getLogger(__name__)


def main(access):
    cli = access()
    body = cli['BODY']  # TODO: move to stdin

    attachments = [(cli[role], cli.openrd(role).read())
                   for role in ['--readme', '--terms-report']]

    for site in cli.sites():
        subject = 'Breast Cancer QA report for %s' % site.name
        site.send(subject, site.submitter + '\n\n' + body, attachments)


class Site(object):
    def __init__(self, mailer, report, name, submitter):
        self.send = partial(self.send_, mailer, report)
        self.name = name
        self.submitter = submitter

    def send_(self, mailer, report, subject, body, attachments):
        mailer(subject, body, attachments + [report])


def build_message(send_from, send_to, subject, body, attachments):
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    # Date=formatdate(localtime=True),
    msg['Subject'] = subject

    msg.attach(MIMEText(body))

    for (f, txt) in attachments:
        attachment = MIMEText(
            txt,
            '.html' if f.endswith('.html') else 'plain')
        attachment['Content-Disposition'] = (
            'attachment; filename="%s"' % basename(f))
        msg.attach(attachment)
    return msg


class CLI(object):
    def __init__(self, environ, openrd, arguments, SMTP):
        self.openrd = partial(self.openrd_, openrd, arguments)
        self.lookup = partial(self.lookup_, arguments)
        self.report_for = partial(self.report_for_, openrd)
        self.site_mailer = partial(self.site_mailer_, SMTP, environ)

    @classmethod
    def parse(cls, argv):
        return docopt(__doc__, argv[1:])

    def openrd_(self, openrd, arguments, which):
        return openrd(arguments[which])

    def report_for_(self, openrd, site):
        report = '%s/report-%s.html' % (self['--datadir'], site)
        return report, openrd(report).read()

    def sites(self):
        cli_sites = self['<site>']

        to_mbox = dict(
            ((site['site'], site['name']), site['email'])
            for site in csv.DictReader(self.openrd('--site-email')))

        for item in csv.DictReader(self.openrd('--datasets')):
            site = item['site']

            if cli_sites and site not in cli_sites:
                continue

            site_mbox = to_mbox[(item['site'], item['name'])]
            mailer = self.site_mailer(site_mbox)
            yield Site(mailer, self.report_for(site), site, item['name'])

    def site_mailer_(self, SMTP, environ, site_mbox):
        sender = self['--sender'] % environ

        # TODO: move to --dry-run option
        if 'test' in self['BODY']:
            cc = []
        else:
            cc = self['--cc'].split(',')
        server = self['--server']

        def mailer(subject, body, attachments):
            # ack: http://stackoverflow.com/a/3363254
            send_to = [site_mbox] + cc

            msg = build_message(sender, send_to, subject, body, attachments)

            log.info("sending to %s: %s", send_to, subject)
            smtp = SMTP(server)
            smtp.sendmail(sender, send_to, msg.as_string())
            smtp.close()

        return mailer

    def lookup_(self, arguments, n):
        return arguments[n]

    def __getitem__(self, n):
        return self.lookup(n)


class DrySMTP(object):
    def __init__(self, server):
        self.sent = []

    def sendmail(self, sender, to, msg):
        self.sent.append((sender, to, msg))

    def close(self):
        pass


if __name__ == '__main__':
    def _trusted_main():
        from __builtin__ import open as openf
        from sys import argv
        from os import environ
        import smtplib

        def access():
            opt = CLI.parse(argv)
            level = logging.DEBUG if opt['--debug'] else logging.INFO
            logging.basicConfig(level=level)
            return CLI(environ,
                       openrd=lambda n: openf(n),
                       arguments=opt,
                       SMTP=DrySMTP if opt['--dry-run'] else smtplib.SMTP)

        main(access)

    _trusted_main()
