'''report_mail -- send reports to GPC sites by email

Usage:
  report_mail [options] BODY [<site>]...

Options:
  --readme=FILE      first attachment to send [default: README.html]
  --terms-report=F   file explaining expected terms [default: bc_qa2.html]
  --datadir=DIR      directory of data files to send [default: data-files]
  --sender=ADDR      SMTP sender of messages [default: dconnolly@kumc.edu]
  --cc=ADDR...       addresses to copy [default: dconnolly@kumc.edu,jhe@kumc.edu,tmcmahon@kumc.edu,e-chrischilles@uiowa.edu,bradley-mcdowell@uiowa.edu,tshireman@kumc.edu,vleonardo@kumc.edu]  # noqa
  --site-email=FILE  file of site to email address mappings [default: bc_site_email.txt]  # noqa

'''

import csv
import sys

import smtplib
from os.path import basename
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE

from docopt import docopt


def main(argv):
    cli = docopt(__doc__, argv[1:])
    body = cli['BODY']
    cli_sites = argv[2:]
    for item in csv.DictReader(open(cli['--site-email'])):
        site = item['site']

    if cli_sites and site not in cli_sites:
        continue
    if 'test' in body:
        cc = []
    else:
        cc = cli['--cc'].split(',')

    subject = 'Breast Cancer QA report for %s' % site
    report = '%s/report-%s.html' % (cli['--datadir'], site)
    mbox = item['email']
    send_mail(cli['sender'], [mbox] + cc, subject,
              '%s,\n%s' % (item['name'], body),
              files=[cli['--readme'], cli['--terms-report'], report])


def send_mail(send_from, send_to, subject, text, files=None,
              server="smtp.kumc.edu"):
    # ack: http://stackoverflow.com/a/3363254
    assert isinstance(send_to, list)

    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    # Date=formatdate(localtime=True),
    msg['Subject'] = subject

    msg.attach(MIMEText(text))

    for f in files or []:
        with open(f, "rb") as fil:
            attachment = MIMEText(
                fil.read(),
                '.html' if f.endswith('.html') else 'plain')
            attachment['Content-Disposition'] = (
                'attachment; filename="%s"' % basename(f))
            msg.attach(attachment)

    print "sending to", send_to, subject
    smtp = smtplib.SMTP(server)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()


if __name__ == '__main__':
    main(sys.argv)
