import csv
import sys

import smtplib
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate


def main(argv,
         report_mail='bc_site_email.txt',
         readme='README.md',
         terms_article='bc_qa2.html',
         df='data-files',
         sender='dconnolly@kumc.edu',
         cc=['%s@kumc.edu' % who
             for who in ['dconnolly',
                         'tmcmahon',
                         'vleonardo']]):
  body = argv[1]
  cli_sites = argv[2:]
  for item in csv.DictReader(open(report_mail)):
    site = item['site']

    if cli_sites and site not in cli_sites:
      continue
    if 'test' in body:
      cc = []

    subject = 'Breast Cancer QA report for %s' % site
    report = '%s/report-%s.html' % (df, site)
    mbox = item['email']
    send_mail(sender, [mbox] + cc, subject,
              '%s,\n%s' % (item['name'], body),
              files=[readme, terms_article, report])


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
