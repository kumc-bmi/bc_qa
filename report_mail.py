import csv
import sys

import smtplib
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate


def main(report_mail='bc_site_email.txt',
         df='data-files',
         sender='dconnolly@kumc.edu',
         cc=['%s@kumc.edu' % who
             for who in ['dconnolly',
                         'tmcmahon',
                         'vleonardo']]):
  for item in csv.DictReader(open(report_mail)):
    site = item['site']
    if site != 'KUMC':
      continue
    cc = []#@@
    subject = 'Breast Cancer QA report for %s' % site
    report = '%s/report-%s.html' % (df, site)
    #print item, report, len(open(report).read())
    mbox = item['email']
    send_mail(sender, [mbox] + cc, subject,
              'Please acknowledge receipt.',
              files=[report])


def send_mail(send_from, send_to, subject, text, files=None,
              server="127.0.0.1"):
    # ack: http://stackoverflow.com/a/3363254
    assert isinstance(send_to, list)

    msg = MIMEMultipart(
        From=send_from,
        To=COMMASPACE.join(send_to),
        Date=formatdate(localtime=True),
        Subject=subject
    )
    msg.attach(MIMEText(text))

    for f in files or []:
        with open(f, "rb") as fil:
            msg.attach(MIMEText(
                fil.read(), 'html'))

    print "sending to", send_to
    smtp = smtplib.SMTP(server)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()


if __name__ == '__main__':
  main()
