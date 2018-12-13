import smtplib
from email.mime.text import MIMEText
import logging

log = logging.getLogger(__name__)

MAIL_BODY = """PBS Job Id: {0}
Job Name:   {1}

Submitted:  {2}
Started:    {3}
Finished:   {4}

Filename:   {5}
Exit code:  {6}

Last 10 lines of standard output (if any):
{7}

Last 10 lines of error output (if any):
{8}"""


class Mailer:
    """Sends emails to a given email address."""

    def __init__(self, sender: str, host: str):
        """Creates a new Mailer.

        Args:
            sender: Value for FROM field in email.
            host: SMTP host to send email through.
        """
        self._sender = sender
        self._host = host

    def send(self, header: dict, job: 'Job', return_code: int, outs: list, errs: list):
        """Send the email.

        Args:
            header: PBS header for job.
            job: The database entry for the job.
            return_code: Return code from the script.
            outs: Output lines from job script.
            errs: Error lines from job script.
        """

        # no sender or host given?
        if self._sender is None or self._host is None:
            return

        # was an email requested for this return code?
        mode = header['send_mail']
        if ('e' not in mode and return_code == 0) or ('a' not in mode and return_code != 0):
            return

        # out and err
        out, err = None, None
        if outs is not None and errs is not None:
            out = '\n'.join(outs.decode('utf-8').split('\n')[-10:])
            err = '\n'.join(errs.decode('utf-8').split('\n')[-10:])

        # compile body
        body = MAIL_BODY.format(job.id, job.name, job.submitted, job.started, job.finished, job.filename,
                                return_code, out, err)

        # create message
        msg = MIMEText(body)
        msg['From'] = self._sender
        msg['To'] = header['email']
        msg['Subject'] = 'PyBS JOB {0} {1} {2}'.format(job.id, job.name, 'finished' if return_code == 0 else 'failed')

        # send email
        try:
            smtp = smtplib.SMTP(self._host)
            smtp.send_message(msg)
            smtp.quit()
        except:
            # Could not send email
            log.exception('Could not send email.')


__all__ = ['Mailer']
