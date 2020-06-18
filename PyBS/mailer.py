import smtplib
from email.mime.text import MIMEText
import logging

log = logging.getLogger(__name__)


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

    def send(self, to: str, subject: str, body: str):
        """Send the email.

        Args:
            to: Email address to send to
            subject: Email subject
            body: Message body
        """

        # no sender or host given?
        if self._sender is None or self._host is None:
            log.error('Either sender or host not set for email.')
            return

        # create message
        msg = MIMEText(body)
        msg['From'] = self._sender
        msg['To'] = to
        msg['Subject'] = subject

        # send email
        try:
            smtp = smtplib.SMTP(self._host)
            smtp.send_message(msg)
            smtp.quit()
        except:
            # Could not send email
            log.exception('Could not send email.')


class Slack:
    """Sends message to a Slack channel."""

    def __init__(self, token: str):
        """Creates a new Slack.

        Args:
            token: Slack API token.
        """
        self._token = token

    def send(self, to: str, body: str):
        """Send the email.

        Args:
            to: Slack channel
            body: Message body
        """
        import requests

        # no sender or host given?
        if self._token is None:
            log.error('No API token set for Slack.')
            return

        # send message
        requests.post('https://slack.com/api/chat.postMessage',
                      data={'token': self._token, 'channel': to, 'text': body})


__all__ = ['Mailer', 'Slack']
