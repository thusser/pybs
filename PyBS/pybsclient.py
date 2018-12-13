import pwd
import os

from .rpcclient import RpcClient


class PyBSclient:
    """Client for accessing the PyBS daemon."""

    def __init__(self):
        """Creates a new client."""
        self._rpc_client = RpcClient()

    def list(self, started: bool = True, finished: bool = False, sort_asc: bool = False, limit: int = None) -> list:
        """Get a list of jobs from the daemon.

        Args:
            started: Return only jobs that have (True) or have not (False) started.
            finished: Return only jobs that have (True) or have not (False) finished.
            sort_asc: Sort ascending (True) or descending (False).
            limit: Limit number of returned jobs.

        Returns:
            List of dictionaries with job infos.
        """
        return self._rpc_client('list', started=started, finished=finished, sort_asc=sort_asc, limit=limit)

    def submit(self, filename: str) -> dict:
        """Submit a new script to the queue.

        Args:
            filename: Name of file to submit.

        Returns:
            Dictionary with new job ID.
        """
        return self._rpc_client('submit', filename=os.path.abspath(filename), user=pwd.getpwuid(os.getuid()).pw_name)

    def remove(self, job_id: int) -> dict:
        """Remove an existing job.

        Args:
            job_id: ID of job to remove.

        Returns:
            Dictionary with success message.
        """
        return self._rpc_client('remove', job_id=job_id)

    def run(self, job_id: int) -> dict:
        """Start a waiting job now.
        Args:
            job_id: ID of job to start.

        Returns:
            Dictionary with success message.
        """
        return self._rpc_client('run', job_id=job_id)


__all__ = ['PyBSclient']
