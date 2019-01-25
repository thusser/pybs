import os
import pwd

from .rpcclient import RpcClient


class PyBSclient:
    """Client for accessing the PyBS daemon."""

    def __init__(self):
        """Creates a new client."""
        self._rpc_client = RpcClient()

    def list_waiting(self):
        """Get a list of waiting jobs.

        Returns:
            List of dictionaries with job infos.
        """
        return self._rpc_client('list_waiting')

    def list_running(self):
        """Get a list of running jobs.

        Returns:
            List of dictionaries with job infos.
        """
        return self._rpc_client('list_running')

    def list(self):
        """Get a list of all unfinished jobs, i.e. returns list_waiting+list_running.

        Returns:
            List of dictionaries with job infos.
        """
        return self.list_waiting() + self.list_running()

    def list_finished(self, limit: int = 5):
        """Get a list of running jobs.

        Args:
            limit: Maximum number of entries to return.

        Returns:
            List of dictionaries with job infos.
        """
        return self._rpc_client('list_finished', limit=limit)

    def submit(self, filename: str) -> dict:
        """Submit a new script to the queue.

        Args:
            filename: Name of file to submit.

        Returns:
            Dictionary with new job ID.
        """

        # before submitting it, make it executable
        os.chmod(filename, 0o777)

        # submit job
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
