import asyncio
import json
import argparse
import pwd
import os

from .rpcclient import RpcClient, RpcError


class PyBSclient:
    def __init__(self):
        self._rpc_client = RpcClient()

    def list(self, started=True, finished=False, sort_asc=False, limit=None):
        return self._rpc_client('list', started=started, finished=finished, sort_asc=sort_asc, limit=limit)

    def remove(self, job_id):
        return self._rpc_client('remove', job_id=job_id)

    def submit(self, filename):
        return self._rpc_client('submit', filename=os.path.abspath(filename), user=pwd.getpwuid(os.getuid()).pw_name)
